"""Batch de entrada Unidad 3: silver + gold para info churn (IBM) y crm (lotes).

Pensado para Dataproc Serverless (runtime 2.2, Spark 3.5.3, Python 3.12) y para
correr igual en local (Cloud Shell) apuntando a carpetas locales.

No modifica los modulos `churn.silver` / `churn.gold`: solo los orquesta.
"""

from __future__ import annotations

import argparse
import os
import sys

# Ajuste minimo para poder correr `python jobs/u3_batch.py` desde la raiz del
# repo en local (pip install pyspark, sin empaquetar): agrega la carpeta
# PADRE de jobs/ (donde vive churn/) al sys.path. Es inofensivo en Dataproc:
# alli el script se sube solo (sin una carpeta padre con churn/ al lado) y
# `churn` llega por --py-files/churn.zip via PYTHONPATH, que ya esta en
# sys.path por su cuenta; agregar una ruta local que no trae `churn` no
# afecta esa resolucion.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession

from churn.gold import FECHA_FOTO_INFO_CHURN, construir_gold
from churn.silver import COLUMNAS_INFO_CHURN, COLUMNAS_CRM, procesar

# Los 5 lotes que le corresponden a la Unidad 3 (los lotes 6-10 son de U4 y
# NO se leen aqui, aunque existan en bronze).
LOTES_U3_POR_DEFECTO = [
    "2026-08-03",
    "2026-08-10",
    "2026-08-17",
    "2026-08-24",
    "2026-08-31",
]


def _resolver_ruta(ruta: str) -> str:
    """Expande "~" solo para rutas locales; gs:// se deja tal cual."""
    if ruta.startswith("gs://"):
        return ruta
    return os.path.expanduser(ruta)


def _crear_spark() -> SparkSession:
    """Crea la SparkSession con timezone UTC.

    No tocamos el master: el entorno lo decide (Dataproc lo fija via
    spark-submit; en local, spark-submit / `python` con pyspark instalado por
    pip ya arrancan en local[*] por defecto). Chequear `spark.master` con
    SparkConf() ANTES de crear el SparkContext es poco confiable: en PySpark
    ese SparkConf puede leerse sin la JVM y dar False aunque spark-submit ya
    haya fijado el master, forzando local[*] encima de un cluster real.
    """
    return (
        SparkSession.builder.appName("u3-class-silver-gold")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _resumen_conteo(nombre: str, df) -> None:
    n = df.count()
    print(f"RESUMEN | {nombre}: {n} filas")


def _resumen_gold(nombre: str, df) -> None:
    from pyspark.sql import functions as F

    fila = df.agg(
        F.sum("n_llamadas").alias("suma_n_llamadas"),
        F.count(F.when(F.col("n_llamadas") == 0, 1)).alias("n_clientes_sin_llamadas"),
        F.count("*").alias("total_filas"),
    ).first()
    print(
        f"RESUMEN | {nombre}: {fila['total_filas']} filas, "
        f"suma n_llamadas = {fila['suma_n_llamadas']}, "
        f"clientes con n_llamadas = 0: {fila['n_clientes_sin_llamadas']}"
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Batch Unidad 3: silver + gold")
    parser.add_argument("--bronze", required=True, help="Raiz de bronze (local o gs://)")
    parser.add_argument("--lake", required=True, help="Raiz del lake de salida (local o gs://)")
    parser.add_argument("--id-ejecucion", required=True, help="Identificador de esta corrida")
    parser.add_argument(
        "--lotes",
        default=",".join(LOTES_U3_POR_DEFECTO),
        help="Fechas de lotes crm separadas por coma (por defecto los 5 de U3)",
    )
    args = parser.parse_args(argv)

    bronze = _resolver_ruta(args.bronze)
    lake = _resolver_ruta(args.lake)
    id_ejecucion = args.id_ejecucion
    lotes = [f.strip() for f in args.lotes.split(",") if f.strip()]

    # rutas de archivo explicitas: nada de listar el bucket ni usar globs
    ruta_info_churn = f"{bronze}/base/telco_entrenamiento.csv"
    rutas_crm = [f"{bronze}/crm/clientes_{fecha}.csv" for fecha in lotes]
    ruta_uso = f"{bronze}/uso"

    spark = _crear_spark()
    # confirma en el log (driver, y en Dataproc el log del job) que el master
    # es el del entorno real, no local[*] forzado por error
    print(f"RESUMEN | master: {spark.sparkContext.master}")

    # --- paso 1: silver ---
    silver_info_churn, cuarentena_info_churn = procesar(spark, [ruta_info_churn], COLUMNAS_INFO_CHURN, id_ejecucion)
    silver_crm, cuarentena_crm = procesar(spark, rutas_crm, COLUMNAS_CRM, id_ejecucion)

    ruta_silver_info_churn = f"{lake}/silver/clientes_entrenamiento"
    ruta_silver_crm = f"{lake}/silver/clientes_lotes"
    ruta_cuarentena = f"{lake}/cuarentena/u3_silver"

    # overwrite: re-ejecutar el batch reemplaza la salida anterior (idempotente)
    silver_info_churn.coalesce(1).write.mode("overwrite").parquet(ruta_silver_info_churn)
    silver_crm.coalesce(1).write.mode("overwrite").parquet(ruta_silver_crm)
    cuarentena = cuarentena_info_churn.unionByName(cuarentena_crm)
    cuarentena.coalesce(1).write.mode("overwrite").parquet(ruta_cuarentena)

    # --- paso 2: gold, releyendo silver desde parquet ---
    # asi gold usa EXACTAMENTE lo que quedo escrito y no se recalcula
    # _fecha_ingesta (current_timestamp se congela al leer, no al construir).
    silver_info_churn_leido = spark.read.parquet(ruta_silver_info_churn)
    silver_crm_leido = spark.read.parquet(ruta_silver_crm)
    uso = spark.read.parquet(ruta_uso)

    gold_info_churn = construir_gold(silver_info_churn_leido, uso, fecha_foto=FECHA_FOTO_INFO_CHURN)
    gold_crm = construir_gold(silver_crm_leido, uso, fecha_foto=None)

    ruta_gold_info_churn = f"{lake}/gold/perfil_entrenamiento"
    ruta_gold_crm = f"{lake}/gold/perfil_lotes"

    # salida pequena (miles de filas): coalesce(1) evita el problema de
    # muchos archivos chiquitos
    gold_info_churn.coalesce(1).write.mode("overwrite").parquet(ruta_gold_info_churn)
    gold_crm.coalesce(1).write.mode("overwrite").parquet(ruta_gold_crm)

    # --- resumen final: RELEYENDO lo ya escrito en disco ---
    # nunca desde los DataFrames en memoria: usar gold_info_churn/gold_crm aqui
    # volveria a escanear el uso completo (95 M eventos), y usar la
    # cuarentena en memoria la recalcularia desde bronze. Releer del
    # parquet ya escrito es barato (son las salidas chicas del batch).
    _resumen_conteo("silver/clientes_entrenamiento", spark.read.parquet(ruta_silver_info_churn))
    _resumen_conteo("silver/clientes_lotes", spark.read.parquet(ruta_silver_crm))
    _resumen_conteo("cuarentena/u3_silver", spark.read.parquet(ruta_cuarentena))
    _resumen_gold("gold/perfil_entrenamiento", spark.read.parquet(ruta_gold_info_churn))
    _resumen_gold("gold/perfil_lotes", spark.read.parquet(ruta_gold_crm))


if __name__ == "__main__":
    main()
