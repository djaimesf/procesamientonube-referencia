"""
Script de PERFILADO (data profiling) de datos crudos (capa Bronze).
Solo diagnostica calidad de datos: no transforma ni escribe nada.
Uso: python revisar_calidad.py 
"""
import sys
import glob
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

COLS_NUMERICAS = ["tenure", "MonthlyCharges", "TotalCharges"]
COLS_CATEGORICAS = ["Contract", "PaymentMethod", "InternetService", "Churn"]


def obtener_columna_archivo(df):
    """Usa la columna oculta _metadata.file_path (columna que guarda cosas como ruta, nombre, tamaño, fecha). 
    Si algun formato/version no la expone, usa input_file_name()
    como respaldo."""
    try:
        df2 = df.withColumn("archivo_origen", F.col("_metadata.file_path"))
        df2.select("archivo_origen").limit(1).collect()  # fuerza validacion
        return df2
    except Exception:
        return df.withColumn("archivo_origen", F.input_file_name())


def main():
    if len(sys.argv) != 2:
        print("Uso: python revisar_calidad.py <ruta_archivo_o_carpeta>")
        sys.exit(1)
    ruta = sys.argv[1]

    spark = SparkSession.builder.appName("revisar_calidad").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1) HEADERS POR ARCHIVO (leidas con Python plano, sin Spark).
    # Cuando Spark lee varios CSV a la vez con header=True, SOLO toma el
    # encabezado del PRIMER archivo y asume que el resto tiene la misma
    # estructura. Por eso, para detectar un header distinto (columna
    # faltante u orden distinto) hay que leer la 1a linea de cada archivo.
    
    archivos = sorted(glob.glob(os.path.join(ruta, "*.csv"))) if os.path.isdir(ruta) else [ruta]
    print("\n=== 1) HEADERS POR ARCHIVO ===")
    for archivo in archivos:
        with open(archivo, "r", encoding="utf-8") as f:
            header = f.readline().strip().split(",")
        print(f"[{os.path.basename(archivo)}] ({len(header)} columnas): {header}")

    # Lectura con Spark: TODO como STRING (header=True, sin inferSchema)
    # para que ningun valor sucio se convierta o descarte silenciosamente.
    df = spark.read.csv(ruta, header=True, inferSchema=False)
    df = obtener_columna_archivo(df)
    df = df.withColumn("archivo_origen", F.element_at(F.split(F.col("archivo_origen"), "/"), -1))

    print(f"\nTotal de filas leidas: {df.count()}")
    print("\n=== 2) FILAS POR ARCHIVO ===")
    df.groupBy("archivo_origen").count().orderBy("archivo_origen").show(truncate=False)

    # 3) NULOS Y VACIOS POR COLUMNA (solo se listan columnas con problemas)
    print("=== 3) NULOS Y VACIOS POR COLUMNA ===")
    for c in [c for c in df.columns if c != "archivo_origen"]:
        nulos = df.filter(F.col(c).isNull()).count()
        vacios = df.filter(F.trim(F.col(c)) == "").count()
        if nulos > 0 or vacios > 0:
            print(f"  - {c}: nulos={nulos}, vacios('')={vacios}")

    # 4) VALORES NO NUMERICOS EN COLUMNAS NUMERICAS.
    # Nota: con ANSI desactivado (por defecto en Spark), cast(double) sobre
    # un texto invalido (p.ej. " ") NO lanza error: produce NULL en silencio.
    # Por eso "no convertible" se calcula asi: el valor original NO es nulo,
    # pero su cast a double SI es nulo (se perdio informacion al convertir).

    print("\n=== 4) VALORES NO NUMERICOS EN COLUMNAS NUMERICAS ===")
    for c in COLS_NUMERICAS:
        if c not in df.columns:
            continue
        no_numerico = df.filter(F.col(c).isNotNull() & F.col(c).cast(DoubleType()).isNull())
        cantidad = no_numerico.count()
        print(f"  - {c}: {cantidad} valores no convertibles a numero")
        if cantidad > 0:
            ejemplos = [f"[{fila[c]}]" for fila in no_numerico.select(c).limit(5).collect()]
            print(f"      ejemplos: {ejemplos}")

    # 5) DUPLICADOS DE customerID
    print("\n=== 5) DUPLICADOS DE customerID ===")
    if "customerID" in df.columns:
        # Duplicados DENTRO del mismo archivo: si es un defecto de calidad.
        dup_en_archivo = df.groupBy("archivo_origen", "customerID").count().filter(F.col("count") > 1)
        n_dup = dup_en_archivo.count()
        print(f"  - customerID repetidos dentro del mismo archivo: {n_dup}")
        if n_dup > 0:
            dup_en_archivo.orderBy(F.desc("count")).show(5, truncate=False)

        # Mismo customerID en archivos distintos: NO es un error, son lotes
        # distintos del mismo cliente a lo largo del tiempo.
        en_varios = (
            df.select("customerID", "archivo_origen").distinct()
            .groupBy("customerID").agg(F.countDistinct("archivo_origen").alias("n"))
            .filter(F.col("n") > 1)
        )
        if len(archivos) > 1:
            print(f"  - customerID en mas de un archivo (normal, no es duplicado): {en_varios.count()}")
        else:
            print("  - customerID en mas de un archivo: no aplica (se leyo 1 solo archivo)")
    else:
        print("  (la columna customerID no existe en esta ruta)")

    # 6) VALORES DISTINTOS EN COLUMNAS CATEGORICAS
    print("\n=== 6) VALORES DISTINTOS EN COLUMNAS CATEGORICAS ===")
    for c in COLS_CATEGORICAS:
        if c in df.columns:
            print(f"\n  -- {c} --")
            df.groupBy(c).count().orderBy(F.desc("count")).show(20, truncate=False)

    # 7) RANGOS DE COLUMNAS NUMERICAS (solo valores convertibles; los no
    # convertibles ya se contaron en la seccion 4). Percentiles aproximados.
    print("\n=== 7) RANGOS DE COLUMNAS NUMERICAS ===")
    cols_num = [c for c in COLS_NUMERICAS if c in df.columns]
    if cols_num:
        df.select([F.col(c).cast(DoubleType()).alias(c) for c in cols_num]).summary().show(truncate=False)
    spark.stop()



if __name__ == "__main__":
    main()
