"""
explorar_uso.py
----------------
Script de EXPLORACION (solo lectura) de datos de uso (parquet particionado
por fecha), un CSV base de clientes (con Churn) y un CRM en lotes de CSV.

Uso:
    python explorar_uso.py <carpeta_uso> <csv_base> <carpeta_crm>

Pensado para estudiantes de maestria: no escribe nada, solo imprime.
"""
import sys
from pyspark.sql import SparkSession, functions as F

if len(sys.argv) != 4:
    print("Uso: python explorar_uso.py <carpeta_uso> <csv_base> <carpeta_crm>")
    sys.exit(1)

carpeta_uso, csv_base, carpeta_crm = sys.argv[1], sys.argv[2], sys.argv[3]

spark = SparkSession.builder.master("local[*]").appName("explorar_uso").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")  # menos ruido en consola

# ---------------------------------------------------------------------
# Lectura de las 3 fuentes
# ---------------------------------------------------------------------
uso = spark.read.parquet(carpeta_uso)
# Spark descubre la columna "fecha" a partir del nombre de la carpeta
# (fecha=2026-08-01/) -> "partition discovery". No viene dentro del parquet.
base = spark.read.csv(csv_base, header=True, inferSchema=False)
crm = spark.read.csv(f"{carpeta_crm}/*.csv", header=True, inferSchema=False)

print("\n=== 1) SCHEMA DE USO (fecha aparece por descubrimiento de particion) ===")
uso.printSchema()

# ---------------------------------------------------------------------
print("\n=== 2) EVENTOS, CLIENTES DISTINTOS Y RANGO DE event_ts POR fecha ===")
uso.groupBy("fecha").agg(
    F.count("*").alias("num_eventos"),
    F.countDistinct("customerID").alias("clientes_distintos"),
    F.min("event_ts").alias("min_event_ts"),
    F.max("event_ts").alias("max_event_ts"),
).orderBy("fecha").show(truncate=False)

# ---------------------------------------------------------------------
print("\n=== 3) EVENTOS POR fecha Y tipo_evento ===")
uso.groupBy("fecha", "tipo_evento").count().orderBy("fecha", "tipo_evento").show(50, truncate=False)

# ---------------------------------------------------------------------
print("\n=== 4) NULOS POR COLUMNA (sobre la muestra de uso) ===")
uso.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in uso.columns
]).show(truncate=False)

# ---------------------------------------------------------------------
print("\n=== 5) RANGOS BASICOS POR tipo_evento ===")
uso.groupBy("tipo_evento").agg(
    F.min("duracion_seg").alias("min_duracion_seg"),
    F.max("duracion_seg").alias("max_duracion_seg"),
    F.min("mb").alias("min_mb"),
    F.max("mb").alias("max_mb"),
).orderBy("tipo_evento").show(truncate=False)

print("--- valores distintos de llamada_caida por tipo_evento ---")
uso.groupBy("tipo_evento", "llamada_caida").count().orderBy("tipo_evento", "llamada_caida").show(truncate=False)

# ---------------------------------------------------------------------
print("\n=== 6) COBERTURA POR FUENTE (info churn y crm en cualquier lote) ===")
clientes_uso = uso.select("customerID", "fecha").distinct()
clientes_a = base.select("customerID", "Churn").distinct()
clientes_b = crm.select("customerID").distinct().withColumn("en_b", F.lit(True))

cobertura = (
    clientes_uso
    .join(clientes_a, on="customerID", how="left")
    .join(clientes_b, on="customerID", how="left")
)

resumen_cobertura = cobertura.groupBy("fecha").agg(
    F.countDistinct(F.when(F.col("Churn").isNotNull(), F.col("customerID"))).alias("en_churn"),
    F.countDistinct(F.when((F.col("Churn") == "Yes"), F.col("customerID"))).alias("churn_yes"),
    F.countDistinct(F.when((F.col("Churn") == "No"), F.col("customerID"))).alias("churn_no"),
    F.countDistinct(F.when(F.col("en_b").isNotNull(), F.col("customerID"))).alias("en_crm"),
    F.countDistinct(F.when(F.col("Churn").isNull() & F.col("en_b").isNull(), F.col("customerID"))).alias("en_ninguna"),
).orderBy("fecha")
resumen_cobertura.show(truncate=False)

# ---------------------------------------------------------------------
print("\n=== 7) CLIENTES DE CADA LOTE DEL CRM CON EVENTOS EN CADA fecha DE USO ===")
clientes_por_lote = crm.select("customerID", "fecha_lote").distinct()

cruce = clientes_por_lote.join(clientes_uso, on="customerID", how="inner")
cruce.groupBy("fecha_lote", "fecha").agg(
    F.countDistinct("customerID").alias("clientes_con_uso")
).orderBy("fecha_lote", "fecha").show(50, truncate=False)

spark.stop()
