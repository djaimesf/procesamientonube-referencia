"""buscar_sucios.py - Uso: python buscar_sucios.py <carpeta_con_csv>
Detecta filas "sucias" (tenure/MonthlyCharges/TotalCharges nulos, vacios o no
numericos) y customerID duplicados por archivo. Solo lectura, no escribe nada."""
import sys, os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

carpeta = sys.argv[1]
spark = SparkSession.builder.master("local[*]").appName("buscar_sucios").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Lectura como texto (sin inferSchema) + nombre de archivo de origen (solo basename)
df = (spark.read.option("header", True)
      .csv(os.path.join(carpeta, "clientes_2026-08-*.csv"))
      .withColumn("archivo", F.element_at(F.split(F.col("_metadata.file_path"), "/"), -1)))

cols_num = ["tenure", "MonthlyCharges", "TotalCharges"]
def falla(c):  # nulo, vacio tras trim, o no convertible a double
    o = F.col(c)
    return o.isNull() | (F.trim(o) == "") | (F.trim(o).cast("double").isNull())

for c in cols_num:
    df = df.withColumn(f"falla_{c}", falla(c))
df = df.withColumn("es_sucia", F.col("falla_tenure") | F.col("falla_MonthlyCharges") | F.col("falla_TotalCharges"))
df = df.withColumn("num_fallas", sum(F.col(f"falla_{c}").cast("int") for c in cols_num))

print("\n=== 1) Resumen por archivo ===")
resumen = df.groupBy("archivo").agg(
    F.count("*").alias("total_filas"),
    F.sum(F.col("es_sucia").cast("int")).alias("filas_sucias"),
    *[F.sum(F.col(f"falla_{c}").cast("int")).alias(f"falla_{c}") for c in cols_num],
).orderBy("archivo")
resumen.show(truncate=False)

print("=== 2) Filas con mas de una columna fallando ===")
print(f"Total: {df.filter(F.col('num_fallas') > 1).count()}")

print("=== 3) Duplicados de customerID (filas completas) ===")
cols_orig = [c for c in df.columns if not c.startswith("falla_") and c not in ("es_sucia", "num_fallas")]
claves = df.groupBy("archivo", "customerID").count().filter(F.col("count") > 1).select("archivo", "customerID")
dup = df.join(claves, ["archivo", "customerID"], "inner")
dup.select(*cols_orig).orderBy("archivo", "customerID").show(truncate=False)

comp = dup.groupBy("archivo", "customerID").agg(F.countDistinct(*cols_orig).alias("distintas"))
for r in comp.orderBy("archivo", "customerID").collect():
    print(f"archivo={r['archivo']} customerID={r['customerID']} -> filas {'idénticas' if r['distintas'] == 1 else 'distintas'}")

print("=== 4) Duplicados que ademas son sucios ===")
dup_sucios = dup.filter(F.col("es_sucia")).select("archivo", "customerID").distinct()
if dup_sucios.count() > 0:
    dup_sucios.orderBy("archivo", "customerID").show(truncate=False)
else:
    print("Ninguno.")

spark.stop()
