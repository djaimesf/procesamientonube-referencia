#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# lanzar_batch.sh
#
# Que hace:
#   Envia el job PySpark (u3_batch.py + churn.zip) a Dataproc Serverless
#   (Managed Service for Apache Spark) para que corra en un batch efimero,
#   leyendo desde el bucket bronze comun del curso y escribiendo el
#   resultado (bronze -> silver -> gold, en formato parquet) en el bucket
#   lake del grupo.
#
# Uso:
#   bash jobs/lanzar_batch.sh BATCH_ID LAKE SA GRUPO
#
#   BATCH_ID  id unico del batch en Dataproc, ej: u3-class-sb-20261008-01
#   LAKE      nombre del bucket lake del grupo, SIN el prefijo gs://
#             ej: u3-class-lake-20261001
#   SA        correo completo de la service account del grupo
#             ej: churn-profe@nube-20262-main.iam.gserviceaccount.com
#   GRUPO     valor de la etiqueta (label) grupo=, ej: class o g03
#
# Ejemplo (valores del profesor, solo de referencia):
#   bash jobs/lanzar_batch.sh u3-class-sb-20261008-02 u3-class-lake-20261001 \
#     churn-profe@nube-20262-main.iam.gserviceaccount.com class
#
# IMPORTANTE:
#   - BATCH_ID debe ser UNICO en el proyecto. Si vuelves a correr el batch,
#     cambia el sufijo (de -01 a -02, etc). Este valor queda grabado en los
#     datos de salida como "id_ejecucion", y es la clave de trazabilidad
#     (lineage) para poder rastrear de que corrida salio cada fila.
#   - El codigo (u3_batch.py y churn.zip) debe estar YA subido de antemano
#     a gs://LAKE/codigo/. Este script no lo sube, solo lo referencia.
#   - El bucket bronze es fijo y comun para todos los grupos del curso.
# ---------------------------------------------------------------------------

BATCH_ID="${1:?Falta el ID del batch, p. ej. u3-class-sb-20261008-01}"
LAKE="${2:?Falta el nombre del bucket lake (sin gs://), p. ej. u3-class-lake-20261001}"
SA="${3:?Falta el correo de la service account, p. ej. churn-profe@nube-20262-main.iam.gserviceaccount.com}"
GRUPO="${4:?Falta el valor del label grupo, p. ej. class o g03}"

# Bucket bronze: comun a todos los grupos, no se parametriza
BRONZE="data-telco-bronze-20260926"

# Validacion: LAKE no debe traer el prefijo gs://, el script lo agrega solo
if [[ "${LAKE}" == gs://* ]]; then
  echo "Error: LAKE no debe incluir el prefijo gs://. Usa solo el nombre del bucket." >&2
  echo "Ejemplo correcto: u3-class-lake-20261001" >&2
  exit 1
fi

gcloud dataproc batches submit pyspark "gs://${LAKE}/codigo/u3_batch.py" \
  --batch="${BATCH_ID}" \
  --region=us-central1 \
  --version=2.2 \
  --service-account="${SA}" \
  --staging-bucket="${LAKE}" \
  --py-files="gs://${LAKE}/codigo/churn.zip" \
  --ttl=15m \
  --labels=grupo="${GRUPO}" \
  --properties=spark.dynamicAllocation.maxExecutors=2 \
  -- \
  --bronze="gs://${BRONZE}" \
  --lake="gs://${LAKE}" \
  --id-ejecucion="${BATCH_ID}"

echo "Para revisar el costo/consumo del batch, ejecuta:"
echo "  gcloud dataproc batches describe ${BATCH_ID} --region=us-central1 --format=\"yaml(state,createTime,stateTime,runtimeInfo.approximateUsage)\""
