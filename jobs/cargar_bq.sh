#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# cargar_bq.sh
#
# Que hace:
#   Carga en BigQuery los resultados parquet que el batch de Dataproc dejo
#   en el bucket lake del grupo (tablas gold + cuarentena de U3), y las
#   mezcla en las tablas finales del dataset del grupo.
#
# Uso:
#   bash jobs/cargar_bq.sh LAKE DATASET
#
#   LAKE     nombre del bucket lake del grupo, SIN el prefijo gs://
#   DATASET  dataset de BigQuery del grupo, ej: class_gold_20261001
#
# Ejemplo:
#   bash jobs/cargar_bq.sh u3-class-lake-20261001 class_gold_20261001
#
# Idempotencia:
#   - Las tablas gold (perfil_entrenamiento, perfil_lotes) se cargan con
#     --replace: cada corrida reemplaza el contenido completo, sin importar
#     cuantas veces se ejecute el script.
#   - La tabla cuarentena NUNCA se carga con --replace, porque es una tabla
#     compartida entre unidades (U3, U4, U6, U8 le agregan filas con su
#     propio unidad_etapa). En su lugar, se hace primero un DELETE de las
#     filas con el mismo unidad_etapa + id_ejecucion (para poder re-correr
#     esta unidad sin duplicar), y luego un INSERT de las filas nuevas,
#     todo dentro de una transaccion.
#
# Permisos:
#   Este script debe correrse con la IDENTIDAD DEL USUARIO (la cuenta con
#   la que se entra a Cloud Shell), no con la service account de Spark:
#   por diseno, esa service account no tiene permisos sobre BigQuery.
#
# Nota sobre las consultas SQL:
#   Las consultas necesitan interpolar ${DATASET} (variable de bash) y a la
#   vez contener literales de texto (strings SQL). Para no mezclar comillas
#   dobles de bash con comillas dobles de SQL, las consultas se escriben en
#   strings de bash con comillas dobles (para que ${DATASET} se expanda),
#   y los literales de texto SQL (incluida la descripcion en OPTIONS) usan
#   comillas simples, que en BigQuery Standard SQL son validas para strings.
# ---------------------------------------------------------------------------

LAKE="${1:?Falta el nombre del bucket lake (sin gs://), p. ej. u3-class-lake-20261001}"
DATASET="${2:?Falta el nombre del dataset de BigQuery, p. ej. class_gold_20261001}"

LOCATION="us-central1"

# Validacion: LAKE no debe traer el prefijo gs://
if [[ "${LAKE}" == gs://* ]]; then
  echo "Error: LAKE no debe incluir el prefijo gs://. Usa solo el nombre del bucket." >&2
  echo "Ejemplo correcto: u3-class-lake-20261001" >&2
  exit 1
fi

# Validacion: DATASET solo puede tener letras, numeros y guion bajo,
# porque su valor se interpola directamente dentro de sentencias SQL.
if ! [[ "${DATASET}" =~ ^[A-Za-z0-9_]+$ ]]; then
  echo "Error: DATASET invalido '${DATASET}'. Solo se permiten letras, numeros y guion bajo." >&2
  exit 1
fi

# Bandera de control para el trap: solo limpiamos la tabla staging si
# alcanzamos a crearla (paso 3) y algo falla despues de eso.
STAGING_CREADA=0

limpiar_staging() {
  if [[ "${STAGING_CREADA}" -eq 1 ]]; then
    echo "Ocurrio un error: limpiando tabla staging ${DATASET}.stg_cuarentena_u3 ..." >&2
    bq rm -f -t "${DATASET}.stg_cuarentena_u3" || true
  fi
}
trap limpiar_staging ERR

echo "[1/7] Cargando ${DATASET}.perfil_entrenamiento desde gold/perfil_entrenamiento ..."
bq load --location="${LOCATION}" --source_format=PARQUET --replace \
  "${DATASET}.perfil_entrenamiento" \
  "gs://${LAKE}/gold/perfil_entrenamiento/part-*"

echo "[2/7] Cargando ${DATASET}.perfil_lotes desde gold/perfil_lotes ..."
bq load --location="${LOCATION}" --source_format=PARQUET --replace \
  "${DATASET}.perfil_lotes" \
  "gs://${LAKE}/gold/perfil_lotes/part-*"

echo "[3/7] Cargando staging ${DATASET}.stg_cuarentena_u3 desde cuarentena/u3_silver ..."
bq load --location="${LOCATION}" --source_format=PARQUET --replace \
  "${DATASET}.stg_cuarentena_u3" \
  "gs://${LAKE}/cuarentena/u3_silver/part-*"
STAGING_CREADA=1

echo "[4/7] Asegurando que exista la tabla ${DATASET}.cuarentena ..."
bq query --location="${LOCATION}" --use_legacy_sql=false \
"CREATE TABLE IF NOT EXISTS ${DATASET}.cuarentena (
  fecha_error       TIMESTAMP NOT NULL,
  unidad_etapa      STRING    NOT NULL,
  regla             STRING    NOT NULL,
  severidad         STRING    NOT NULL,
  archivo_origen    STRING,
  customerID        STRING,
  id_ejecucion      STRING    NOT NULL,
  registro_original JSON
)
OPTIONS (description = 'Cuarentena unica del curso U3-U8: una fila por regla fallida')"

echo "[5/7] Mezclando staging en ${DATASET}.cuarentena (delete + insert, transaccional) ..."
bq query --location="${LOCATION}" --use_legacy_sql=false \
"BEGIN TRANSACTION;
DELETE FROM ${DATASET}.cuarentena
WHERE unidad_etapa = 'u3_silver'
  AND id_ejecucion IN (SELECT DISTINCT id_ejecucion FROM ${DATASET}.stg_cuarentena_u3);
INSERT INTO ${DATASET}.cuarentena
SELECT fecha_error, unidad_etapa, regla, severidad, archivo_origen, customerID, id_ejecucion,
       PARSE_JSON(registro_original)
FROM ${DATASET}.stg_cuarentena_u3;
COMMIT TRANSACTION;"

echo "[6/7] Verificacion: filas de cuarentena por regla ..."
bq query --location="${LOCATION}" --use_legacy_sql=false \
"SELECT regla, COUNT(*) AS filas
FROM ${DATASET}.cuarentena
GROUP BY regla
ORDER BY filas DESC"

echo "[7/7] Eliminando tabla staging ${DATASET}.stg_cuarentena_u3 ..."
bq rm -f -t "${DATASET}.stg_cuarentena_u3"
STAGING_CREADA=0

echo "Conteo final de filas por tabla:"
bq query --location="${LOCATION}" --use_legacy_sql=false \
"SELECT '${DATASET}.perfil_entrenamiento' AS tabla, COUNT(*) AS filas FROM ${DATASET}.perfil_entrenamiento
UNION ALL
SELECT '${DATASET}.perfil_lotes', COUNT(*) FROM ${DATASET}.perfil_lotes
UNION ALL
SELECT '${DATASET}.cuarentena', COUNT(*) FROM ${DATASET}.cuarentena"

echo "Listo."
