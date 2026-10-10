"""Capa silver del pipeline de churn.

Fase TDD "roja": solo se definen las firmas y las descripciones (docstrings).
Todavia no hay implementacion; cada funcion levanta NotImplementedError.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def validar_encabezados(columnas: list[str], esperadas: list[str]) -> list[dict]:
    """Valida los encabezados de un archivo CSV contra las columnas esperadas.

    La normalizacion (strip + lower) se usa SOLO para comparar nombres;
    no modifica los datos ni decide el nombre canonico final.

    Reglas de validacion:
    - "encabezado_repetido": un nombre de columna aparece mas de una vez
      en `columnas` despues de normalizar. Se reporta un error por cada
      nombre repetido (no uno por cada ocurrencia extra).
    - "columna_faltante": una columna de `esperadas` no aparece (normalizada)
      en `columnas`. Se reporta un error por cada columna esperada ausente.

    Columnas de mas (presentes en `columnas` pero no en `esperadas`) NO son
    error en la Unidad 3: simplemente se descartaran mas adelante al leer.

    El orden de las columnas no importa para esta validacion.

    Returns:
        Lista de errores. Cada error es un dict con EXACTAMENTE las claves:
        - "regla": "encabezado_repetido" | "columna_faltante"
        - "severidad": siempre "bloquea"
        - "detalle": texto legible que incluye el nombre de columna involucrado.
        Una lista vacia significa que el archivo esta OK.
    """
    raise NotImplementedError("Paso 4: TDD verde")


def leer_csv(spark: SparkSession, ruta: str, esperadas: list[str]) -> DataFrame:
    """Lee UN archivo CSV de bronze y lo deja listo para silver.

    Precondicion: los encabezados ya fueron validados con validar_encabezados.

    Contrato de lectura:
    - header=True, inferSchema=False (todo texto), SIN esquema explicito.
    - Empareja columnas por NOMBRE normalizado (strip + lower), nunca por
      posicion.
    - Devuelve exactamente las columnas de `esperadas`, con su nombre canonico
      y en ese orden, mas `_archivo_origen` al final (de `_metadata.file_path`).
    - Descarta las columnas que no esten en `esperadas`.
    """
    raise NotImplementedError("Paso 4: TDD verde")


# Columnas que deben tiparse en silver: nombre -> tipo destino.
# Se usa tanto en `tipar` como en el nombre de la regla de cuarentena
# (no_numerico_<nombre en minusculas>).
COLUMNAS_NUMERICAS = {"tenure": "int", "MonthlyCharges": "double", "TotalCharges": "double"}


def a_cuarentena(df: DataFrame, regla: str, id_ejecucion: str) -> DataFrame:
    """Convierte filas rechazadas al esquema comun de cuarentena.

    `df` es una porcion de la salida de `leer_csv`: todas las columnas
    string mas `_archivo_origen`. El resultado tiene EXACTAMENTE estas
    columnas, en este orden:
    - "fecha_error" (timestamp): momento en que se ejecuta esta funcion,
      igual para todas las filas de la llamada.
    - "unidad_etapa" (string): siempre "u3_silver".
    - "regla" (string): el valor recibido en `regla`.
    - "severidad" (string): siempre "bloquea".
    - "archivo_origen" (string): valor de `_archivo_origen` de cada fila.
    - "customerID" (string): tomado de la fila original.
    - "id_ejecucion" (string): el valor recibido en `id_ejecucion`.
    - "registro_original" (string): JSON de la fila original, con TODAS
      las columnas cuyo nombre NO empieza por "_" (ni `_archivo_origen`
      ni ninguna otra columna de linaje). Los valores nulos se escriben
      EXPLICITAMENTE como null: to_json por defecto omite las claves con
      valor nulo, y con eso perderiamos la evidencia de que campo vino
      vacio y motivo el rechazo.
    """
    raise NotImplementedError("Paso 4: TDD verde")


def resolver_duplicados(df: DataFrame, id_ejecucion: str) -> tuple[DataFrame, DataFrame]:
    """Resuelve duplicados en dos pasadas y devuelve (ok, cuarentena).

    Paso 1 - filas identicas: si dos o mas filas son identicas en TODAS
    sus columnas, se quedan como una sola (no es un conflicto: es el
    mismo registro repetido, p. ej. por un reintento de ingesta).

    Paso 2 - mismo customerID, mismo archivo, valores distintos: tras el
    paso 1, si un customerID sigue apareciendo mas de una vez dentro del
    MISMO `_archivo_origen`, es un conflicto real (dos versiones distintas
    del mismo cliente en el mismo lote) y NO se puede resolver eligiendo
    una arbitrariamente: eso seria no determinista. TODAS esas filas van
    a cuarentena con regla "id_duplicado_conflictivo".

    El mismo customerID en archivos `_archivo_origen` DISTINTOS no es un
    duplicado: es una nueva foto en el tiempo del mismo cliente (p. ej.
    lotes mensuales), y ambas filas quedan en ok.

    `ok` conserva exactamente las columnas de `df` (mismo nombre y orden).
    No se usa dropDuplicates(["customerID"]): elegiria una fila al azar
    entre las conflictivas, lo cual no es reproducible entre ejecuciones.

    `cuarentena` tiene el esquema de `a_cuarentena`.
    """
    raise NotImplementedError("Paso 4: TDD verde")


def tipar(df: DataFrame, id_ejecucion: str) -> tuple[DataFrame, DataFrame]:
    """Convierte las columnas de COLUMNAS_NUMERICAS a su tipo destino.

    Para cada columna de COLUMNAS_NUMERICAS: un valor nulo o no convertible
    al tipo destino (vacio, " ", "abc", etc.) hace que la FILA COMPLETA no
    pase a `ok`. Se genera UNA fila de cuarentena POR CADA columna que
    falla en esa fila (una fila con dos columnas malas genera dos filas
    de cuarentena), con regla "no_numerico_<columna en minusculas>"
    (p. ej. "no_numerico_totalcharges").

    `df` es la salida de `resolver_duplicados` (incluye `_archivo_origen`).

    `ok` tiene las mismas columnas y el mismo orden que `df`: las columnas
    de COLUMNAS_NUMERICAS ya tipadas al tipo destino, el resto de columnas
    intacta como string. `cuarentena` tiene el esquema de `a_cuarentena`.
    """
    raise NotImplementedError("Paso 4: TDD verde")


def churn_a_binario(df: DataFrame) -> DataFrame:
    """Convierte la columna "Churn" de texto a entero: "Yes" -> 1, "No" -> 0.

    Las demas columnas quedan intactas. En la Unidad 3 solo llegan valores
    "Yes"/"No" (ya filtrados por las etapas previas); el comportamiento
    ante otros valores es contrato de la Unidad 4 y no se prueba aqui.
    """
    raise NotImplementedError("Paso 4: TDD verde")


# Columnas comunes a los dos archivos de origen (info churn, entrenamiento
# IBM, y lotes del crm). No incluye "Churn" ni "fecha_lote": esas son las
# columnas que distinguen a cada fuente.
COLUMNAS_BASE = [
    "customerID",
    "gender",
    "SeniorCitizen",
    "Partner",
    "Dependents",
    "tenure",
    "PhoneService",
    "MultipleLines",
    "InternetService",
    "OnlineSecurity",
    "OnlineBackup",
    "DeviceProtection",
    "TechSupport",
    "StreamingTV",
    "StreamingMovies",
    "Contract",
    "PaperlessBilling",
    "PaymentMethod",
    "MonthlyCharges",
    "TotalCharges",
]

# Info churn: entrenamiento IBM (21 columnas, incluye "Churn").
COLUMNAS_INFO_CHURN = COLUMNAS_BASE + ["Churn"]

# CRM: lotes semanales (21 columnas, incluye "fecha_lote"). En el archivo
# real "fecha_lote" esta en la posicion 2, no al final; por eso leer_csv
# empareja por NOMBRE y no por posicion.
COLUMNAS_CRM = COLUMNAS_BASE + ["fecha_lote"]


def agregar_linaje(df: DataFrame, id_ejecucion: str) -> DataFrame:
    """Agrega columnas de linaje al final de `df`.

    Se agregan, en este orden, al final de las columnas existentes:
    - "_fecha_ingesta" (timestamp): mismo valor para TODAS las filas de
      esta ejecucion (una sola ejecucion es un solo instante).
    - "_id_ejecucion" (string): el valor recibido en `id_ejecucion`.

    `_archivo_origen` y las demas columnas de `df` se conservan tal cual.
    """
    raise NotImplementedError("Paso 4: TDD verde")


def procesar(
    spark: SparkSession, rutas: list[str], esperadas: list[str], id_ejecucion: str
) -> tuple[DataFrame, DataFrame]:
    """Encadena el pipeline completo de silver sobre uno o varios archivos.

    Para cada ruta de `rutas`:
    1. Lee sus encabezados (primera linea del CSV) y los valida con
       `validar_encabezados` contra `esperadas`.
    2. Si hay errores, el ARCHIVO COMPLETO va a cuarentena: no se lee
       ninguna fila de datos de ese archivo. Se genera una fila de
       cuarentena POR CADA error (mismo esquema que `a_cuarentena`), con:
       - "regla": la regla del error ("columna_faltante" o
         "encabezado_repetido").
       - "severidad": "bloquea".
       - "unidad_etapa": "u3_silver".
       - "archivo_origen": la ruta del archivo.
       - "customerID": nulo (no se leyo ninguna fila).
       - "registro_original": JSON `{"encabezados": [...]}` con los
         nombres de columna EXACTOS tal como venian en el archivo (sin
         normalizar).
    3. Si no hay errores, el archivo se lee con `leer_csv`.

    Los archivos validos se unen en un solo DataFrame y pasan por, en
    este orden: `resolver_duplicados` -> `tipar` -> `churn_a_binario`
    (SOLO si "Churn" esta en `esperadas`) -> `agregar_linaje`.

    Devuelve (silver, cuarentena):
    - `silver`: el resultado final, con las columnas de linaje agregadas.
    - `cuarentena`: la union de la cuarentena de archivo (paso 2), la de
      `resolver_duplicados` y la de `tipar`.
    """
    raise NotImplementedError("Paso 4: TDD verde")
