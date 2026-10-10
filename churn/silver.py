"""Capa silver del pipeline de churn (bronze -> silver + cuarentena).

Lee cada CSV de bronze por separado, valida encabezados, resuelve
duplicados, tipa las columnas numericas, convierte Churn a 1/0 (solo info churn)
y agrega linaje. Lo ilegible va a cuarentena con su motivo.
Punto de entrada: `procesar`. Probado con pytest (tests/test_*.py).
"""

from __future__ import annotations

import csv
import json
from collections import Counter

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


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
    errores: list[dict] = []

    # normalizamos solo para comparar, sin tocar los nombres originales
    normalizadas = [c.strip().lower() for c in columnas]
    conteos = Counter(normalizadas)

    # repetidos: uno por nombre, en orden de primera aparicion
    ya_reportados: set[str] = set()
    for original, norm in zip(columnas, normalizadas):
        if conteos[norm] > 1 and norm not in ya_reportados:
            ya_reportados.add(norm)
            errores.append({
                "regla": "encabezado_repetido",
                "severidad": "bloquea",
                "detalle": f"la columna '{original.strip()}' esta repetida en el encabezado",
            })

    # faltantes: en orden de `esperadas`
    normalizadas_set = set(normalizadas)
    for esperada in esperadas:
        if esperada.strip().lower() not in normalizadas_set:
            errores.append({
                "regla": "columna_faltante",
                "severidad": "bloquea",
                "detalle": f"falta la columna esperada '{esperada}'",
            })

    return errores


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
    # sin esquema explicito: dejamos que Spark lea todo como texto
    df = spark.read.csv(ruta, header=True, inferSchema=False)

    # mapa normalizado -> nombre real, para emparejar por nombre y no posicion
    normalizado_a_real = {c.strip().lower(): c for c in df.columns}

    columnas_select = [
        df[normalizado_a_real[esperada.strip().lower()]].alias(esperada)
        for esperada in esperadas
    ]
    columnas_select.append(F.col("_metadata.file_path").alias("_archivo_origen"))

    return df.select(*columnas_select)


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
    # columnas del registro original: todas menos las que empiezan por "_"
    cols_registro = [c for c in df.columns if not c.startswith("_")]
    registro_original = F.to_json(
        F.struct(*cols_registro), {"ignoreNullFields": "false"}
    )

    return df.select(
        F.current_timestamp().alias("fecha_error"),
        F.lit("u3_silver").cast("string").alias("unidad_etapa"),
        F.lit(regla).cast("string").alias("regla"),
        F.lit("bloquea").cast("string").alias("severidad"),
        F.col("_archivo_origen").alias("archivo_origen"),
        F.col("customerID").alias("customerID"),
        F.lit(id_ejecucion).cast("string").alias("id_ejecucion"),
        registro_original.alias("registro_original"),
    )


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
    # paso 1: filas identicas en TODO colapsan a una sola (determinista,
    # porque todas las copias son iguales entre si)
    sin_repetidas = df.dropDuplicates()

    # paso 2: contamos filas por (archivo, customerID) con una ventana.
    # nota: si customerID es nulo, los nulos del mismo archivo cuentan
    # juntos (Spark trata nulo como un valor mas al particionar).
    ventana = Window.partitionBy("_archivo_origen", "customerID")
    con_conteo = sin_repetidas.withColumn("_n", F.count("*").over(ventana))

    ok = con_conteo.filter(F.col("_n") == 1).drop("_n").select(*df.columns)
    conflictivas = con_conteo.filter(F.col("_n") > 1).drop("_n").select(*df.columns)

    cuarentena = a_cuarentena(conflictivas, "id_duplicado_conflictivo", id_ejecucion)

    return ok, cuarentena


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
    cols_num = [c for c in COLUMNAS_NUMERICAS if c in df.columns]

    # bandera _ok_<col> por cada columna numerica: cast(...) es null tanto si
    # el valor original ya era nulo como si no se pudo convertir (" ", "abc");
    # el contrato trata ambos casos igual: la fila no pasa a ok.
    con_flags = df
    for col in cols_num:
        tipo = COLUMNAS_NUMERICAS[col]
        con_flags = con_flags.withColumn(f"_ok_{col}", F.col(col).cast(tipo).isNotNull())

    valida = None
    for col in cols_num:
        bandera = F.col(f"_ok_{col}")
        valida = bandera if valida is None else (valida & bandera)

    select_ok = [
        F.col(c).cast(COLUMNAS_NUMERICAS[c]).alias(c) if c in cols_num else F.col(c)
        for c in df.columns
    ]
    ok = con_flags.filter(valida).select(*select_ok)

    # una fila de cuarentena por cada columna que fallo, con el registro
    # ORIGINAL en texto (no el valor tipado)
    cuarentenas = [
        a_cuarentena(
            con_flags.filter(~F.col(f"_ok_{col}")).select(*df.columns),
            f"no_numerico_{col.lower()}",
            id_ejecucion,
        )
        for col in cols_num
    ]
    cuarentena = cuarentenas[0]
    for extra in cuarentenas[1:]:
        cuarentena = cuarentena.unionByName(extra)

    return ok, cuarentena


def churn_a_binario(df: DataFrame) -> DataFrame:
    """Convierte la columna "Churn" de texto a entero: "Yes" -> 1, "No" -> 0.

    Las demas columnas quedan intactas. En la Unidad 3 solo llegan valores
    "Yes"/"No" (ya filtrados por las etapas previas); el comportamiento
    ante otros valores es contrato de la Unidad 4 y no se prueba aqui.
    """
    # withColumn sobre "Churn" conserva su posicion; otros valores (fuera de
    # Yes/No) quedan nulos, comportamiento de U4 no probado aqui
    return df.withColumn(
        "Churn",
        F.when(F.col("Churn") == "Yes", 1).when(F.col("Churn") == "No", 0).cast("int"),
    )


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
    # current_timestamp() se evalua una sola vez al inicio de la consulta:
    # todas las filas de esta llamada reciben el mismo valor
    return df.withColumn("_fecha_ingesta", F.current_timestamp()).withColumn(
        "_id_ejecucion", F.lit(id_ejecucion)
    )


def _leer_encabezados(spark: SparkSession, ruta: str) -> list[str]:
    """Lee la primera linea del CSV con SPARK (funciona con rutas locales
    y gs://) y la parsea con el modulo csv para respetar comillas.

    No se usa spark.read.csv(..., header=True).columns: con encabezados
    repetidos (p. ej. "tenure,TENURE") Spark les agrega sufijos propios
    ("tenure0", "TENURE1") y perderiamos los nombres EXACTOS que exige
    registro_original.
    """
    primera_linea = spark.read.text(ruta).first()[0]
    encabezados = next(csv.reader([primera_linea]))
    # quita el BOM si el archivo lo trae; solo afecta al primer nombre
    if encabezados and encabezados[0].startswith("﻿"):
        encabezados[0] = encabezados[0][1:]
    return encabezados


def _cuarentena_de_archivo(
    spark: SparkSession,
    ruta: str,
    encabezados: list[str],
    errores: list[dict],
    id_ejecucion: str,
) -> DataFrame:
    """Arma las filas de cuarentena de un archivo rechazado por encabezados
    invalidos (una fila por error), con el esquema de `a_cuarentena`."""
    registro = json.dumps({"encabezados": encabezados}, ensure_ascii=False)
    esquema = StructType(
        [
            StructField("unidad_etapa", StringType(), True),
            StructField("regla", StringType(), True),
            StructField("severidad", StringType(), True),
            StructField("archivo_origen", StringType(), True),
            StructField("customerID", StringType(), True),
            StructField("id_ejecucion", StringType(), True),
            StructField("registro_original", StringType(), True),
        ]
    )
    filas = [
        ("u3_silver", error["regla"], error["severidad"], ruta, None, id_ejecucion, registro)
        for error in errores
    ]
    df = spark.createDataFrame(filas, schema=esquema)
    return df.withColumn("fecha_error", F.current_timestamp()).select(
        "fecha_error",
        "unidad_etapa",
        "regla",
        "severidad",
        "archivo_origen",
        "customerID",
        "id_ejecucion",
        "registro_original",
    )


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
    dfs_validos = []
    cuarentenas_archivo = []

    for ruta in rutas:
        encabezados = _leer_encabezados(spark, ruta)
        errores = validar_encabezados(encabezados, esperadas)
        if errores:
            cuarentenas_archivo.append(
                _cuarentena_de_archivo(spark, ruta, encabezados, errores, id_ejecucion)
            )
        else:
            dfs_validos.append(leer_csv(spark, ruta, esperadas))

    columnas_leidas = esperadas + ["_archivo_origen"]
    if dfs_validos:
        crudo = dfs_validos[0]
        for df in dfs_validos[1:]:
            crudo = crudo.unionByName(df)
    else:
        # ningun archivo valido: seguimos el mismo pipeline con un
        # DataFrame vacio para no duplicar logica (silver queda vacio)
        esquema_vacio = StructType(
            [StructField(c, StringType(), True) for c in columnas_leidas]
        )
        crudo = spark.createDataFrame([], schema=esquema_vacio)

    sin_dup, cuarentena_dup = resolver_duplicados(crudo, id_ejecucion)
    tipado, cuarentena_tipo = tipar(sin_dup, id_ejecucion)
    if "Churn" in esperadas:
        tipado = churn_a_binario(tipado)
    silver = agregar_linaje(tipado, id_ejecucion)

    cuarentenas = cuarentenas_archivo + [cuarentena_dup, cuarentena_tipo]
    cuarentena = cuarentenas[0]
    for extra in cuarentenas[1:]:
        cuarentena = cuarentena.unionByName(extra)

    return silver, cuarentena
