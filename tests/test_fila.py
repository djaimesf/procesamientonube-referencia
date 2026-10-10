"""Tests de NIVEL FILA (fase TDD roja) para churn/silver.py.

Describen el CONTRATO de las funciones que operan fila a fila antes de
implementarlas. Con los stubs actuales (que lanzan NotImplementedError)
todos estos tests deben FALLAR con NotImplementedError.
"""

import json

from pyspark.sql.types import StructType, StructField, StringType
from pyspark.testing import assertDataFrameEqual

from churn.silver import (
    a_cuarentena,
    resolver_duplicados,
    tipar,
    churn_a_binario,
    agregar_linaje,
)


# ---------------------------------------------------------------------------
# Helpers locales: construir DataFrames de prueba sin repetir codigo.
# ---------------------------------------------------------------------------


def esquema_string(columnas):
    """Esquema con todas las columnas como string nullable (como leer_csv)."""
    return StructType([StructField(c, StringType(), True) for c in columnas])


def hacer_df(spark, columnas, filas):
    """Crea un DataFrame de columnas string a partir de tuplas de Python."""
    return spark.createDataFrame(filas, schema=esquema_string(columnas))


ARCHIVO_CRM_1 = "file:/datos/clientes_2026-08-17.csv"
ARCHIVO_CRM_2 = "file:/datos/clientes_2026-09-01.csv"


# ---------------------------------------------------------------------------
# a_cuarentena
# ---------------------------------------------------------------------------


def test_a_cuarentena_esquema_exacto(spark):
    """Nombres, orden y tipos de las columnas de salida."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [("0001-AAAAA", "5", ARCHIVO_CRM_1)]
    df = hacer_df(spark, columnas, filas)

    resultado = a_cuarentena(df, "no_numerico_tenure", "run-001")

    assert resultado.dtypes == [
        ("fecha_error", "timestamp"),
        ("unidad_etapa", "string"),
        ("regla", "string"),
        ("severidad", "string"),
        ("archivo_origen", "string"),
        ("customerID", "string"),
        ("id_ejecucion", "string"),
        ("registro_original", "string"),
    ]


def test_a_cuarentena_valores_fijos(spark):
    """unidad_etapa, severidad, regla, id_ejecucion, archivo_origen y
    customerID quedan con los valores esperados."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [("0001-AAAAA", "5", ARCHIVO_CRM_1)]
    df = hacer_df(spark, columnas, filas)

    fila = a_cuarentena(df, "no_numerico_tenure", "run-001").collect()[0]

    assert fila["unidad_etapa"] == "u3_silver"
    assert fila["severidad"] == "bloquea"
    assert fila["regla"] == "no_numerico_tenure"
    assert fila["id_ejecucion"] == "run-001"
    assert fila["archivo_origen"] == ARCHIVO_CRM_1
    assert fila["customerID"] == "0001-AAAAA"
    assert fila["fecha_error"] is not None


def test_a_cuarentena_registro_original_contiene_columnas_sin_guion_bajo(spark):
    """registro_original es JSON valido con todas las columnas sin "_" y
    ninguna que empiece por "_"; valores iguales al original.

    Se agrega una segunda columna de linaje (_id_lote) para comprobar que la
    regla es "todo lo que empieza por _", no solo _archivo_origen."""
    columnas = ["customerID", "tenure", "TotalCharges", "_archivo_origen", "_id_lote"]
    filas = [("0001-AAAAA", "5", "351.75", ARCHIVO_CRM_1, "lote-7")]
    df = hacer_df(spark, columnas, filas)

    fila = a_cuarentena(df, "no_numerico_tenure", "run-001").collect()[0]
    registro = json.loads(fila["registro_original"])

    assert set(registro) == {"customerID", "tenure", "TotalCharges"}
    assert registro["customerID"] == "0001-AAAAA"
    assert registro["tenure"] == "5"
    assert registro["TotalCharges"] == "351.75"
    assert not any(clave.startswith("_") for clave in registro)


def test_a_cuarentena_conserva_nulos_en_registro_original(spark):
    """Un nulo en la fila original debe aparecer en el JSON como null, no
    omitirse. to_json omite por defecto las claves con valor nulo; con eso
    se perderia la evidencia de por que se rechazo la fila."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [("0001-AAAAA", None, ARCHIVO_CRM_1)]
    df = hacer_df(spark, columnas, filas)

    fila = a_cuarentena(df, "no_numerico_tenure", "run-001").collect()[0]
    registro = json.loads(fila["registro_original"])

    assert "tenure" in registro
    assert registro["tenure"] is None


# ---------------------------------------------------------------------------
# resolver_duplicados
# ---------------------------------------------------------------------------


def test_resolver_duplicados_filas_identicas_colapsan(spark):
    """Dos filas identicas en todo -> 1 en ok, cuarentena vacia."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [
        ("0001-AAAAA", "5", ARCHIVO_CRM_1),
        ("0001-AAAAA", "5", ARCHIVO_CRM_1),
    ]
    df = hacer_df(spark, columnas, filas)

    ok, cuarentena = resolver_duplicados(df, "run-001")

    assertDataFrameEqual(ok, hacer_df(spark, columnas, [filas[0]]))
    assert cuarentena.count() == 0


def test_resolver_duplicados_mismo_id_mismo_archivo_valores_distintos(spark):
    """Mismo customerID, mismo archivo, valores distintos -> conflicto:
    0 filas de ese ID en ok, las 2 a cuarentena con la regla correcta."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [
        ("0001-AAAAA", "5", ARCHIVO_CRM_1),
        ("0001-AAAAA", "6", ARCHIVO_CRM_1),
    ]
    df = hacer_df(spark, columnas, filas)

    ok, cuarentena = resolver_duplicados(df, "run-001")

    assert ok.count() == 0
    filas_c = cuarentena.collect()
    assert len(filas_c) == 2
    assert {f["regla"] for f in filas_c} == {"id_duplicado_conflictivo"}
    assert {f["customerID"] for f in filas_c} == {"0001-AAAAA"}


def test_resolver_duplicados_colapsa_identicas_antes_de_evaluar_conflicto(spark):
    """2 filas identicas + 1 distinta del mismo customerID: las identicas se
    colapsan primero, y las 2 filas distintas resultantes son el conflicto."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [
        ("0001-AAAAA", "5", ARCHIVO_CRM_1),
        ("0001-AAAAA", "5", ARCHIVO_CRM_1),
        ("0001-AAAAA", "6", ARCHIVO_CRM_1),
    ]
    df = hacer_df(spark, columnas, filas)

    ok, cuarentena = resolver_duplicados(df, "run-001")

    assert ok.count() == 0
    filas_c = cuarentena.collect()
    assert len(filas_c) == 2
    assert {f["regla"] for f in filas_c} == {"id_duplicado_conflictivo"}


def test_resolver_duplicados_mismo_id_archivos_distintos_no_es_conflicto(spark):
    """El mismo customerID en archivos distintos es una nueva foto en el
    tiempo, no un duplicado: ambas filas quedan en ok."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [
        ("0001-AAAAA", "5", ARCHIVO_CRM_1),
        ("0001-AAAAA", "6", ARCHIVO_CRM_2),
    ]
    df = hacer_df(spark, columnas, filas)

    ok, cuarentena = resolver_duplicados(df, "run-001")

    assert ok.count() == 2
    assert cuarentena.count() == 0


def test_resolver_duplicados_sin_duplicados_ok_igual_a_entrada(spark):
    """Sin duplicados, ok es igual a la entrada y cuarentena queda vacia."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [
        ("0001-AAAAA", "5", ARCHIVO_CRM_1),
        ("0002-BBBBB", "6", ARCHIVO_CRM_1),
    ]
    df = hacer_df(spark, columnas, filas)

    ok, cuarentena = resolver_duplicados(df, "run-001")

    # assertDataFrameEqual compara el esquema (incluido el ORDEN de columnas)
    # e ignora el orden de filas, que Spark no garantiza.
    assertDataFrameEqual(ok, df)
    assert cuarentena.count() == 0


def test_resolver_duplicados_es_idempotente(spark):
    """Dos ejecuciones sobre la misma entrada dan el mismo ok: por eso no se
    usa dropDuplicates(["customerID"]), que elegiria una fila al azar."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [
        ("0001-AAAAA", "5", ARCHIVO_CRM_1),
        ("0001-AAAAA", "6", ARCHIVO_CRM_1),
        ("0002-BBBBB", "7", ARCHIVO_CRM_1),
        ("0002-BBBBB", "7", ARCHIVO_CRM_1),
        ("0003-CCCCC", "8", ARCHIVO_CRM_1),
    ]
    df = hacer_df(spark, columnas, filas)

    ok_1, _ = resolver_duplicados(df, "run-001")
    ok_2, _ = resolver_duplicados(df, "run-002")

    assertDataFrameEqual(ok_1, ok_2)


def test_resolver_duplicados_no_afecta_filas_de_otros_clientes(spark):
    """Un cliente sano junto a uno conflictivo: el sano queda en ok, el
    conflictivo va completo a cuarentena."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [
        ("0001-AAAAA", "5", ARCHIVO_CRM_1),
        ("0002-BBBBB", "6", ARCHIVO_CRM_1),
        ("0002-BBBBB", "7", ARCHIVO_CRM_1),
    ]
    df = hacer_df(spark, columnas, filas)

    ok, cuarentena = resolver_duplicados(df, "run-001")

    ids_ok = [f["customerID"] for f in ok.collect()]
    assert ids_ok == ["0001-AAAAA"]
    ids_cuarentena = [f["customerID"] for f in cuarentena.collect()]
    assert ids_cuarentena == ["0002-BBBBB", "0002-BBBBB"]


# ---------------------------------------------------------------------------
# tipar
# ---------------------------------------------------------------------------


# Incluye _archivo_origen: la entrada real de tipar es la salida de
# resolver_duplicados, y a_cuarentena lo necesita para archivo_origen.
COLUMNAS_TIPAR = ["customerID", "tenure", "MonthlyCharges", "TotalCharges", "_archivo_origen"]


def test_tipar_valores_validos(spark):
    """Valores validos se convierten al tipo destino; customerID sigue
    string; el orden de columnas de ok es igual al de la entrada."""
    filas = [("0001-AAAAA", "5", "70.35", "351.75", ARCHIVO_CRM_1)]
    df = hacer_df(spark, COLUMNAS_TIPAR, filas)

    ok, cuarentena = tipar(df, "run-001")

    assert ok.columns == COLUMNAS_TIPAR
    assert dict(ok.dtypes) == {
        "customerID": "string",
        "tenure": "int",
        "MonthlyCharges": "double",
        "TotalCharges": "double",
        "_archivo_origen": "string",
    }
    fila = ok.collect()[0]
    assert fila["customerID"] == "0001-AAAAA"
    assert fila["tenure"] == 5
    # Comparacion exacta valida: "70.35" y el literal 70.35 producen el mismo
    # double IEEE-754. Si hubiera aritmetica, se usaria pytest.approx.
    assert fila["MonthlyCharges"] == 70.35
    assert fila["TotalCharges"] == 351.75
    assert cuarentena.count() == 0


def test_tipar_tenure_vacio_va_a_cuarentena(spark):
    """tenure nulo (celda vacia en el CSV original) -> no pasa a ok; 1 fila
    de cuarentena con regla no_numerico_tenure."""
    filas = [("0001-AAAAA", None, "70.35", "351.75", ARCHIVO_CRM_1)]
    df = hacer_df(spark, COLUMNAS_TIPAR, filas)

    ok, cuarentena = tipar(df, "run-001")

    assert ok.count() == 0
    assert cuarentena.count() == 1
    assert cuarentena.collect()[0]["regla"] == "no_numerico_tenure"


def test_tipar_totalcharges_con_espacio_va_a_cuarentena(spark):
    """Caso real IBM: TotalCharges con un solo espacio " " (11 filas en el
    dataset original) no es convertible -> regla no_numerico_totalcharges."""
    filas = [("0001-AAAAA", "5", "70.35", " ", ARCHIVO_CRM_1)]
    df = hacer_df(spark, COLUMNAS_TIPAR, filas)

    ok, cuarentena = tipar(df, "run-001")

    assert ok.count() == 0
    assert cuarentena.count() == 1
    assert cuarentena.collect()[0]["regla"] == "no_numerico_totalcharges"


def test_tipar_monthlycharges_no_numerico_va_a_cuarentena(spark):
    """MonthlyCharges "abc" no es convertible -> regla
    no_numerico_monthlycharges."""
    filas = [("0001-AAAAA", "5", "abc", "351.75", ARCHIVO_CRM_1)]
    df = hacer_df(spark, COLUMNAS_TIPAR, filas)

    ok, cuarentena = tipar(df, "run-001")

    assert ok.count() == 0
    assert cuarentena.count() == 1
    assert cuarentena.collect()[0]["regla"] == "no_numerico_monthlycharges"


def test_tipar_dos_columnas_malas_generan_dos_filas_de_cuarentena(spark):
    """Una fila con tenure y MonthlyCharges invalidos genera 2 filas de
    cuarentena (una por columna) y 0 filas en ok."""
    filas = [("0001-AAAAA", None, "abc", "351.75", ARCHIVO_CRM_1)]
    df = hacer_df(spark, COLUMNAS_TIPAR, filas)

    ok, cuarentena = tipar(df, "run-001")

    assert ok.count() == 0
    reglas = sorted(f["regla"] for f in cuarentena.select("regla").collect())
    assert reglas == ["no_numerico_monthlycharges", "no_numerico_tenure"]


def test_tipar_conserva_todas_las_filas_entre_ok_y_cuarentena(spark):
    """Ninguna fila se pierde ni se duplica en ok: ok.count() mas el numero
    de customerID distintos en cuarentena debe ser igual a las filas de
    entrada. Se cuentan IDs distintos porque una fila con dos columnas
    malas genera dos filas de cuarentena."""
    filas = [
        ("0001-AAAAA", "5", "70.35", "351.75", ARCHIVO_CRM_1),  # buena
        ("0002-BBBBB", None, "80.00", "400.00", ARCHIVO_CRM_1),  # tenure malo
        ("0003-CCCCC", "10", "90.00", " ", ARCHIVO_CRM_1),  # totalcharges malo
        ("0004-DDDDD", None, "abc", "500.00", ARCHIVO_CRM_1),  # dos columnas malas
    ]
    df = hacer_df(spark, COLUMNAS_TIPAR, filas)

    ok, cuarentena = tipar(df, "run-001")

    ids_cuarentena = cuarentena.select("customerID").distinct().count()
    assert ok.count() + ids_cuarentena == len(filas)


# ---------------------------------------------------------------------------
# churn_a_binario
# ---------------------------------------------------------------------------


def test_churn_a_binario_convierte_yes_no(spark):
    """"Yes" -> 1, "No" -> 0, tipo int; las demas columnas quedan intactas."""
    columnas = ["customerID", "Churn", "tenure"]
    filas = [
        ("0001-AAAAA", "Yes", "5"),
        ("0002-BBBBB", "No", "6"),
    ]
    df = hacer_df(spark, columnas, filas)

    resultado = churn_a_binario(df)

    assert resultado.dtypes == [
        ("customerID", "string"),
        ("Churn", "int"),
        ("tenure", "string"),
    ]
    filas_resultado = {f["customerID"]: f["Churn"] for f in resultado.collect()}
    assert filas_resultado == {"0001-AAAAA": 1, "0002-BBBBB": 0}
    tenures = {f["customerID"]: f["tenure"] for f in resultado.collect()}
    assert tenures == {"0001-AAAAA": "5", "0002-BBBBB": "6"}


# ---------------------------------------------------------------------------
# agregar_linaje
# ---------------------------------------------------------------------------


def test_agregar_linaje_columnas_finales(spark):
    """Se agregan _fecha_ingesta y _id_ejecucion al final, con los tipos y
    valores correctos; _archivo_origen se conserva."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [("0001-AAAAA", "5", ARCHIVO_CRM_1)]
    df = hacer_df(spark, columnas, filas)

    resultado = agregar_linaje(df, "run-001")

    assert resultado.columns == columnas + ["_fecha_ingesta", "_id_ejecucion"]
    tipos = dict(resultado.dtypes)
    assert tipos["_fecha_ingesta"] == "timestamp"
    assert tipos["_id_ejecucion"] == "string"
    fila = resultado.collect()[0]
    assert fila["_id_ejecucion"] == "run-001"
    assert fila["_archivo_origen"] == ARCHIVO_CRM_1


def test_agregar_linaje_misma_fecha_para_toda_la_ejecucion(spark):
    """Una sola ejecucion es un solo instante: _fecha_ingesta debe ser igual
    para todas las filas."""
    columnas = ["customerID", "tenure", "_archivo_origen"]
    filas = [
        ("0001-AAAAA", "5", ARCHIVO_CRM_1),
        ("0002-BBBBB", "6", ARCHIVO_CRM_1),
    ]
    df = hacer_df(spark, columnas, filas)

    resultado = agregar_linaje(df, "run-001")

    fechas_distintas = resultado.select("_fecha_ingesta").distinct().count()
    assert fechas_distintas == 1
