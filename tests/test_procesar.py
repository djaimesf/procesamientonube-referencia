
"""Tests de `procesar` (fase TDD roja) para churn/silver.py.

Definen lo que debe hacer el pipeline completo de silver ANTES de
escribirlo. Mientras procesar este vacia (lanza NotImplementedError),
todos estos tests deben FALLAR: es el rojo esperado.

Parte 1: datos INVENTADOS (CSV pequenos escritos en una carpeta temporal).
Parte 2: muestra LOCAL de los datos reales del curso (info churn y crm).
Si la muestra no existe en este entorno, la Parte 2 se salta (pytest.skip).
"""


import json
import os
from collections import Counter

import pytest

from churn.silver import procesar, COLUMNAS_INFO_CHURN, COLUMNAS_CRM


def escribir_csv(ruta, contenido: str):
    """Helper local: escribe un CSV pequeno para las pruebas."""
    ruta.write_text(contenido, encoding="utf-8")
    return str(ruta)


# ---------------------------------------------------------------------------
# Parte 1: datos inventados
# ---------------------------------------------------------------------------


def test_archivo_con_columna_faltante_va_completo_a_cuarentena(spark, tmp_path):
    """Un archivo con una columna esperada faltante va COMPLETO a
    cuarentena: no se lee ninguna de sus filas, ni siquiera las que
    estarian bien formadas."""
    esperadas = ["customerID", "tenure", "MonthlyCharges", "TotalCharges"]

    bueno = escribir_csv(
        tmp_path / "bueno.csv",
        "customerID,tenure,MonthlyCharges,TotalCharges\n"
        "0001-AAAAA,5,70.35,351.75\n"
        "0002-BBBBB,10,80.00,800.00\n",
    )
    malo = escribir_csv(
        tmp_path / "malo_sin_monthlycharges.csv",
        "CustomerID,tenure,TotalCharges\n"
        "0003-CCCCC,3,120.00\n"
        "0004-DDDDD,4,140.00\n",
    )

    silver, cuarentena = procesar(spark, [bueno, malo], esperadas, "run-001")

    assert silver.count() == 2
    ids_silver = {f["customerID"] for f in silver.select("customerID").collect()}
    assert ids_silver == {"0001-AAAAA", "0002-BBBBB"}

    assert cuarentena.count() == 1
    fila = cuarentena.collect()[0]
    assert fila["regla"] == "columna_faltante"
    assert fila["severidad"] == "bloquea"
    assert fila["unidad_etapa"] == "u3_silver"
    assert fila["id_ejecucion"] == "run-001"
    assert fila["customerID"] is None
    assert fila["archivo_origen"] == malo

    # Nombres EXACTOS tal como venian (sin normalizar: "CustomerID").
    registro = json.loads(fila["registro_original"])
    assert registro == {"encabezados": ["CustomerID", "tenure", "TotalCharges"]}


def test_columna_faltante_no_corre_valores_de_otros_archivos(spark, tmp_path):
    """Hallazgo real del Paso 2: si varios CSV se leen JUNTOS con un solo
    encabezado (el del primer archivo), y a otro archivo le falta una
    columna del MEDIO, Spark corre los valores en silencio (lo que venia
    despues de la columna faltante se desplaza a la columna siguiente).

    `procesar` valida y lee CADA archivo por separado (por eso las filas
    del archivo bueno deben llegar a silver con sus valores EXACTOS, y
    ninguna fila del archivo malo debe llegar a silver: el archivo malo
    va completo a cuarentena por `columna_faltante`)."""
    esperadas = ["customerID", "tenure", "MonthlyCharges", "TotalCharges"]

    bueno = escribir_csv(
        tmp_path / "bueno.csv",
        "customerID,tenure,MonthlyCharges,TotalCharges\n"
        "0001-AAAAA,5,70.35,351.75\n",
    )
    # Falta MonthlyCharges (columna del medio): sin validar por archivo,
    # el valor de TotalCharges "999.99" se leeria en la posicion de
    # MonthlyCharges al unir ambos archivos en una sola lectura.
    malo = escribir_csv(
        tmp_path / "malo.csv",
        "customerID,tenure,TotalCharges\n" "0002-BBBBB,9,999.99\n",
    )

    silver, _ = procesar(spark, [bueno, malo], esperadas, "run-001")

    filas = silver.collect()
    assert len(filas) == 1
    fila = filas[0]
    assert fila["customerID"] == "0001-AAAAA"
    assert fila["tenure"] == 5
    assert fila["MonthlyCharges"] == 70.35
    assert fila["TotalCharges"] == 351.75

    ids = {f["customerID"] for f in silver.select("customerID").collect()}
    assert "0002-BBBBB" not in ids


def test_columnas_de_silver(spark, tmp_path):
    """silver tiene las columnas de `esperadas` (en orden) mas las 3
    columnas de linaje al final. Cuando "Churn" esta en `esperadas` queda
    tipada como int; cuando no esta, la columna simplemente no existe."""
    esperadas_con_churn = [
        "customerID",
        "tenure",
        "MonthlyCharges",
        "TotalCharges",
        "Churn",
    ]
    ruta_con_churn = escribir_csv(
        tmp_path / "con_churn.csv",
        "customerID,tenure,MonthlyCharges,TotalCharges,Churn\n"
        "0001-AAAAA,5,70.35,351.75,Yes\n",
    )

    silver_con, _ = procesar(spark, [ruta_con_churn], esperadas_con_churn, "run-001")

    assert silver_con.columns == esperadas_con_churn + [
        "_archivo_origen",
        "_fecha_ingesta",
        "_id_ejecucion",
    ]
    assert dict(silver_con.dtypes)["Churn"] == "int"

    esperadas_sin_churn = ["customerID", "tenure", "MonthlyCharges", "TotalCharges"]
    ruta_sin_churn = escribir_csv(
        tmp_path / "sin_churn.csv",
        "customerID,tenure,MonthlyCharges,TotalCharges\n"
        "0002-BBBBB,6,80.00,480.00\n",
    )

    silver_sin, _ = procesar(spark, [ruta_sin_churn], esperadas_sin_churn, "run-002")

    assert silver_sin.columns == esperadas_sin_churn + [
        "_archivo_origen",
        "_fecha_ingesta",
        "_id_ejecucion",
    ]
    assert "Churn" not in silver_sin.columns


def test_cuarentena_tiene_esquema_comun(spark, tmp_path):
    """cuarentena mantiene el MISMO esquema (nombres y orden de columnas)
    tanto para errores de archivo (columna_faltante) como de fila
    (no_numerico_*), aunque se mezclen en una sola ejecucion."""
    esperadas = ["customerID", "tenure", "MonthlyCharges", "TotalCharges"]

    malo = escribir_csv(
        tmp_path / "malo.csv",
        "customerID,tenure,TotalCharges\n" "0001-AAAAA,5,351.75\n",
    )
    bueno_con_espacio = escribir_csv(
        tmp_path / "bueno_con_espacio.csv",
        "customerID,tenure,MonthlyCharges,TotalCharges\n" "0002-BBBBB,6,80.00, \n",
    )

    _, cuarentena = procesar(spark, [malo, bueno_con_espacio], esperadas, "run-001")

    assert cuarentena.columns == [
        "fecha_error",
        "unidad_etapa",
        "regla",
        "severidad",
        "archivo_origen",
        "customerID",
        "id_ejecucion",
        "registro_original",
    ]
    assert dict(cuarentena.dtypes) == {
        "fecha_error": "timestamp",
        "unidad_etapa": "string",
        "regla": "string",
        "severidad": "string",
        "archivo_origen": "string",
        "customerID": "string",
        "id_ejecucion": "string",
        "registro_original": "string",
    }
    reglas = {f["regla"] for f in cuarentena.select("regla").collect()}
    assert reglas == {"columna_faltante", "no_numerico_totalcharges"}


def test_mismo_cliente_en_dos_archivos_se_conserva(spark, tmp_path):
    """El mismo customerID en dos archivos VALIDOS distintos NO es un
    duplicado (es una nueva foto en el tiempo del mismo cliente): ambas
    filas quedan en silver."""
    esperadas = ["customerID", "tenure", "MonthlyCharges", "TotalCharges"]

    archivo_1 = escribir_csv(
        tmp_path / "lote1.csv",
        "customerID,tenure,MonthlyCharges,TotalCharges\n"
        "0001-AAAAA,5,70.35,351.75\n",
    )
    archivo_2 = escribir_csv(
        tmp_path / "lote2.csv",
        "customerID,tenure,MonthlyCharges,TotalCharges\n"
        "0001-AAAAA,6,75.00,450.00\n",
    )

    silver, _ = procesar(spark, [archivo_1, archivo_2], esperadas, "run-001")

    filas = silver.select("customerID", "tenure", "_archivo_origen").collect()
    assert len(filas) == 2
    assert all(f["customerID"] == "0001-AAAAA" for f in filas)
    # Una fila de cada archivo (sin depender del orden de las filas).
    assert sorted(f["tenure"] for f in filas) == [5, 6]
    assert len({f["_archivo_origen"] for f in filas}) == 2


# ---------------------------------------------------------------------------
# Parte 2: datos reales locales (info churn y crm).
#
# Los numeros de esta seccion salen del perfilado del Paso 2 (KPB S11) sobre
# las copias locales de los datos del curso. En el CI de la Unidad 4 esas
# copias no estan disponibles (son archivos grandes que no se versionan),
# por eso los tests se SALTAN con pytest.skip en vez de fallar cuando no
# encuentran la ruta esperada.
# ---------------------------------------------------------------------------


def _ruta_datos_local() -> str:
    return os.environ.get("DATOS_LOCAL", os.path.expanduser("~/curso-nube/datos_local"))


LOTES_CRM = [
    "clientes_2026-08-03.csv",
    "clientes_2026-08-10.csv",
    "clientes_2026-08-17.csv",
    "clientes_2026-08-24.csv",
    "clientes_2026-08-31.csv",
]


@pytest.fixture(scope="module")
def resultado_info_churn(spark):
    """Ejecuta `procesar` UNA sola vez sobre el archivo de entrenamiento
    IBM (COLUMNAS_INFO_CHURN, es costoso) y comparte (silver, cuarentena) entre
    todos los tests de info churn de la Parte 2."""
    ruta = os.path.join(_ruta_datos_local(), "base", "telco_entrenamiento.csv")
    if not os.path.exists(ruta):
        pytest.skip(f"No se encontro {ruta}: datos locales no disponibles en este entorno")
    return procesar(spark, [ruta], COLUMNAS_INFO_CHURN, "run-info-churn")


@pytest.fixture(scope="module")
def rutas_crm():
    """Rutas de los 5 lotes CRM, ya ordenadas por fecha."""
    base = _ruta_datos_local()
    rutas = [os.path.join(base, "crm", nombre) for nombre in LOTES_CRM]
    if not all(os.path.exists(r) for r in rutas):
        pytest.skip(
            f"No se encontraron los lotes CRM en {os.path.join(base, 'crm')}: "
            "datos locales no disponibles en este entorno"
        )
    return rutas


@pytest.fixture(scope="module")
def resultado_crm(spark, rutas_crm):
    """Ejecuta `procesar` UNA sola vez sobre los 5 lotes CRM (COLUMNAS_CRM,
    es costoso) y comparte (silver, cuarentena) entre todos los tests de
    crm de la Parte 2."""
    return procesar(spark, rutas_crm, COLUMNAS_CRM, "run-crm")


def test_info_churn_conteos_y_regla(resultado_info_churn):
    """Caso real IBM: 11 filas con TotalCharges " " (no convertible), todas
    con la misma regla de cuarentena."""
    silver, cuarentena = resultado_info_churn
    assert silver.count() == 7032
    assert cuarentena.count() == 11
    reglas = {f["regla"] for f in cuarentena.select("regla").collect()}
    assert reglas == {"no_numerico_totalcharges"}


def test_info_churn_churn_binario(resultado_info_churn):
    """Los 11 rechazados son todos Churn=No (no afectan el conteo de
    churners): tras binarizar, suma(Churn)=1869 y Churn==0 son 5163."""
    silver, _ = resultado_info_churn
    churns = [f["Churn"] for f in silver.select("Churn").collect()]
    assert sum(churns) == 1869
    assert len(churns) - sum(churns) == 5163


def test_crm_conteos_y_reglas(resultado_crm):
    """Los 5 lotes CRM juntos: 13 filas de cuarentena, todas por columnas
    numericas no convertibles (no_numerico_*)."""
    silver, cuarentena = resultado_crm
    assert silver.count() == 3563
    assert cuarentena.count() == 13
    reglas = [f["regla"] for f in cuarentena.select("regla").collect()]
    assert all(r.startswith("no_numerico_") for r in reglas)


def test_crm_duplicados_del_17_de_agosto(resultado_crm):
    """Los 3 duplicados IDENTICOS del lote 2026-08-17 se colapsan a una
    sola fila cada uno (resolver_duplicados, paso 1: filas identicas), no
    se pierden ni se repiten."""
    silver, _ = resultado_crm
    ids_esperados = {"7312-UBYIC", "9072-IPIFU", "9972-VWXCM"}

    filas_lote = silver.filter(silver["_archivo_origen"].endswith("clientes_2026-08-17.csv"))
    ids_en_lote = [
        f["customerID"]
        for f in filas_lote.select("customerID").collect()
        if f["customerID"] in ids_esperados
    ]

    assert set(ids_en_lote) == ids_esperados
    conteos = Counter(ids_en_lote)
    assert all(conteo == 1 for conteo in conteos.values())


def test_crm_cuadre_de_filas(resultado_crm, rutas_crm, spark):
    """Cuadre de conservacion: filas crudas de los 5 archivos == silver +
    cuarentena + 3 (los 3 duplicados identicos que se colapsan a 1 fila
    cada uno, y por eso no aparecen ni en silver ni en cuarentena como
    filas de mas)."""
    silver, cuarentena = resultado_crm
    crudas = sum(spark.read.csv(ruta, header=True).count() for ruta in rutas_crm)
    # cuarentena tiene una fila por ERROR: una fila cruda con dos columnas
    # malas aparece dos veces. Para el cuadre se cuentan filas crudas
    # rechazadas distintas.
    rechazadas = cuarentena.select("archivo_origen", "registro_original").distinct().count()

    assert crudas == 3579
    assert crudas == silver.count() + rechazadas + 3


def test_crm_linaje(resultado_crm):
    """Toda fila de silver tiene el _id_ejecucion usado en esta ejecucion
    y un _archivo_origen no nulo."""
    silver, _ = resultado_crm
    filas = silver.select("_id_ejecucion", "_archivo_origen").collect()
    assert all(f["_id_ejecucion"] == "run-crm" for f in filas)
    assert all(f["_archivo_origen"] is not None for f in filas)
