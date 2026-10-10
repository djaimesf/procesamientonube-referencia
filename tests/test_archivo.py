"""Tests de NIVEL ARCHIVO (fase TDD roja) para churn/silver.py.

Estos tests describen el CONTRATO antes de implementarlo. Con los stubs
actuales (que lanzan NotImplementedError) todos los tests de
validar_encabezados y leer_csv deben FALLAR con NotImplementedError.

La excepcion es el test "de aprendizaje": no prueba nuestro codigo, sino
el comportamiento real de Spark, asi que debe PASAR ya en la fase roja.
"""

import re

import pytest
from pyspark.sql.types import StructType, StructField, StringType

from churn.silver import validar_encabezados, leer_csv


def escribir_csv(ruta, contenido: str):
    """Helper local: escribe un CSV pequeno para las pruebas."""
    ruta.write_text(contenido, encoding="utf-8")
    return str(ruta)


# ---------------------------------------------------------------------------
# validar_encabezados (Python puro, no necesita Spark)
# ---------------------------------------------------------------------------


def test_encabezados_correctos_sin_errores():
    """Si columnas == esperadas, no hay errores."""
    columnas = ["customerID", "tenure", "TotalCharges"]
    esperadas = ["customerID", "tenure", "TotalCharges"]
    assert validar_encabezados(columnas, esperadas) == []


def test_normaliza_espacios_y_mayusculas():
    """La comparacion ignora espacios sobrantes y mayusculas/minusculas."""
    columnas = [" CustomerID ", "TENURE"]
    esperadas = ["customerID", "tenure"]
    assert validar_encabezados(columnas, esperadas) == []


def test_orden_distinto_no_es_error():
    """El orden de las columnas en el archivo no importa para validar."""
    columnas = ["tenure", "customerID", "TotalCharges"]
    esperadas = ["customerID", "tenure", "TotalCharges"]
    assert validar_encabezados(columnas, esperadas) == []


def test_columna_faltante():
    """Falta 'TotalCharges': debe reportarse exactamente 1 error bloqueante."""
    columnas = ["customerID", "tenure"]
    esperadas = ["customerID", "tenure", "TotalCharges"]

    errores = validar_encabezados(columnas, esperadas)

    assert len(errores) == 1
    error = errores[0]
    assert error["regla"] == "columna_faltante"
    assert set(error) == {"regla", "severidad", "detalle"}
    assert error["severidad"] == "bloquea"
    assert "totalcharges" in error["detalle"].lower()


def test_dos_columnas_faltantes_dan_dos_errores():
    """Cada columna esperada ausente genera su propio error."""
    columnas = ["customerID"]
    esperadas = ["customerID", "tenure", "TotalCharges"]

    errores = validar_encabezados(columnas, esperadas)

    assert len(errores) == 2
    assert all(e["regla"] == "columna_faltante" for e in errores)
    detalles = " ".join(e["detalle"].lower() for e in errores)
    assert "tenure" in detalles and "totalcharges" in detalles


def test_columnas_sobrantes_no_son_error():
    """Columnas de mas (BancoPago) no generan error: se descartan al leer."""
    columnas = ["customerID", "tenure", "BancoPago"]
    esperadas = ["customerID", "tenure"]
    assert validar_encabezados(columnas, esperadas) == []


def test_encabezado_repetido_tras_normalizar():
    """'tenure', 'TENURE' y ' Tenure ' son el mismo nombre normalizado:
    se reporta UN error por nombre repetido, no uno por ocurrencia extra."""
    columnas = ["customerID", "tenure", "TENURE", " Tenure "]
    esperadas = ["customerID", "tenure"]

    errores = validar_encabezados(columnas, esperadas)

    assert len(errores) == 1
    assert errores[0]["regla"] == "encabezado_repetido"
    assert errores[0]["severidad"] == "bloquea"
    assert "tenure" in errores[0]["detalle"].lower()


def test_repetido_y_faltante_reporta_ambos():
    """Un archivo puede tener a la vez un encabezado repetido y uno faltante."""
    columnas = ["customerID", "tenure", "TENURE"]
    esperadas = ["customerID", "tenure", "TotalCharges"]

    errores = validar_encabezados(columnas, esperadas)

    assert sorted(e["regla"] for e in errores) == [
        "columna_faltante",
        "encabezado_repetido",
    ]


# ---------------------------------------------------------------------------
# leer_csv (requiere fixture spark)
# ---------------------------------------------------------------------------


def test_lee_por_nombre_no_por_posicion(spark, tmp_path):
    """Caso real: en los lotes CRM 'fecha_lote' esta en la posicion 2, no al
    final. La lectura debe emparejar por nombre, no por orden de columnas."""
    ruta = escribir_csv(
        tmp_path / "lote.csv",
        "fecha_lote,customerID,tenure\n2026-08-03,0001-AAAAA,5\n",
    )
    esperadas = ["customerID", "tenure", "fecha_lote"]

    df = leer_csv(spark, ruta, esperadas)
    fila = df.collect()[0]

    assert fila["customerID"] == "0001-AAAAA"
    assert fila["tenure"] == "5"
    assert fila["fecha_lote"] == "2026-08-03"


def test_nombres_canonicos_y_orden(spark, tmp_path):
    """Las columnas resultantes usan el nombre canonico de `esperadas`, en ese
    orden, y descartan cualquier columna sobrante (BancoPago)."""
    ruta = escribir_csv(
        tmp_path / "lote.csv",
        " CUSTOMERID ,Tenure,BancoPago\n0001-AAAAA,5,Bancolombia\n",
    )
    esperadas = ["customerID", "tenure"]

    df = leer_csv(spark, ruta, esperadas)

    assert df.columns == ["customerID", "tenure", "_archivo_origen"]


def test_todo_se_lee_como_texto(spark, tmp_path):
    """inferSchema=False: todas las columnas esperadas quedan como string,
    aunque el contenido parezca numerico."""
    ruta = escribir_csv(
        tmp_path / "lote.csv",
        "customerID,tenure\n0001-AAAAA,5\n",
    )
    esperadas = ["customerID", "tenure"]

    df = leer_csv(spark, ruta, esperadas)

    tipos = dict(df.dtypes)
    for columna in esperadas:
        assert tipos[columna] == "string"


def test_celda_vacia_es_nulo_y_espacio_se_conserva(spark, tmp_path):
    """Perfilado real de IBM: una celda vacia es nulo, pero un espacio " " NO
    es nulo para Spark (se conserva tal cual)."""
    ruta = escribir_csv(
        tmp_path / "lote.csv",
        "customerID,TotalCharges,tenure\n"
        "0001-AAAAA,,5\n"
        "0002-BBBBB, ,5\n",
    )
    esperadas = ["customerID", "TotalCharges", "tenure"]

    df = leer_csv(spark, ruta, esperadas)
    filas = df.orderBy("customerID").collect()

    assert filas[0]["TotalCharges"] is None
    assert filas[1]["TotalCharges"] == " "


def test_archivo_origen_registra_el_archivo(spark, tmp_path):
    """_archivo_origen debe terminar en el nombre real del archivo leido.
    (En Spark 3.5 local, _metadata.file_path tiene la forma 'file:/.../x.csv'.)"""
    ruta = escribir_csv(
        tmp_path / "lote_2026_08.csv",
        "customerID,tenure\n0001-AAAAA,5\n",
    )
    esperadas = ["customerID", "tenure"]

    df = leer_csv(spark, ruta, esperadas)
    fila = df.collect()[0]

    assert fila["_archivo_origen"].endswith("lote_2026_08.csv")


# ---------------------------------------------------------------------------
# Test de APRENDIZAJE: caracteriza a Spark, no a nuestro codigo.
# Debe PASAR ya en la fase roja (no depende de churn.silver).
# ---------------------------------------------------------------------------


ESQUEMA_APRENDIZAJE = StructType(
    [
        StructField("customerID", StringType(), True),
        StructField("fecha_lote", StringType(), True),
    ]
)


def test_aprendizaje_enforce_schema_false_no_reordena(spark, tmp_path):
    """Con enforceSchema=False, Spark SI valida los encabezados contra el
    esquema explicito, por POSICION, y falla si no coinciden; NO reordena
    automaticamente por nombre. Por eso en leer_csv leemos SIN esquema
    (schema=None) y emparejamos nosotros mismos por nombre.

    Este test no ejercita churn.silver: documenta el comportamiento real
    de Spark que motiva nuestro diseno. Debe pasar aunque leer_csv todavia
    no este implementado.

    Spark 3.5.3 (PySpark clasico) lanza py4j.protocol.Py4JJavaError que
    envuelve java.lang.IllegalArgumentException; el tipo en Python cambia
    segun el entorno (p. ej. Spark Connect), por eso se verifica el MENSAJE.
    El error aparece al ejecutar una accion (collect), no al definir la lectura.
    """
    ruta = escribir_csv(
        tmp_path / "lote.csv",
        "fecha_lote,customerID\n2026-08-03,0001-AAAAA\n",
    )

    with pytest.raises(
        Exception,
        match=re.escape("CSV header does not conform to the schema"),
    ) as info:
        spark.read.csv(
            ruta, header=True, schema=ESQUEMA_APRENDIZAJE, enforceSchema=False
        ).collect()

    assert "Expected: customerID but found: fecha_lote" in str(info.value)


def test_aprendizaje_enforce_schema_false_mismo_orden_funciona(spark, tmp_path):
    """Control positivo: con el encabezado en el MISMO orden que el esquema,
    enforceSchema=False lee sin error. Asi el test anterior falla por el
    ORDEN, no por la ruta, el esquema o la configuracion."""
    ruta = escribir_csv(
        tmp_path / "lote.csv",
        "customerID,fecha_lote\n0001-AAAAA,2026-08-03\n",
    )

    filas = spark.read.csv(
        ruta, header=True, schema=ESQUEMA_APRENDIZAJE, enforceSchema=False
    ).collect()

    assert [f.asDict() for f in filas] == [
        {"customerID": "0001-AAAAA", "fecha_lote": "2026-08-03"}
    ]
