"""
Tests TDD (fase ROJA) del modulo churn/gold.py.

Gold toma la capa Silver (clientes de info churn y crm) y le agrega un
resumen de USO reciente (uso: llamadas, datos, tickets de soporte),
calculado con una ventana "point-in-time": solo se miran los 90 dias
ANTERIORES a la fecha_foto de cada cliente, sin incluir el dia de la foto.

Estos tests usan datos sinteticos pequenos (no datos reales de Telco) y el
fixture `spark` de tests/conftest.py del repo.
"""

from datetime import date, datetime

import pytest
from pyspark.sql.types import (
    BooleanType,
    DateType,
    FloatType,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from churn import gold


# ---------------------------------------------------------------------------
# Helpers para construir DataFrames sinteticos con esquema explicito.
# ---------------------------------------------------------------------------

def _mk_silver_info_churn(spark, filas):
    """Silver sintetico estilo info churn (IBM Telco): tiene Churn, no tiene fecha_lote."""
    esquema = StructType(
        [
            StructField("customerID", StringType(), True),
            StructField("tenure", IntegerType(), True),
            StructField("Contract", StringType(), True),
            StructField("Churn", IntegerType(), True),
            StructField("_id_ejecucion", StringType(), True),
        ]
    )
    return spark.createDataFrame(filas, schema=esquema)


def _mk_silver_crm(spark, filas):
    """Silver sintetico estilo crm (lotes CRM): tiene fecha_lote, no tiene Churn."""
    esquema = StructType(
        [
            StructField("customerID", StringType(), True),
            StructField("tenure", IntegerType(), True),
            StructField("Contract", StringType(), True),
            StructField("fecha_lote", StringType(), True),
            StructField("_id_ejecucion", StringType(), True),
        ]
    )
    return spark.createDataFrame(filas, schema=esquema)


def _mk_fotos(spark, filas):
    """filas: lista de tuplas (customerID, fecha_foto como date)."""
    esquema = StructType(
        [
            StructField("customerID", StringType(), True),
            StructField("fecha_foto", DateType(), True),
        ]
    )
    return spark.createDataFrame(filas, schema=esquema)


def _mk_uso(spark, filas):
    """
    filas: lista de tuplas
    (customerID, event_ts, tipo_evento, duracion_seg, mb, llamada_caida, antena_id, fecha)
    Usar None en los campos que no apliquen segun el tipo de evento.
    """
    esquema = StructType(
        [
            StructField("customerID", StringType(), True),
            StructField("event_ts", TimestampNTZType(), True),
            StructField("tipo_evento", StringType(), True),
            StructField("duracion_seg", IntegerType(), True),
            StructField("mb", FloatType(), True),
            StructField("llamada_caida", BooleanType(), True),
            StructField("antena_id", StringType(), True),
            StructField("fecha", DateType(), True),
        ]
    )
    return spark.createDataFrame(filas, schema=esquema)


def _evento_llamada(customer_id, fecha, duracion_seg=60, caida=False, antena="A1"):
    """Atajo para crear una fila de evento tipo 'llamada' en `fecha` (mediodia)."""
    return (
        customer_id,
        datetime(fecha.year, fecha.month, fecha.day, 12, 0, 0),
        "llamada",
        duracion_seg,
        None,
        caida,
        antena,
        fecha,
    )


def _evento_datos(customer_id, fecha, mb=100.0, antena="A1"):
    """Atajo para crear una fila de evento tipo 'datos' en `fecha` (mediodia)."""
    return (
        customer_id,
        datetime(fecha.year, fecha.month, fecha.day, 12, 0, 0),
        "datos",
        None,
        mb,
        None,
        antena,
        fecha,
    )


def _evento_ticket(customer_id, fecha, antena="A1"):
    """Atajo para crear una fila de evento tipo 'ticket_soporte' en `fecha` (mediodia)."""
    return (
        customer_id,
        datetime(fecha.year, fecha.month, fecha.day, 12, 0, 0),
        "ticket_soporte",
        None,
        None,
        None,
        antena,
        fecha,
    )


def _fila_de(df, customer_id, fecha_foto=None):
    """Devuelve como dict la unica fila de `customer_id` (y `fecha_foto` si se da)."""
    filtrado = df.filter(df.customerID == customer_id)
    if fecha_foto is not None:
        filtrado = filtrado.filter(filtrado.fecha_foto == fecha_foto)
    filas = filtrado.collect()
    assert len(filas) == 1, f"se esperaba 1 fila para {customer_id}, hay {len(filas)}"
    return filas[0].asDict()


# ---------------------------------------------------------------------------
# agregar_fecha_foto
# ---------------------------------------------------------------------------

def test_agregar_fecha_foto_caso_info_churn_usa_literal_para_todas_las_filas(spark):
    """En info churn, fecha_foto es el mismo literal para todos los clientes."""
    silver = _mk_silver_info_churn(
        spark,
        [
            ("C001", 12, "Month-to-month", 1, "run-1"),
            ("C002", 24, "Two year", 0, "run-1"),
        ],
    )

    resultado = gold.agregar_fecha_foto(silver, gold.FECHA_FOTO_INFO_CHURN)

    valores = sorted(r["fecha_foto"] for r in resultado.select("fecha_foto").collect())
    assert valores == [date(2026, 8, 3), date(2026, 8, 3)]


def test_agregar_fecha_foto_caso_info_churn_columna_es_tipo_date(spark):
    """La columna fecha_foto debe quedar como DateType, no string ni timestamp."""
    silver = _mk_silver_info_churn(spark, [("C001", 12, "Month-to-month", 1, "run-1")])

    resultado = gold.agregar_fecha_foto(silver, gold.FECHA_FOTO_INFO_CHURN)

    tipo = dict(resultado.dtypes)["fecha_foto"]
    assert tipo == "date"


def test_agregar_fecha_foto_caso_crm_se_calcula_desde_fecha_lote(spark):
    """En el crm, fecha_foto sale de convertir fecha_lote (string) a date."""
    silver = _mk_silver_crm(
        spark,
        [
            ("C010", 5, "Month-to-month", "2026-08-10", "run-2"),
            ("C011", 8, "One year", "2026-07-15", "run-2"),
        ],
    )

    resultado = gold.agregar_fecha_foto(silver, None)

    fila_1 = _fila_de(resultado, "C010")
    fila_2 = _fila_de(resultado, "C011")
    assert fila_1["fecha_foto"] == date(2026, 8, 10)
    assert fila_2["fecha_foto"] == date(2026, 7, 15)


def test_agregar_fecha_foto_caso_crm_columna_es_tipo_date(spark):
    """Igual que en info churn, fecha_foto en crm tambien debe ser DateType."""
    silver = _mk_silver_crm(spark, [("C010", 5, "Month-to-month", "2026-08-10", "run-2")])

    resultado = gold.agregar_fecha_foto(silver, None)

    tipo = dict(resultado.dtypes)["fecha_foto"]
    assert tipo == "date"


# ---------------------------------------------------------------------------
# resumir_uso: bordes de la ventana de 90 dias
# ---------------------------------------------------------------------------

def test_ventana_incluye_evento_exactamente_90_dias_antes_de_la_foto(spark):
    """fecha_foto - 90 dias SI debe contar (borde inferior incluido)."""
    fotos = _mk_fotos(spark, [("C100", date(2026, 8, 3))])
    uso = _mk_uso(spark, [_evento_llamada("C100", date(2026, 5, 5))])

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C100")
    assert fila["n_llamadas"] == 1


def test_ventana_excluye_evento_91_dias_antes_de_la_foto(spark):
    """fecha_foto - 91 dias ya quedo fuera de la ventana de 90 dias."""
    fotos = _mk_fotos(spark, [("C101", date(2026, 8, 3))])
    uso = _mk_uso(spark, [_evento_llamada("C101", date(2026, 5, 4))])

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C101")
    assert fila["n_llamadas"] == 0


def test_ventana_incluye_el_dia_anterior_a_la_foto(spark):
    """fecha_foto - 1 dia es el ultimo dia que si cuenta."""
    fotos = _mk_fotos(spark, [("C102", date(2026, 8, 3))])
    uso = _mk_uso(spark, [_evento_llamada("C102", date(2026, 8, 2))])

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C102")
    assert fila["n_llamadas"] == 1


def test_ventana_excluye_el_mismo_dia_de_la_foto(spark):
    """El dia en que se toma la foto NO se cuenta (evitar fuga de futuro)."""
    fotos = _mk_fotos(spark, [("C103", date(2026, 8, 3))])
    uso = _mk_uso(spark, [_evento_llamada("C103", date(2026, 8, 3))])

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C103")
    assert fila["n_llamadas"] == 0


def test_ventana_excluye_evento_posterior_a_la_foto(spark):
    """Un evento despues de la fecha_foto tampoco debe contar."""
    fotos = _mk_fotos(spark, [("C104", date(2026, 8, 3))])
    uso = _mk_uso(spark, [_evento_llamada("C104", date(2026, 8, 10))])

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C104")
    assert fila["n_llamadas"] == 0


# ---------------------------------------------------------------------------
# resumir_uso: cada metrica por separado
# ---------------------------------------------------------------------------

def test_n_llamadas_cuenta_solo_eventos_tipo_llamada(spark):
    """n_llamadas solo cuenta eventos de tipo 'llamada', no 'datos' ni tickets."""
    fotos = _mk_fotos(spark, [("C200", date(2026, 8, 3))])
    uso = _mk_uso(
        spark,
        [
            _evento_llamada("C200", date(2026, 7, 1)),
            _evento_llamada("C200", date(2026, 7, 2)),
            _evento_datos("C200", date(2026, 7, 3)),
            _evento_ticket("C200", date(2026, 7, 4)),
        ],
    )

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C200")
    assert fila["n_llamadas"] == 2


def test_min_llamadas_es_la_suma_de_duracion_seg_dividida_en_60(spark):
    """min_llamadas = suma(duracion_seg) / 60, en minutos."""
    fotos = _mk_fotos(spark, [("C201", date(2026, 8, 3))])
    uso = _mk_uso(
        spark,
        [
            _evento_llamada("C201", date(2026, 7, 1), duracion_seg=120),
            _evento_llamada("C201", date(2026, 7, 2), duracion_seg=180),
        ],
    )

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C201")
    assert fila["min_llamadas"] == pytest.approx(5.0)


def test_mb_total_suma_el_consumo_de_datos(spark):
    """mb_total = suma(mb) de los eventos tipo 'datos' en la ventana."""
    fotos = _mk_fotos(spark, [("C202", date(2026, 8, 3))])
    uso = _mk_uso(
        spark,
        [
            _evento_datos("C202", date(2026, 7, 1), mb=250.5),
            _evento_datos("C202", date(2026, 7, 2), mb=100.0),
        ],
    )

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C202")
    assert fila["mb_total"] == pytest.approx(350.5)


def test_n_tickets_cuenta_eventos_tipo_ticket_soporte(spark):
    """n_tickets solo cuenta eventos de tipo 'ticket_soporte'."""
    fotos = _mk_fotos(spark, [("C203", date(2026, 8, 3))])
    uso = _mk_uso(
        spark,
        [
            _evento_ticket("C203", date(2026, 7, 1)),
            _evento_ticket("C203", date(2026, 7, 2)),
            _evento_ticket("C203", date(2026, 7, 3)),
            _evento_llamada("C203", date(2026, 7, 4)),
        ],
    )

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C203")
    assert fila["n_tickets"] == 3


def test_n_llamadas_caidas_cuenta_solo_llamadas_con_llamada_caida_true(spark):
    """n_llamadas_caidas cuenta llamadas con llamada_caida = true, nada mas."""
    fotos = _mk_fotos(spark, [("C204", date(2026, 8, 3))])
    uso = _mk_uso(
        spark,
        [
            _evento_llamada("C204", date(2026, 7, 1), caida=True),
            _evento_llamada("C204", date(2026, 7, 2), caida=False),
            _evento_llamada("C204", date(2026, 7, 3), caida=True),
        ],
    )

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C204")
    assert fila["n_llamadas"] == 3
    assert fila["n_llamadas_caidas"] == 2


# ---------------------------------------------------------------------------
# resumir_uso: tasa_caida y casos sin eventos
# ---------------------------------------------------------------------------

def test_tasa_caida_es_llamadas_caidas_sobre_llamadas_totales(spark):
    """tasa_caida = n_llamadas_caidas / n_llamadas."""
    fotos = _mk_fotos(spark, [("C300", date(2026, 8, 3))])
    uso = _mk_uso(
        spark,
        [
            _evento_llamada("C300", date(2026, 7, 1), caida=True),
            _evento_llamada("C300", date(2026, 7, 2), caida=False),
            _evento_llamada("C300", date(2026, 7, 3), caida=False),
            _evento_llamada("C300", date(2026, 7, 4), caida=False),
        ],
    )

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C300")
    assert fila["tasa_caida"] == pytest.approx(0.25)


def test_tasa_caida_es_null_si_no_hay_llamadas_aunque_haya_otros_eventos(spark):
    """Sin llamadas, tasa_caida es NULL aunque el cliente si tenga datos o tickets."""
    fotos = _mk_fotos(spark, [("C301", date(2026, 8, 3))])
    uso = _mk_uso(
        spark,
        [
            _evento_datos("C301", date(2026, 7, 1), mb=50.0),
            _evento_ticket("C301", date(2026, 7, 2)),
        ],
    )

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C301")
    assert fila["n_llamadas"] == 0
    assert fila["n_llamadas_caidas"] == 0
    assert fila["mb_total"] == pytest.approx(50.0)
    assert fila["n_tickets"] == 1
    assert fila["tasa_caida"] is None


def test_cliente_sin_ningun_evento_en_la_ventana_queda_en_ceros_y_tasa_null(spark):
    """Un cliente que esta en `fotos` pero no tiene ningun evento de uso: todo en 0/NULL."""
    fotos = _mk_fotos(spark, [("C302", date(2026, 8, 3))])
    uso = _mk_uso(spark, [])  # sin eventos de nadie

    resultado = gold.resumir_uso(uso, fotos)

    fila = _fila_de(resultado, "C302")
    assert fila["n_llamadas"] == 0
    assert fila["min_llamadas"] == pytest.approx(0.0)
    assert fila["mb_total"] == pytest.approx(0.0)
    assert fila["n_tickets"] == 0
    assert fila["n_llamadas_caidas"] == 0
    assert fila["tasa_caida"] is None


def test_resumir_uso_devuelve_una_sola_fila_por_par_customerid_fecha_foto(spark):
    """No debe haber duplicados: exactamente 1 fila por cada par de `fotos`."""
    fotos = _mk_fotos(spark, [("C303", date(2026, 8, 3)), ("C304", date(2026, 8, 3))])
    uso = _mk_uso(
        spark,
        [
            _evento_llamada("C303", date(2026, 7, 1)),
            _evento_llamada("C303", date(2026, 7, 2)),
        ],
    )

    resultado = gold.resumir_uso(uso, fotos)

    assert resultado.count() == 2


# ---------------------------------------------------------------------------
# resumir_uso: aislamiento entre clientes y entre lotes (point-in-time)
# ---------------------------------------------------------------------------

def test_ignora_eventos_de_un_cliente_que_no_esta_en_fotos(spark):
    """Eventos de un customerID ausente de `fotos` no deben afectar ni aparecer."""
    fotos = _mk_fotos(spark, [("C400", date(2026, 8, 3))])
    uso = _mk_uso(
        spark,
        [
            _evento_llamada("C400", date(2026, 7, 1)),
            _evento_llamada("C999_AJENO", date(2026, 7, 1)),
            _evento_llamada("C999_AJENO", date(2026, 7, 2)),
        ],
    )

    resultado = gold.resumir_uso(uso, fotos)

    assert resultado.count() == 1
    fila = _fila_de(resultado, "C400")
    assert fila["n_llamadas"] == 1


def test_mismo_cliente_en_dos_lotes_produce_metricas_distintas_por_ventana(spark):
    """
    El mismo customerID con dos fecha_foto distintas (dos lotes de crm) debe
    quedar en dos filas, cada una con su propia ventana de 90 dias (point-in-time).
    """
    fotos = _mk_fotos(
        spark,
        [
            ("C500", date(2026, 6, 1)),
            ("C500", date(2026, 8, 10)),
        ],
    )
    uso = _mk_uso(
        spark,
        [
            # solo entra en la ventana de la foto de junio (2026-03-03 a 2026-05-31)
            _evento_llamada("C500", date(2026, 5, 1)),
            _evento_llamada("C500", date(2026, 5, 2)),
            # solo entra en la ventana de la foto de agosto (2026-05-12 a 2026-08-09)
            _evento_llamada("C500", date(2026, 8, 1)),
            _evento_llamada("C500", date(2026, 8, 2)),
            _evento_llamada("C500", date(2026, 8, 3)),
        ],
    )

    resultado = gold.resumir_uso(uso, fotos)

    assert resultado.count() == 2
    fila_junio = _fila_de(resultado, "C500", fecha_foto=date(2026, 6, 1))
    fila_agosto = _fila_de(resultado, "C500", fecha_foto=date(2026, 8, 10))
    assert fila_junio["n_llamadas"] == 2
    assert fila_agosto["n_llamadas"] == 3


# ---------------------------------------------------------------------------
# construir_gold
# ---------------------------------------------------------------------------

def test_construir_gold_conserva_el_numero_de_filas_de_silver(spark):
    """construir_gold no debe perder ni duplicar filas de silver (info churn)."""
    silver = _mk_silver_info_churn(
        spark,
        [
            ("C600", 10, "Month-to-month", 1, "run-3"),
            ("C601", 20, "Two year", 0, "run-3"),
            ("C602", 30, "One year", 0, "run-3"),
        ],
    )
    uso = _mk_uso(spark, [_evento_llamada("C600", date(2026, 7, 1))])

    resultado = gold.construir_gold(silver, uso, gold.FECHA_FOTO_INFO_CHURN)

    assert resultado.count() == silver.count()


def test_construir_gold_incluye_columnas_de_silver_mas_fecha_foto_y_columnas_uso(spark):
    """El resultado debe tener todas las columnas de silver + fecha_foto + COLUMNAS_USO."""
    silver = _mk_silver_info_churn(spark, [("C610", 10, "Month-to-month", 1, "run-4")])
    uso = _mk_uso(spark, [])

    resultado = gold.construir_gold(silver, uso, gold.FECHA_FOTO_INFO_CHURN)

    columnas_esperadas = set(silver.columns) | {"fecha_foto"} | set(gold.COLUMNAS_USO)
    assert columnas_esperadas.issubset(set(resultado.columns))


def test_construir_gold_conserva_la_columna_churn_en_info_churn(spark):
    """El linaje y Churn de silver info churn no se deben perder al construir gold."""
    silver = _mk_silver_info_churn(
        spark,
        [
            ("C620", 15, "Month-to-month", 1, "run-5"),
            ("C621", 40, "Two year", 0, "run-5"),
        ],
    )
    uso = _mk_uso(spark, [])

    resultado = gold.construir_gold(silver, uso, gold.FECHA_FOTO_INFO_CHURN)

    fila_1 = _fila_de(resultado, "C620")
    fila_2 = _fila_de(resultado, "C621")
    assert fila_1["Churn"] == 1
    assert fila_2["Churn"] == 0


def test_construir_gold_usa_fecha_lote_cuando_fecha_foto_es_none(spark):
    """Con crm (fecha_foto=None), gold debe calcular fecha_foto desde fecha_lote."""
    silver = _mk_silver_crm(spark, [("C630", 7, "Month-to-month", "2026-08-10", "run-6")])
    uso = _mk_uso(spark, [_evento_llamada("C630", date(2026, 7, 1))])

    resultado = gold.construir_gold(silver, uso, None)

    fila = _fila_de(resultado, "C630")
    assert fila["fecha_foto"] == date(2026, 8, 10)
    assert fila["n_llamadas"] == 1


def test_construir_gold_tipos_de_las_columnas_de_uso_son_long_y_double(spark):
    """Contrato de tipos: conteos como long, montos/tasas como double."""
    silver = _mk_silver_info_churn(spark, [("C640", 10, "Month-to-month", 0, "run-7")])
    uso = _mk_uso(spark, [_evento_llamada("C640", date(2026, 7, 1))])

    resultado = gold.construir_gold(silver, uso, gold.FECHA_FOTO_INFO_CHURN)

    tipos = dict(resultado.dtypes)
    assert tipos["n_llamadas"] == "bigint"
    assert tipos["min_llamadas"] == "double"
    assert tipos["mb_total"] == "double"
    assert tipos["n_tickets"] == "bigint"
    assert tipos["n_llamadas_caidas"] == "bigint"
    assert tipos["tasa_caida"] == "double"


# ---------------------------------------------------------------------------
# Constantes del contrato
# ---------------------------------------------------------------------------

def test_columnas_uso_tiene_exactamente_las_6_columnas_esperadas(spark):
    """COLUMNAS_USO debe ser exactamente esta lista, en este orden."""
    assert gold.COLUMNAS_USO == [
        "n_llamadas",
        "min_llamadas",
        "mb_total",
        "n_tickets",
        "n_llamadas_caidas",
        "tasa_caida",
    ]


def test_dias_ventana_es_90():
    """La ventana de uso siempre es de 90 dias."""
    assert gold.DIAS_VENTANA == 90


def test_fecha_foto_info_churn_tiene_el_valor_esperado():
    """FECHA_FOTO_INFO_CHURN es la foto fija del snapshot de info churn."""
    assert gold.FECHA_FOTO_INFO_CHURN == "2026-08-03"
