"""
Capa Gold del laboratorio de churn.

Idea simple: la capa Silver ya tiene los datos limpios de clientes (info churn,
snapshot de IBM Telco, y crm, lotes del CRM). La capa Gold le agrega a
cada cliente un resumen de su USO reciente (llamadas, datos, tickets) para
poder entrenar o explicar un modelo de churn.

Para que el resumen sea "point-in-time" (sin fuga de información del futuro),
cada cliente tiene una `fecha_foto`: el día en el que "tomamos la foto" de su
comportamiento. La regla de la ventana es siempre la misma:

    fecha_foto - DIAS_VENTANA  <=  fecha del evento  <  fecha_foto

Es decir, se usan los DIAS_VENTANA días ANTERIORES a la foto, sin incluir el
día de la foto (para no mirar el futuro respecto al evento que se quiere
predecir).

El módulo es determinista: no usa la hora actual ni números aleatorios, así
que con las mismas entradas produce siempre la misma tabla Gold.
"""

from datetime import timedelta

from pyspark.sql import DataFrame, functions as F

# Fecha fija de la foto para info churn (snapshot único de IBM Telco).
FECHA_FOTO_INFO_CHURN = "2026-08-03"

# Tamaño de la ventana de uso, en días, hacia atrás desde la fecha_foto.
DIAS_VENTANA = 90

# Columnas de métricas de uso que agrega `resumir_uso` y que añade `construir_gold`.
COLUMNAS_USO = [
    "n_llamadas",
    "min_llamadas",
    "mb_total",
    "n_tickets",
    "n_llamadas_caidas",
    "tasa_caida",
]


def agregar_fecha_foto(silver: DataFrame, fecha_foto: str = None) -> DataFrame:
    """
    Añade la columna `fecha_foto` (tipo date) a un DataFrame Silver.

    - Si se pasa `fecha_foto` (string 'YYYY-MM-DD'): se usa ese mismo valor
      como literal para TODAS las filas. Este es el caso de info churn, que
      tiene una sola foto fija (FECHA_FOTO_INFO_CHURN).
    - Si `fecha_foto` es None: la fecha_foto se calcula por fila a partir de
      la columna `fecha_lote` (string 'YYYY-MM-DD') del DataFrame. Este es el
      caso del crm, donde cada lote del CRM es su propia foto.
    """
    if fecha_foto is not None:
        # Info churn: la misma fecha para todos (literal convertido a date).
        return silver.withColumn("fecha_foto", F.to_date(F.lit(fecha_foto)))
    # CRM: cada fila trae su lote; lo convertimos de texto a date.
    return silver.withColumn("fecha_foto", F.to_date(F.col("fecha_lote"), "yyyy-MM-dd"))


def _podar_por_rango(uso: DataFrame, fotos: DataFrame) -> DataFrame:
    """
    Filtro "grueso" por la columna de partición `fecha`.

    Calculamos en el driver (una sola fila pequeña) la primera y la última
    foto, y filtramos con LITERALES. Así Spark puede aplicar poda de
    particiones (PartitionFilters) y ni siquiera abre las carpetas
    `fecha=...` que quedan fuera. Si el filtro dependiera de otro DataFrame,
    Spark no podría podar y leería todo el histórico.
    """
    rango = fotos.agg(F.min("fecha_foto").alias("min_f"), F.max("fecha_foto").alias("max_f")).first()
    if rango is None or rango["min_f"] is None:
        # No hay fotos: no hace falta leer ningún evento.
        return uso.limit(0)
    desde = rango["min_f"] - timedelta(days=DIAS_VENTANA)  # borde inferior incluido
    hasta = rango["max_f"]  # borde superior excluido (el día de la foto no cuenta)
    return uso.filter((F.col("fecha") >= F.lit(desde)) & (F.col("fecha") < F.lit(hasta)))


def resumir_uso(uso: DataFrame, fotos: DataFrame) -> DataFrame:
    """
    Resume los eventos de uso (llamadas, datos, tickets) para cada par
    (customerID, fecha_foto) presente en `fotos`.

    Devuelve exactamente una fila por cada par distinto de `fotos`, con las
    columnas customerID, fecha_foto y las 6 columnas de COLUMNAS_USO,
    calculadas SOLO con eventos de `uso` cuya columna `fecha` cae dentro de
    la ventana [fecha_foto - DIAS_VENTANA, fecha_foto) (ver regla de ventana
    en el docstring del módulo).

    Si un par (cliente, foto) no tiene ningún evento en la ventana, las
    primeras 5 métricas quedan en 0 (0.0 para las que son double) y
    `tasa_caida` queda en NULL. Los eventos de clientes que no aparecen en
    `fotos` se ignoran por completo.
    """
    # 1) Un solo registro por par (cliente, foto): evita contar dos veces.
    fotos = fotos.select("customerID", "fecha_foto").distinct()

    # 2) Poda de particiones + solo las columnas que usamos (menos lectura).
    eventos = _podar_por_rango(uso, fotos).select(
        "customerID", "tipo_evento", "duracion_seg", "mb", "llamada_caida", "fecha"
    )

    # 3) Cruzamos cada evento con las fotos de SU cliente cuya ventana lo
    #    contiene. `fotos` es pequeño (miles de filas): broadcast = se copia
    #    a cada executor y los 95 M eventos no se barajan por la red.
    #    Inner join: los eventos de clientes ajenos o fuera de ventana caen aquí.
    en_ventana = eventos.join(
        F.broadcast(fotos),
        (eventos["customerID"] == fotos["customerID"])
        & (eventos["fecha"] >= F.date_sub(fotos["fecha_foto"], DIAS_VENTANA))
        & (eventos["fecha"] < fotos["fecha_foto"]),
        "inner",
    ).select(fotos["customerID"], fotos["fecha_foto"], "tipo_evento", "duracion_seg", "mb",
             "llamada_caida")

    # 4) Métricas por (cliente, foto). Un when() sin otherwise() da NULL y
    #    sum/count ignoran NULL, así cada métrica mira solo su tipo de evento.
    es_llamada = F.col("tipo_evento") == "llamada"
    metricas = en_ventana.groupBy("customerID", "fecha_foto").agg(
        F.count(F.when(es_llamada, 1)).alias("n_llamadas"),
        (F.sum(F.when(es_llamada, F.col("duracion_seg"))) / 60.0).alias("min_llamadas"),
        # mb viene como float: lo pasamos a double ANTES de sumar para no
        # acumular error de precisión de float en millones de eventos.
        F.sum(F.when(F.col("tipo_evento") == "datos", F.col("mb").cast("double"))).alias("mb_total"),
        F.count(F.when(F.col("tipo_evento") == "ticket_soporte", 1)).alias("n_tickets"),
        F.count(F.when(es_llamada & F.col("llamada_caida"), 1)).alias("n_llamadas_caidas"),
    )

    # 5) Volvemos a partir de TODAS las fotos (left join) para que los
    #    clientes sin eventos también tengan su fila, con ceros.
    #    Ambos lados son pequeños (una fila por par), así que es barato.
    resumen = fotos.join(metricas, on=["customerID", "fecha_foto"], how="left")
    resumen = resumen.select(
        "customerID",
        "fecha_foto",
        F.coalesce("n_llamadas", F.lit(0)).cast("long").alias("n_llamadas"),
        F.coalesce("min_llamadas", F.lit(0.0)).cast("double").alias("min_llamadas"),
        F.coalesce("mb_total", F.lit(0.0)).cast("double").alias("mb_total"),
        F.coalesce("n_tickets", F.lit(0)).cast("long").alias("n_tickets"),
        F.coalesce("n_llamadas_caidas", F.lit(0)).cast("long").alias("n_llamadas_caidas"),
    )

    # 6) tasa_caida: sin llamadas no hay tasa (NULL), no es 0.
    return resumen.withColumn(
        "tasa_caida",
        F.when(F.col("n_llamadas") > 0, F.col("n_llamadas_caidas") / F.col("n_llamadas")).cast("double"),
    )


def construir_gold(silver: DataFrame, uso: DataFrame, fecha_foto: str = None) -> DataFrame:
    """
    Construye la tabla Gold final: toma `silver`, le calcula `fecha_foto`
    (con `agregar_fecha_foto`) y le agrega las columnas de uso (con
    `resumir_uso`), uniendo por customerID y fecha_foto.

    El resultado tiene el MISMO número de filas que `silver` (un cliente en
    silver siempre encuentra su propia fila de uso, aunque sea toda en cero)
    y conserva TODAS las columnas originales de `silver` (incluido el
    linaje y `Churn` cuando exista), más `fecha_foto` y COLUMNAS_USO.
    """
    con_foto = agregar_fecha_foto(silver, fecha_foto)
    # resumir_uso devuelve UNA fila por par (cliente, foto), por eso el left
    # join no puede multiplicar filas de silver.
    resumen = resumir_uso(uso, con_foto.select("customerID", "fecha_foto"))
    return con_foto.join(resumen, on=["customerID", "fecha_foto"], how="left")
