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
"""

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
    raise NotImplementedError("Paso 5: TDD verde")


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
    raise NotImplementedError("Paso 5: TDD verde")


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
    raise NotImplementedError("Paso 5: TDD verde")
