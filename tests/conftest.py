import os

# Debe fijarse ANTES de importar pyspark: silencia un UserWarning que lanza
# pyspark.pandas al no encontrar esta variable configurada.
os.environ.setdefault("PYARROW_IGNORE_TIMEZONE", "1")

import pytest
from pyspark.sql import SparkSession


# scope="session": arrancar una SparkSession tarda varios segundos, asi que
# la creamos una sola vez y la reutilizamos en todos los tests.
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")#corre spark en shell con 2 nucleos, rapido y gratis no nube
        .appName("tests-u3")
        .config("spark.sql.shuffle.partitions", "2")#se obliga a spark organizar en 2 pates y no 200
        .config("spark.ui.enabled", "false")#nadie mira las purebas, masahorro
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield spark
    spark.stop()
