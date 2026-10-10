from pyspark.testing import assertDataFrameEqual


def test_spark_arranca(spark):
    # confirma que la SparkSession quedo bien configurada y con la version esperada
    assert spark.range(3).count() == 3
    assert spark.version == "3.5.3"


def test_assert_dataframe_equal_funciona(spark):
    # dos DataFrames con las mismas filas pero en distinto orden
    df1 = spark.createDataFrame([("a", 1), ("b", 2)], ["id", "n"])
    df2 = spark.createDataFrame([("b", 2), ("a", 1)], ["id", "n"])

    # por defecto, assertDataFrameEqual no exige el mismo orden de filas
    # (checkRowOrder=False), asi que estos DataFrames se consideran iguales
    assertDataFrameEqual(df1, df2)
