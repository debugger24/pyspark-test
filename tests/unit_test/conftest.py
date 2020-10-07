import pytest
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


@pytest.fixture()
def spark_session():
    """
    Get the running Spark Session if exist otherwise create new session
    """

    conf = (
        SparkConf()
        .setMaster("local")
        .setAppName("dmp-unit-test")
        .set("spark.default.parallelism", "1")
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.shuffle.service.enabled", "false")
        .set("spark.sql.catalogImplementation", "hive")
    )

    sc = SparkContext.getOrCreate(conf=conf)

    return SparkSession(sc)
