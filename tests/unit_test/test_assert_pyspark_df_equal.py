import datetime

import pyspark
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.assert_pysaprk_df_equal import assert_pysaprk_df_equal


class TestAssertPysaprkDfEqual:
    def test_assert_pysaprk_df_equal(self, spark_session: pyspark.sql.SparkSession):
        left_df = spark_session.createDataFrame(
            data=[
                [datetime.date(2020, 1, 1), "demo", 1.123, 10],
                [None, None, None, None],
            ],
            schema=StructType(
                [
                    StructField("col_a", DateType(), True),
                    StructField("col_b", StringType(), True),
                    StructField("col_c", DoubleType(), True),
                    StructField("col_d", LongType(), True),
                ]
            ),
        )
        right_df = spark_session.createDataFrame(
            data=[
                [datetime.date(2020, 1, 1), "demo", 1.123, 10],
                [None, None, None, None],
            ],
            schema=StructType(
                [
                    StructField("col_a", DateType(), True),
                    StructField("col_b", StringType(), True),
                    StructField("col_c", DoubleType(), True),
                    StructField("col_d", LongType(), True),
                ]
            ),
        )
        assert_pysaprk_df_equal(left_df, right_df)
