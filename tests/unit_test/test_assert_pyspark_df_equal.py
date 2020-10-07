import datetime
import pytest
import pyspark
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.assert_pyspark_df_equal import assert_pyspark_df_equal


class TestAssertPysparkDfEqual:
    def test_assert_pyspark_df_equal_success(
        self, spark_session: pyspark.sql.SparkSession
    ):
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
        assert_pyspark_df_equal(left_df, right_df)

    def test_assert_pyspark_df_equal_different_string_value(
        self, spark_session: pyspark.sql.SparkSession
    ):
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
                [datetime.date(2020, 1, 1), "demo1", 1.123, 10],
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
        with pytest.raises(AssertionError, match="\nRow = 1 : Column = col_b\n\nACTUAL: demo \nEXPECTED: demo1"):
            assert_pyspark_df_equal(left_df, right_df)

    def test_assert_pyspark_df_equal_different_date_value(
        self, spark_session: pyspark.sql.SparkSession
    ):
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
                [datetime.date(2020, 1, 3), "demo", 1.123, 10],
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
        with pytest.raises(AssertionError, match="\nRow = 1 : Column = col_a\n\nACTUAL: 2020-01-01 \nEXPECTED: 2020-01-03"):
            assert_pyspark_df_equal(left_df, right_df)

    def test_assert_pyspark_df_equal_different_long_value(
        self, spark_session: pyspark.sql.SparkSession
    ):
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
                [datetime.date(2020, 1, 1), "demo", 1.123, 20],
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
        with pytest.raises(AssertionError, match="\nRow = 1 : Column = col_d\n\nACTUAL: 10 \nEXPECTED: 20"):
            assert_pyspark_df_equal(left_df, right_df)

    def test_assert_pyspark_df_equal_different_double_value(
        self, spark_session: pyspark.sql.SparkSession
    ):
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
                [datetime.date(2020, 1, 1), "demo", 1.1236, 10],
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
        with pytest.raises(AssertionError, match="\nRow = 1 : Column = col_c\n\nACTUAL: 1.123 \nEXPECTED: 1.1236"):
            assert_pyspark_df_equal(left_df, right_df)
