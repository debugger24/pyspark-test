import datetime

import pyspark
import pytest
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.pyspark_test import assert_pyspark_df_equal


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

    def test_assert_pyspark_df_equal_one_is_not_pysaprk_df(
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
        right_df = "Demo"
        with pytest.raises(
            AssertionError,
            match="Right expected type <class 'pyspark.sql.dataframe.DataFrame'>, found <class 'str'> instead",
        ):
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
        with pytest.raises(
            AssertionError,
            match="Data mismatch\n  \n  Row = 1 : Column = col_b\n  \n  ACTUAL: demo\n  EXPECTED: demo1",
        ):
            assert_pyspark_df_equal(left_df, right_df)

    def test_assert_pyspark_df_equal_different_string_value_where_one_of_the_value_is_Null(
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
                [datetime.date(2020, 1, 1), None, 1.123, 10],
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
        with pytest.raises(
            AssertionError,
            match="Data mismatch\n  \n  Row = 1 : Column = col_b\n  \n  ACTUAL: demo\n  EXPECTED: None",
        ):
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
        with pytest.raises(
            AssertionError,
            match="Data mismatch\n  \n  Row = 1 : Column = col_a\n  \n  ACTUAL: 2020-01-01\n  EXPECTED: 2020-01-03",
        ):
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
        with pytest.raises(
            AssertionError,
            match="Data mismatch\n  \n  Row = 1 : Column = col_d\n  \n  ACTUAL: 10\n  EXPECTED: 20",
        ):
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
        with pytest.raises(
            AssertionError,
            match="Data mismatch\n  \n  Row = 1 : Column = col_c\n  \n  ACTUAL: 1.123\n  EXPECTED: 1.1236",
        ):
            assert_pyspark_df_equal(left_df, right_df)

    def test_assert_pyspark_df_equal_different_columns(
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
                [datetime.datetime(2020, 1, 1), "demo", 10],
                [None, None, None],
            ],
            schema=StructType(
                [
                    StructField("col_a", DateType(), True),
                    StructField("col_b", StringType(), True),
                    StructField("col_d", LongType(), True),
                ]
            ),
        )
        with pytest.raises(AssertionError, match="df schema type mismatch"):
            assert_pyspark_df_equal(left_df, right_df)

    def test_assert_pyspark_df_equal_different_row_count(
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
        with pytest.raises(
            AssertionError,
            match="Number of rows are not same.\n  \n  Actual Rows: 2\n  Expected Rows: 3",
        ):
            assert_pyspark_df_equal(left_df, right_df)
