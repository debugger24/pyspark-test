# pyspark-test

[![Code Style: Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Unit Test](https://github.com/debugger24/pyspark-test/workflows/Unit%20Test/badge.svg?branch=main)](https://github.com/debugger24/pyspark-test/actions?query=workflow%3A%22Unit+Test%22)
[![PyPI version](https://badge.fury.io/py/pyspark-test.svg)](https://badge.fury.io/py/pyspark-test)

Check that left and right spark DataFrame are equal.

This function is intended to compare two spark DataFrames and output any differences. It is inspired from pandas testing module for pysaprk, and for use in unit tests. Additional parameters allow varying the strictness of the equality checks performed.

# Installation

```
pip install pyspark-test
```

# Usage

```py
assert_pyspark_df_equal(left_df, actual_df)
```

## Additional Arguments

* `check_dtype` : To compare the data types of spark dataframe. Default true
* `check_column_names` : To compare column names. Default false. Not required of we are checking data types.
* `check_columns_in_order` : To check the columns should be in order or not. Default to false
* `order_by` : Column names with which dataframe must be sorted before comparing. Default None.

# Example

```py
from pyspark_test import assert_pyspark_df_equal

df_1 = spark_session.createDataFrame(
    data=[
        [datetime.date(2020, 1, 1), 'demo', 1.123, 10],
        [None, None, None, None],
    ],
    schema=StructType(
        [
            StructField('col_a', DateType(), True),
            StructField('col_b', StringType(), True),
            StructField('col_c', DoubleType(), True),
            StructField('col_d', LongType(), True),
        ]
    ),
)

df_2 = spark_session.createDataFrame(
    data=[
        [datetime.date(2020, 1, 1), 'demo', 1.123, 10],
        [None, None, None, None],
    ],
    schema=StructType(
        [
            StructField('col_a', DateType(), True),
            StructField('col_b', StringType(), True),
            StructField('col_c', DoubleType(), True),
            StructField('col_d', LongType(), True),
        ]
    ),
)

assert_pyspark_df_equal(df_1, df_2)
```
