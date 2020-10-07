# pyspark-df-assert

Check that left and right spark DataFrame are equal.

This function is intended to compare two spark DataFrames and output any differences. Is is mostly intended for use in unit tests. Additional parameters allow varying the strictness of the equality checks performed.

# Example

```py
from assert_pysaprk_df_equal import assert_pysaprk_df_equal

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

assert_pysaprk_df_equal(df_1, df_2)
```