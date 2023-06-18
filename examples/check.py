from pyspark.sql import SparkSession
from gresearch.spark.diff import DiffOptions, diff_with_options
from pyspark.sql.functions import max, size, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    IntegerType,
)

spark = SparkSession.builder.appName(__name__).getOrCreate()

schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("list", ArrayType(StringType(), True), True),
        StructField(
            "cpg1",
            StructType(
                [
                    StructField("cpg2", StringType(), True),
                    StructField(
                        "cpg3",
                        StructType(
                            [
                                StructField(
                                    "cpg4",
                                    ArrayType(
                                        StructType(
                                            [StructField("cpg5", IntegerType(), True)]
                                        )
                                    ),
                                )
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)
data1 = [
    {
        "id": "1",
        "list": ["list1", "list2"],
        "cpg1": {"cpg2": "2_value", "cpg3": {"cpg4": [{"cpg5": 1}]}},
    }
]
data2 = [
    {
        "id": "1",
        "list": ["list1", "list2"],
        "cpg1": {"cpg2": "2_value", "cpg3": {"cpg4": [{"cpg5": 2}]}},
    }
]

left_df = spark.createDataFrame(data1, schema=schema)
right_df = spark.createDataFrame(data2, schema=schema)


NESTED_FIELDS_SEP = "__"


def flat_df(df):
    flattened = False
    fields = df.schema.fields
    for field in fields:
        if field.dataType.typeName() == StructType.typeName():
            new_cols_df = df.select("id", col(f"{field.name}.*"))
            cols_and_aliases = [
                col(c).alias(f"{field.name}{NESTED_FIELDS_SEP}{c}")
                for c in new_cols_df.columns
                if c != "id"
            ]
            new_cols_df = new_cols_df.select("id", *cols_and_aliases)
            df = df.join(new_cols_df, on="id").drop(field.name)
            flattened = True
        elif field.dataType.typeName() == ArrayType.typeName():
            mx_len = df.select(max(size(field.name)).alias("max")).collect()[0].max
            new_cols_df = df.select("id", *[col(field.name)[i] for i in range(mx_len)])
            df = df.join(new_cols_df, on="id").drop(field.name)
            flattened = True
    if flattened:
        df = flat_df(df)
    return df


left_fdf = flat_df(left_df)
right_fdf = flat_df(right_df)

options = DiffOptions().with_change_column("changes")
diff_with_options(left_fdf, right_fdf, options, "id").filter(
    col("diff") != DiffOptions.nochange_diff_value
).show()
