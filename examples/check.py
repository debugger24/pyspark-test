from pyspark.sql import SparkSession
from pyspark_diff import diff
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

df1 = spark.createDataFrame(data1, schema=schema)
df2 = spark.createDataFrame(data2, schema=schema)

differences = diff(
    left_df=df1,
    right_df=df2,
    id_field="id",
    order_by=["id"],
    spark_process=True,
)
