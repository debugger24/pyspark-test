# pyspark_diff

Returns a list of differences between two PySpark dataframes. Example:

`/tmp/data1.json`
``` json
{"id": 1, "values_list": ["a", "b"], "values_dict": {"a": "b"}}
{"id": 2, "values_list": ["a", "b"], "values_dict": {"a": "b"}}
{"id": 3, "values_list": ["a", "b"], "values_dict": {"a": "b"}}
```

`/tmp/data2.json`
``` json
{"id": 1, "values_list": ["a", "b"], "values_dict": {"a": "b"}}
{"id": 2, "values_list": ["a", "bdiff"], "values_dict": {"a": "b"}}
{"id": 3, "values_list": ["a", "b"], "values_dict": {"a": "bdiff"}}
```

``` python
from pyspark.sql import SparkSession
from pyspark_diff import diff

spark = SparkSession.builder.appName(__name__).getOrCreate()

df1 = spark.read.json("/tmp/data1.json")
df2 = spark.read.json("/tmp/data2.json")

differences = diff(
    left_df=df1,
    right_df=df2,
    id_field="id",
    order_by=["id"],
)
```

And `differences` look like this:
``` python
[
    Difference(
        row_id=2,
        column_name="[1]",
        column_name_parent="values_list",
        left="b",
        right="bdiff",
        reason="diff_value",
    ),
    Difference(
        row_id=3,
        column_name="a",
        column_name_parent="values_dict",
        left="b",
        right="bdiff",
        reason="diff_value",
    ),
]
```

# Documentation

For parameters documentation for now check directly the method as it's still changing because it's in dev mode and the readme is not always updated:

https://github.com/oalfonso-o/pyspark_diff/blob/main/pyspark_diff/pyspark_diff.py#L264


-----

Note:

Initially forked from https://github.com/debugger24/pyspark-test as this repo was intended to add minor features and open a pull request to the original repo but now the idea of this project is not testing pyspark and more extracting a diff from the pyspark. So the purpose changed from testing to debugging.
