import csv

from pyspark.sql import SparkSession
from pyspark_diff import diff_objs


spark = (
    SparkSession.builder.config("spark.driver.memory", "8g")
    .appName(__name__)
    .getOrCreate()
)

df1 = spark.read.json(
    "/home/oalfonso/Documents/digital/cpg/icf__deezer_it__OLD__id_369682.json"
)
df2 = spark.read.json(
    "/home/oalfonso/Documents/digital/cpg/icf__deezer_it__NEW__id_369682.json"
)

differences = diff_objs(
    left_df=df1,
    right_df=df2,
    id_field="resource_id",
    order_by=["resource_id"],
    # skip_n_first_rows=1,
    skip_n_first_rows=0,
    # sorting_keys={"repertoires": lambda r: r["name"]},
)
with open("differences_icf.csv", "w") as fd:
    writer = csv.DictWriter(
        fd, ["row_id", "column_name", "column_name_parent", "left", "right", "reason"]
    )
    writer.writeheader()
    writer.writerows([diff.dict() for diff in differences])
