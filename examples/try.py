import csv
import json
from time import time

from pyspark.sql import SparkSession

from pyspark_diff import diff_df

spark = (
    SparkSession.builder.config("spark.driver.memory", "8g")
    .appName(__name__)
    .getOrCreate()
)

left_df = spark.read.json("/tmp/head10_1.json")
right_df = spark.read.json("/tmp/head10_2.json")

t1 = time()
df = diff_df(left_df, right_df, id_field="id", spark_process=True)
t2 = time() - t1

changes = []
for row in df.collect():
    for change in row["changes"]:
        changes.append(
            {
                "id": row["id"],
                "diff": row["diff"],
                "key": change,
                "left_value": row[f"left_{change}"],
                "right_value": row[f"right_{change}"],
                "left": json.dumps(
                    left_df.select("*")
                    .where(left_df.id == row["id"])
                    .collect()[0]
                    .asDict(recursive=True)
                ),
                "right": json.dumps(
                    right_df.select("*")
                    .where(right_df.id == row["id"])
                    .collect()[0]
                    .asDict(recursive=True)
                ),
            }
        )

if changes:
    with open("differences.csv", "w") as fd:
        writer = csv.DictWriter(fd, fieldnames=changes[0].keys())
        writer.writeheader()
        writer.writerows(changes)
