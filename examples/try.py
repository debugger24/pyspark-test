from pyspark.sql import SparkSession

from pyspark_diff import diff

spark = (
    SparkSession.builder.config("spark.driver.memory", "8g")
    .appName(__name__)
    .getOrCreate()
)

left_df = spark.read.json("/tmp/head10_1.json")
right_df = spark.read.json("/tmp/head10_2.json")

from time import time

t1 = time()
diff_df = diff(left_df, right_df, id_field="id", spark_process=True)
t2 = time() - t1
import pudb

pu.db

diff_df.show()
