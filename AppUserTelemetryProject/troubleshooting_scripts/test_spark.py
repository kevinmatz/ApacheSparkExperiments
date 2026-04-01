import os
import sys
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.appName("test").getOrCreate()

df = spark.createDataFrame([(1, "hello"), (2, "world")], ["id", "msg"])
df.show()

spark.stop()
