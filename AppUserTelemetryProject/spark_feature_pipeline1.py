import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit, count, sum as spark_sum, avg, countDistinct,
    max as spark_max, datediff
)

# These lines ensure that PySpark uses the same Python interpreter as the one
# running this script; without these lines, various spurious error messages are
# generated and nothing works.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


input_file_path = "events.csv"


spark = SparkSession.builder.appName("MyUserFeaturePipeline").getOrCreate()

# df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
# df.withColumn('age2', df.age + 2).show()


# Create a DataFrameReader for the input CSV file
events = spark.read.option("header", True).csv(input_file_path)

events.show()



# events = events
#    .withColumn("timestamp", to_timestamp(col("timestamp")))
#    .withColumn("price", when(col("price") == "", None).otherwise(col("price").cast("double")))
#    .dropna(subset=["event_id", "user_id", "event_type", "timestamp"])      # Drop any rows where any of these critical columns are null/empty




spark.stop()
