from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit, count, sum as spark_sum, avg, countDistinct,
    max as spark_max, datediff
)

input_file_path = "events.csv"

spark = SparkSession.builder.appName("MyUserFeaturePipeline").getOrCreate()

# Create a DataFrameReader for the input CSV file
events = spark.read.option("header", True).csv(input_file_path)

# events.show()


# events = events
#    .withColumn("timestamp", to_timestamp(col("timestamp")))
#    .withColumn("price", when(col("price") == "", None).otherwise(col("price").cast("double")))
#    .dropna(subset=["event_id", "user_id", "event_type", "timestamp"])      # Drop any rows where any of these critical columns are null/empty




