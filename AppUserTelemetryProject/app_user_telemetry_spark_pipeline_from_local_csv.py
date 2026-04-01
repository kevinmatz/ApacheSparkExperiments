# App User Telemetry Project: A PySpark feature pipeline for user behavior analysis
#
# This is a small feature-engineering pipeline using Apache Spark and PySpark.
# It transforms raw app event data (currently just reading from a .csv file)
# from a fictional e-commerce site into a user-level feature table that can be used
# for training a downstream machine learning model to predict user behavior (e.g.,
# likelihood of purchase).
#
# The generate_events.py file can be used to generate a sample events.csv file
# with fake data for testing this pipeline.
#
# Note: Warnings on Windows about "winutils.exe", HADOOP_HOME, and hadoop.home.dir
# can be ignored.
#
# Kevin Matz, 2026-03-31
# Acknowledgement: Assistance from OpenAI Codex and ChatGPT


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

# -----
# Hadoop is not working on Windows (winutils.exe won't start)
# Workaround based on https://github.com/globalmentor/hadoop-bare-naked-local-fs
# Create a SparkSession with the necessary configuration to use the BareLocalFileSystem
spark = (
    SparkSession.builder
    .appName("AppUserTelemetryProject")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.file.impl",
        "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
    )
    .getOrCreate()
)

print("Spark:", spark.version)
print("fs.file.impl:", spark.sparkContext._jsc.hadoopConfiguration().get("fs.file.impl"))
# -----


input_file_path = "events.csv"

# Note for future reference: Pattern for creating a Spark DataFrame, modifying it,
# and showing (partial) results on the console:
# df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
# df.withColumn('age2', df.age + 2).show()

# Create a DataFrameReader for the input CSV file
events = spark.read.option("header", True).csv(input_file_path)

# For testing, show the first few rows of the DataFrame
events.show()

# Clean data: Handle missing values, convert data types, and remove duplicates
events = (
    events
        .withColumn("timestamp", to_timestamp(col("timestamp")))  # Normalization: Convert timestamp strings to actual timestamp data type
        .withColumn("price", when(col("price") == "", None).otherwise(col("price").cast("double")))  # Normalization: Convert missing or invalid price values to null and ensure values are numeric
        .withColumn("price", when(col("price").isNull(), lit(0.0)).otherwise(col("price")))  # Replace null prices with 0.0
        .dropna(subset=["event_id", "user_id", "event_type", "timestamp"])      # Drop any rows where any of these critical columns are null/empty
        .dropDuplicates((["event_id"]))
)

# Feature Engineering: Create a binary feature indicating whether each event is a view, click, add-to-cart, or purchase
events = (
    events
        .withColumn("is_view", when(col("event_type") == "view", 1).otherwise(0))  
        .withColumn("is_click", when(col("event_type") == "click", 1).otherwise(0))
        .withColumn("is_cart", when(col("event_type") == "add_to_cart", 1).otherwise(0))
        .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0))
)

# Aggregation: Group by user_id and aggregate features to create a user-level feature set
user_features = (
    events.groupBy("user_id")
    .agg(
        spark_sum("is_view").alias("views"),    # Count the total number of "is_view" events for each user
        spark_sum("is_click").alias("clicks"),
        spark_sum("is_cart").alias("add_to_cart"),
        spark_sum("is_purchase").alias("purchases"),
        spark_sum("price").alias("total_spend"),
        avg(when(col("event_type") == "purchase", col("price"))).alias("avg_order_value"),
        countDistinct("product_id").alias("distinct_products"),
        countDistinct(col("timestamp").cast("date")).alias("active_days"),
        spark_max("timestamp").alias("last_event_timestamp")
    )
)

user_features = user_features.fillna({
    "avg_order_value": 0.0,  # Replace null average order values with 0.0
})

# For testing, show the first few rows of the user-level feature DataFrame
user_features.show()

# Write user feature tables to CSV and Parquet formats; this creates potentially
# multiple files (for multiple data partitions) in the output directory
user_features.write.mode("overwrite").option("header", True).csv("output/user_features_csv")
user_features.write.mode("overwrite").parquet("output/user_features_parquet")

# To output one single .csv file, use this pattern, although it still creates
# a file such as part-00000-e9943fe2-0368-4872-977c-a2df6448e64a-c000.csv in the
# output directory
# user_features \
#    .coalesce(1) \
#    .write \
#    .mode("overwrite") \
#    .option("header", True) \
#    .csv("output/user_features_csv_2")

spark.stop()
