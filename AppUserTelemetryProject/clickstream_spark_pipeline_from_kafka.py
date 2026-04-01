# Clickstream (App User Telemetry) Project: A demo PySpark feature pipeline for
# user behavior analysis
#
# This is a small feature-engineering pipeline using Apache Spark and PySpark.
#
# It receives a stream of raw app event data from a Kafka topic, transforms it
# into a user-level feature table, and writes the results to an output directory
# in both CSV and Parquet formats.
#
# The feature table can be used for training a downstream machine learning model
# to predict user behavior (e.g., likelihood of purchase).
#
# See readme.md for instructions on how to generate test data and run the
# producer to provide data to Kafka, which then makes the events available on 
# the "app-user-events" topic for consumption by this Spark pipeline.
#
# This version is set up to run under WSL2 with Java JDK 21, Docker Desktop
# configured to use WSL2, and a venv with PySpark / Spark 4.1.1.
#
# It will not run on Windows without WSL2 due to issues with Hadoop and
# winutils.exe.
#
# Kevin Matz, 2026-03-31
# Acknowledgement: Assistance from OpenAI Codex and ChatGPT


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, when, lit,
    sum as spark_sum, avg, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

spark = (
    SparkSession.builder
    .appName("ClickstreamKafkaFeaturePipelinePOC")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("device_type", StringType(), True),
    StructField("country", StringType(), True),
])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "app-user-events")
    .option("startingOffsets", "earliest")
    .load()
)

events = (
    raw
    .selectExpr("CAST(value AS STRING) AS json_string")
    .select(from_json(col("json_string"), event_schema).alias("event"))
    .select("event.*")
)

clean_events = (
    events
    .filter(col("event_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .filter(col("event_type").isNotNull())
    .filter(col("timestamp").isNotNull())
    .withColumn("timestamp", to_timestamp(col("timestamp")))
    .withColumn("price", when(col("price").isNull(), lit(0.0)).otherwise(col("price")))
    .withColumn("is_view", when(col("event_type") == "view", 1).otherwise(0))
    .withColumn("is_click", when(col("event_type") == "click", 1).otherwise(0))
    .withColumn("is_cart", when(col("event_type") == "add_to_cart", 1).otherwise(0))
    .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0))
)

def process_batch(batch_df, batch_id: int) -> None:
    print(f"Processing batch_id={batch_id}")

    if batch_df.rdd.isEmpty():
        print("Empty batch; skipping")
        return

    user_features = (
        batch_df.groupBy("user_id")
        .agg(
            spark_sum("is_view").alias("views"),
            spark_sum("is_click").alias("clicks"),
            spark_sum("is_cart").alias("add_to_cart"),
            spark_sum("is_purchase").alias("purchases"),
            spark_sum("price").alias("total_spend"),
            avg(when(col("event_type") == "purchase", col("price"))).alias("avg_order_value"),
            spark_max("timestamp").alias("last_event_timestamp"),
        )
        .fillna({"avg_order_value": 0.0})
    )

    user_features.show(truncate=False)

    # Write one folder per batch so runs are easy to inspect
    (
        user_features.write
        .mode("overwrite")
        .parquet(f"output/user_features_parquet/batch_id={batch_id}")
    )

    (
        user_features.write
        .mode("overwrite")
        .option("header", True)
        .csv(f"output/user_features_csv/batch_id={batch_id}")
    )

query = (
    clean_events.writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime="10 seconds")       # Run the batch processing every 10 seconds
    .option("checkpointLocation", "checkpoints/kafka_feature_pipeline")
    .start()
)

query.awaitTermination()

