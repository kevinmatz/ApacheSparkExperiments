# Test program to subscribe to the Kafka topic "app-user-events" and print the raw events to the console

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split

spark = (
    SparkSession.builder
    .appName("AppUserTelemetryProject")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
    )
    .getOrCreate()
)

raw_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "app-user-events")
    .option("startingOffsets", "earliest")
    .load()
)

lines = raw_kafka.selectExpr(
    "CAST(key AS STRING) AS kafka_key",
    "CAST(value AS STRING) AS csv_line",
    "timestamp AS kafka_timestamp"
)

parts = split(col("csv_line"), ",")

events = lines.select(
    parts.getItem(0).alias("event_id"),
    parts.getItem(1).alias("user_id"),
    parts.getItem(2).alias("event_type"),
    parts.getItem(3).alias("product_id"),
    parts.getItem(4).alias("timestamp"),
    parts.getItem(5).alias("price"),
    parts.getItem(6).alias("device_type"),
    parts.getItem(7).alias("country"),
)

query = (
    events.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()