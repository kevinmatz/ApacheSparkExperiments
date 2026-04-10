# Demo of live inference scoring from Kafka Stream with Spark Structured Streaming
# and scikit-learn logistic regression model
#
# Kevin Matz, 2026-04-09
# Acknowledgement: Significant assistance from OpenAI Codex and ChatGPT, with further modifications

import joblib
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    sum as spark_sum,
    when,
    approx_count_distinct,
    count,
    avg,
    max as spark_max,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# --------------------------------------------------
# 1. Spark session
# --------------------------------------------------

# "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1" -- known bug SPARK-55271 "NullPointerException in Kafka Micro-Batch Streaming Progress Reporting" with 4.1.1, need to downgrade to 4.0.1 for now

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
print("Using Kafka package:", repr(KAFKA_PACKAGE))

spark = (
    SparkSession.builder
    .appName("KafkaSparkSklearnLiveScoring")
    .master("local[*]")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 2. Load trained sklearn model
# --------------------------------------------------
model = joblib.load("purchase_model.joblib")

feature_cols = [
    "views_7d",
    "clicks_7d",
    "carts_7d",
    "purchases_7d",
    "spend_7d",
    "avg_order_value_7d",
    "distinct_products_7d",
    "active_days_7d",
    "days_since_last_event_7d",
    "click_to_view_ratio_7d",
    "cart_to_click_ratio_7d",
    "views_30d",
    "clicks_30d",
    "carts_30d",
    "purchases_30d",
    "spend_30d",
    "avg_order_value_30d",
    "distinct_products_30d",
    "active_days_30d",
    "days_since_last_event_30d",
    "click_to_view_ratio_30d",
    "cart_to_click_ratio_30d"
]

# --------------------------------------------------
# 3. Kafka JSON schema
# --------------------------------------------------
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

# --------------------------------------------------
# 4. Read Kafka stream
# --------------------------------------------------
raw_kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "app-user-events")
    .option("startingOffsets", "latest")
    .load()
)

events_df = (
    raw_kafka_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), event_schema).alias("data"))
    .select("data.*")
    .withColumn("price", when(col("price").isNull(), lit(0.0)).otherwise(col("price")))
)

# --------------------------------------------------
# 5. Add event flags
# --------------------------------------------------
flagged_df = (
    events_df
    .withColumn("is_view", when(col("event_type") == "view", 1).otherwise(0))
    .withColumn("is_click", when(col("event_type") == "click", 1).otherwise(0))
    .withColumn("is_cart", when(col("event_type") == "add_to_cart", 1).otherwise(0))
    .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0))
)

# --------------------------------------------------
# 6. Aggregate per user (micro-batch features)
# --------------------------------------------------
user_batch_features = (
    flagged_df
    .groupBy("user_id")
    .agg(
        spark_sum("is_view").alias("views_batch"),
        spark_sum("is_click").alias("clicks_batch"),
        spark_sum("is_cart").alias("carts_batch"),
        spark_sum("is_purchase").alias("purchases_batch"),
        spark_sum("price").alias("spend_batch"),
        avg(when(col("event_type") == "purchase", col("price"))).alias("avg_order_value_batch"),
        approx_count_distinct("product_id").alias("distinct_products_batch"),
        # countDistinct("timestamp").alias("active_points_batch"),
        count("*").alias("event_count_batch"),
        spark_max("timestamp").alias("last_event_ts"),
    )
)

# --------------------------------------------------
# 7. Scoring function
# --------------------------------------------------
def score_microbatch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # 1. Build flags on raw event rows
    flagged_batch_df = (
        batch_df
        .withColumn("is_view", when(col("event_type") == "view", 1).otherwise(0))
        .withColumn("is_click", when(col("event_type") == "click", 1).otherwise(0))
        .withColumn("is_cart", when(col("event_type") == "add_to_cart", 1).otherwise(0))
        .withColumn("is_purchase", when(col("event_type") == "purchase", 1).otherwise(0))
    )

    # 2. Aggregate to one row per user for this micro-batch
    batch_features_df = (
        flagged_batch_df
        .groupBy("user_id")
        .agg(
            spark_sum("is_view").alias("views_batch"),
            spark_sum("is_click").alias("clicks_batch"),
            spark_sum("is_cart").alias("carts_batch"),
            spark_sum("is_purchase").alias("purchases_batch"),
            spark_sum("price").alias("spend_batch"),
            avg(when(col("event_type") == "purchase", col("price"))).alias("avg_order_value_batch"),
            approx_count_distinct("product_id").alias("distinct_products_batch"),
            count("*").alias("active_points_batch"),
            spark_max("timestamp").alias("last_event_ts"),
        )
    )

    # 3. Convert aggregated batch features to pandas
    pdf = batch_features_df.toPandas()
    if pdf.empty:
        return

    # 4. Fill nulls from aggregation
    pdf["avg_order_value_batch"] = pdf["avg_order_value_batch"].fillna(0.0)

    # 5. Map batch features to training schema
    pdf["views_7d"] = pdf["views_batch"]
    pdf["clicks_7d"] = pdf["clicks_batch"]
    pdf["carts_7d"] = pdf["carts_batch"]
    pdf["purchases_7d"] = pdf["purchases_batch"]
    pdf["spend_7d"] = pdf["spend_batch"]
    pdf["avg_order_value_7d"] = pdf["avg_order_value_batch"]
    pdf["distinct_products_7d"] = pdf["distinct_products_batch"]
    pdf["active_days_7d"] = pdf["active_points_batch"]
    pdf["days_since_last_event_7d"] = 0

    pdf["click_to_view_ratio_7d"] = pdf.apply(
        lambda r: r["clicks_7d"] / r["views_7d"] if r["views_7d"] > 0 else 0.0,
        axis=1
    )
    pdf["cart_to_click_ratio_7d"] = pdf.apply(
        lambda r: r["carts_7d"] / r["clicks_7d"] if r["clicks_7d"] > 0 else 0.0,
        axis=1
    )

    pdf["views_30d"] = pdf["views_batch"]
    pdf["clicks_30d"] = pdf["clicks_batch"]
    pdf["carts_30d"] = pdf["carts_batch"]
    pdf["purchases_30d"] = pdf["purchases_batch"]
    pdf["spend_30d"] = pdf["spend_batch"]
    pdf["avg_order_value_30d"] = pdf["avg_order_value_batch"]
    pdf["distinct_products_30d"] = pdf["distinct_products_batch"]
    pdf["active_days_30d"] = pdf["active_points_batch"]
    pdf["days_since_last_event_30d"] = 0

    pdf["click_to_view_ratio_30d"] = pdf["click_to_view_ratio_7d"]
    pdf["cart_to_click_ratio_30d"] = pdf["cart_to_click_ratio_7d"]

    # 6. Score with sklearn
    X = pdf[feature_cols]

    pdf["prediction"] = model.predict(X)
    pdf["purchase_probability"] = model.predict_proba(X)[:, 1]
    pdf["batch_id"] = batch_id

    result = pdf[
        [
            "user_id",
            "views_batch",
            "clicks_batch",
            "carts_batch",
            "purchases_batch",
            "spend_batch",
            "prediction",
            "purchase_probability",
            "batch_id",
        ]
    ].sort_values(by="purchase_probability", ascending=False)

    print("\n" + "=" * 80)
    print(f"MICRO-BATCH {batch_id} SCORES")
    print("=" * 80)
    print(result.to_string(index=False))

    result.to_csv(f"output/live_scores_batch_{batch_id}.csv", index=False)

# --------------------------------------------------
# 8. Start streaming query
# --------------------------------------------------

# - Spark reads new Kafka events
# - Each micro-batch of raw events is passed to score_microbatch(batch_df, batch_id)
# - Inside that function, it:
#   - Adds flags
#   - Aggregates by user_id
#   - Converts to pandas
#   - Scores with sklearn

query = (
    events_df.writeStream
    .outputMode("append")
    .foreachBatch(score_microbatch)
    .option("checkpointLocation", "output/checkpoints/live_scoring")
    .start()
)

query.awaitTermination()
