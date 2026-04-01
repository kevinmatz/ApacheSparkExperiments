from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

print (f"{spark}")
print (f"Spark version: {spark.version}")
