from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, expr

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

schema = StructType([
    StructField("transaction_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", IntegerType())
])

parsed_df = df.selectExpr("CAST(value AS STRING)") \
              .select(from_json(col("value"), schema).alias("data")) \
              .select("data.*")

# Added for debugging
query_debug = parsed_df.writeStream \
    .format("console") \
    .start()

# Simple fraud rule: amount > 900
flagged_df = parsed_df.withColumn("is_fraud", expr("amount > 900"))

# Write flagged transactions to Cassandra
query = flagged_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "fraud") \
    .option("table", "flagged_transactions") \
    .option("checkpointLocation", "/tmp/checkpoints") \
    .start()

query.awaitTermination()