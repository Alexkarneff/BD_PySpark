"""
Code shared by the Spark jobs: Kafka settings, message schema, parsing.

The jobs import it with a plain `import common`: spark-submit puts the script's
folder on the Python path. Parsing runs in the JVM (no Python UDF), so the
executors don't need this module.
"""
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "random_names")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")   # earliest = replay what's already in the topic

# Same contract as producer/producer.py -> format_user()
USER_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("gender", StringType()),
    StructField("email", StringType()),
    StructField("username", StringType()),
    StructField("phone", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("postcode", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("registered_at", StringType()),
    StructField("ingested_at", StringType()),
])


def read_kafka(spark: SparkSession) -> DataFrame:
    """Streaming DataFrame of raw Kafka records (binary key/value + metadata)."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")     # Kafka data is not persisted in dev: offsets may vanish
        .load()
    )


def parse_users(kafka_df: DataFrame) -> DataFrame:
    """Kafka rows -> one typed column per JSON field."""
    return (
        kafka_df
        .select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(F.col("value").cast("string"), USER_SCHEMA).alias("user"),
        )
        .select("kafka_partition", "kafka_offset", "kafka_timestamp", "user.*")
        .withColumn("registered_at", F.to_timestamp("registered_at"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        # from_json returns nulls for malformed messages: drop them instead of crashing
        .filter(F.col("id").isNotNull())
    )
