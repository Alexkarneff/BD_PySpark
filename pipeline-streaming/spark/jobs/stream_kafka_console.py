"""
Step 2 — Read the `random_names` Kafka topic and print the parsed records to the console.

Kept for debugging: run it instead of the Cassandra job with
    PowerShell:  $env:SPARK_JOB="stream_kafka_console.py"; docker compose up -d spark
    bash:        SPARK_JOB=stream_kafka_console.py docker compose up -d spark
"""
import os

from pyspark.sql import SparkSession

from common import KAFKA_BOOTSTRAP, KAFKA_TOPIC, STARTING_OFFSETS, parse_users, read_kafka

# Not persisted on purpose: each run replays the topic from the beginning
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/console")


def main():
    spark = (
        SparkSession.builder
        .appName("kafka-to-console")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    users = parse_users(read_kafka(spark)).select(
        "kafka_partition", "kafka_offset", "first_name", "last_name", "gender",
        "city", "country", "postcode", "latitude", "longitude", "ingested_at",
    )

    query = (
        users.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", 50)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"Streaming {KAFKA_TOPIC} from {KAFKA_BOOTSTRAP} (startingOffsets={STARTING_OFFSETS}) -> console", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
