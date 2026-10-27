"""
Step 3 — Read the `random_names` Kafka topic and write each micro-batch into Cassandra.

Launched by docker compose (service `spark`, default job):
    spark-submit --master local[2] /opt/spark/jobs/stream_kafka_to_cassandra.py

Why foreachBatch instead of writeStream.format("cassandra") directly?
Inside foreachBatch each micro-batch is a normal DataFrame: we use the plain batch
writer of the connector and can log how many rows each batch wrote.
"""
import os

from pyspark.sql import DataFrame, SparkSession

from common import KAFKA_BOOTSTRAP, KAFKA_TOPIC, STARTING_OFFSETS, parse_users, read_kafka

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "spark_streaming")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE", "random_names")
# On a Docker volume: after a restart, the job resumes where it stopped instead of replaying everything.
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/spark/checkpoints/kafka_to_cassandra")

# Columns written to Cassandra: must exist in the table (cassandra/init/schema.cql)
CASSANDRA_COLUMNS = [
    "id", "first_name", "last_name", "gender", "email", "username", "phone",
    "city", "country", "postcode", "latitude", "longitude",
    "registered_at", "ingested_at", "kafka_partition", "kafka_offset",
]


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Called by Spark for every micro-batch."""
    batch_df = batch_df.select(*CASSANDRA_COLUMNS).persist()   # used twice: count + write
    try:
        rows = batch_df.count()
        if rows == 0:
            return
        (
            batch_df.write
            .format("org.apache.spark.sql.cassandra")
            .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE)
            .mode("append")          # Cassandra append = upsert on the primary key
            .save()
        )
        print(f"Batch {batch_id}: {rows} rows written to {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}", flush=True)
    finally:
        batch_df.unpersist()


def main():
    spark = (
        SparkSession.builder
        .appName("kafka-to-cassandra")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    users = parse_users(read_kafka(spark))

    query = (
        users.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="10 seconds")
        .start()
    )
    print(f"Streaming {KAFKA_TOPIC} from {KAFKA_BOOTSTRAP} (startingOffsets={STARTING_OFFSETS}) "
          f"-> Cassandra {CASSANDRA_HOST}/{CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
