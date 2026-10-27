"""
Step 4 — Airflow schedules the producer.

Every 10 minutes, the task runs the producer for 60 seconds: it fetches random
users and publishes them to Kafka. Spark (step 3) picks them up and writes them
to Cassandra on its own: Airflow only drives the start of the pipeline.

The producer code is not copied here: /opt/airflow/producer is the same
producer/ folder you run from your venv (mounted read-only by docker compose).
"""
import os
import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task

PRODUCER_DIR = "/opt/airflow/producer"


@dag(
    dag_id="random_users_to_kafka",
    description="Fetch random users and publish them to the random_names Kafka topic",
    schedule="*/10 * * * *",            # every 10 minutes (the original tutorial ran daily at 01:00)
    start_date=datetime(2026, 10, 1),
    catchup=False,                      # don't backfill the runs missed since start_date
    max_active_runs=1,                  # never two producers at the same time
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["pipeline-streaming", "kafka"],
)
def random_users_to_kafka():

    @task(execution_timeout=timedelta(minutes=5))
    def stream_random_users() -> dict:
        # Imported inside the task: the DAG file stays fast to parse,
        # and a producer bug fails the task instead of breaking the whole DAG.
        if PRODUCER_DIR not in sys.path:
            sys.path.insert(0, PRODUCER_DIR)
        from producer import run

        # The returned counters are stored as XCom (visible in the UI)
        return run(
            bootstrap=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
            topic=os.getenv("KAFKA_TOPIC", "random_names"),
            duration=float(os.getenv("PRODUCER_DURATION_S", 60)),
            interval=float(os.getenv("PRODUCER_INTERVAL_S", 2)),
            source=os.getenv("PRODUCER_SOURCE", "api"),
        )

    stream_random_users()


random_users_to_kafka()
