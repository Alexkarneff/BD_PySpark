-- Step 3 — Cassandra schema, applied by the one-shot `cassandra-init` container.
-- Idempotent: safe to run on every `docker compose up`.

CREATE KEYSPACE IF NOT EXISTS spark_streaming
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- One row per user. `id` is the primary key, so Cassandra upserts:
-- if Spark replays a batch after a restart, rows are overwritten, not duplicated.
CREATE TABLE IF NOT EXISTS spark_streaming.random_names (
    id               uuid PRIMARY KEY,
    first_name       text,
    last_name        text,
    gender           text,
    email            text,
    username         text,
    phone            text,
    city             text,
    country          text,
    postcode         text,
    latitude         double,
    longitude        double,
    registered_at    timestamp,
    ingested_at      timestamp,
    kafka_partition  int,
    kafka_offset     bigint
);
