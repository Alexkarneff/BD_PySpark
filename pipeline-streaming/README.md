# pipeline-streaming

Containerised streaming pipeline, rebuilt step by step with pinned versions:

```
randomuser.me ──> producer (Python) ──> Kafka ──> Spark Structured Streaming ──> Cassandra
                        ▲
                     Airflow (schedules the producer)
```

| Step | Content | Status |
|---|---|---|
| 1 | Kafka (KRaft) + topic + Kafka UI + producer | ✅ done |
| 2 | Spark reads Kafka, prints to console | ✅ done |
| 3 | Spark writes to Cassandra | ✅ done |
| 4 | Airflow schedules the producer | ✅ this version |

## Pinned versions

| Component | Version | Why |
|---|---|---|
| Kafka | `apache/kafka:4.2.2` | official image, KRaft mode (no ZooKeeper) |
| Kafka UI | `ghcr.io/kafbat/kafka-ui:v1.5.0` | maintained fork of provectus kafka-ui |
| Producer image | `python:3.12-slim` | |
| confluent-kafka | `2.15.1` | Python client; same version as Airflow 3.3.2's constraints |
| requests | `2.34.2` | same version as Airflow 3.3.2's constraints |
| Spark | `spark:3.5.9-scala2.12-java17-python3-ubuntu` | official image; the Cassandra connector stops at Spark 3.5 |
| spark-sql-kafka-0-10 | `_2.12:3.5.9` | must match Spark version and Scala binary exactly |
| kafka-clients / commons-pool2 | `3.4.1` / `2.11.1` | versions from Spark 3.5.9's own `pom.xml` |
| Cassandra | `cassandra:5.0` | 5.0 branch, heap capped at 512 MB |
| spark-cassandra-connector-assembly | `_2.12:3.5.1` | latest connector; built for Spark 3.5, supports Cassandra 5.0 |
| Airflow | `apache/airflow:3.3.2-python3.12` | + confluent-kafka installed with the official constraints |
| Airflow metadata DB | `postgres:16` | same as Airflow's official compose |

## Layout

```
pipeline-streaming/
├── docker-compose.yml
├── airflow/
│   ├── Dockerfile                      # Airflow 3.3.2 + confluent-kafka
│   └── dags/
│       └── random_users_to_kafka.py    # runs the producer every 10 minutes
├── cassandra/
│   └── init/
│       └── schema.cql                  # keyspace + table, applied by cassandra-init
├── producer/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
└── spark/
    ├── Dockerfile                      # Spark 3.5.9 + Kafka & Cassandra connector jars
    └── jobs/                           # mounted read-only into the container
        ├── common.py                   # Kafka settings, JSON schema, parsing
        ├── stream_kafka_console.py     # step 2: Kafka -> console (debug)
        └── stream_kafka_to_cassandra.py# step 3: Kafka -> Cassandra (default job)
```

## Prerequisites

- Docker Desktop (WSL 2 backend on Windows), running
- Python 3.10+ if you run the producer from your host
- Ports 9094, 8888, 4040, 9042 and 8080 free
- At least 8 GB of RAM for Docker with Airflow running (6 GB without it).
  Docker Desktop → Settings → Resources, or `.wslconfig` with the WSL 2 backend

## 1. Start Kafka

```powershell
docker compose up -d
docker compose ps
```

Expected: `kafka` is `healthy`, `kafka-ui` is `running`, `kafka-init` has `exited (0)`.

Check the topic was created:

```powershell
docker compose logs kafka-init
```

You should see `random_names` in the topic list.

Kafka UI: <http://localhost:8888>

## 2. Produce messages

### Option A — from your host (venv)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r producer\requirements.txt

python producer\producer.py --duration 30 --interval 2
```

Each message logs `Delivered to random_names [partition N] offset M`.

If randomuser.me is blocked on your network (common on a company network):

```powershell
python producer\producer.py --source fake --duration 30
```

### Option B — inside Docker

```powershell
docker compose --profile producer run --rm producer --duration 30
```

## 3. Check the messages

Read the first 5 messages from inside the broker:

```powershell
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
  --bootstrap-server localhost:9092 --topic random_names `
  --from-beginning --max-messages 5
```

Or open **Topics → random_names → Messages** in Kafka UI.

Step 1 is done when you see JSON records like:

```json
{"id": "…", "first_name": "Léa", "last_name": "Roux", "gender": "female", "email": "…",
 "username": "…", "phone": "…", "city": "Besançon", "country": "France", "postcode": "25000",
 "latitude": 47.2378, "longitude": 6.0241, "registered_at": "…", "ingested_at": "…"}
```

This JSON schema is the contract for step 2 (Spark parsing) and step 3 (Cassandra table).

## 4. Step 2 — Spark reads the topic

Build the Spark image (first time only, or after editing `spark/Dockerfile`) and start the job:

```powershell
docker compose build spark
docker compose up -d spark
docker compose logs -f spark
```

The first build downloads the Spark image (~1 GB) and the 4 connector jars from Maven Central.

Expected in the logs, every 10 seconds:

```
Streaming random_names from kafka:9092 (startingOffsets=earliest)
-------------------------------------------
Batch: 0
-------------------------------------------
+---------------+------------+----------+---------+------+--------+-------+--------+--------+---------+--------------------------+
|kafka_partition|kafka_offset|first_name|last_name|gender|city    |country|postcode|latitude|longitude|ingested_at               |
+---------------+------------+----------+---------+------+--------+-------+--------+--------+---------+--------------------------+
|0              |0           |Léa       |Roux     |female|Besançon|France |25000   |47.2378 |6.0241   |2026-10-27 12:48:59.906211|
```

`Batch: 0` replays what is already in the topic (`startingOffsets=earliest`).
Then run the producer again in another terminal: new batches appear with the new messages.
Empty batches are not printed, so silence just means no new message.

Spark UI: <http://localhost:4040> → **Structured Streaming** tab (input rate, batch duration).

Edit `spark/jobs/stream_kafka_console.py` on your host, then `docker compose restart spark`: no rebuild needed.

Step 2 is done when new producer messages show up as new batches in `docker compose logs -f spark`.

> Since step 3, the `spark` service runs the Cassandra job by default.
> To run the console job again (debugging):
> PowerShell `$env:SPARK_JOB="stream_kafka_console.py"; docker compose up -d spark`,
> then `Remove-Item Env:SPARK_JOB; docker compose up -d spark` to go back.

## 5. Step 3 — Spark writes to Cassandra

The Spark image changed (Cassandra connector added): rebuild it, then start everything.

```powershell
docker compose build spark
docker compose up -d
docker compose ps
```

Cassandra takes 1 to 2 minutes to start. Expected in `docker compose ps`:

| Service | State |
|---|---|
| `kafka`, `cassandra` | `healthy` |
| `kafka-init`, `cassandra-init` | `exited (0)` |
| `kafka-ui`, `spark` | `running` |

`spark` waits for both init containers, so it starts last.

Check the table was created:

```powershell
docker compose logs cassandra-init
```

Follow the Spark job, then run the producer in another terminal:

```powershell
docker compose logs -f spark
```

Expected:

```
Streaming random_names from kafka:9092 (startingOffsets=earliest) -> Cassandra cassandra/spark_streaming.random_names
Batch 0: 12 rows written to spark_streaming.random_names
Batch 1: 5 rows written to spark_streaming.random_names
```

Query Cassandra:

```powershell
docker compose exec cassandra cqlsh -e "SELECT first_name, last_name, city, country, ingested_at FROM spark_streaming.random_names LIMIT 10;"
docker compose exec cassandra cqlsh -e "SELECT COUNT(*) FROM spark_streaming.random_names;"
```

Step 3 is done when the count grows each time you run the producer.

### Two things worth testing

- **Restart without duplicates**: `docker compose restart spark`. Thanks to the checkpoint (Docker volume `spark-checkpoints`),
  the job resumes from the last processed offset. Even if a batch is replayed, `id` is the primary key, so Cassandra overwrites the row instead of duplicating it.
- **Data survives a restart**: `docker compose down` then `up -d`. Cassandra data is on the `cassandra-data` volume and is still there.
  Kafka data is not persisted (topic recreated empty); `failOnDataLoss=false` lets Spark continue.

## 6. Step 4 — Airflow schedules the producer

The producer moved to `confluent-kafka==2.15.1` (Airflow's version). Update your venv once:

```powershell
pip install -r producer\requirements.txt
```

Build the Airflow image, then start everything:

```powershell
docker compose build airflow-init
docker compose up -d
docker compose ps
```

Expected, after 1 to 2 minutes:

| Service | State |
|---|---|
| `airflow-postgres` | `healthy` |
| `airflow-init` | `exited (0)` |
| `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor` | `healthy` |

Airflow UI: <http://localhost:8080>, login `airflow` / `airflow`.

1. The DAG `random_users_to_kafka` appears **paused** (new DAGs are paused on purpose).
   It can take up to a minute for the dag-processor to pick it up.
2. Unpause it with the toggle, then click **Trigger** to run it now without waiting for the schedule.
3. Open the run → task `stream_random_users` → **Logs**: one `Delivered to random_names …` line every 2 seconds for 60 seconds,
   then `Done: 30 produced, 0 API errors, 0 not delivered`.
   The **XCom** tab shows the same counters.
4. Meanwhile, `docker compose logs -f spark` shows the batches written to Cassandra, and the Cassandra count grows.

Once unpaused, the DAG runs every 10 minutes (`schedule="*/10 * * * *"` in the DAG file).

Step 4 is done when a DAG run is green and the Cassandra count has grown by the number of messages it produced.

If randomuser.me is blocked on your network, start Airflow with the fake source:

```powershell
$env:PRODUCER_SOURCE="fake"; docker compose up -d
```

### How the pieces fit

- `airflow/dags/` is mounted into Airflow: edit the DAG on your host, the dag-processor reloads it within a minute.
- `producer/` is mounted read-only at `/opt/airflow/producer`: the DAG imports `run()` from the **same** `producer.py` you run from your venv.
- With LocalExecutor, tasks run inside the `airflow-scheduler` container, which is on the compose network: that's why the DAG uses `kafka:9092`.
- If Kafka is unreachable, the task fails with an explicit message and Airflow retries once after 1 minute.

## Listeners: the classic trap

Kafka gives clients back the address they must reconnect to. There are two:

| Client runs… | Use | Listener |
|---|---|---|
| on your Windows host | `localhost:9094` | `PLAINTEXT_HOST` |
| in a container on the compose network (Spark, Airflow, producer) | `kafka:9092` | `PLAINTEXT` |

Using `kafka:9092` from the host, or `localhost:9094` from a container, fails.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `kafka` stays `unhealthy` | `docker compose logs kafka`; restart with `docker compose down` then `up -d` |
| `port is already allocated` | another program uses 9094 or 8888: stop it or change the left side of `ports:` |
| Producer: `Messages not delivered` | Kafka not up, or wrong bootstrap address (see table above) |
| Producer: `API call failed` | network blocks randomuser.me: use `--source fake` |
| Spark build fails on `ADD … repo1.maven.org` | network/proxy blocks Maven Central: build from another network, or set Docker Desktop's proxy |
| Spark: `Failed to find data source: kafka` | the connector jars are missing: `docker compose build --no-cache spark` |
| Spark: `ClassNotFoundException` / `NoSuchMethodError` | a jar version doesn't match Spark 3.5.9 / Scala 2.12: don't change one version without the others |
| Spark: `UnknownTopicOrPartitionException` | `kafka-init` hasn't run: `docker compose up -d kafka-init` |
| Spark restarts in a loop | `docker compose logs spark`, look for the first `Exception` / `Error` line |
| `cassandra` stays `starting` / `unhealthy` | wait 2 minutes; if it exits with code 137, Docker lacks RAM (see prerequisites) |
| `cassandra-init` fails | `docker compose logs cassandra-init`; it is safe to rerun: `docker compose up -d cassandra-init` |
| Spark: `Failed to find data source: org.apache.spark.sql.cassandra` | the image was not rebuilt: `docker compose build spark` |
| Spark: `Keyspace spark_streaming does not exist` | `cassandra-init` hasn't run (see above) |
| Spark: `NoNodeAvailableException` / `All host(s) tried` | Cassandra not ready yet: `docker compose restart spark` once `cassandra` is healthy |
| Spark: `AccessDeniedException` on `/opt/spark/checkpoints` | volume created by an older image: `docker volume rm pipeline-streaming_spark-checkpoints` |
| `airflow-init` fails | `docker compose logs airflow-init`; usually Postgres not ready or not enough RAM |
| `pull access denied for pipeline-streaming/airflow` | the image isn't built yet: `docker compose build airflow-init` |
| Airflow UI: DAG not listed | wait 1 minute; then `docker compose logs airflow-dag-processor` for import errors |
| Task log: `ModuleNotFoundError: confluent_kafka` | Airflow image built without it: `docker compose build --no-cache airflow-init` |
| Task log: `message(s) not delivered` | Kafka down, or `KAFKA_BOOTSTRAP` is not `kafka:9092` |
| Task log: `No message produced` | randomuser.me blocked: use `PRODUCER_SOURCE=fake` (see step 4) |
| Port 8080 already in use | another service uses it: change `"8080:8080"` to `"8081:8080"` and open <http://localhost:8081> |
| Start over from scratch (Cassandra data and Spark checkpoints included) | `docker compose down -v` |

Note: Kafka data is not persisted to a volume. `docker compose down` deletes the messages, and `kafka-init` recreates the topic on the next `up`.
Cassandra data, Spark checkpoints and Airflow's database and logs are on volumes: only `down -v` deletes them.

To save RAM, you can run only part of the stack, for example without Airflow:

```powershell
docker compose up -d kafka kafka-init kafka-ui cassandra cassandra-init spark
```

## Stop

```powershell
docker compose down
```
