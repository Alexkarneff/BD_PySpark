"""
Fetch random users from randomuser.me and publish them to a Kafka topic.

Run from your host (venv):      python producer/producer.py
Run inside Docker:              docker compose --profile producer run --rm producer
Without internet / API blocked: python producer/producer.py --source fake

Settings come from CLI arguments, falling back to environment variables:
    KAFKA_BOOTSTRAP  (default localhost:9094 = host listener)
    KAFKA_TOPIC      (default random_names)
"""
import argparse
import json
import logging
import os
import random
import signal
import threading
import time
import uuid
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

API_URL = "https://randomuser.me/api/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


# ---------------------------------------------------------------- data sources

def format_user(raw: dict) -> dict:
    """Flatten one randomuser.me result into the record we send to Kafka.

    This schema is the contract for the next steps: Spark will parse it
    and the Cassandra table will mirror it.
    """
    loc = raw["location"]
    return {
        "id": raw["login"]["uuid"],
        "first_name": raw["name"]["first"],
        "last_name": raw["name"]["last"],
        "gender": raw["gender"],
        "email": raw["email"],
        "username": raw["login"]["username"],
        "phone": raw["phone"],
        "city": loc["city"],
        "country": loc["country"],
        "postcode": str(loc["postcode"]),          # int or str depending on the country
        "latitude": float(loc["coordinates"]["latitude"]),
        "longitude": float(loc["coordinates"]["longitude"]),
        "registered_at": raw["registered"]["date"],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_api_user(session: requests.Session) -> dict:
    resp = session.get(API_URL, timeout=10)
    resp.raise_for_status()
    return format_user(resp.json()["results"][0])


FIRST = ["Alice", "Bruno", "Chloé", "David", "Emma", "Farid", "Gaëlle", "Hugo"]
LAST = ["Martin", "Bernard", "Dubois", "Moreau", "Laurent", "Garcia", "Roux"]
CITIES = [("Besançon", "France"), ("Lyon", "France"), ("Genève", "Switzerland"), ("Bruxelles", "Belgium")]


def fake_user() -> dict:
    """Offline fallback with the same schema (useful if randomuser.me is blocked)."""
    first, last = random.choice(FIRST), random.choice(LAST)
    city, country = random.choice(CITIES)
    username = f"{first.lower()}{random.randint(10, 999)}"
    return {
        "id": str(uuid.uuid4()),
        "first_name": first,
        "last_name": last,
        "gender": random.choice(["female", "male"]),
        "email": f"{username}@example.com",
        "username": username,
        "phone": f"0{random.randint(100000000, 999999999)}",
        "city": city,
        "country": country,
        "postcode": str(random.randint(10000, 99999)),
        "latitude": round(random.uniform(-90, 90), 4),
        "longitude": round(random.uniform(-180, 180), 4),
        "registered_at": datetime(random.randint(2005, 2025), 1, 1, tzinfo=timezone.utc).isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------- kafka

def on_delivery(err, msg):
    if err is not None:
        log.error("Delivery failed: %s", err)
    else:
        log.info("Delivered to %s [partition %d] offset %d", msg.topic(), msg.partition(), msg.offset())


def parse_args():
    p = argparse.ArgumentParser(description="Publish random users to Kafka")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9094"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "random_names"))
    p.add_argument("--duration", type=float, default=float(os.getenv("DURATION_S", 60)),
                   help="seconds to run (0 = until Ctrl+C)")
    p.add_argument("--interval", type=float, default=float(os.getenv("INTERVAL_S", 2)),
                   help="seconds between two messages")
    p.add_argument("--source", choices=["api", "fake"], default=os.getenv("SOURCE", "api"))
    return p.parse_args()


def run(bootstrap: str, topic: str, duration: float, interval: float, source: str = "api",
        stop_event: threading.Event | None = None) -> dict:
    """Produce messages for `duration` seconds and return the counters.

    Reusable from Airflow (step 4): no signal handling here, and it raises if
    nothing reached Kafka so the Airflow task is marked as failed.
    """
    log.info("Bootstrap=%s topic=%s source=%s duration=%ss interval=%ss",
             bootstrap, topic, source, duration, interval)
    stop_event = stop_event or threading.Event()

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "random-user-producer",
        "acks": "all",
    })
    session = requests.Session()

    sent, errors = 0, 0
    end = time.time() + duration if duration > 0 else float("inf")
    while not stop_event.is_set() and time.time() < end:
        try:
            user = fetch_api_user(session) if source == "api" else fake_user()
            producer.produce(
                topic,
                key=user["id"],
                value=json.dumps(user, ensure_ascii=False).encode("utf-8"),
                on_delivery=on_delivery,
            )
            sent += 1
        except requests.RequestException as e:
            errors += 1
            log.warning("API call failed (%s). Try --source fake if randomuser.me is blocked.", e)
        except BufferError:
            log.warning("Local queue full, waiting for deliveries...")
            producer.poll(1)
        producer.poll(0)            # triggers delivery callbacks
        stop_event.wait(interval)   # like time.sleep, but stops early on Ctrl+C

    remaining = producer.flush(15)
    stats = {"produced": sent, "api_errors": errors, "not_delivered": remaining}
    log.info("Done: %(produced)d produced, %(api_errors)d API errors, %(not_delivered)d not delivered", stats)

    if remaining:
        raise RuntimeError(
            f"{remaining} message(s) not delivered: is Kafka up, and is {bootstrap} the right address? "
            "From your host use localhost:9094, from a container use kafka:9092.")
    if sent == 0:
        raise RuntimeError("No message produced (every API call failed?). Try source='fake'.")
    return stats


def main():
    args = parse_args()

    # Ctrl+C / docker stop: finish the current message, flush, exit cleanly
    stop_event = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    try:
        run(args.bootstrap, args.topic, args.duration, args.interval, args.source, stop_event)
    except RuntimeError as e:
        log.error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
