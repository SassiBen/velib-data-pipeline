"""Boucle de collecte : GBFS -> Kafka, avec métriques Prometheus."""

import json
import logging
import os
import signal
import time

import httpx
from confluent_kafka import Producer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from .gbfs import INFO_URL, STATUS_URL, ChangeFilter, fetch, info_messages, status_messages

log = logging.getLogger("producer")

MESSAGES = Counter("velib_messages_total", "Messages publiés", ["topic"])
ERRORS = Counter("velib_poll_errors_total", "Collectes en échec", ["feed"])
STATIONS = Gauge("velib_stations_reporting", "Stations présentes dans le dernier relevé")
LAST_OK = Gauge("velib_last_success_timestamp", "Horodatage de la dernière collecte réussie")
POLL = Histogram("velib_poll_seconds", "Durée d'une collecte", buckets=(0.2, 0.5, 1, 2, 5, 10))

STATUS_TOPIC = os.getenv("STATUS_TOPIC", "velib.station_status")
INFO_TOPIC = os.getenv("INFO_TOPIC", "velib.station_information")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))
INFO_EVERY = int(os.getenv("INFO_EVERY", "60"))

running = True


def stop(*_):
    global running
    running = False


def publish(producer: Producer, topic: str, messages: list[dict]) -> None:
    for m in messages:
        producer.produce(topic, key=m["station_id"], value=json.dumps(m).encode())
        producer.poll(0)
    producer.flush(30)
    MESSAGES.labels(topic).inc(len(messages))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start_http_server(8000)
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 50,
    })
    changed = ChangeFilter()
    client = httpx.Client(timeout=20)
    cycle = 0
    while running:
        started = time.monotonic()
        polled_at = int(time.time())
        try:
            with POLL.time():
                if cycle % INFO_EVERY == 0:
                    publish(producer, INFO_TOPIC, info_messages(fetch(INFO_URL, client), polled_at))
                statuses = status_messages(fetch(STATUS_URL, client), polled_at)
                fresh = changed(statuses)
                publish(producer, STATUS_TOPIC, fresh)
            STATIONS.set(len(statuses))
            LAST_OK.set(polled_at)
            log.info("%d stations, %d nouveaux relevés", len(statuses), len(fresh))
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            ERRORS.labels("gbfs").inc()
            log.warning("collecte en échec : %s", exc)
        cycle += 1
        time.sleep(max(0.0, POLL_SECONDS - (time.monotonic() - started)))
    producer.flush(30)


if __name__ == "__main__":
    main()
