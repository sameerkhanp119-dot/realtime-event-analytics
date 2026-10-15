"""Publishes simulated order and payment events to Kafka at a steady rate.

    KAFKA_BOOTSTRAP=localhost:9092 ORDERS_PER_SECOND=5 python -m rta.producer
"""
from __future__ import annotations

import logging
import os
import random
import signal
import time
from datetime import datetime, timezone
from decimal import Decimal

from rta.events import order_flow

log = logging.getLogger("rta.producer")


def main() -> None:
    from confluent_kafka import Producer  # imported here so tests don't need the client

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    producer = Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
    })
    rate = float(os.environ.get("ORDERS_PER_SECOND", "5"))
    duplicate_rate = float(os.environ.get("DUPLICATE_RATE", "0.02"))
    decline_above = Decimal(os.environ.get("DECLINE_ABOVE", "1000.00"))
    rng = random.Random(os.environ.get("SEED"))

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    sent = 0
    while running:
        for event in order_flow(rng, datetime.now(timezone.utc), decline_above, duplicate_rate):
            producer.produce(event.topic, key=event.key, value=event.value_bytes(), headers=event.headers())
            sent += 1
        producer.poll(0)
        if sent % 500 < 3:
            log.info("published %d events", sent)
        time.sleep(1 / rate)

    producer.flush(10)
    log.info("stopped after %d events", sent)


if __name__ == "__main__":
    main()
