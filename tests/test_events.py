import json
import random
from datetime import datetime, timezone
from decimal import Decimal

from rta.events import (
    ORDER_CREATED,
    ORDER_EVENTS_TOPIC,
    PAYMENT_COMPLETED,
    PAYMENT_EVENTS_TOPIC,
    PAYMENT_FAILED,
    order_flow,
)

NOW = datetime(2026, 10, 15, 18, 0, tzinfo=timezone.utc)


def test_each_order_gets_exactly_one_payment_outcome():
    rng = random.Random(7)
    for _ in range(200):
        events = order_flow(rng, NOW)
        assert [e.event_type for e in events][0] == ORDER_CREATED
        assert events[0].topic == ORDER_EVENTS_TOPIC
        assert events[1].topic == PAYMENT_EVENTS_TOPIC
        assert events[1].event_type in (PAYMENT_COMPLETED, PAYMENT_FAILED)
        assert {e.key for e in events} == {events[0].payload["orderId"]}


def test_orders_above_limit_are_declined():
    rng = random.Random(1)
    for _ in range(300):
        created, outcome = order_flow(rng, NOW, decline_above=Decimal("500.00"))
        expected = PAYMENT_FAILED if Decimal(str(created.payload["amount"])) > Decimal("500.00") else PAYMENT_COMPLETED
        assert outcome.event_type == expected


def test_duplicates_reuse_the_same_event_id():
    rng = random.Random(3)
    flows = [order_flow(rng, NOW, duplicate_rate=1.0) for _ in range(20)]
    for events in flows:
        assert len(events) == 3
        assert events[2].payload["eventId"] in {events[0].payload["eventId"], events[1].payload["eventId"]}


def test_payload_is_json_with_header():
    created = order_flow(random.Random(5), NOW)[0]
    body = json.loads(created.value_bytes())
    assert body["occurredAt"].endswith("Z")
    assert created.headers() == [("event-type", b"OrderCreated")]


def test_generated_events_parse_cleanly(kafka_records):
    from rta.transforms import parse_events

    rng = random.Random(11)
    events = [e for _ in range(50) for e in order_flow(rng, NOW)]
    df = kafka_records([(e.event_type, e.value_bytes().decode()) for e in events])

    assert parse_events(df).count() == len(events)
