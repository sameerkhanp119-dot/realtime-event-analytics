import json
from datetime import datetime
from decimal import Decimal

from rta.transforms import deduplicate, minute_kpis, parse_events


def order(event_id, order_id, amount, at):
    return ("OrderCreated", json.dumps({
        "eventId": event_id, "orderId": order_id, "customerId": "cust-1",
        "amount": amount, "currency": "USD", "occurredAt": at,
    }))


def paid(event_id, order_id, amount, at):
    return ("PaymentCompleted", json.dumps({
        "eventId": event_id, "orderId": order_id, "paymentId": "p-" + order_id, "amount": amount, "occurredAt": at,
    }))


def failed(event_id, order_id, at):
    return ("PaymentFailed", json.dumps({
        "eventId": event_id, "orderId": order_id, "reason": "declined", "occurredAt": at,
    }))


def test_parses_typed_events(kafka_records):
    rows = parse_events(kafka_records([order("e1", "o1", 49.99, "2026-10-15T18:00:05.123Z")])).collect()

    assert len(rows) == 1
    row = rows[0]
    assert (row.event_id, row.event_type, row.order_id, row.currency) == ("e1", "OrderCreated", "o1", "USD")
    assert row.amount == Decimal("49.99")
    assert row.occurred_at == datetime(2026, 10, 15, 18, 0, 5, 123000)


def test_accepts_java_nanosecond_instants(kafka_records):
    rows = parse_events(kafka_records([order("e1", "o1", 10, "2026-10-15T18:00:05.123456789Z")])).collect()

    assert rows[0].occurred_at == datetime(2026, 10, 15, 18, 0, 5, 123456)


def test_drops_unknown_malformed_and_unlabelled_records(kafka_records):
    records = [
        ("InventoryReserved", json.dumps({"eventId": "x", "orderId": "o", "occurredAt": "2026-10-15T18:00:00Z"})),
        ("OrderCreated", "{not json"),
        (None, json.dumps({"eventId": "y", "orderId": "o", "occurredAt": "2026-10-15T18:00:00Z"})),
        ("OrderCreated", json.dumps({"orderId": "o", "occurredAt": "2026-10-15T18:00:00Z"})),
        order("ok", "o1", 5, "2026-10-15T18:00:00Z"),
    ]

    assert [r.event_id for r in parse_events(kafka_records(records)).collect()] == ["ok"]


def test_deduplicates_redelivered_events(kafka_records):
    records = [order("e1", "o1", 10, "2026-10-15T18:00:00Z")] * 3 + [order("e2", "o2", 20, "2026-10-15T18:00:01Z")]

    ids = sorted(r.event_id for r in deduplicate(parse_events(kafka_records(records))).collect())

    assert ids == ["e1", "e2"]


def test_minute_kpis(kafka_records):
    records = [
        order("e1", "o1", 100.00, "2026-10-15T18:00:10Z"),
        order("e2", "o2", 1500.00, "2026-10-15T18:00:20Z"),
        paid("e3", "o1", 100.00, "2026-10-15T18:00:11Z"),
        failed("e4", "o2", "2026-10-15T18:00:21Z"),
        order("e5", "o3", 25.50, "2026-10-15T18:01:05Z"),
    ]

    kpis = {r.window_start.minute: r for r in minute_kpis(parse_events(kafka_records(records))).collect()}

    first = kpis[0]
    assert first.orders_created == 2
    assert first.gross_order_value == Decimal("1600.00")
    assert first.payments_completed == 1
    assert first.payments_failed == 1
    assert first.revenue == Decimal("100.00")
    assert first.payment_success_rate == 0.5

    second = kpis[1]
    assert second.orders_created == 1
    assert second.revenue == Decimal("0.00")
    assert second.payment_success_rate is None
