"""Event contracts and a realistic event generator.

Payloads mirror the Java records in commerce-order-platform (camelCase fields, ISO-8601 UTC
timestamps) so the streaming job can consume either the real services or this simulator.
"""
from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

ORDER_EVENTS_TOPIC = "commerce.order.events.v1"
PAYMENT_EVENTS_TOPIC = "commerce.payment.events.v1"
EVENT_TYPE_HEADER = "event-type"

ORDER_CREATED = "OrderCreated"
PAYMENT_COMPLETED = "PaymentCompleted"
PAYMENT_FAILED = "PaymentFailed"
KNOWN_EVENT_TYPES = (ORDER_CREATED, PAYMENT_COMPLETED, PAYMENT_FAILED)


@dataclass(frozen=True)
class Event:
    topic: str
    key: str
    event_type: str
    payload: dict

    def value_bytes(self) -> bytes:
        return json.dumps(self.payload, separators=(",", ":")).encode("utf-8")

    def headers(self) -> list[tuple[str, bytes]]:
        return [(EVENT_TYPE_HEADER, self.event_type.encode("utf-8"))]


def iso(ts: datetime) -> str:
    """UTC ISO-8601 with microseconds and a Z suffix, as Jackson writes java.time.Instant."""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _money(rng: random.Random, low: float, high: float) -> Decimal:
    return Decimal(str(rng.uniform(low, high))).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def order_flow(
    rng: random.Random,
    now: datetime,
    decline_above: Decimal = Decimal("1000.00"),
    duplicate_rate: float = 0.0,
) -> list[Event]:
    """One order's life: OrderCreated, then PaymentCompleted or PaymentFailed a moment later.

    ``duplicate_rate`` re-emits an event with the same eventId to simulate at-least-once delivery.
    """
    order_id = str(uuid.UUID(int=rng.getrandbits(128)))
    customer_id = f"cust-{rng.randint(1, 5000):05d}"
    amount = _money(rng, 8, 1500)

    created = Event(
        topic=ORDER_EVENTS_TOPIC,
        key=order_id,
        event_type=ORDER_CREATED,
        payload={
            "eventId": str(uuid.UUID(int=rng.getrandbits(128))),
            "orderId": order_id,
            "customerId": customer_id,
            "amount": float(amount),
            "currency": "USD",
            "occurredAt": iso(now),
        },
    )

    paid_at = iso(now + timedelta(milliseconds=rng.randint(150, 2500)))
    if amount <= decline_above:
        outcome = Event(
            topic=PAYMENT_EVENTS_TOPIC,
            key=order_id,
            event_type=PAYMENT_COMPLETED,
            payload={
                "eventId": str(uuid.UUID(int=rng.getrandbits(128))),
                "orderId": order_id,
                "paymentId": str(uuid.UUID(int=rng.getrandbits(128))),
                "amount": float(amount),
                "occurredAt": paid_at,
            },
        )
    else:
        outcome = Event(
            topic=PAYMENT_EVENTS_TOPIC,
            key=order_id,
            event_type=PAYMENT_FAILED,
            payload={
                "eventId": str(uuid.UUID(int=rng.getrandbits(128))),
                "orderId": order_id,
                "reason": f"amount exceeds limit of {decline_above} USD",
                "occurredAt": paid_at,
            },
        )

    events = [created, outcome]
    if duplicate_rate and rng.random() < duplicate_rate:
        events.append(rng.choice(events))
    return events
