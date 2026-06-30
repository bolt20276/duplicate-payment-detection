"""
producer/payment_simulator.py
─────────────────────────────────────────────────────────────────────────────
Payment Event Simulator
Generates realistic payment events and publishes them to Kafka.
Injects a controlled percentage of duplicate events to simulate real-world
network timeout retries.

Usage:
    python producer/payment_simulator.py
─────────────────────────────────────────────────────────────────────────────
"""

import json
import random
import time
import hashlib
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional

from faker import Faker
from kafka import KafkaProducer
from loguru import logger

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import settings

# ─────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────
PAYMENT_METHODS = ["card", "bank_transfer", "wallet", "ussd"]
CURRENCIES      = ["NGN", "USD", "GBP", "EUR", "KES", "GHS"]
STATUSES        = ["initiated", "pending"]

# Realistic merchant pool (fintech-style)
MERCHANTS = [
    "merchant_flutterwave", "merchant_paystack", "merchant_moniepoint",
    "merchant_opay",        "merchant_kuda",      "merchant_palmpay",
    "merchant_gtbank",      "merchant_zenith",    "merchant_access",
    "merchant_firstbank",
]

fake = Faker()


# ─────────────────────────────────────────────────────────────
# Payment Event Schema
# ─────────────────────────────────────────────────────────────
@dataclass
class PaymentEvent:
    payment_id:          str
    idempotency_key:     str
    customer_id:         str
    merchant_id:         str
    amount:              float
    currency:            str
    payment_method:      str
    status:              str
    source_ip:           str
    user_agent:          str
    retry_count:         int
    is_retry_flag:       bool
    original_payment_id: Optional[str]
    event_timestamp:     str
    raw_payload:         dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["raw_payload"] = self.raw_payload
        return d

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict(), default=str).encode("utf-8")


# ─────────────────────────────────────────────────────────────
# Idempotency Key Construction
# SHA-256( customer_id + merchant_id + amount + currency + truncated_timestamp )
# 5-minute window: allows legitimate retries within timeout window
# but treats a real second payment after 5min as a new transaction
# ─────────────────────────────────────────────────────────────
def build_idempotency_key(
    customer_id: str,
    merchant_id: str,
    amount: float,
    currency: str,
    event_timestamp: datetime,
) -> str:
    # Truncate timestamp to 5-minute bucket
    truncated = event_timestamp.replace(
        minute=(event_timestamp.minute // 5) * 5,
        second=0,
        microsecond=0,
    )
    raw = f"{customer_id}|{merchant_id}|{amount:.4f}|{currency}|{truncated.isoformat()}"
    return hashlib.sha256(raw.encode()).hexdigest()


# ─────────────────────────────────────────────────────────────
# Event Factory
# ─────────────────────────────────────────────────────────────
def generate_payment_event(
    customer_id: Optional[str] = None,
    merchant_id: Optional[str] = None,
    amount: Optional[float] = None,
    currency: Optional[str] = None,
    is_retry: bool = False,
    original_payment_id: Optional[str] = None,
    retry_count: int = 0,
    original_timestamp: Optional[datetime] = None,
) -> PaymentEvent:
    """
    Generate a single payment event.
    If is_retry=True, reuse the same customer/merchant/amount/currency/timestamp
    so the idempotency key matches the original — simulating a network retry.
    """
    now = original_timestamp if (is_retry and original_timestamp) else datetime.now(timezone.utc)

    cid = customer_id or f"cust_{fake.uuid4()[:8]}"
    mid = merchant_id or random.choice(MERCHANTS)
    amt = amount     or round(random.uniform(100, 500000), 2)   # NGN range
    cur = currency   or random.choice(CURRENCIES)

    payment_id      = str(uuid.uuid4())
    idempotency_key = build_idempotency_key(cid, mid, amt, cur, now)

    event = PaymentEvent(
        payment_id          = payment_id,
        idempotency_key     = idempotency_key,
        customer_id         = cid,
        merchant_id         = mid,
        amount              = amt,
        currency            = cur,
        payment_method      = random.choice(PAYMENT_METHODS),
        status              = random.choice(STATUSES),
        source_ip           = fake.ipv4(),
        user_agent          = fake.user_agent(),
        retry_count         = retry_count,
        is_retry_flag       = is_retry,
        original_payment_id = original_payment_id,
        event_timestamp     = now.isoformat(),
    )

    # Embed full event as raw_payload for Bronze audit trail
    event.raw_payload = event.to_dict()
    event.raw_payload.pop("raw_payload", None)   # avoid circular nesting

    return event


# ─────────────────────────────────────────────────────────────
# Kafka Producer Setup
# ─────────────────────────────────────────────────────────────
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka.bootstrap_servers,
        value_serializer=lambda v: v,          # already bytes from .to_json()
        key_serializer=lambda k: k.encode(),   # partition by customer_id
        acks="all",                            # wait for all replicas
        retries=3,
        max_in_flight_requests_per_connection=1,
        compression_type="gzip",
    )


def on_send_success(record_metadata):
    logger.debug(
        f"✓ Sent → topic={record_metadata.topic} "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )

def on_send_error(excp):
    logger.error(f"✗ Failed to send: {excp}")


# ─────────────────────────────────────────────────────────────
# Main Simulation Loop
# ─────────────────────────────────────────────────────────────
def run_simulator():
    logger.info("═" * 60)
    logger.info("  Duplicate Payment Detection — Event Simulator")
    logger.info("═" * 60)
    logger.info(f"  Kafka broker  : {settings.kafka.bootstrap_servers}")
    logger.info(f"  Topic         : {settings.kafka.topic_raw}")
    logger.info(f"  Total events  : {settings.simulator.total_events}")
    logger.info(f"  Duplicate rate: {settings.simulator.duplicate_rate * 100:.0f}%")
    logger.info(f"  Events/sec    : {settings.simulator.events_per_second}")
    logger.info("═" * 60)

    producer = create_producer()
    logger.info("✓ Connected to Kafka")

    total_sent       = 0
    total_duplicates = 0
    total_fresh      = 0

    # Rolling buffer of recent events eligible for retry injection
    # Keeps last 50 events — retries are drawn from this pool
    recent_events: list[dict] = []

    delay = 1.0 / settings.simulator.events_per_second

    try:
        while total_sent < settings.simulator.total_events:

            # Decide: fresh payment or duplicate retry?
            is_duplicate = (
                len(recent_events) > 0
                and random.random() < settings.simulator.duplicate_rate
            )

            if is_duplicate:
                # Pick a random recent event to retry
                original = random.choice(recent_events)
                event = generate_payment_event(
                    customer_id         = original["customer_id"],
                    merchant_id         = original["merchant_id"],
                    amount              = original["amount"],
                    currency            = original["currency"],
                    is_retry            = True,
                    original_payment_id = original["payment_id"],
                    retry_count         = original["retry_count"] + 1,
                    original_timestamp  = datetime.fromisoformat(original["event_timestamp"]),
                )
                total_duplicates += 1
                logger.info(
                    f"[DUPLICATE #{total_duplicates:>4}] "
                    f"customer={event.customer_id} "
                    f"amount={event.amount:.2f} {event.currency} "
                    f"retry_count={event.retry_count}"
                )
            else:
                event = generate_payment_event()
                total_fresh += 1
                # Add to recent pool (cap at 50)
                recent_events.append(event.to_dict())
                if len(recent_events) > 50:
                    recent_events.pop(0)
                logger.info(
                    f"[FRESH    #{total_fresh:>4}] "
                    f"customer={event.customer_id} "
                    f"amount={event.amount:.2f} {event.currency}"
                )

            # Publish to Kafka
            producer.send(
                settings.kafka.topic_raw,
                key=event.customer_id,
                value=event.to_json(),
            ).add_callback(on_send_success).add_errback(on_send_error)

            total_sent += 1
            time.sleep(delay)

            # Flush every 100 messages
            if total_sent % 100 == 0:
                producer.flush()
                logger.info(
                    f"── Progress: {total_sent}/{settings.simulator.total_events} sent │ "
                    f"Fresh={total_fresh} │ Duplicates={total_duplicates}"
                )

    except KeyboardInterrupt:
        logger.warning("Simulator stopped by user.")

    finally:
        producer.flush()
        producer.close()
        logger.info("═" * 60)
        logger.info(f"  Simulation complete")
        logger.info(f"  Total sent      : {total_sent}")
        logger.info(f"  Fresh payments  : {total_fresh}")
        logger.info(f"  Duplicate events: {total_duplicates}")
        logger.info(
            f"  Actual dupe rate: "
            f"{(total_duplicates/total_sent*100) if total_sent else 0:.1f}%"
        )
        logger.info("═" * 60)


if __name__ == "__main__":
    run_simulator()
