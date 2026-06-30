"""
producer/dedup_engine.py
─────────────────────────────────────────────────────────────────────────────
Deduplication Engine — Silver Layer Writer
Consumes from payments.raw, runs every event through the idempotency
registry, and writes decisions to silver.payment_decisions.

Decision logic:
    ACCEPTED         → first time we've seen this idempotency key
    REJECTED_DUPLICATE → key already exists in Redis
─────────────────────────────────────────────────────────────────────────────
"""

import json
import time
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from loguru import logger
import psycopg

from config.settings import settings
from registry.idempotency_registry import IdempotencyRegistry


# ─────────────────────────────────────────────────────────────
# Database connection
# ─────────────────────────────────────────────────────────────
def get_connection():
    return psycopg.connect(
        host=settings.postgres.host,
        port=settings.postgres.port,
        dbname=settings.postgres.dbname,
        user=settings.postgres.user,
        password=settings.postgres.password,
    )


# ─────────────────────────────────────────────────────────────
# Silver insert
# ─────────────────────────────────────────────────────────────
INSERT_SILVER_SQL = """
    INSERT INTO silver.payment_decisions (
        payment_id,
        idempotency_key,
        idempotency_key_hash,
        customer_id,
        merchant_id,
        amount,
        currency,
        payment_method,
        dedup_status,
        rejection_reason,
        original_payment_id,
        processing_latency_ms,
        event_timestamp,
        decision_at
    ) VALUES (
        %(payment_id)s,
        %(idempotency_key)s,
        %(idempotency_key_hash)s,
        %(customer_id)s,
        %(merchant_id)s,
        %(amount)s,
        %(currency)s,
        %(payment_method)s,
        %(dedup_status)s,
        %(rejection_reason)s,
        %(original_payment_id)s,
        %(processing_latency_ms)s,
        %(event_timestamp)s,
        NOW()
    )
    ON CONFLICT (payment_id) DO NOTHING
"""


def insert_decision(cursor, record: dict):
    cursor.execute(INSERT_SILVER_SQL, record)


# ─────────────────────────────────────────────────────────────
# Main dedup loop
# ─────────────────────────────────────────────────────────────
def run_dedup_engine():
    logger.info("═" * 60)
    logger.info("  Deduplication Engine — Starting")
    logger.info("═" * 60)
    logger.info(f"  Kafka broker : {settings.kafka.bootstrap_servers}")
    logger.info(f"  Topic        : {settings.kafka.topic_raw}")
    logger.info(f"  Redis TTL    : {settings.redis.ttl}s ({settings.redis.ttl//3600}h)")
    logger.info("═" * 60)

    # Initialise components
    registry = IdempotencyRegistry()
    conn     = get_connection()
    logger.info("✓ Connected to PostgreSQL")

    consumer = KafkaConsumer(
        settings.kafka.topic_raw,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id="dedup-engine-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=200,
    )
    logger.info("✓ Kafka consumer ready")

    # Counters
    total_processed = 0
    total_accepted  = 0
    total_rejected  = 0
    total_errors    = 0
    batch           = []
    batch_size      = 100

    try:
        for message in consumer:
            event = message.value
            start_time = time.perf_counter()

            try:
                payment_id      = event["payment_id"]
                idempotency_key = event["idempotency_key"]

                # ── Core idempotency check ──────────────────────
                result = registry.check_and_register(idempotency_key, payment_id)
                # ───────────────────────────────────────────────

                latency_ms = int((time.perf_counter() - start_time) * 1000)
                status     = result["status"]

                decision = {
                    "payment_id":            payment_id,
                    "idempotency_key":       idempotency_key,
                    "idempotency_key_hash":  idempotency_key,   # already SHA-256 from simulator
                    "customer_id":           event["customer_id"],
                    "merchant_id":           event["merchant_id"],
                    "amount":                event["amount"],
                    "currency":              event["currency"],
                    "payment_method":        event["payment_method"],
                    "dedup_status":          status,
                    "rejection_reason":      "Duplicate idempotency key detected" if status == "REJECTED_DUPLICATE" else None,
                    "original_payment_id":   result["original_payment_id"],
                    "processing_latency_ms": latency_ms,
                    "event_timestamp":       event["event_timestamp"],
                }

                batch.append(decision)

                if status == "ACCEPTED":
                    total_accepted += 1
                    logger.info(
                        f"[ACCEPTED  ] payment={payment_id[:8]}... "
                        f"customer={event['customer_id']} "
                        f"amount={event['amount']:.2f} {event['currency']} "
                        f"latency={latency_ms}ms"
                    )
                else:
                    total_rejected += 1
                    logger.warning(
                        f"[DUPLICATE ] payment={payment_id[:8]}... "
                        f"original={result['original_payment_id'][:8] if result['original_payment_id'] else 'unknown'}... "
                        f"latency={latency_ms}ms"
                    )

                # Batch write to Silver
                if len(batch) >= batch_size:
                    with conn.cursor() as cur:
                        for d in batch:
                            insert_decision(cur, d)
                    conn.commit()
                    consumer.commit()
                    total_processed += len(batch)

                    logger.info(
                        f"── Batch committed: {len(batch)} rows │ "
                        f"Total={total_processed} │ "
                        f"Accepted={total_accepted} │ "
                        f"Rejected={total_rejected}"
                    )
                    batch = []

            except Exception as e:
                logger.error(f"Error processing event: {e}")
                conn.rollback()
                total_errors += 1
                batch = []
                continue

        # Flush remaining
        if batch:
            with conn.cursor() as cur:
                for d in batch:
                    insert_decision(cur, d)
            conn.commit()
            consumer.commit()
            total_processed += len(batch)
            logger.info(f"── Final batch committed: {len(batch)} rows")

    except KeyboardInterrupt:
        logger.warning("Dedup engine stopped by user.")

    finally:
        consumer.close()
        conn.close()

        # Redis stats
        stats = registry.get_registry_stats()

        logger.info("═" * 60)
        logger.info(f"  Deduplication complete")
        logger.info(f"  Total processed : {total_processed}")
        logger.info(f"  Accepted        : {total_accepted}")
        logger.info(f"  Rejected (dupe) : {total_rejected}")
        logger.info(f"  Errors          : {total_errors}")
        logger.info(f"  Redis keys      : {stats['total_keys']}")
        logger.info(f"  Redis memory    : {stats['used_memory_mb']} MB")
        logger.info("═" * 60)


if __name__ == "__main__":
    run_dedup_engine()