"""
producer/bronze_consumer.py
─────────────────────────────────────────────────────────────────────────────
Bronze Layer Ingestion Consumer
Reads every payment event from Kafka topic payments.raw and writes it
into bronze.payment_events in PostgreSQL. No filtering, no deduplication.
Duplicates included. This is the immutable audit trail.

Usage:
    py -3.13 producer/bronze_consumer.py
─────────────────────────────────────────────────────────────────────────────
"""

import json
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
from kafka import KafkaConsumer
from loguru import logger
import psycopg
from config.settings import settings


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
# Insert into Bronze
# ─────────────────────────────────────────────────────────────
INSERT_SQL = """
    INSERT INTO bronze.payment_events (
        payment_id,
        idempotency_key,
        customer_id,
        merchant_id,
        amount,
        currency,
        payment_method,
        status,
        source_ip,
        user_agent,
        retry_count,
        is_retry_flag,
        original_payment_id,
        event_timestamp,
        raw_payload
    ) VALUES (
        %(payment_id)s,
        %(idempotency_key)s,
        %(customer_id)s,
        %(merchant_id)s,
        %(amount)s,
        %(currency)s,
        %(payment_method)s,
        %(status)s,
        %(source_ip)s,
        %(user_agent)s,
        %(retry_count)s,
        %(is_retry_flag)s,
        %(original_payment_id)s,
        %(event_timestamp)s,
        %(raw_payload)s
    )
    ON CONFLICT DO NOTHING
"""


def insert_event(cursor, event: dict):
    cursor.execute(INSERT_SQL, {
        "payment_id":           event.get("payment_id"),
        "idempotency_key":      event.get("idempotency_key"),
        "customer_id":          event.get("customer_id"),
        "merchant_id":          event.get("merchant_id"),
        "amount":               event.get("amount"),
        "currency":             event.get("currency"),
        "payment_method":       event.get("payment_method"),
        "status":               event.get("status"),
        "source_ip":            event.get("source_ip"),
        "user_agent":           event.get("user_agent"),
        "retry_count":          event.get("retry_count", 0),
        "is_retry_flag":        event.get("is_retry_flag", False),
        "original_payment_id":  event.get("original_payment_id"),
        "event_timestamp":      event.get("event_timestamp"),
        "raw_payload":          json.dumps(event),
    })


# ─────────────────────────────────────────────────────────────
# Main consumer loop
# ─────────────────────────────────────────────────────────────
def run_consumer():
    logger.info("═" * 60)
    logger.info("  Bronze Layer Consumer — Starting")
    logger.info("═" * 60)
    logger.info(f"  Kafka broker : {settings.kafka.bootstrap_servers}")
    logger.info(f"  Topic        : {settings.kafka.topic_raw}")
    logger.info(f"  PostgreSQL   : {settings.postgres.host}:{settings.postgres.port}/{settings.postgres.dbname}")
    logger.info("═" * 60)

    consumer = KafkaConsumer(
        settings.kafka.topic_raw,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id="bronze-ingestion-group",
        auto_offset_reset="earliest",       # read from beginning
        enable_auto_commit=False,           # manual commit for reliability
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=500,
    )

    logger.info("✓ Connected to Kafka — consuming from beginning")

    total_inserted  = 0
    total_skipped   = 0
    total_errors    = 0
    batch_size      = 100

    try:
        conn = get_connection()
        logger.info("✓ Connected to PostgreSQL")

        batch = []

        for message in consumer:
            event = message.value

            try:
                batch.append(event)

                # Write in batches of 100
                if len(batch) >= batch_size:
                    with conn.cursor() as cur:
                        for e in batch:
                            insert_event(cur, e)
                    conn.commit()
                    consumer.commit()

                    inserted_in_batch = len(batch)
                    total_inserted += inserted_in_batch
                    batch = []

                    logger.info(
                        f"── Batch committed: {inserted_in_batch} rows │ "
                        f"Total inserted: {total_inserted}"
                    )

            except Exception as e:
                logger.error(f"Error processing event {event.get('payment_id')}: {e}")
                conn.rollback()
                total_errors += 1
                batch = []
                continue

        # Flush remaining events
        if batch:
            with conn.cursor() as cur:
                for e in batch:
                    insert_event(cur, e)
            conn.commit()
            consumer.commit()
            total_inserted += len(batch)
            logger.info(f"── Final batch committed: {len(batch)} rows")

    except KeyboardInterrupt:
        logger.warning("Consumer stopped by user.")

    finally:
        consumer.close()
        conn.close()
        logger.info("═" * 60)
        logger.info(f"  Bronze ingestion complete")
        logger.info(f"  Total inserted : {total_inserted}")
        logger.info(f"  Total skipped  : {total_skipped}")
        logger.info(f"  Total errors   : {total_errors}")
        logger.info("═" * 60)


if __name__ == "__main__":
    run_consumer()