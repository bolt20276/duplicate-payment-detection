"""
airflow/dags/gold_merchant_duplicate_rate.py
─────────────────────────────────────────────────────────────────────────────
Gold Layer DAG — Merchant Duplicate Rate (Hourly)
─────────────────────────────────────────────────────────────────────────────
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg
import logging

log = logging.getLogger(__name__)

DB_CONN = {
    "host":     "postgres",
    "port":     5432,
    "dbname":   "payments_dw",
    "user":     "payments_user",
    "password": "payments_pass",
}

PIPELINE_NAME = "merchant_duplicate_rate_hourly"

DEFAULT_ARGS = {
    "owner":            "data_engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def get_watermark(conn) -> datetime:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_processed_at FROM gold.pipeline_watermarks WHERE pipeline_name = %s",
            (PIPELINE_NAME,)
        )
        row = cur.fetchone()
        return row[0] if row else datetime(1970, 1, 1)


def update_watermark(conn, new_watermark: datetime, rows_processed: int):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE gold.pipeline_watermarks
            SET last_processed_at = %s,
                last_run_at       = NOW(),
                rows_processed    = rows_processed + %s
            WHERE pipeline_name = %s
        """, (new_watermark, rows_processed, PIPELINE_NAME))
    conn.commit()


def run_merchant_duplicate_rate(**context):
    conn = psycopg.connect(**DB_CONN)
    try:
        watermark = get_watermark(conn)
        log.info(f"Processing Silver records after: {watermark}")

        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(decision_at) FROM silver.payment_decisions WHERE decision_at > %s",
                (watermark,)
            )
            max_ts = cur.fetchone()[0]

            if not max_ts:
                log.info("No new Silver records found. Skipping.")
                return

            cur.execute("""
                INSERT INTO gold.merchant_duplicate_rate_hourly (
                    merchant_id,
                    hour_bucket,
                    total_payments,
                    accepted_payments,
                    rejected_duplicates,
                    duplicate_rate_pct,
                    avg_latency_ms,
                    p95_latency_ms,
                    p99_latency_ms,
                    aggregated_at
                )
                SELECT
                    merchant_id,
                    DATE_TRUNC('hour', decision_at)         AS hour_bucket,
                    COUNT(*)                                AS total_payments,
                    COUNT(*) FILTER (WHERE dedup_status = 'ACCEPTED')            AS accepted_payments,
                    COUNT(*) FILTER (WHERE dedup_status = 'REJECTED_DUPLICATE')  AS rejected_duplicates,
                    ROUND(
                        COUNT(*) FILTER (WHERE dedup_status = 'REJECTED_DUPLICATE')::NUMERIC
                        / NULLIF(COUNT(*), 0) * 100, 2
                    )                                                            AS duplicate_rate_pct,
                    ROUND(AVG(processing_latency_ms)::NUMERIC, 2)               AS avg_latency_ms,
                    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_latency_ms)::NUMERIC, 2) AS p95_latency_ms,
                    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY processing_latency_ms)::NUMERIC, 2) AS p99_latency_ms,
                    NOW()                                                        AS aggregated_at
                FROM silver.payment_decisions
                WHERE decision_at > %s AND decision_at <= %s
                GROUP BY merchant_id, DATE_TRUNC('hour', decision_at)
                ON CONFLICT (merchant_id, hour_bucket)
                DO UPDATE SET
                    total_payments      = EXCLUDED.total_payments,
                    accepted_payments   = EXCLUDED.accepted_payments,
                    rejected_duplicates = EXCLUDED.rejected_duplicates,
                    duplicate_rate_pct  = EXCLUDED.duplicate_rate_pct,
                    avg_latency_ms      = EXCLUDED.avg_latency_ms,
                    p95_latency_ms      = EXCLUDED.p95_latency_ms,
                    p99_latency_ms      = EXCLUDED.p99_latency_ms,
                    aggregated_at       = NOW()
            """, (watermark, max_ts))

            rows = cur.rowcount
            conn.commit()

        update_watermark(conn, max_ts, rows)
        log.info(f"Merchant duplicate rate: {rows} rows upserted. Watermark -> {max_ts}")

    finally:
        conn.close()


with DAG(
    dag_id="gold_merchant_duplicate_rate",
    default_args=DEFAULT_ARGS,
    description="Hourly merchant duplicate rate aggregation — Gold layer",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "merchant", "duplicate-rate"],
) as dag:

    aggregate = PythonOperator(
        task_id="aggregate_merchant_duplicate_rate",
        python_callable=run_merchant_duplicate_rate,
    )
