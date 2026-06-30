"""
airflow/dags/gold_customer_retry_patterns.py
─────────────────────────────────────────────────────────────────────────────
Gold Layer DAG — Customer Retry Patterns (Daily)
Identifies customers with abnormal retry behaviour.
Flags customers with more than 3 duplicate attempts in a single day
for fraud/UX review. Uses high-water mark incremental loading.
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

PIPELINE_NAME       = "customer_retry_patterns_daily"
FLAG_THRESHOLD      = 3     # Flag customers with > 3 duplicates in a day

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


def run_customer_retry_patterns(**context):
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
                INSERT INTO gold.customer_retry_patterns_daily (
                    customer_id,
                    day_bucket,
                    total_payments,
                    unique_payments,
                    duplicate_attempts,
                    max_retries_single_txn,
                    flagged_for_review,
                    aggregated_at
                )
                SELECT
                    customer_id,
                    DATE_TRUNC('day', decision_at)::DATE        AS day_bucket,
                    COUNT(*)                                    AS total_payments,
                    COUNT(*) FILTER (WHERE dedup_status = 'ACCEPTED')           AS unique_payments,
                    COUNT(*) FILTER (WHERE dedup_status = 'REJECTED_DUPLICATE') AS duplicate_attempts,
                    COALESCE(
                        MAX(
                            CASE WHEN dedup_status = 'REJECTED_DUPLICATE'
                            THEN 1 ELSE 0 END
                        ), 0
                    )                                           AS max_retries_single_txn,
                    COUNT(*) FILTER (WHERE dedup_status = 'REJECTED_DUPLICATE') > %s
                                                                AS flagged_for_review,
                    NOW()                                       AS aggregated_at
                FROM silver.payment_decisions
                WHERE decision_at > %s AND decision_at <= %s
                GROUP BY customer_id, DATE_TRUNC('day', decision_at)::DATE
                ON CONFLICT (customer_id, day_bucket)
                DO UPDATE SET
                    total_payments         = EXCLUDED.total_payments,
                    unique_payments        = EXCLUDED.unique_payments,
                    duplicate_attempts     = EXCLUDED.duplicate_attempts,
                    max_retries_single_txn = EXCLUDED.max_retries_single_txn,
                    flagged_for_review     = EXCLUDED.flagged_for_review,
                    aggregated_at          = NOW()
            """, (FLAG_THRESHOLD, watermark, max_ts))

            rows = cur.rowcount
            conn.commit()

        update_watermark(conn, max_ts, rows)
        log.info(f"Customer retry patterns: {rows} rows upserted. Watermark → {max_ts}")

        # Log flagged customers
        with conn.cursor() as cur:
            cur.execute("""
                SELECT customer_id, day_bucket, duplicate_attempts
                FROM gold.customer_retry_patterns_daily
                WHERE flagged_for_review = TRUE
                ORDER BY duplicate_attempts DESC
                LIMIT 10
            """)
            flagged = cur.fetchall()
            if flagged:
                log.warning(f"⚠ {len(flagged)} customers flagged for review:")
                for row in flagged:
                    log.warning(f"  customer={row[0]} day={row[1]} duplicates={row[2]}")

    finally:
        conn.close()


with DAG(
    dag_id="gold_customer_retry_patterns",
    default_args=DEFAULT_ARGS,
    description="Daily customer retry pattern analysis — Gold layer",
    schedule_interval="0 1 * * *",     # Daily at 1am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "customer", "fraud", "retry"],
) as dag:

    aggregate = PythonOperator(
        task_id="aggregate_customer_retry_patterns",
        python_callable=run_customer_retry_patterns,
    )
