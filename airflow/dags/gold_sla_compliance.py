"""
airflow/dags/gold_sla_compliance.py
─────────────────────────────────────────────────────────────────────────────
Gold Layer DAG — SLA Compliance (Hourly)
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

PIPELINE_NAME = "sla_compliance_hourly"
SLA_THRESHOLD = 50

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


def run_sla_compliance(**context):
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
                INSERT INTO gold.sla_compliance_hourly (
                    hour_bucket,
                    total_decisions,
                    within_sla_count,
                    breached_sla_count,
                    sla_compliance_pct,
                    avg_latency_ms,
                    max_latency_ms,
                    p50_latency_ms,
                    p95_latency_ms,
                    p99_latency_ms,
                    aggregated_at
                )
                SELECT
                    DATE_TRUNC('hour', decision_at)                              AS hour_bucket,
                    COUNT(*)                                                     AS total_decisions,
                    COUNT(*) FILTER (WHERE processing_latency_ms < %s)           AS within_sla_count,
                    COUNT(*) FILTER (WHERE processing_latency_ms >= %s)          AS breached_sla_count,
                    ROUND(
                        COUNT(*) FILTER (WHERE processing_latency_ms < %s)::NUMERIC
                        / NULLIF(COUNT(*), 0) * 100, 2
                    )                                                            AS sla_compliance_pct,
                    ROUND(AVG(processing_latency_ms)::NUMERIC, 2)               AS avg_latency_ms,
                    MAX(processing_latency_ms)                                   AS max_latency_ms,
                    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY processing_latency_ms)::NUMERIC, 2) AS p50_latency_ms,
                    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY processing_latency_ms)::NUMERIC, 2) AS p95_latency_ms,
                    ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY processing_latency_ms)::NUMERIC, 2) AS p99_latency_ms,
                    NOW()                                                        AS aggregated_at
                FROM silver.payment_decisions
                WHERE decision_at > %s AND decision_at <= %s
                GROUP BY DATE_TRUNC('hour', decision_at)
                ON CONFLICT (hour_bucket)
                DO UPDATE SET
                    total_decisions    = EXCLUDED.total_decisions,
                    within_sla_count   = EXCLUDED.within_sla_count,
                    breached_sla_count = EXCLUDED.breached_sla_count,
                    sla_compliance_pct = EXCLUDED.sla_compliance_pct,
                    avg_latency_ms     = EXCLUDED.avg_latency_ms,
                    max_latency_ms     = EXCLUDED.max_latency_ms,
                    p50_latency_ms     = EXCLUDED.p50_latency_ms,
                    p95_latency_ms     = EXCLUDED.p95_latency_ms,
                    p99_latency_ms     = EXCLUDED.p99_latency_ms,
                    aggregated_at      = NOW()
            """, (SLA_THRESHOLD, SLA_THRESHOLD, SLA_THRESHOLD, watermark, max_ts))

            rows = cur.rowcount
            conn.commit()

        update_watermark(conn, max_ts, rows)
        log.info(f"SLA compliance: {rows} rows upserted. Watermark -> {max_ts}")

    finally:
        conn.close()


with DAG(
    dag_id="gold_sla_compliance",
    default_args=DEFAULT_ARGS,
    description="Hourly SLA compliance tracking — Gold layer",
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gold", "sla", "latency"],
) as dag:

    aggregate = PythonOperator(
        task_id="aggregate_sla_compliance",
        python_callable=run_sla_compliance,
    )