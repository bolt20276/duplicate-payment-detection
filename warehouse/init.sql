-- =============================================================================
-- warehouse/init.sql
-- Medallion Architecture DDL for Duplicate Payment Detection
-- Runs automatically on first postgres container start
-- =============================================================================


-- =============================================================================
-- SCHEMAS
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- =============================================================================
-- BRONZE LAYER — Raw, unfiltered payment events
-- Every event lands here first. Duplicates included. This is the audit trail.
-- Written by: Kafka consumer / Flink bronze sink
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.payment_events (
    id                  BIGSERIAL PRIMARY KEY,

    -- Core payment fields (as received — no transformation)
    payment_id          VARCHAR(64)     NOT NULL,
    idempotency_key     VARCHAR(128)    NOT NULL,
    customer_id         VARCHAR(64)     NOT NULL,
    merchant_id         VARCHAR(64)     NOT NULL,
    amount              NUMERIC(18, 4)  NOT NULL,
    currency            CHAR(3)         NOT NULL,
    payment_method      VARCHAR(32)     NOT NULL,       -- card, bank_transfer, wallet
    status              VARCHAR(32)     NOT NULL,       -- initiated, pending, completed, failed

    -- Network / retry context
    source_ip           INET,
    user_agent          VARCHAR(256),
    retry_count         SMALLINT        NOT NULL DEFAULT 0,
    is_retry_flag       BOOLEAN         NOT NULL DEFAULT FALSE,
    original_payment_id VARCHAR(64),                   -- populated when is_retry_flag = TRUE

    -- Timestamp fields
    event_timestamp     TIMESTAMPTZ     NOT NULL,      -- timestamp from the event itself
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Full raw payload for auditability
    raw_payload         JSONB           NOT NULL
);

-- Indexes for Bronze (query-time lookups, not dedup — dedup is in Redis)
CREATE INDEX IF NOT EXISTS idx_bronze_payment_id      ON bronze.payment_events(payment_id);
CREATE INDEX IF NOT EXISTS idx_bronze_idempotency_key ON bronze.payment_events(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_bronze_customer_id     ON bronze.payment_events(customer_id);
CREATE INDEX IF NOT EXISTS idx_bronze_ingested_at     ON bronze.payment_events(ingested_at DESC);
CREATE INDEX IF NOT EXISTS idx_bronze_event_ts        ON bronze.payment_events(event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_bronze_is_retry        ON bronze.payment_events(is_retry_flag) WHERE is_retry_flag = TRUE;

COMMENT ON TABLE bronze.payment_events IS
    'Raw payment events as received from Kafka. Includes duplicates. Immutable audit trail.';


-- =============================================================================
-- SILVER LAYER — Deduplicated, validated payment decisions
-- Only written after idempotency check. Includes dedup metadata.
-- Written by: Flink deduplication job
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.payment_decisions (
    id                      BIGSERIAL PRIMARY KEY,

    -- Payment identity
    payment_id              VARCHAR(64)     NOT NULL,
    idempotency_key         VARCHAR(128)    NOT NULL,
    idempotency_key_hash    CHAR(64)        NOT NULL,   -- SHA-256 of the constructed key
    customer_id             VARCHAR(64)     NOT NULL,
    merchant_id             VARCHAR(64)     NOT NULL,
    amount                  NUMERIC(18, 4)  NOT NULL,
    currency                CHAR(3)         NOT NULL,
    payment_method          VARCHAR(32)     NOT NULL,

    -- Deduplication decision
    dedup_status            VARCHAR(32)     NOT NULL,   -- ACCEPTED | REJECTED_DUPLICATE | REJECTED_INVALID
    rejection_reason        VARCHAR(256),               -- populated when rejected
    original_payment_id     VARCHAR(64),               -- if REJECTED_DUPLICATE, the first-seen payment_id

    -- Performance SLA tracking
    processing_latency_ms   INTEGER         NOT NULL,   -- end-to-end time: event_ts → decision
    redis_lookup_ms         SMALLINT,                   -- isolated Redis lookup time
    kafka_lag_ms            INTEGER,                    -- time spent in Kafka queue

    -- Timestamps
    event_timestamp         TIMESTAMPTZ     NOT NULL,
    decision_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Source traceability
    flink_job_id            VARCHAR(64),
    kafka_partition         SMALLINT,
    kafka_offset            BIGINT
);

-- Unique constraint: one decision per payment_id
CREATE UNIQUE INDEX IF NOT EXISTS uidx_silver_payment_id
    ON silver.payment_decisions(payment_id);

CREATE INDEX IF NOT EXISTS idx_silver_idempotency_key  ON silver.payment_decisions(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_silver_customer_id      ON silver.payment_decisions(customer_id);
CREATE INDEX IF NOT EXISTS idx_silver_merchant_id      ON silver.payment_decisions(merchant_id);
CREATE INDEX IF NOT EXISTS idx_silver_dedup_status     ON silver.payment_decisions(dedup_status);
CREATE INDEX IF NOT EXISTS idx_silver_decision_at      ON silver.payment_decisions(decision_at DESC);
CREATE INDEX IF NOT EXISTS idx_silver_latency          ON silver.payment_decisions(processing_latency_ms);

COMMENT ON TABLE silver.payment_decisions IS
    'Deduplicated payment decisions with idempotency metadata. One row per payment_id. '
    'ACCEPTED = forwarded to downstream. REJECTED_DUPLICATE = blocked, already processed.';


-- =============================================================================
-- GOLD LAYER — Aggregated business metrics
-- Populated by Airflow DAGs via high-water mark incremental loading
-- =============================================================================

-- Hourly duplicate rate by merchant
CREATE TABLE IF NOT EXISTS gold.merchant_duplicate_rate_hourly (
    id                  BIGSERIAL PRIMARY KEY,
    merchant_id         VARCHAR(64)     NOT NULL,
    hour_bucket         TIMESTAMPTZ     NOT NULL,   -- truncated to hour
    total_payments      INTEGER         NOT NULL DEFAULT 0,
    accepted_payments   INTEGER         NOT NULL DEFAULT 0,
    rejected_duplicates INTEGER         NOT NULL DEFAULT 0,
    duplicate_rate_pct  NUMERIC(5, 2)   NOT NULL DEFAULT 0.00,
    avg_latency_ms      NUMERIC(8, 2),
    p95_latency_ms      NUMERIC(8, 2),
    p99_latency_ms      NUMERIC(8, 2),
    aggregated_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (merchant_id, hour_bucket)
);

CREATE INDEX IF NOT EXISTS idx_gold_merch_hour ON gold.merchant_duplicate_rate_hourly(merchant_id, hour_bucket DESC);


-- SLA compliance report (sub-50ms target)
CREATE TABLE IF NOT EXISTS gold.sla_compliance_hourly (
    id                      BIGSERIAL PRIMARY KEY,
    hour_bucket             TIMESTAMPTZ     NOT NULL UNIQUE,
    total_decisions         INTEGER         NOT NULL DEFAULT 0,
    within_sla_count        INTEGER         NOT NULL DEFAULT 0,  -- latency < 50ms
    breached_sla_count      INTEGER         NOT NULL DEFAULT 0,  -- latency >= 50ms
    sla_compliance_pct      NUMERIC(5, 2)   NOT NULL DEFAULT 0.00,
    avg_latency_ms          NUMERIC(8, 2),
    max_latency_ms          INTEGER,
    p50_latency_ms          NUMERIC(8, 2),
    p95_latency_ms          NUMERIC(8, 2),
    p99_latency_ms          NUMERIC(8, 2),
    aggregated_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_sla_hour ON gold.sla_compliance_hourly(hour_bucket DESC);


-- Customer retry behaviour patterns
CREATE TABLE IF NOT EXISTS gold.customer_retry_patterns_daily (
    id                      BIGSERIAL PRIMARY KEY,
    customer_id             VARCHAR(64)     NOT NULL,
    day_bucket              DATE            NOT NULL,
    total_payments          INTEGER         NOT NULL DEFAULT 0,
    unique_payments         INTEGER         NOT NULL DEFAULT 0,
    duplicate_attempts      INTEGER         NOT NULL DEFAULT 0,
    max_retries_single_txn  SMALLINT        NOT NULL DEFAULT 0,
    avg_retry_interval_sec  NUMERIC(10, 2),
    flagged_for_review      BOOLEAN         NOT NULL DEFAULT FALSE,  -- > 3 dupes in a day
    aggregated_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (customer_id, day_bucket)
);

CREATE INDEX IF NOT EXISTS idx_gold_cust_day ON gold.customer_retry_patterns_daily(customer_id, day_bucket DESC);
CREATE INDEX IF NOT EXISTS idx_gold_flagged  ON gold.customer_retry_patterns_daily(flagged_for_review) WHERE flagged_for_review = TRUE;


-- Overall system summary (daily rollup)
CREATE TABLE IF NOT EXISTS gold.system_summary_daily (
    id                      BIGSERIAL PRIMARY KEY,
    day_bucket              DATE            NOT NULL UNIQUE,
    total_events_received   INTEGER         NOT NULL DEFAULT 0,
    total_accepted          INTEGER         NOT NULL DEFAULT 0,
    total_rejected          INTEGER         NOT NULL DEFAULT 0,
    overall_duplicate_rate  NUMERIC(5, 2)   NOT NULL DEFAULT 0.00,
    overall_sla_compliance  NUMERIC(5, 2)   NOT NULL DEFAULT 0.00,
    avg_latency_ms          NUMERIC(8, 2),
    p99_latency_ms          NUMERIC(8, 2),
    unique_customers        INTEGER,
    unique_merchants        INTEGER,
    aggregated_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);


-- =============================================================================
-- WATERMARK TABLE — Airflow high-water mark for incremental loading
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.pipeline_watermarks (
    pipeline_name       VARCHAR(128)    PRIMARY KEY,
    last_processed_at   TIMESTAMPTZ     NOT NULL,
    last_run_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    rows_processed      BIGINT          NOT NULL DEFAULT 0
);

-- Seed initial watermarks
INSERT INTO gold.pipeline_watermarks (pipeline_name, last_processed_at, rows_processed)
VALUES
    ('merchant_duplicate_rate_hourly',      '1970-01-01 00:00:00+00', 0),
    ('sla_compliance_hourly',               '1970-01-01 00:00:00+00', 0),
    ('customer_retry_patterns_daily',       '1970-01-01 00:00:00+00', 0),
    ('system_summary_daily',                '1970-01-01 00:00:00+00', 0)
ON CONFLICT (pipeline_name) DO NOTHING;


-- =============================================================================
-- GRANTS — Allow the app user to read/write all schemas
-- =============================================================================
GRANT USAGE ON SCHEMA bronze, silver, gold TO payments_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO payments_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA silver TO payments_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold   TO payments_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA bronze TO payments_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA silver TO payments_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA gold   TO payments_user;

-- Future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON TABLES    TO payments_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON TABLES    TO payments_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT ALL ON TABLES    TO payments_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze GRANT ALL ON SEQUENCES TO payments_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA silver GRANT ALL ON SEQUENCES TO payments_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA gold   GRANT ALL ON SEQUENCES TO payments_user;
