Real-Time Duplicate Payment Detection & Idempotency Enforcement
A streaming idempotency gateway that ingests every payment request, checks it against a registry of processed payments using a deterministic idempotency key, and either forwards it or blocks it — in under 50 milliseconds.
Built on an open-source stack — Kafka, Redis, PostgreSQL, Airflow, Grafana — following medallion architecture (Bronze → Silver → Gold) with incremental, watermark-based loading.
The Problem
In payment systems, network timeouts cause merchants to retry payments that already succeeded, resulting in customers being charged twice. Batch reconciliation catches this a day later, after the damage is done. This system catches it in real time, before the second charge ever happens.
Workflow
```
Customer clicks "Pay"
        ↓
Payment Simulator (Python)
Generates the event + builds idempotency key
        ↓
Kafka — payments.raw topic
Acts as the buffer. Holds every event durably.
Decouples the producer from the consumer.
If the dedup engine goes down, no events are lost.
        ↓
Dedup Engine (Python consumer)
Reads from Kafka one event at a time
        ↓
Redis — Idempotency Registry
"Have I seen this key before?"
SET key IF NOT EXISTS — answered in <1ms
        ↓
    ┌─── NO → ACCEPTED ───────────────────┐
    └─── YES → REJECTED_DUPLICATE ────────┘
        ↓
PostgreSQL — Silver layer
Decision written with status + latency metadata
        ↓
Airflow DAGs (batch, hourly/daily)
Aggregates Silver into Gold tables
        ↓
Grafana
Reads Gold tables and visualises everything
```
Kafka guarantees no event is lost. Redis makes the actual duplicate/fresh decision in under a millisecond. PostgreSQL holds the data across three layers of increasing refinement. Airflow turns raw decisions into business metrics. Grafana makes it visible.
Stack
Kafka · Redis · PostgreSQL · Apache Airflow · Grafana · Docker Compose · Python
Results
Metric	Result
Events processed	10,000
Duplicates blocked	1,542 (15.4%)
Average latency	1.75ms
P99 latency	10ms
SLA target	<50ms
SLA compliance	100%
False positives / negatives	0

Project Structure
```
duplicate-payment-detection/
├── docker-compose.yml
├── config/settings.py
├── producer/
│   ├── payment_simulator.py
│   ├── bronze_consumer.py
│   └── dedup_engine.py
├── registry/idempotency_registry.py
├── warehouse/init.sql
├── airflow/dags/
├── observability/
├── notebooks/reconciliation_analysis.ipynb
└── docs/SETUP_GUIDE.md
