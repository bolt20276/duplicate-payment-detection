"""
config/settings.py
─────────────────────────────────────────────────────────────────────────────
Centralised configuration loader. All credentials and settings come from
environment variables (sourced from .env). Nothing is hardcoded.

Usage:
    from config.settings import settings
    conn = psycopg2.connect(**settings.postgres_conn)
─────────────────────────────────────────────────────────────────────────────
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load .env from project root (works whether script is run from root or subdir)
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))


@dataclass(frozen=True)
class PostgresConfig:
    host: str     = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    port: int     = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    dbname: str   = field(default_factory=lambda: os.getenv("POSTGRES_DB", "payments_dw"))
    user: str     = field(default_factory=lambda: os.getenv("POSTGRES_USER", "payments_user"))
    password: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "payments_pass"))

    @property
    def conn_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
        }

    @property
    def conn_string(self) -> str:
        return (
            f"postgresql+psycopg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.dbname}"
        )


@dataclass(frozen=True)
class RedisConfig:
    host: str = field(default_factory=lambda: os.getenv("REDIS_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("REDIS_PORT", "6379")))
    db: int   = field(default_factory=lambda: int(os.getenv("REDIS_DB", "0")))
    ttl: int  = field(default_factory=lambda: int(os.getenv("REDIS_TTL_SECONDS", "86400")))


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic_raw: str         = field(default_factory=lambda: os.getenv("KAFKA_TOPIC_RAW", "payments.raw"))
    topic_decisions: str   = field(default_factory=lambda: os.getenv("KAFKA_TOPIC_DECISIONS", "payments.decisions"))
    topic_deadletter: str  = field(default_factory=lambda: os.getenv("KAFKA_TOPIC_DEADLETTER", "payments.deadletter"))
    consumer_group: str    = field(default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP", "dedup-flink-group"))


@dataclass(frozen=True)
class SimulatorConfig:
    events_per_second: int = field(
        default_factory=lambda: int(os.getenv("SIMULATOR_EVENTS_PER_SECOND", "50"))
    )
    duplicate_rate: float = field(
        default_factory=lambda: float(os.getenv("SIMULATOR_DUPLICATE_RATE", "0.15"))
    )
    total_events: int = field(
        default_factory=lambda: int(os.getenv("SIMULATOR_TOTAL_EVENTS", "10000"))
    )


@dataclass(frozen=True)
class Settings:
    postgres: PostgresConfig   = field(default_factory=PostgresConfig)
    redis: RedisConfig         = field(default_factory=RedisConfig)
    kafka: KafkaConfig         = field(default_factory=KafkaConfig)
    simulator: SimulatorConfig = field(default_factory=SimulatorConfig)


# Singleton — import this everywhere
settings = Settings()


if __name__ == "__main__":
    # Quick sanity check
    print("=== Settings loaded ===")
    print(f"Postgres  : {settings.postgres.host}:{settings.postgres.port}/{settings.postgres.dbname}")
    print(f"Redis     : {settings.redis.host}:{settings.redis.port}  TTL={settings.redis.ttl}s")
    print(f"Kafka     : {settings.kafka.bootstrap_servers}")
    print(f"Topics    : {settings.kafka.topic_raw} | {settings.kafka.topic_decisions}")
    print(f"Simulator : {settings.simulator.events_per_second} eps  |  {settings.simulator.duplicate_rate*100:.0f}% dupe rate")
