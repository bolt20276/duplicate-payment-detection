"""
redis/idempotency_registry.py
─────────────────────────────────────────────────────────────────────────────
Idempotency Registry
Wraps Redis SETNX logic for idempotency key lookups.
This is the single source of truth for whether a payment has been seen before.

Core operation:
    SETNX idempotency_key payment_id EX ttl
    - If key does NOT exist → sets it, returns ACCEPTED
    - If key ALREADY exists → returns REJECTED_DUPLICATE + original payment_id
─────────────────────────────────────────────────────────────────────────────
"""

import redis
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import settings
from loguru import logger


class IdempotencyRegistry:

    def __init__(self):
        self.client = redis.Redis(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=2,
        )
        self.ttl = settings.redis.ttl
        self._verify_connection()

    def _verify_connection(self):
        try:
            self.client.ping()
            logger.info(f"✓ Connected to Redis at {settings.redis.host}:{settings.redis.port}")
        except redis.ConnectionError as e:
            logger.error(f"✗ Redis connection failed: {e}")
            raise

    def check_and_register(self, idempotency_key: str, payment_id: str) -> dict:
        """
        Core idempotency check.

        Returns:
            {
                "status": "ACCEPTED" | "REJECTED_DUPLICATE",
                "original_payment_id": str | None,
                "redis_key": str,
            }
        """
        redis_key = f"idem:{idempotency_key}"

        # SET key value EX ttl NX — atomic set-if-not-exists
        result = self.client.set(
            redis_key,
            payment_id,
            ex=self.ttl,
            nx=True,        # Only set if key does NOT exist
        )

        if result is True:
            # Key was newly set — this is a fresh payment
            return {
                "status": "ACCEPTED",
                "original_payment_id": None,
                "redis_key": redis_key,
            }
        else:
            # Key already existed — this is a duplicate
            original_payment_id = self.client.get(redis_key)
            return {
                "status": "REJECTED_DUPLICATE",
                "original_payment_id": original_payment_id,
                "redis_key": redis_key,
            }

    def get_registry_stats(self) -> dict:
        """Returns basic Redis memory and key stats."""
        info = self.client.info("memory")
        keys = self.client.dbsize()
        return {
            "total_keys": keys,
            "used_memory_mb": round(info["used_memory"] / 1024 / 1024, 2),
            "peak_memory_mb": round(info["used_memory_peak"] / 1024 / 1024, 2),
        }
