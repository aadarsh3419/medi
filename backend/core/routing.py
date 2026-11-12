from __future__ import annotations

from backend.core.ids import compute_bucket


def account_partition(account_id: str, partitions: int = 32) -> int:
    if partitions < 1:
        raise ValueError("partitions must be positive")
    return compute_bucket(account_id) % partitions


def redis_idempotency_key(account_id: str, key_hash: str) -> str:
    return f"idem:{{{account_id}}}:{key_hash}"


def redis_velocity_key(account_id: str) -> str:
    return f"vel:{{{account_id}}}:10s"


def redis_stream_key(account_id: str) -> str:
    return f"ws:stream:{{{account_id}}}"
