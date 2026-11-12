from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://payscale:local-dev-only@localhost:5432/payscale"
    )
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    host: str = os.getenv("HOST", "127.0.0.1")
    port: int = int(os.getenv("PORT", "8000"))
    statement_timeout_ms: int = 180
    lock_timeout_ms: int = 20
    request_timeout_ms: int = 100
    hot_slot_count: int = 64
    hot_epoch_ms: int = 200
    total_db_concurrency: int = 90
    campaign_db_concurrency: int = 45


settings = Settings()
