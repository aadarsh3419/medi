from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Final

from redis.asyncio import Redis

from backend.config import settings
from backend.core.routing import redis_idempotency_key, redis_velocity_key

_SCRIPT_PATH: Final[Path] = Path(__file__).with_name("idem_velocity.lua")


class GuardResult(StrEnum):
    CLAIMED = "claimed"
    REPLAY = "replay"
    CANCELLED = "cancelled"
    VELOCITY_LIMIT = "velocity_limit"


class RedisGuard:
    def __init__(self, client: Redis, max_velocity: int = 100) -> None:
        self.client = client
        self.max_velocity = max_velocity
        self._script_sha: str | None = None

    @classmethod
    def from_settings(cls) -> RedisGuard:
        return cls(Redis.from_url(settings.redis_url, decode_responses=True))

    async def load(self) -> str:
        source = _SCRIPT_PATH.read_text(encoding="utf-8")
        self._script_sha = await self.client.script_load(source)
        return self._script_sha

    async def claim(self, account_id: str, key_hash: str, payment_id: str) -> GuardResult:
        if self._script_sha is None:
            raise RuntimeError("Redis guard script must be loaded before use")
        idem_key = redis_idempotency_key(account_id, key_hash)
        velocity_key = redis_velocity_key(account_id)
        response = await self.client.evalsha(
            self._script_sha,
            2,
            idem_key,
            velocity_key,
            payment_id,
            10_000,
            self.max_velocity,
        )
        return GuardResult(str(response[1]))

    async def cancel(self, account_id: str, key_hash: str) -> None:
        key = redis_idempotency_key(account_id, key_hash)
        async with self.client.pipeline(transaction=True) as pipeline:
            await pipeline.set(key, "cancelled", px=10_000)
            await pipeline.execute()

    async def close(self) -> None:
        await self.client.aclose()
