from __future__ import annotations

import json
from typing import Any

import asyncpg

from backend.config import settings


async def create_pool(database_url: str | None = None) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn=database_url or settings.database_url,
        min_size=20,
        max_size=120,
        command_timeout=settings.statement_timeout_ms / 1000,
        server_settings={
            "statement_timeout": f"{settings.statement_timeout_ms}ms",
            "lock_timeout": f"{settings.lock_timeout_ms}ms",
            "synchronous_commit": "on",
        },
        init=_initialize_connection,
    )


async def _initialize_connection(connection: Any) -> None:
    await connection.set_type_codec(
        "numeric",
        schema="pg_catalog",
        encoder=str,
        decoder=str,
        format="text",
    )
    for json_type in ("json", "jsonb"):
        await connection.set_type_codec(
            json_type,
            schema="pg_catalog",
            encoder=json.dumps,
            decoder=json.loads,
            format="text",
        )
