from __future__ import annotations

import asyncio
import logging

import asyncpg

from backend.config import settings
from backend.db.store import Store

logger = logging.getLogger(__name__)


async def run_hot_slot_folder(store: Store, stop: asyncio.Event) -> None:
    interval = settings.hot_epoch_ms / 1000
    while not stop.is_set():
        try:
            await store.fold_hot_slots()
        except (OSError, RuntimeError, asyncpg.PostgresError):
            logger.exception("hot-slot folding failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue
