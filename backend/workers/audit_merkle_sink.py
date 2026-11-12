from __future__ import annotations

import hashlib
import hmac
import json
import os
import asyncio
from collections import defaultdict
from typing import Any

from backend.core.routing import account_partition
from backend.db.store import AuditHeadConflict, Store


class AuditMerkleSink:
    def __init__(self, store: Store, signing_key: bytes | None = None) -> None:
        self.store = store
        self._key = signing_key or os.getenv("AUDIT_HMAC_KEY", "local-verification-key-change-me").encode()
        self._sequences: dict[int, int] = defaultdict(int)
        self._heads: dict[int, str] = defaultdict(lambda: "0" * 64)
        self._rows: dict[int, list[dict[str, Any]]] = defaultdict(list)
        self._events: dict[int, str] = {}
        self._lock = asyncio.Lock()

    async def restore(self) -> None:
        await self._reload()

    async def _reload(self) -> None:
        rows = await self.store.read_audit()
        self._sequences.clear()
        self._heads.clear()
        self._rows.clear()
        self._events.clear()
        for row in rows:
            partition = int(row["partition_id"])
            self._rows[partition].append(row)
            self._sequences[partition] = max(self._sequences[partition], int(row["sequence"]))
            self._heads[partition] = str(row["event_hash"])
            self._events[int(row["event_id"])] = str(row["event_hash"])

    async def append(self, event_id: int, payload: dict[str, Any]) -> None:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        event_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        async with self._lock:
            previous = self._events.get(event_id)
            if previous is not None:
                if previous != event_hash:
                    raise ValueError(f"audit event {event_id} was replayed with different data")
                return
            partition = account_partition(str(payload["sender_id"]))
            for attempt in range(3):
                sequence = self._sequences[partition] + 1
                previous_hash = self._heads[partition]
                message = f"{partition}:{sequence}:{previous_hash}:{event_hash}".encode("ascii")
                mac = hmac.new(self._key, message, hashlib.sha256).hexdigest()
                row = {
                    "partition_id": partition,
                    "sequence": sequence,
                    "event_id": event_id,
                    "previous_hash": previous_hash,
                    "event_hash": event_hash,
                    "mac": mac,
                }
                try:
                    await self.store.append_audit(row)
                except AuditHeadConflict:
                    await self._reload()
                    if attempt == 2:
                        raise
                    continue
                self._rows[partition].append(row)
                self._events[event_id] = event_hash
                self._sequences[partition] = sequence
                self._heads[partition] = event_hash
                return

    def verify_all_chains(self) -> bool:
        for partition, rows in self._rows.items():
            previous = "0" * 64
            for expected_sequence, row in enumerate(rows, start=1):
                message = (
                    f"{partition}:{expected_sequence}:{previous}:{row['event_hash']}"
                ).encode("ascii")
                expected_mac = hmac.new(self._key, message, hashlib.sha256).hexdigest()
                if (
                    row["partition_id"] != partition
                    or row["sequence"] != expected_sequence
                    or row["previous_hash"] != previous
                    or not hmac.compare_digest(row["mac"], expected_mac)
                ):
                    return False
                previous = row["event_hash"]
        return True


audit_merkle_chain: AuditMerkleSink | None = None
