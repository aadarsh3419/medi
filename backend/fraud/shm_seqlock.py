from __future__ import annotations

import hashlib
import struct
import time
import threading
from multiprocessing import shared_memory

RECORD_FORMAT = "<QQqIIQ24s"
RECORD_SIZE = struct.calcsize(RECORD_FORMAT)
FLAG_DEGRADED = 1
if RECORD_SIZE != 64:
    raise RuntimeError("fraud table record must be exactly 64 bytes")


def account_tag(account_id: str) -> int:
    digest = hashlib.blake2b(account_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "little") or 1


class SharedMemoryFraudTable:
    def __init__(self, name: str, capacity: int = 4096, create: bool = True) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self._shm = shared_memory.SharedMemory(name=name, create=create, size=capacity * RECORD_SIZE if create else 0)
        if create:
            self._shm.buf[:] = b"\0" * (capacity * RECORD_SIZE)
        self._write_lock = threading.Lock()

    def _index(self, tag: int, create: bool) -> int | None:
        start = tag % self.capacity
        for offset in range(self.capacity):
            index = (start + offset) % self.capacity
            record_offset = index * RECORD_SIZE
            sequence = struct.unpack_from("<Q", self._shm.buf, record_offset)[0]
            if sequence == 0:
                return index if create else None
            stored_tag = struct.unpack_from("<Q", self._shm.buf, record_offset + 8)[0]
            if stored_tag == tag:
                return index
        return None

    def write_limit(self, account_id: str, risk_limit: int, flags: int = 0) -> None:
        if risk_limit < 0:
            raise ValueError("risk_limit must be non-negative")
        if not 0 <= flags <= 0xFFFFFFFF:
            raise ValueError("flags must fit in an unsigned 32-bit integer")
        tag = account_tag(account_id)
        with self._write_lock:
            index = self._index(tag, create=True)
            if index is None:
                raise RuntimeError("fraud table is full")
            offset = index * RECORD_SIZE
            old = struct.unpack_from(RECORD_FORMAT, self._shm.buf, offset)
            sequence = old[0]
            if sequence & 1:
                sequence += 1
            struct.pack_into(
                RECORD_FORMAT, self._shm.buf, offset,
                sequence + 1, tag, risk_limit, flags, 0, time.time_ns(), b"\0" * 24,
            )
            struct.pack_into("<Q", self._shm.buf, offset, sequence + 2)

    def read_profile(self, account_id: str, retries: int = 100) -> tuple[int, int] | None:
        tag = account_tag(account_id)
        index = self._index(tag, create=False)
        if index is None:
            return None
        offset = index * RECORD_SIZE
        for _ in range(retries):
            first = struct.unpack_from("<Q", self._shm.buf, offset)[0]
            if first & 1:
                continue
            record = struct.unpack_from(RECORD_FORMAT, self._shm.buf, offset)
            second = struct.unpack_from("<Q", self._shm.buf, offset)[0]
            if first == second and not second & 1:
                if record[1] != tag:
                    return None
                return record[2], record[3]
        raise RuntimeError("unable to read a stable fraud-table record")

    def read_limit(self, account_id: str, retries: int = 100) -> int | None:
        profile = self.read_profile(account_id, retries)
        return profile[0] if profile is not None else None

    def set_degraded_mode(self, account_id: str, enabled: bool) -> None:
        profile = self.read_profile(account_id)
        if profile is None:
            raise KeyError(f"fraud profile not found for {account_id}")
        risk_limit, flags = profile
        flags = flags | FLAG_DEGRADED if enabled else flags & ~FLAG_DEGRADED
        self.write_limit(account_id, risk_limit, flags)

    def close(self) -> None:
        self._shm.close()

    def unlink(self) -> None:
        self._shm.unlink()
