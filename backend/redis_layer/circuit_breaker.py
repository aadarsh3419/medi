from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RedisCircuitBreaker:
    state: str = "closed"
    failures: int = 0
    fallbacks: int = 0
    reason: str | None = None

    def open(self, reason: str) -> None:
        self.failures += 1
        self.reason = reason
        self.state = "open"

    def force_open(self, reason: str) -> None:
        self.open(reason)

    def record_fallback(self) -> None:
        self.fallbacks += 1

    def reset(self) -> None:
        self.state = "closed"
        self.failures = 0
        self.fallbacks = 0
        self.reason = None
