from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Protocol, Sequence


class RiskTier(StrEnum):
    LOW = "low"
    ELEVATED = "elevated"
    HIGH = "high"


@dataclass(frozen=True, slots=True)
class FraudDecision:
    approved: bool
    tier: RiskTier
    score: Decimal
    reason: str


class InferenceSession(Protocol):
    def run(
        self, output_names: list[str] | None, inputs: dict[str, object]
    ) -> Sequence[Sequence[object]]: ...


class Tier2FraudScorer:
    def __init__(self, session: InferenceSession | None, timeout_ms: int = 20) -> None:
        if timeout_ms <= 0:
            raise ValueError("timeout_ms must be positive")
        self.session = session
        self.timeout_seconds = timeout_ms / 1000

    async def score(self, amount: Decimal, velocity: int, risk_limit: Decimal) -> FraudDecision:
        if amount <= 0 or velocity < 0 or risk_limit < 0:
            raise ValueError("amount, velocity, and risk limit are outside valid ranges")
        if self.session is None:
            return self._fallback(amount, velocity, risk_limit, "model_unavailable")
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(
                    self.session.run,
                    None,
                    {
                        "features": [[
                            int(amount * 10_000),
                            velocity,
                            int(risk_limit * 10_000),
                        ]]
                    },
                ),
                timeout=self.timeout_seconds,
            )
            score = Decimal(str(result[0][0]))
            if not score.is_finite() or not Decimal(0) <= score <= Decimal(1):
                return self._fallback(amount, velocity, risk_limit, "model_invalid_score")
        except (
            asyncio.TimeoutError,
            IndexError,
            InvalidOperation,
            TypeError,
            ValueError,
            RuntimeError,
        ):
            return self._fallback(amount, velocity, risk_limit, "model_timeout_or_invalid")
        if score >= Decimal("0.90"):
            return FraudDecision(False, RiskTier.HIGH, score, "model_high_risk")
        if score >= Decimal("0.65"):
            return FraudDecision(True, RiskTier.ELEVATED, score, "model_elevated_risk")
        return FraudDecision(True, RiskTier.LOW, score, "model_approved")

    @staticmethod
    def _fallback(amount: Decimal, velocity: int, risk_limit: Decimal, reason: str) -> FraudDecision:
        if amount > risk_limit or velocity > 50:
            return FraudDecision(False, RiskTier.HIGH, Decimal("1"), f"{reason}:hard_limit")
        if velocity > 20:
            return FraudDecision(True, RiskTier.ELEVATED, Decimal("0.70"), f"{reason}:elevated")
        return FraudDecision(True, RiskTier.LOW, Decimal("0.10"), f"{reason}:safe")
