from __future__ import annotations

import hmac
import os
from typing import Literal

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

router = APIRouter(prefix="/__chaos", tags=["chaos-test-only"])
_CHAOS_TOKEN = os.getenv("HTTPE_CHAOS_TOKEN", "")


class FraudModeRequest(BaseModel):
    account_id: Literal["acc_asha", "acc_ravi", "acc_merchant_diwali"]
    enabled: bool


class SimulatedRedisTimeout:
    async def claim(self, account_id: str, key_hash: str, payment_id: str) -> None:
        raise TimeoutError("chaos-injected Redis timeout")


def _authorize(token: str) -> None:
    if not _CHAOS_TOKEN or not hmac.compare_digest(token, _CHAOS_TOKEN):
        raise HTTPException(status_code=404, detail="Not found")


@router.post("/arm/redis-timeout")
async def arm_redis_timeout(
    request: Request,
    chaos_token: str = Header(alias="X-Chaos-Token"),
) -> dict[str, str]:
    _authorize(chaos_token)
    if not hasattr(request.app.state, "chaos_original_redis_guard"):
        request.app.state.chaos_original_redis_guard = request.app.state.redis_guard
    request.app.state.redis_guard = SimulatedRedisTimeout()
    return {"state": request.app.state.redis_circuit_breaker.state, "armed": "true"}


@router.post("/reset/redis")
async def reset_redis_failure(
    request: Request,
    chaos_token: str = Header(alias="X-Chaos-Token"),
) -> dict[str, str]:
    _authorize(chaos_token)
    if hasattr(request.app.state, "chaos_original_redis_guard"):
        request.app.state.redis_guard = request.app.state.chaos_original_redis_guard
        del request.app.state.chaos_original_redis_guard
    request.app.state.redis_circuit_breaker.reset()
    return {"state": "closed"}


@router.post("/arm/fraud-degraded")
async def set_fraud_degraded_mode(
    body: FraudModeRequest,
    request: Request,
    chaos_token: str = Header(alias="X-Chaos-Token"),
) -> dict[str, str | bool]:
    _authorize(chaos_token)
    request.app.state.fraud_table.set_degraded_mode(body.account_id, body.enabled)
    return {"account_id": body.account_id, "degraded": body.enabled}


@router.get("/summary")
async def diagnostics(
    request: Request,
    chaos_token: str = Header(alias="X-Chaos-Token"),
) -> dict[str, object]:
    _authorize(chaos_token)
    breaker = request.app.state.redis_circuit_breaker
    return {
        **await request.app.state.store.diagnostics(),
        "correctness_gates": await request.app.state.store.verify_gates(),
        "redis_circuit_state": breaker.state,
        "redis_failures": breaker.failures,
        "redis_fallbacks": breaker.fallbacks,
        "audit_chains_valid": request.app.state.audit_merkle_chain.verify_all_chains(),
    }
