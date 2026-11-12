from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import uuid
from decimal import Decimal
from typing import Literal

from fastapi import APIRouter, Header, Request, Response
from redis.exceptions import RedisError

from backend.api.errors import PaymentError
from backend.api.schemas import CancelRequest, PaymentRequest, PaymentResponse
from backend.core.ids import compute_bucket, mint_payment_id
from backend.redis_layer.client import GuardResult

router = APIRouter(prefix="/v2/payments", tags=["payments"])
_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{8,128}$")
_DEGRADED_TRANSFER_LIMIT = Decimal("25000.0000")
logger = logging.getLogger(__name__)


def _key_hash(key: str) -> str:
    if not _KEY_PATTERN.fullmatch(key):
        raise PaymentError(400, "INVALID_IDEMPOTENCY_KEY", "Invalid idempotency key", "The key must contain 8 to 128 safe characters")
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _request_hash(body: PaymentRequest, flow: str) -> str:
    value = json.dumps(
        {
            "sender_id": body.sender_id,
            "receiver_id": body.receiver_id,
            "amount": body.amount,
            "flow": flow,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


async def _submit(
    body: PaymentRequest,
    request: Request,
    idempotency_key: str,
    flow: Literal["p2p", "merchant"],
) -> PaymentResponse:
    if body.sender_id == body.receiver_id:
        raise PaymentError(422, "SELF_TRANSFER", "Invalid transfer", "Sender and receiver must be different accounts")
    key_hash = _key_hash(idempotency_key)
    amount = Decimal(body.amount)
    payment_id = mint_payment_id(compute_bucket(body.sender_id))
    fraud_started = time.perf_counter()
    profile = request.app.state.fraud_table.read_profile(body.sender_id)
    if profile is None:
        raise PaymentError(503, "FRAUD_PROFILE_UNAVAILABLE", "Fraud check unavailable", "No risk profile is available for this account", True)
    risk_limit_units, fraud_flags = profile
    from backend.fraud.shm_seqlock import FLAG_DEGRADED

    if fraud_flags & FLAG_DEGRADED and amount > _DEGRADED_TRANSFER_LIMIT:
        request.state.fraud_decision_ms = (time.perf_counter() - fraud_started) * 1000
        raise PaymentError(
            403,
            "FRAUD_DECLINED",
            "Payment declined",
            "Degraded fraud mode limits transfers to 25000.0000",
        )
    decision = await request.app.state.fraud_scorer.score(
        amount,
        velocity=0,
        risk_limit=Decimal(risk_limit_units).scaleb(-4),
    )
    if not decision.approved:
        raise PaymentError(422, "FRAUD_REJECTED", "Payment rejected", decision.reason)
    total_sem = request.app.state.total_db_sem
    campaign_sem = request.app.state.campaign_db_sem
    async with total_sem:
        if flow == "merchant":
            async with campaign_sem:
                await _redis_claim(request, body.sender_id, key_hash, payment_id)
                outcome = await request.app.state.store.transfer(
                    body.sender_id,
                    body.receiver_id,
                    amount,
                    key_hash,
                    _request_hash(body, flow),
                    request.state.request_id,
                    flow,
                    payment_id,
                )
        else:
            await _redis_claim(request, body.sender_id, key_hash, payment_id)
            outcome = await request.app.state.store.transfer(
                body.sender_id,
                body.receiver_id,
                amount,
                key_hash,
                _request_hash(body, flow),
                request.state.request_id,
                flow,
                payment_id,
            )
    if outcome.state == "cancelled":
        raise PaymentError(422, "IDEMPOTENCY_KEY_CANCELLED", "Idempotency key cancelled", outcome.detail)
    if outcome.state == "conflict":
        raise PaymentError(409, "IDEMPOTENCY_KEY_REUSED", "Idempotency conflict", outcome.detail)
    if outcome.state == "insufficient_funds":
        raise PaymentError(422, "INSUFFICIENT_FUNDS", "Insufficient funds", outcome.detail)
    if outcome.state == "account_not_found":
        raise PaymentError(404, "ACCOUNT_NOT_FOUND", "Account not found", outcome.detail)
    if outcome.payment_id is None:
        raise PaymentError(503, "PAYMENT_UNAVAILABLE", "Payment unavailable", "The payment could not be completed", True)
    return PaymentResponse(
        payment_id=outcome.payment_id,
        status="completed",
        amount=body.amount,
        sender_id=body.sender_id,
        receiver_id=body.receiver_id,
        replayed=outcome.replayed,
    )


async def _redis_claim(request: Request, account_id: str, key_hash: str, payment_id: str) -> None:
    breaker = request.app.state.redis_circuit_breaker
    if breaker.state == "open":
        breaker.record_fallback()
        return
    guard = request.app.state.redis_guard
    if guard is None:
        breaker.record_fallback()
        return
    try:
        result = await guard.claim(account_id, key_hash, payment_id)
    except (RedisError, OSError, TimeoutError) as error:
        breaker.open(type(error).__name__)
        breaker.record_fallback()
        logger.warning("Redis idempotency guard failed; falling back to database uniqueness: %s", error)
        return
    if result == GuardResult.VELOCITY_LIMIT:
        raise PaymentError(429, "VELOCITY_LIMIT", "Payment rate limit exceeded", "Retry after the velocity window expires", True)


@router.post("/p2p", response_model=PaymentResponse, status_code=201)
async def create_p2p_payment(
    body: PaymentRequest,
    request: Request,
    response: Response,
    idempotency_key: str = Header(alias="Idempotency-Key"),
) -> PaymentResponse:
    result = await _submit(body, request, idempotency_key, "p2p")
    if result.replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return result


@router.post("/merchant", response_model=PaymentResponse, status_code=201)
async def create_merchant_payment(
    body: PaymentRequest,
    request: Request,
    response: Response,
    idempotency_key: str = Header(alias="Idempotency-Key"),
) -> PaymentResponse:
    if body.receiver_id != "acc_merchant_diwali":
        raise PaymentError(422, "INVALID_MERCHANT", "Invalid merchant", "Merchant payments must target acc_merchant_diwali")
    result = await _submit(body, request, idempotency_key, "merchant")
    if result.replayed:
        response.headers["Idempotent-Replayed"] = "true"
    return result


@router.post("/idempotency/{idempotency_key}/cancel", status_code=200)
async def cancel_idempotency_key(
    idempotency_key: str,
    body: CancelRequest,
    request: Request,
) -> dict[str, str]:
    key_hash = _key_hash(idempotency_key)
    state = await request.app.state.store.cancel_idempotency(body.account_id, key_hash)
    if state == "completed":
        raise PaymentError(409, "PAYMENT_ALREADY_COMPLETED", "Payment already completed", "A completed payment cannot be cancelled")
    guard = request.app.state.redis_guard
    if guard is not None:
        await guard.cancel(body.account_id, key_hash)
    return {"state": "cancelled"}


@router.get("/{payment_id}")
async def get_payment(payment_id: str, request: Request) -> dict[str, object]:
    payment = await request.app.state.store.payment(payment_id)
    if payment is None:
        raise PaymentError(404, "PAYMENT_NOT_FOUND", "Payment not found", "The requested payment does not exist")
    return payment
