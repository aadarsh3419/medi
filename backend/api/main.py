from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from redis.exceptions import RedisError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import Response
from redis.asyncio import Redis

from backend.api.errors import PaymentError
from backend.api.routes_accounts import router as accounts_router
from backend.api.routes_chaos import router as chaos_router
from backend.api.routes_payments import router as payments_router
from backend.api.routes_ws import router as websocket_router
from backend.config import settings
from backend.db.store import PostgresStore, create_store
from backend.fraud.shm_seqlock import SharedMemoryFraudTable
from backend.fraud.tier2_onnx import Tier2FraudScorer
from backend.redis_layer.client import RedisGuard
from backend.redis_layer.circuit_breaker import RedisCircuitBreaker
from backend.workers.audit_merkle_sink import AuditMerkleSink
from backend.workers.hot_slot_folder import run_hot_slot_folder
from backend.workers.outbox_relay import WebSocketHub, run_outbox_relay
import backend.workers.audit_merkle_sink as audit_module

logger = logging.getLogger(__name__)

class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        supplied = request.headers.get("X-Request-ID", "")
        request_id = (
            supplied
            if re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", supplied)
            else str(uuid.uuid4())
        )
        request.state.request_id = request_id
        started = time.perf_counter()
        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id
        response.headers["X-Request-Processing-Ms"] = (
            f"{(time.perf_counter() - started) * 1000:.4f}"
        )
        return response


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    is_postgres = settings.database_url.startswith(("postgresql://", "postgres://"))
    audit_key = os.getenv("AUDIT_HMAC_KEY")
    if is_postgres and (audit_key is None or len(audit_key.encode("utf-8")) < 32):
        raise RuntimeError("AUDIT_HMAC_KEY must contain at least 32 bytes for PostgreSQL deployments")
    store = await create_store(settings.database_url)
    stop = asyncio.Event()
    hub = WebSocketHub()
    redis: Redis | None = None
    fraud_table: SharedMemoryFraudTable | None = None
    audit = AuditMerkleSink(store, audit_key.encode("utf-8") if audit_key else None)
    redis_guard: RedisGuard | None = None
    redis_circuit_breaker = RedisCircuitBreaker()
    tasks: list[asyncio.Task[None]] = []
    try:
        if isinstance(store, PostgresStore):
            redis = Redis.from_url(settings.redis_url, decode_responses=True)
            redis_guard = RedisGuard(redis)
            try:
                await redis_guard.load()
            except (RedisError, OSError, TimeoutError) as error:
                redis_circuit_breaker.open(type(error).__name__)
                logger.warning(
                    "Redis unavailable at startup; database uniqueness will guard idempotency: %s",
                    error,
                )
        fraud_name = f"payscale_fraud_{os.getpid()}_{uuid.uuid4().hex[:12]}"
        fraud_table = SharedMemoryFraudTable(fraud_name, capacity=4096)
        for account_id in ("acc_asha", "acc_ravi", "acc_merchant_diwali"):
            fraud_table.write_limit(account_id, 1_000_000_000)
        await audit.restore()
        if not audit.verify_all_chains():
            raise RuntimeError("persisted audit HMAC chains failed verification")
        app.state.store = store
        app.state.total_db_sem = asyncio.Semaphore(settings.total_db_concurrency)
        app.state.campaign_db_sem = asyncio.Semaphore(settings.campaign_db_concurrency)
        app.state.websocket_hub = hub
        app.state.audit_merkle_chain = audit
        app.state.redis = redis
        app.state.redis_guard = redis_guard
        app.state.redis_circuit_breaker = redis_circuit_breaker
        app.state.fraud_table = fraud_table
        app.state.fraud_scorer = Tier2FraudScorer(session=None, timeout_ms=20)
        audit_module.audit_merkle_chain = audit
        tasks = [
            asyncio.create_task(run_hot_slot_folder(store, stop), name="hot-slot-folder"),
            asyncio.create_task(run_outbox_relay(store, audit, hub, stop, redis), name="outbox-relay"),
        ]
        yield
    finally:
        stop.set()
        if tasks:
            await asyncio.gather(*tasks)
        if fraud_table is not None:
            fraud_table.close()
            fraud_table.unlink()
        if redis_guard is not None:
            await redis_guard.close()
        elif redis is not None:
            await redis.aclose()
        await store.close()
        audit_module.audit_merkle_chain = None


app = FastAPI(title="PayScale HTTPE 3.0", version="3.0.0", lifespan=lifespan)
app.add_middleware(RequestIdMiddleware)
app.include_router(payments_router)
app.include_router(accounts_router)
app.include_router(websocket_router)
if os.getenv("HTTPE_CHAOS_TOKEN"):
    app.include_router(chaos_router)


@app.exception_handler(PaymentError)
async def payment_error_handler(request: Request, exc: PaymentError) -> JSONResponse:
    headers = {}
    fraud_decision_ms = getattr(request.state, "fraud_decision_ms", None)
    if fraud_decision_ms is not None:
        headers["X-Fraud-Decision-Ms"] = f"{fraud_decision_ms:.4f}"
    return JSONResponse(
        status_code=exc.status,
        content={
            "code": exc.code,
            "title": exc.title,
            "status": exc.status,
            "detail": exc.detail,
            "request_id": getattr(request.state, "request_id", str(uuid.uuid4())),
            "retryable": exc.retryable,
        },
        headers=headers,
    )


@app.exception_handler(StarletteHTTPException)
async def http_error_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    status = exc.status_code
    return JSONResponse(
        status_code=status,
        content={
            "code": f"HTTP_{status}",
            "title": "HTTP error",
            "status": status,
            "detail": str(exc.detail),
            "request_id": getattr(request.state, "request_id", str(uuid.uuid4())),
            "retryable": status == 429 or status >= 500,
        },
        headers=exc.headers,
    )


@app.exception_handler(RequestValidationError)
async def request_validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    status = 422
    return JSONResponse(
        status_code=status,
        content={
            "code": "VALIDATION_ERROR",
            "title": "Request validation failed",
            "status": status,
            "detail": "One or more request fields are invalid",
            "request_id": getattr(request.state, "request_id", str(uuid.uuid4())),
            "retryable": False,
        },
    )


@app.exception_handler(Exception)
async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("unhandled request failure", exc_info=exc)
    status = 500
    return JSONResponse(
        status_code=status,
        content={
            "code": "INTERNAL_ERROR",
            "title": "Internal server error",
            "status": status,
            "detail": "The request could not be completed",
            "request_id": getattr(request.state, "request_id", str(uuid.uuid4())),
            "retryable": True,
        },
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
