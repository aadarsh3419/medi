from __future__ import annotations

import concurrent.futures
import json
import os
import time
import uuid
import urllib.error
import urllib.request
from decimal import Decimal
from typing import Any

from backend.db.queries import ZERO_DEFECT_ATOMIC_CTE_SQL

BASE_URL = os.getenv("HTTPE_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
CHAOS_TOKEN = os.getenv("HTTPE_CHAOS_TOKEN", "")


def _request(
    path: str,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any], dict[str, str], float]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request_headers = {"Content-Type": "application/json", **(headers or {})}
    request = urllib.request.Request(
        f"{BASE_URL}{path}", data=data, headers=request_headers, method=method
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            payload = json.loads(response.read())
            return response.status, payload, dict(response.headers.items()), time.perf_counter() - started
    except urllib.error.HTTPError as error:
        payload = json.loads(error.read())
        return error.code, payload, dict(error.headers.items()), time.perf_counter() - started


def _admin_headers() -> dict[str, str]:
    if not CHAOS_TOKEN:
        raise RuntimeError(
            "HTTPE_CHAOS_TOKEN is required; restart the API with the test-only chaos controls enabled"
        )
    return {"X-Chaos-Token": CHAOS_TOKEN}


def _payment(
    key: str, sender: str, receiver: str, amount: str = "1.0000"
) -> tuple[int, dict[str, Any], dict[str, str], float]:
    return _request(
        "/v2/payments/p2p",
        "POST",
        {"sender_id": sender, "receiver_id": receiver, "amount": amount},
        {"Idempotency-Key": key},
    )


def _check_server() -> None:
    status, payload, _, _ = _request("/health")
    assert status == 200 and payload.get("status") == "ok", (
        "FastAPI server is not ready",
        status,
        payload,
    )


def run() -> None:
    _check_server()
    admin = _admin_headers()
    run_id = uuid.uuid4().hex[:8]

    _, initial, _, _ = _request("/__chaos/summary", headers=admin)
    print(f"Database backend: {initial['backend']}")

    status, result, _, _ = _request(
        "/__chaos/arm/redis-timeout", "POST", {}, admin
    )
    assert status == 200 and result["armed"] == "true", (status, result)

    duplicate_keys = [f"ce003-{run_id}-{index:02d}" for index in range(10)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        redis_failure_results = list(
            executor.map(
                lambda index: _payment(
                    duplicate_keys[index % len(duplicate_keys)],
                    "acc_asha",
                    "acc_ravi",
                ),
                range(50),
            )
        )
    statuses = [result[0] for result in redis_failure_results]
    assert all(status == 201 for status in statuses), statuses
    assert not any(status == 500 for status in statuses), statuses
    payment_ids = {result[1]["payment_id"] for result in redis_failure_results}
    assert len(payment_ids) == 10, payment_ids
    replay_count = sum(bool(result[1].get("replayed")) for result in redis_failure_results)
    assert replay_count == 40, replay_count
    status, after_redis, _, _ = _request("/__chaos/summary", headers=admin)
    assert status == 200 and after_redis["redis_circuit_state"] == "open", after_redis
    assert after_redis["redis_failures"] >= 1, after_redis
    assert after_redis["redis_fallbacks"] >= 50, after_redis
    print("PASS CE-003 Redis timeout fallback: 50 requests, 10 unique debits, 40 replays, 0 HTTP 500")
    reset_redis_status, reset_redis_result, _, _ = _request(
        "/__chaos/reset/redis", "POST", {}, admin
    )
    assert reset_redis_status == 200 and reset_redis_result["state"] == "closed", (
        reset_redis_status,
        reset_redis_result,
    )

    status, mode_result, _, _ = _request(
        "/__chaos/arm/fraud-degraded",
        "POST",
        {"account_id": "acc_asha", "enabled": True},
        admin,
    )
    assert status == 200 and mode_result["degraded"] is True, (status, mode_result)
    try:
        before_fraud_status, before_fraud_balance, _, _ = _request(
            "/v2/accounts/acc_asha/balance"
        )
        assert before_fraud_status == 200, before_fraud_balance
        fraud_status, fraud_error, fraud_headers, fraud_elapsed = _payment(
            f"ce007-{run_id}-high-risk",
            "acc_asha",
            "acc_ravi",
            "30000.0000",
        )
        assert fraud_status == 403, (fraud_status, fraud_error)
        assert fraud_error["code"] == "FRAUD_DECLINED", fraud_error
        decision_ms = float(fraud_headers["x-fraud-decision-ms"])
        assert decision_ms < 5, decision_ms
        processing_ms = float(fraud_headers["x-request-processing-ms"])
        assert processing_ms < 5, processing_ms
        after_fraud_status, after_fraud_balance, _, _ = _request(
            "/v2/accounts/acc_asha/balance"
        )
        assert after_fraud_status == 200, after_fraud_balance
        assert (
            before_fraud_balance["available_balance"]
            == after_fraud_balance["available_balance"]
        ), (before_fraud_balance, after_fraud_balance)
        print(
            "PASS CE-007 degraded fraud decline: "
            f"{decision_ms:.3f}ms fraud decision, "
            f"{processing_ms:.3f}ms server processing "
            f"({fraud_elapsed * 1000:.3f}ms client round trip)"
        )
    finally:
        reset_status, reset_result, _, _ = _request(
            "/__chaos/arm/fraud-degraded",
            "POST",
            {"account_id": "acc_asha", "enabled": False},
            admin,
        )
        assert reset_status == 200 and reset_result["degraded"] is False, (
            reset_status,
            reset_result,
        )

    transfer_cases = [
        ("acc_asha", "acc_ravi", f"ce-f-{run_id}-{index:03d}")
        for index in range(50)
    ] + [
        ("acc_ravi", "acc_asha", f"ce-r-{run_id}-{index:03d}")
        for index in range(50)
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        opposite_results = list(
            executor.map(
                lambda case: _payment(case[2], case[0], case[1]),
                transfer_cases,
            )
        )
    failed_transfers = [result for result in opposite_results if result[0] != 201]
    assert not failed_transfers, failed_transfers
    lock_clause = "ORDER BY a.account_id ASC\n    FOR UPDATE"
    assert lock_clause in ZERO_DEFECT_ATOMIC_CTE_SQL, "PostgreSQL transfer query lost canonical lock ordering"
    print("PASS opposite-direction concurrent transfers: 100/100 completed; canonical SQL lock order present")

    status, final, _, _ = _request("/__chaos/summary", headers=admin)
    assert status == 200, final
    assert final["audit_chains_valid"] is True, final
    assert Decimal(final["ledger_drift"]) == Decimal("0.0000"), final
    failed_gates = [
        name for name, passed in final["correctness_gates"].items() if not passed
    ]
    assert not failed_gates, (failed_gates, final["correctness_gates"])
    for account_id in ("acc_asha", "acc_ravi", "acc_merchant_diwali"):
        balance = final["balances"][account_id]
        assert balance is not None, (account_id, final)
        assert Decimal(balance["available_balance"]) == Decimal(balance["ledger_balance"]), (
            account_id,
            balance,
        )
    assert final["transaction_count"] == initial["transaction_count"] + 110, (
        initial["transaction_count"],
        final["transaction_count"],
    )

    print("FINAL DATABASE SUMMARY")
    for account_id, balance in final["balances"].items():
        print(f"{account_id}: {balance['available_balance']}")
    print(f"Total transactions: {final['transaction_count']}")
    print(
        f"Ledger conservation drift: {final['ledger_drift']} "
        f"(debits={final['debit_total']}, credits={final['credit_total']})"
    )
    print("PASS ledger invariants: zero conservation drift, reconciled account balances, valid audit chains")
    print("PASS database correctness gates: " + ", ".join(final["correctness_gates"]))
    print("ALL CHAOS AND LOAD ASSERTIONS PASSED")


if __name__ == "__main__":
    run()
