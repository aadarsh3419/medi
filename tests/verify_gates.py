from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import os
import socket
import threading
import time
import urllib.error
import urllib.request
from typing import Any

import uvicorn

os.environ["DATABASE_URL"] = "sqlite:///:memory:"

from backend.api.main import app
from backend.core.ids import compute_hot_slot


def _request(
    url: str,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any], dict[str, str]]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request_headers = {"Content-Type": "application/json", **(headers or {})}
    request = urllib.request.Request(url, data=data, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.loads(response.read()), dict(response.headers.items())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read()), dict(error.headers.items())


def _post_payment(base_url: str, endpoint: str, key: str, body: dict[str, str]):
    return _request(
        f"{base_url}/v2/payments/{endpoint}",
        "POST",
        body,
        {"Idempotency-Key": key},
    )


def run() -> None:
    with socket.socket() as candidate:
        candidate.bind(("127.0.0.1", 0))
        port = candidate.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="error",
            access_log=False,
        )
    )
    thread = threading.Thread(target=server.run, name="htpe-verification-server", daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                status, _, _ = _request(f"{base_url}/health")
                if status == 200:
                    break
            except (OSError, TimeoutError):
                time.sleep(0.05)
        else:
            raise AssertionError("FastAPI server did not become ready")

        p2p_body = {
            "sender_id": "acc_asha",
            "receiver_id": "acc_ravi",
            "amount": "10.0000",
        }
        idem_key = "same-key-concurrency-01"
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            p2p_results = list(executor.map(
                lambda _: _post_payment(base_url, "p2p", idem_key, p2p_body),
                range(20),
            ))
        assert all(status == 201 for status, _, _ in p2p_results), p2p_results
        replay_headers = [
            headers
            for _, _, headers in p2p_results
            if headers.get("idempotent-replayed") == "true"
        ]
        assert len(replay_headers) == 19, (replay_headers, p2p_results)
        payment_ids = {payload["payment_id"] for _, payload, _ in p2p_results}
        assert len(payment_ids) == 1, payment_ids
        p2p_key_hash = hashlib.sha256(idem_key.encode()).hexdigest()
        stored_p2p_count = app.state.store.connection.execute(
            "SELECT count(*) FROM transactions WHERE idempotency_key_hash=?",
            (p2p_key_hash,),
        ).fetchone()[0]
        assert stored_p2p_count == 1, stored_p2p_count

        merchant_body = {
            "sender_id": "acc_ravi",
            "receiver_id": "acc_merchant_diwali",
            "amount": "1.0000",
        }
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            merchant_results = list(executor.map(
                lambda number: _post_payment(
                    base_url,
                    "merchant",
                    f"flash-sale-merchant-{number:03d}",
                    merchant_body,
                ),
                range(50),
            ))
        assert all(status == 201 for status, _, _ in merchant_results), merchant_results
        merchant_payment_ids = {payload["payment_id"] for _, payload, _ in merchant_results}
        assert len(merchant_payment_ids) == 50
        assert len({compute_hot_slot(payment_id) for payment_id in merchant_payment_ids}) > 10
        time.sleep(0.3)
        balance_status, merchant_balance, _ = _request(
            f"{base_url}/v2/accounts/acc_merchant_diwali/balance"
        )
        assert balance_status == 200, merchant_balance
        assert merchant_balance["available_balance"] == merchant_balance["ledger_balance"], merchant_balance
        assert merchant_balance["available_balance"] == "50.0000", merchant_balance

        cancelled_key = "cancel-key-late-payment-01"
        before_status, before, _ = _request(f"{base_url}/v2/accounts/acc_asha/balance")
        assert before_status == 200
        cancel_status, cancel_result, _ = _request(
            f"{base_url}/v2/payments/idempotency/{cancelled_key}/cancel",
            "POST",
            {"account_id": "acc_asha"},
        )
        assert cancel_status == 200 and cancel_result["state"] == "cancelled", cancel_result
        late_status, late_error, _ = _post_payment(
            base_url,
            "p2p",
            cancelled_key,
            {
                "sender_id": "acc_asha",
                "receiver_id": "acc_ravi",
                "amount": "1.0000",
            },
        )
        assert late_status == 422, (late_status, late_error)
        assert late_error["code"] == "IDEMPOTENCY_KEY_CANCELLED", late_error
        assert set(("code", "title", "status", "detail", "request_id", "retryable")) <= set(late_error)
        after_status, after, _ = _request(f"{base_url}/v2/accounts/acc_asha/balance")
        assert after_status == 200 and before["available_balance"] == after["available_balance"]

        for _ in range(100):
            gate_report = asyncio.run(app.state.store.verify_gates())
            if gate_report["outbox_published_parity"]:
                break
            time.sleep(0.05)
        audit_valid = app.state.audit_merkle_chain.verify_all_chains()
        gates = {
            "1_ledger_debits_equal_credits": gate_report["balanced_ledger"],
            "2_no_stuck_money": gate_report["zero_stuck_money"],
            "3_no_negative_balances": gate_report["zero_negative_balances"],
            "4_all_transactions_final": gate_report["all_transactions_final"],
            "5_two_ledger_entries_per_completion": gate_report["two_entries_per_completion"],
            "6_idempotent_replay_safety": stored_p2p_count == 1 and len(payment_ids) == 1,
            "7_outbox_published_parity": gate_report["outbox_published_parity"],
            "8_audit_merkle_chains": audit_valid,
        }
        failed = [name for name, passed in gates.items() if not passed]
        if failed:
            raise AssertionError(f"Correctness gates failed: {failed}; report={gate_report}")
        print("GATE RESULTS")
        for name in gates:
            print(f"PASS {name}")
        print("ALL 8 CORRECTNESS GATES PASSED WITH ZERO DRIFT!")
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        if thread.is_alive():
            raise RuntimeError("verification server did not shut down cleanly")


if __name__ == "__main__":
    run()
