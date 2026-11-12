from __future__ import annotations

import asyncio
import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from backend.core.ids import compute_hot_slot, compute_bucket, mint_payment_id
from backend.db.queries import (
    CANCEL_IDEMPOTENCY_KEY_SQL,
    FOLD_HOT_ACCOUNT_SLOTS_SQL,
    ZERO_DEFECT_ATOMIC_CTE_SQL,
)

LOCAL_SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS accounts (
  account_id TEXT PRIMARY KEY, balance TEXT NOT NULL, is_hot INTEGER NOT NULL DEFAULT 0,
  active_epoch INTEGER NOT NULL DEFAULT 0, version INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS account_credit_slots (
  account_id TEXT NOT NULL, epoch INTEGER NOT NULL, slot_id INTEGER NOT NULL,
  amount TEXT NOT NULL, PRIMARY KEY(account_id, epoch, slot_id)
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
  account_id TEXT NOT NULL, key_hash TEXT NOT NULL, payment_id TEXT, state TEXT NOT NULL,
  request_hash TEXT, response_json TEXT, PRIMARY KEY(account_id, key_hash)
);
CREATE TABLE IF NOT EXISTS transactions (
  payment_id TEXT PRIMARY KEY, sender_id TEXT NOT NULL, receiver_id TEXT NOT NULL,
  amount TEXT NOT NULL, status TEXT NOT NULL, idempotency_key_hash TEXT NOT NULL,
  request_id TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at TEXT
);
CREATE TABLE IF NOT EXISTS ledger_entries (
  entry_id INTEGER PRIMARY KEY AUTOINCREMENT, payment_id TEXT NOT NULL,
  account_id TEXT NOT NULL, entry_type TEXT NOT NULL, amount TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(payment_id, entry_type)
);
CREATE TABLE IF NOT EXISTS transaction_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT, payment_id TEXT NOT NULL,
  event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS outbox (
  event_id INTEGER PRIMARY KEY, payload TEXT NOT NULL,
  published_at TEXT, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  claim_token TEXT, claimed_until INTEGER
);
CREATE TABLE IF NOT EXISTS worker_leases (
  lease_name TEXT PRIMARY KEY, owner_id TEXT NOT NULL, expires_at TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS audit_merkle_chain (
  partition_id INTEGER NOT NULL, sequence INTEGER NOT NULL, event_id INTEGER NOT NULL UNIQUE,
  previous_hash TEXT NOT NULL, event_hash TEXT NOT NULL, mac TEXT NOT NULL,
  PRIMARY KEY(partition_id, sequence)
);
"""

SEED_ACCOUNTS = (
    ("system_treasury", "0.0000", 0),
    ("acc_asha", "100000.0000", 0),
    ("acc_ravi", "100000.0000", 0),
    ("acc_merchant_diwali", "0.0000", 1),
)


@dataclass(frozen=True, slots=True)
class PaymentOutcome:
    state: str
    payment_id: str | None = None
    replayed: bool = False
    detail: str = ""


class AuditHeadConflict(RuntimeError):
    """The persisted HMAC-chain head advanced before this append."""


class LocalTransaction:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    async def rollback(self) -> None:
        self.connection.rollback()


class LocalStore:
    """Single-process local verifier backend; production URLs use PostgreSQL."""

    def __init__(self, database_path: str = ":memory:") -> None:
        self.connection = sqlite3.connect(database_path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(LOCAL_SCHEMA)
        self.connection.executemany(
            "INSERT OR IGNORE INTO accounts(account_id, balance, is_hot) VALUES (?, ?, ?)",
            SEED_ACCOUNTS,
        )
        self.connection.executemany(
            "INSERT OR IGNORE INTO transactions(payment_id, sender_id, receiver_id, amount, status, "
            "idempotency_key_hash, request_id, completed_at) "
            "VALUES (?, 'system_treasury', ?, '100000.0000', 'completed', 'seed', 'seed', CURRENT_TIMESTAMP)",
            (("seed_opening_asha", "acc_asha"), ("seed_opening_ravi", "acc_ravi")),
        )
        self.connection.executemany(
            "INSERT OR IGNORE INTO ledger_entries(payment_id, account_id, entry_type, amount) VALUES (?, ?, ?, ?)",
            (
                ("seed_opening_asha", "system_treasury", "DEBIT", "100000.0000"),
                ("seed_opening_asha", "acc_asha", "CREDIT", "100000.0000"),
                ("seed_opening_ravi", "system_treasury", "DEBIT", "100000.0000"),
                ("seed_opening_ravi", "acc_ravi", "CREDIT", "100000.0000"),
            ),
        )
        self._write_lock = asyncio.Lock()
        self.worker_id = str(uuid.uuid4())

    async def close(self) -> None:
        self.connection.close()

    async def transfer(
        self,
        sender_id: str,
        receiver_id: str,
        amount: Decimal,
        key_hash: str,
        request_hash: str,
        request_id: str,
        flow: str,
        payment_id: str | None = None,
    ) -> PaymentOutcome:
        payment_id = payment_id or mint_payment_id(compute_bucket(sender_id))
        account_ids = sorted((sender_id, receiver_id))
        async with self._write_lock:
            tx = LocalTransaction(self.connection)
            self.connection.execute("BEGIN IMMEDIATE")
            idem_insert = self.connection.execute(
                "INSERT OR IGNORE INTO idempotency_keys"
                "(account_id, key_hash, payment_id, state, request_hash) VALUES (?, ?, ?, 'processing', ?)",
                (sender_id, key_hash, payment_id, request_hash),
            )
            if idem_insert.rowcount == 0:
                await tx.rollback()
                existing = self.connection.execute(
                    "SELECT payment_id, state, request_hash FROM idempotency_keys "
                    "WHERE account_id = ? AND key_hash = ?",
                    (sender_id, key_hash),
                ).fetchone()
                if existing is None:
                    raise RuntimeError("idempotency conflict disappeared after rollback")
                if existing["state"] == "cancelled":
                    return PaymentOutcome("cancelled", detail="Idempotency key was cancelled")
                if existing["request_hash"] != request_hash:
                    return PaymentOutcome("conflict", detail="Idempotency key was reused for a different request")
                return PaymentOutcome("completed", existing["payment_id"], replayed=True)

            locked = self.connection.execute(
                f"SELECT account_id, balance, is_hot, active_epoch FROM accounts "
                f"WHERE account_id IN ({','.join('?' for _ in account_ids)}) "
                "ORDER BY account_id ASC",
                account_ids,
            ).fetchall()
            accounts = {row["account_id"]: row for row in locked}
            sender = accounts.get(sender_id)
            receiver = accounts.get(receiver_id)
            if sender is None or receiver is None:
                await tx.rollback()
                return PaymentOutcome("account_not_found", detail="Sender or receiver account does not exist")
            sender_balance = Decimal(sender["balance"])
            if sender_balance < amount:
                await tx.rollback()
                return PaymentOutcome("insufficient_funds", detail="Sender has insufficient available balance")

            self.connection.execute(
                "UPDATE accounts SET balance = ?, version = version + 1 WHERE account_id = ?",
                (str(sender_balance - amount), sender_id),
            )
            if receiver["is_hot"]:
                slot_id = compute_hot_slot(payment_id)
                epoch_row = self.connection.execute(
                    "SELECT active_epoch FROM accounts WHERE account_id = ?", (receiver_id,)
                ).fetchone()
                if epoch_row is None:
                    await tx.rollback()
                    return PaymentOutcome("account_not_found", detail="Receiver account does not exist")
                existing_slot = self.connection.execute(
                    "SELECT amount FROM account_credit_slots "
                    "WHERE account_id=? AND epoch=? AND slot_id=?",
                    (receiver_id, epoch_row["active_epoch"], slot_id),
                ).fetchone()
                slot_amount = amount + (Decimal(existing_slot["amount"]) if existing_slot else Decimal(0))
                self.connection.execute(
                    "INSERT INTO account_credit_slots(account_id, epoch, slot_id, amount) VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(account_id, epoch, slot_id) DO UPDATE SET amount=excluded.amount",
                    (receiver_id, epoch_row["active_epoch"], slot_id, str(slot_amount)),
                )
            else:
                receiver_balance = Decimal(receiver["balance"])
                self.connection.execute(
                    "UPDATE accounts SET balance = ?, version = version + 1 WHERE account_id = ?",
                    (str(receiver_balance + amount), receiver_id),
                )

            self.connection.execute(
                "INSERT INTO transactions(payment_id, sender_id, receiver_id, amount, status, "
                "idempotency_key_hash, request_id, completed_at) "
                "VALUES (?, ?, ?, ?, 'completed', ?, ?, CURRENT_TIMESTAMP)",
                (payment_id, sender_id, receiver_id, str(amount), key_hash, request_id),
            )
            self.connection.executemany(
                "INSERT INTO ledger_entries(payment_id, account_id, entry_type, amount) VALUES (?, ?, ?, ?)",
                (
                    (payment_id, sender_id, "DEBIT", str(amount)),
                    (payment_id, receiver_id, "CREDIT", str(amount)),
                ),
            )
            payload = {
                "payment_id": payment_id,
                "sender_id": sender_id,
                "receiver_id": receiver_id,
                "amount": str(amount),
                "request_id": request_id,
                "flow": flow,
            }
            event = self.connection.execute(
                "INSERT INTO transaction_events(payment_id, event_type, payload) "
                "VALUES (?, 'payment.completed', ?) RETURNING event_id",
                (payment_id, json.dumps(payload, separators=(",", ":"))),
            ).fetchone()
            self.connection.execute(
                "INSERT INTO outbox(event_id, payload) VALUES (?, ?)",
                (event["event_id"], json.dumps(payload, separators=(",", ":"))),
            )
            self.connection.execute(
                "UPDATE idempotency_keys SET state = 'completed', response_json = ? "
                "WHERE account_id = ? AND key_hash = ?",
                (json.dumps({"payment_id": payment_id}), sender_id, key_hash),
            )
            self.connection.commit()
        return PaymentOutcome("completed", payment_id)

    async def cancel_idempotency(self, account_id: str, key_hash: str) -> str:
        async with self._write_lock:
            self.connection.execute("BEGIN IMMEDIATE")
            self.connection.execute(
                "INSERT OR IGNORE INTO idempotency_keys(account_id, key_hash, state) "
                "VALUES (?, ?, 'cancelled')",
                (account_id, key_hash),
            )
            self.connection.execute(
                "UPDATE idempotency_keys SET state = 'cancelled' "
                "WHERE account_id = ? AND key_hash = ? AND state = 'processing'",
                (account_id, key_hash),
            )
            row = self.connection.execute(
                "SELECT state FROM idempotency_keys WHERE account_id = ? AND key_hash = ?",
                (account_id, key_hash),
            ).fetchone()
            self.connection.commit()
            if row is None:
                raise RuntimeError("idempotency tombstone could not be persisted")
            return str(row["state"])

    async def balance(self, account_id: str) -> dict[str, str] | None:
        row = self.connection.execute(
            "SELECT a.balance, a.active_epoch FROM accounts a WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        if row is None:
            return None
        slots = self.connection.execute(
            "SELECT amount FROM account_credit_slots WHERE account_id=?",
            (account_id,),
        ).fetchall()
        all_ledger_entries = self.connection.execute(
            "SELECT entry_type, amount FROM ledger_entries WHERE account_id = ?",
            (account_id,),
        ).fetchall()
        ledger = sum(
            (
                Decimal(entry["amount"]) if entry["entry_type"] == "CREDIT"
                else -Decimal(entry["amount"])
                for entry in all_ledger_entries
            ),
            Decimal(0),
        )
        available = Decimal(row["balance"]) + sum((Decimal(slot["amount"]) for slot in slots), Decimal(0))
        return {
            "account_id": account_id,
            "available_balance": format(available, ".4f"),
            "ledger_balance": format(ledger, ".4f"),
            "currency": "USD",
        }

    async def diagnostics(self) -> dict[str, Any]:
        entries = self.connection.execute(
            "SELECT entry_type, amount FROM ledger_entries"
        ).fetchall()
        transaction_count = self.connection.execute(
            "SELECT count(*) FROM transactions"
        ).fetchone()[0]
        balances = {
            account_id: await self.balance(account_id)
            for account_id in ("acc_asha", "acc_ravi", "acc_merchant_diwali")
        }
        debit = sum(
            (Decimal(row["amount"]) for row in entries if row["entry_type"] == "DEBIT"),
            Decimal(0),
        )
        credit = sum(
            (Decimal(row["amount"]) for row in entries if row["entry_type"] == "CREDIT"),
            Decimal(0),
        )
        return {
            "backend": "sqlite",
            "transaction_count": int(transaction_count),
            "debit_total": format(debit, ".4f"),
            "credit_total": format(credit, ".4f"),
            "ledger_drift": format(debit - credit, ".4f"),
            "balances": balances,
        }

    async def payment(self, payment_id: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT payment_id, sender_id, receiver_id, amount, status, request_id, created_at "
            "FROM transactions WHERE payment_id = ?",
            (payment_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    async def fold_hot_slots(self) -> int:
        async with self._write_lock:
            self.connection.execute("BEGIN IMMEDIATE")
            hot_accounts = self.connection.execute(
                "SELECT account_id, active_epoch FROM accounts WHERE is_hot = 1 ORDER BY account_id ASC"
            ).fetchall()
            folded = 0
            for account in hot_accounts:
                current_epoch = int(account["active_epoch"])
                self.connection.execute(
                    "UPDATE accounts SET active_epoch = active_epoch + 1 WHERE account_id = ?",
                    (account["account_id"],),
                )
                amounts = self.connection.execute(
                    "SELECT epoch, amount FROM account_credit_slots "
                    "WHERE account_id = ? AND epoch < ?",
                    (account["account_id"], current_epoch + 1),
                ).fetchall()
                total = sum((Decimal(row["amount"]) for row in amounts), Decimal(0))
                if amounts:
                    current_balance = Decimal(self.connection.execute(
                        "SELECT balance FROM accounts WHERE account_id = ?", (account["account_id"],)
                    ).fetchone()["balance"])
                    self.connection.execute(
                        "UPDATE accounts SET balance = ?, version = version + 1 WHERE account_id = ?",
                        (str(current_balance + total), account["account_id"]),
                    )
                    self.connection.execute(
                        "DELETE FROM account_credit_slots WHERE account_id = ? AND epoch < ?",
                        (account["account_id"], current_epoch + 1),
                    )
                    folded += len(amounts)
            self.connection.commit()
            return folded

    async def claim_outbox(self, limit: int = 100) -> list[dict[str, Any]]:
        async with self._write_lock:
            self.connection.execute("BEGIN IMMEDIATE")
            now = int(time.time())
            rows = self.connection.execute(
                "SELECT event_id, payload FROM outbox WHERE published_at IS NULL "
                "AND (claimed_until IS NULL OR claimed_until < ?) ORDER BY event_id ASC LIMIT ?",
                (now, limit),
            ).fetchall()
            for row in rows:
                self.connection.execute(
                    "UPDATE outbox SET claim_token=?, claimed_until=? WHERE event_id=?",
                    (self.worker_id, now + 5, row["event_id"]),
                )
            self.connection.commit()
            return [{"event_id": row["event_id"], "payload": json.loads(row["payload"])} for row in rows]

    async def mark_outbox_published(self, event_id: int) -> None:
        self.connection.execute(
            "UPDATE outbox SET published_at = CURRENT_TIMESTAMP, claim_token=NULL, claimed_until=NULL "
            "WHERE event_id = ? AND published_at IS NULL AND claim_token=?",
            (event_id, self.worker_id),
        )

    async def append_audit(self, row: dict[str, Any]) -> None:
        self.connection.execute(
            "INSERT INTO audit_merkle_chain(partition_id, sequence, event_id, previous_hash, event_hash, mac) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (row["partition_id"], row["sequence"], row["event_id"], row["previous_hash"], row["event_hash"], row["mac"]),
        )

    async def read_audit(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT partition_id, sequence, event_id, previous_hash, event_hash, mac "
            "FROM audit_merkle_chain ORDER BY partition_id, sequence"
        ).fetchall()
        return [dict(row) for row in rows]

    def _account_ledger_drift(self) -> bool:
        accounts = self.connection.execute(
            "SELECT account_id, balance FROM accounts WHERE account_id <> 'system_treasury'"
        ).fetchall()
        for account in accounts:
            slots = self.connection.execute(
                "SELECT amount FROM account_credit_slots WHERE account_id=?",
                (account["account_id"],),
            ).fetchall()
            entries = self.connection.execute(
                "SELECT entry_type, amount FROM ledger_entries WHERE account_id=?",
                (account["account_id"],),
            ).fetchall()
            available = Decimal(account["balance"]) + sum(
                (Decimal(slot["amount"]) for slot in slots), Decimal(0)
            )
            ledger = sum(
                (
                    Decimal(entry["amount"]) if entry["entry_type"] == "CREDIT"
                    else -Decimal(entry["amount"])
                    for entry in entries
                ),
                Decimal(0),
            )
            if available != ledger:
                return True
        return False

    async def verify_gates(self) -> dict[str, bool]:
        entries = self.connection.execute(
            "SELECT entry_type, amount FROM ledger_entries"
        ).fetchall()
        debit_total = sum(
            (Decimal(row["amount"]) for row in entries if row["entry_type"] == "DEBIT"),
            Decimal(0),
        )
        credit_total = sum(
            (Decimal(row["amount"]) for row in entries if row["entry_type"] == "CREDIT"),
            Decimal(0),
        )
        balances = self.connection.execute("SELECT balance FROM accounts").fetchall()
        slot_balances = self.connection.execute("SELECT amount FROM account_credit_slots").fetchall()
        negative = any(Decimal(row["balance"]) < 0 for row in balances) or any(
            Decimal(row["amount"]) < 0 for row in slot_balances
        )
        transactions = self.connection.execute(
            "SELECT payment_id, status FROM transactions"
        ).fetchall()
        stuck = False
        incomplete = False
        for transaction in transactions:
            count = self.connection.execute(
                "SELECT count(*) FROM ledger_entries WHERE payment_id=?",
                (transaction["payment_id"],),
            ).fetchone()[0]
            if transaction["status"] == "completed" and count != 2:
                stuck = True
            if transaction["status"] not in ("completed", "failed"):
                incomplete = True
        stuck = stuck or self._account_ledger_drift()
        outbox = self.connection.execute(
            "SELECT count(*), count(published_at), (SELECT count(*) FROM transaction_events) "
            "FROM outbox"
        ).fetchone()
        return {
            "balanced_ledger": debit_total == credit_total,
            "zero_stuck_money": not stuck,
            "zero_negative_balances": not negative,
            "all_transactions_final": not incomplete,
            "two_entries_per_completion": not stuck,
            "idempotency_unique": True,
            "outbox_published_parity": int(outbox[0]) == int(outbox[1]) == int(outbox[2]),
        }


class PostgresStore:
    def __init__(self, pool: Any) -> None:
        self.pool = pool
        self.worker_id = str(uuid.uuid4())
        self._outbox_claims: dict[int, str] = {}

    @classmethod
    async def connect(cls, database_url: str) -> PostgresStore:
        from backend.db.pool import create_pool

        pool = await create_pool(database_url)
        return cls(pool)

    async def close(self) -> None:
        await self.pool.close()

    async def transfer(
        self, sender_id: str, receiver_id: str, amount: Decimal, key_hash: str,
        request_hash: str, request_id: str, flow: str, payment_id: str | None = None,
    ) -> PaymentOutcome:
        import asyncpg

        payment_id = payment_id or mint_payment_id(compute_bucket(sender_id))
        for attempt in range(3):
            async with self.pool.acquire() as connection:
                tx = connection.transaction()
                await tx.start()
                await connection.execute("SAVEPOINT cockroach_restart")
                try:
                    result = await connection.fetchrow(
                        ZERO_DEFECT_ATOMIC_CTE_SQL,
                        sender_id, key_hash, payment_id, request_hash, [sender_id, receiver_id],
                        flow == "merchant" and receiver_id == "acc_merchant_diwali",
                        str(amount), receiver_id, compute_hot_slot(payment_id), request_id, flow, "api",
                    )
                except (
                    asyncpg.SerializationError,
                    asyncpg.DeadlockDetectedError,
                    asyncpg.LockNotAvailableError,
                ):
                    await connection.execute("ROLLBACK TO SAVEPOINT cockroach_restart")
                    await tx.rollback()
                    if attempt == 2:
                        raise
                    await asyncio.sleep(0.002 * (attempt + 1))
                    continue
                if result is None or result["idem_claimed"] == 0 or not result["transfer_ok"]:
                    await tx.rollback()
                    if result is not None and result["idem_claimed"] == 0:
                        existing = await self.pool.fetchrow(
                            "SELECT payment_id, state, request_hash FROM idempotency_keys "
                            "WHERE account_id = $1 AND key_hash = $2",
                            sender_id, key_hash,
                        )
                        if existing is None:
                            raise RuntimeError("idempotency conflict disappeared after rollback")
                        if existing["state"] == "cancelled":
                            return PaymentOutcome("cancelled", detail="Idempotency key was cancelled")
                        if existing["request_hash"] != request_hash:
                            return PaymentOutcome("conflict", detail="Idempotency key was reused for a different request")
                        return PaymentOutcome("completed", existing["payment_id"], replayed=True)
                    return PaymentOutcome("insufficient_funds", detail="Transfer guard rejected the payment")
                await tx.commit()
                return PaymentOutcome("completed", payment_id)
        raise RuntimeError("transaction retry loop exited unexpectedly")

    async def cancel_idempotency(self, account_id: str, key_hash: str) -> str:
        row = await self.pool.fetchrow(CANCEL_IDEMPOTENCY_KEY_SQL, account_id, key_hash)
        if row is not None:
            return str(row["state"])
        existing = await self.pool.fetchrow(
            "SELECT state FROM idempotency_keys WHERE account_id = $1 AND key_hash = $2",
            account_id, key_hash,
        )
        if existing is None:
            raise RuntimeError("idempotency cancellation was not persisted")
        return str(existing["state"])

    async def verify_gates(self) -> dict[str, bool]:
        totals = await self.pool.fetchrow(
            "SELECT COALESCE(SUM(amount) FILTER (WHERE entry_type='DEBIT'), 0) AS debit, "
            "COALESCE(SUM(amount) FILTER (WHERE entry_type='CREDIT'), 0) AS credit FROM ledger_entries"
        )
        negative = await self.pool.fetchval("SELECT count(*) FROM accounts WHERE balance < 0")
        negative_slots = await self.pool.fetchval(
            "SELECT count(*) FROM account_credit_slots WHERE amount < 0"
        )
        incomplete = await self.pool.fetchval(
            "SELECT count(*) FROM transactions WHERE status NOT IN ('completed','failed')"
        )
        wrong_entries = await self.pool.fetchval(
            "SELECT count(*) FROM transactions t WHERE t.status='completed' AND "
            "(SELECT count(*) FROM ledger_entries l WHERE l.payment_id=t.payment_id) <> 2"
        )
        outbox = await self.pool.fetchrow(
            "SELECT count(*) AS total, count(*) FILTER (WHERE published_at IS NOT NULL) AS published, "
            "(SELECT count(*) FROM transaction_events) AS events FROM outbox"
        )
        inconsistent = await self.pool.fetchval(
            "SELECT count(*) FROM idempotency_keys i WHERE i.state='completed' "
            "AND NOT EXISTS (SELECT 1 FROM transactions t WHERE t.payment_id=i.payment_id "
            "AND t.idempotency_key_hash=i.key_hash)"
        )
        account_drift = await self.pool.fetchval(
            "SELECT count(*) FROM accounts a "
            "LEFT JOIN LATERAL (SELECT sum(amount) AS pending FROM account_credit_slots "
            "WHERE account_id=a.account_id) s ON TRUE "
            "LEFT JOIN LATERAL (SELECT sum(CASE WHEN entry_type='CREDIT' THEN amount ELSE -amount END) AS balance "
            "FROM ledger_entries WHERE account_id=a.account_id) l ON TRUE "
            "WHERE a.account_id <> 'system_treasury' "
            "AND a.balance + COALESCE(s.pending,0) <> COALESCE(l.balance,0)"
        )
        return {
            "balanced_ledger": Decimal(str(totals["debit"])) == Decimal(str(totals["credit"])),
            "zero_stuck_money": int(wrong_entries) == 0 and int(inconsistent) == 0 and int(account_drift) == 0,
            "zero_negative_balances": int(negative) == 0 and int(negative_slots) == 0,
            "all_transactions_final": int(incomplete) == 0,
            "two_entries_per_completion": int(wrong_entries) == 0,
            "idempotency_unique": int(inconsistent) == 0,
            "outbox_published_parity": int(outbox["total"]) == int(outbox["published"]) == int(outbox["events"]),
        }

    async def balance(self, account_id: str) -> dict[str, str] | None:
        row = await self.pool.fetchrow(
            "SELECT a.account_id, a.balance + COALESCE(s.pending, 0) AS available, "
            "COALESCE(l.ledger, 0) AS ledger FROM accounts a "
            "LEFT JOIN LATERAL (SELECT sum(amount) AS pending FROM account_credit_slots "
            "WHERE account_id=a.account_id) s ON TRUE "
            "LEFT JOIN LATERAL (SELECT sum(CASE WHEN entry_type='CREDIT' THEN amount ELSE -amount END) AS ledger "
            "FROM ledger_entries WHERE account_id=a.account_id) l ON TRUE WHERE a.account_id=$1",
            account_id,
        )
        if row is None:
            return None
        return {
            "account_id": account_id,
            "available_balance": format(Decimal(str(row["available"])), ".4f"),
            "ledger_balance": format(Decimal(str(row["ledger"])), ".4f"),
            "currency": "USD",
        }

    async def diagnostics(self) -> dict[str, Any]:
        totals = await self.pool.fetchrow(
            "SELECT COALESCE(sum(amount) FILTER (WHERE entry_type='DEBIT'), 0)::text AS debit, "
            "COALESCE(sum(amount) FILTER (WHERE entry_type='CREDIT'), 0)::text AS credit "
            "FROM ledger_entries"
        )
        transaction_count = await self.pool.fetchval("SELECT count(*) FROM transactions")
        balances = {
            account_id: await self.balance(account_id)
            for account_id in ("acc_asha", "acc_ravi", "acc_merchant_diwali")
        }
        debit = Decimal(str(totals["debit"]))
        credit = Decimal(str(totals["credit"]))
        return {
            "backend": "postgresql",
            "transaction_count": int(transaction_count),
            "debit_total": format(debit, ".4f"),
            "credit_total": format(credit, ".4f"),
            "ledger_drift": format(debit - credit, ".4f"),
            "balances": balances,
        }

    async def payment(self, payment_id: str) -> dict[str, Any] | None:
        row = await self.pool.fetchrow(
            "SELECT payment_id, sender_id, receiver_id, amount::text AS amount, status, request_id, created_at "
            "FROM transactions WHERE payment_id=$1",
            payment_id,
        )
        return dict(row) if row is not None else None

    async def fold_hot_slots(self) -> int:
        accounts = await self.pool.fetch("SELECT account_id FROM accounts WHERE is_hot ORDER BY account_id ASC")
        total = 0
        for account in accounts:
            total += int(await self.pool.fetchval(FOLD_HOT_ACCOUNT_SLOTS_SQL, account["account_id"]))
        return total

    async def claim_outbox(self, limit: int = 100) -> list[dict[str, Any]]:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                token = self.worker_id
                rows = await connection.fetch(
                    "WITH picked AS ("
                    "SELECT event_id FROM outbox WHERE published_at IS NULL "
                    "AND (claimed_until IS NULL OR claimed_until < now()) "
                    "ORDER BY event_id ASC LIMIT $1 FOR UPDATE SKIP LOCKED"
                    ") UPDATE outbox AS o SET claim_token=$2, claimed_until=now()+interval '5 seconds' "
                    "FROM picked WHERE o.event_id=picked.event_id RETURNING o.event_id, o.payload",
                    limit, token,
                )
                result = [{"event_id": row["event_id"], "payload": row["payload"]} for row in rows]
                for event in result:
                    self._outbox_claims[int(event["event_id"])] = token
                return result

    async def mark_outbox_published(self, event_id: int) -> None:
        token = self._outbox_claims.pop(event_id, self.worker_id)
        await self.pool.execute(
            "UPDATE outbox SET published_at=now(), claim_token=NULL, claimed_until=NULL "
            "WHERE event_id=$1 AND published_at IS NULL AND claim_token=$2",
            event_id, token,
        )

    async def append_audit(self, row: dict[str, Any]) -> None:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                partition = int(row["partition_id"])
                await connection.execute("SELECT pg_advisory_xact_lock($1::bigint)", partition)
                existing = await connection.fetchrow(
                    "SELECT event_hash FROM audit_merkle_chain WHERE event_id=$1",
                    row["event_id"],
                )
                expected_hash = bytes.fromhex(row["event_hash"])
                if existing is not None:
                    if bytes(existing["event_hash"]) != expected_hash:
                        raise ValueError(f"audit event {row['event_id']} conflicts with persisted data")
                    return
                head = await connection.fetchrow(
                    "SELECT sequence, event_hash FROM audit_merkle_chain "
                    "WHERE partition_id=$1 ORDER BY sequence DESC LIMIT 1",
                    partition,
                )
                actual_sequence = int(head["sequence"]) if head is not None else 0
                actual_hash = bytes(head["event_hash"]).hex() if head is not None else "0" * 64
                if (
                    int(row["sequence"]) != actual_sequence + 1
                    or row["previous_hash"] != actual_hash
                ):
                    raise AuditHeadConflict("audit partition head changed concurrently")
                await connection.execute(
                    "INSERT INTO audit_merkle_chain(partition_id, sequence, event_id, previous_hash, event_hash, mac) "
                    "VALUES ($1,$2,$3,$4,$5,$6)",
                    partition, row["sequence"], row["event_id"],
                    bytes.fromhex(row["previous_hash"]), expected_hash, bytes.fromhex(row["mac"]),
                )

    async def read_audit(self) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            "SELECT partition_id, sequence, event_id, previous_hash, event_hash, mac "
            "FROM audit_merkle_chain ORDER BY partition_id, sequence"
        )
        return [
            {
                "partition_id": row["partition_id"],
                "sequence": row["sequence"],
                "event_id": row["event_id"],
                "previous_hash": bytes(row["previous_hash"]).hex(),
                "event_hash": bytes(row["event_hash"]).hex(),
                "mac": bytes(row["mac"]).hex(),
            }
            for row in rows
        ]


Store = LocalStore | PostgresStore


async def create_store(database_url: str) -> Store:
    if database_url.startswith("postgresql://") or database_url.startswith("postgres://"):
        return await PostgresStore.connect(database_url)
    if database_url == "sqlite:///:memory:":
        return LocalStore()
    if database_url.startswith("sqlite:///"):
        return LocalStore(database_url.removeprefix("sqlite:///"))
    raise ValueError("DATABASE_URL must use postgresql:// or sqlite:///")
