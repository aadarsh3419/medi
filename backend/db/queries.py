ZERO_DEFECT_ATOMIC_CTE_SQL = """
WITH claim_idem AS (
    INSERT INTO idempotency_keys(account_id, key_hash, payment_id, state, request_hash)
    VALUES ($1, $2, $3, 'processing', $4)
    ON CONFLICT (account_id, key_hash) DO NOTHING
    RETURNING account_id, key_hash
),
locked_accounts AS MATERIALIZED (
    SELECT a.account_id, a.balance, a.is_hot, a.active_epoch
    FROM accounts AS a
    WHERE a.account_id = ANY($5::text[])
      AND EXISTS (SELECT 1 FROM claim_idem)
      AND (NOT $6::boolean OR a.account_id = $1 OR NOT a.is_hot)
    ORDER BY a.account_id ASC
    FOR UPDATE
),
proposed_transaction AS MATERIALIZED (
    SELECT 1 WHERE EXISTS (SELECT 1 FROM claim_idem)
),
debit AS (
    UPDATE accounts AS a
    SET balance = a.balance - $7::numeric, version = version + 1, updated_at = now()
    FROM locked_accounts AS l
    WHERE a.account_id = $1
      AND l.account_id = $1
      AND a.balance >= $7::numeric
    RETURNING a.account_id
),
credit_regular AS (
    UPDATE accounts AS a
    SET balance = a.balance + $7::numeric, version = version + 1, updated_at = now()
    FROM locked_accounts AS l, debit AS d
    WHERE a.account_id = $8
      AND l.account_id = $8
      AND NOT l.is_hot
    RETURNING a.account_id
),
credit_hot AS (
    INSERT INTO account_credit_slots(account_id, epoch, slot_id, amount)
    SELECT $8, receiver.active_epoch, $9::smallint, $7::numeric
    FROM accounts AS receiver, debit AS d
    WHERE receiver.account_id = $8 AND receiver.is_hot AND $6::boolean
    ON CONFLICT (account_id, epoch, slot_id)
    DO UPDATE SET amount = account_credit_slots.amount + EXCLUDED.amount
    RETURNING account_id
),
transfer_result AS (
    SELECT EXISTS (SELECT 1 FROM debit)
       AND (EXISTS (SELECT 1 FROM credit_regular) OR EXISTS (SELECT 1 FROM credit_hot))
       AS transfer_ok
),
record_transaction AS (
    INSERT INTO transactions(payment_id, sender_id, receiver_id, amount, status,
                             idempotency_key_hash, request_id, completed_at)
    SELECT $3, $1, $8, $7::numeric,
           CASE WHEN transfer_ok THEN 'completed' ELSE 'failed' END,
           $2, $10, now()
    FROM transfer_result, proposed_transaction
    RETURNING payment_id, status
),
record_ledger AS (
    INSERT INTO ledger_entries(payment_id, account_id, entry_type, amount)
    SELECT $3, $1, 'DEBIT', $7::numeric
    FROM record_transaction
    WHERE record_transaction.status = 'completed'
    UNION ALL
    SELECT $3, $8, 'CREDIT', $7::numeric
    FROM record_transaction
    WHERE record_transaction.status = 'completed'
    RETURNING entry_id
),
record_event AS (
    INSERT INTO transaction_events(payment_id, event_type, payload)
    SELECT $3, 'payment.completed',
           jsonb_build_object('payment_id', $3, 'sender_id', $1, 'receiver_id', $8,
                              'amount', $7::numeric::text, 'request_id', $10,
                              'flow', $11::text, 'origin', $12::text)
    FROM record_transaction
    WHERE record_transaction.status = 'completed'
    RETURNING event_id, payload
),
record_outbox AS (
    INSERT INTO outbox(event_id, payload)
    SELECT event_id, payload FROM record_event
    RETURNING event_id
),
finish_idem AS (
    UPDATE idempotency_keys
    SET state = CASE WHEN (SELECT transfer_ok FROM transfer_result) THEN 'completed' ELSE 'processing' END
    WHERE account_id = $1 AND key_hash = $2
      AND EXISTS (SELECT 1 FROM claim_idem)
    RETURNING 1
)
SELECT (SELECT count(*) FROM claim_idem)::integer AS idem_claimed,
       (SELECT transfer_ok FROM transfer_result) AS transfer_ok,
       (SELECT status FROM record_transaction) AS status
"""

CANCEL_IDEMPOTENCY_KEY_SQL = """
INSERT INTO idempotency_keys(account_id, key_hash, state)
VALUES ($1, $2, 'cancelled')
ON CONFLICT (account_id, key_hash) DO UPDATE
SET state = 'cancelled'
WHERE idempotency_keys.state = 'processing'
RETURNING state
"""

FOLD_HOT_ACCOUNT_SLOTS_SQL = """
WITH locked_account AS MATERIALIZED (
    SELECT account_id, active_epoch
    FROM accounts
    WHERE account_id = $1 AND is_hot
    ORDER BY account_id ASC
    FOR UPDATE
),
locked_slots AS MATERIALIZED (
    SELECT slots.account_id, slots.epoch, slots.slot_id, slots.amount
    FROM account_credit_slots AS slots
    JOIN locked_account AS locked ON slots.account_id = locked.account_id
    WHERE slots.epoch < locked.active_epoch
    ORDER BY slots.account_id ASC, slots.epoch ASC, slots.slot_id ASC
    FOR UPDATE OF slots
),
totals AS (
    SELECT COALESCE(SUM(amount), 0) AS total FROM locked_slots
),
folded AS (
    UPDATE accounts AS a
    SET active_epoch = locked.active_epoch + 1,
        balance = a.balance + totals.total,
        version = version + 1,
        updated_at = now()
    FROM locked_account AS locked CROSS JOIN totals
    WHERE a.account_id = locked.account_id
    RETURNING a.account_id, locked.active_epoch
),
deleted_slots AS (
    DELETE FROM account_credit_slots AS slots
    USING locked_slots AS locked
    WHERE slots.account_id = locked.account_id
      AND slots.epoch = locked.epoch
      AND slots.slot_id = locked.slot_id
    RETURNING slots.slot_id
)
SELECT count(*)::integer FROM deleted_slots
"""
