from __future__ import annotations

import hashlib
import secrets

BUCKET_COUNT = 1024
BUCKET_MASK = BUCKET_COUNT - 1
SLOT_MASK = 63


def compute_bucket(account_id: str) -> int:
    digest = hashlib.blake2b(account_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") & BUCKET_MASK


def mint_payment_id(bucket: int, nonce: int | None = None) -> str:
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError("bucket must be between 0 and 1023")
    value = secrets.randbits(118) if nonce is None else nonce
    if not 0 <= value < (1 << 118):
        raise ValueError("nonce must fit in 118 bits")
    return f"{(value << 10) | bucket:032x}"


def extract_bucket_from_payment_id(payment_id: str) -> int:
    if len(payment_id) != 32:
        raise ValueError("payment_id must contain exactly 32 hexadecimal characters")
    if any(character not in "0123456789abcdefABCDEF" for character in payment_id):
        raise ValueError("payment_id must contain only hexadecimal characters")
    try:
        value = int(payment_id, 16)
    except ValueError as exc:
        raise ValueError("payment_id must be a hexadecimal identifier") from exc
    if value < 0 or value >= (1 << 128):
        raise ValueError("payment_id must be a 128-bit identifier")
    return value & BUCKET_MASK


def compute_hot_slot(txn_id: str) -> int:
    digest = hashlib.blake2b(txn_id.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") & SLOT_MASK
