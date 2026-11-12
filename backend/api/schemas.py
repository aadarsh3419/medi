from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator


class PaymentRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sender_id: Annotated[str, Field(min_length=1, max_length=128)]
    receiver_id: Annotated[str, Field(min_length=1, max_length=128)]
    amount: Annotated[str, Field(strict=True, min_length=1, max_length=32)]

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, value: str) -> str:
        try:
            amount = Decimal(value)
        except InvalidOperation as exc:
            raise ValueError("amount must be a decimal string") from exc
        if not amount.is_finite() or amount <= 0 or amount.as_tuple().exponent < -4:
            raise ValueError("amount must be positive with at most 4 decimal places")
        if amount.adjusted() > 13:
            raise ValueError("amount exceeds NUMERIC(18,4)")
        return format(amount, ".4f")


class CancelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account_id: Annotated[str, Field(min_length=1, max_length=128)]


class PaymentResponse(BaseModel):
    payment_id: str
    status: str
    amount: str
    sender_id: str
    receiver_id: str
    replayed: bool = False


class ProblemDetail(BaseModel):
    code: str
    title: str
    status: int
    detail: str
    request_id: str
    retryable: bool
