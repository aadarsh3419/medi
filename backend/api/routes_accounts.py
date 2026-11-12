from __future__ import annotations

from fastapi import APIRouter, Request

from backend.api.errors import PaymentError

router = APIRouter(prefix="/v2/accounts", tags=["accounts"])


@router.get("/{account_id}/balance")
async def get_balance(account_id: str, request: Request) -> dict[str, str]:
    result = await request.app.state.store.balance(account_id)
    if result is None:
        raise PaymentError(404, "ACCOUNT_NOT_FOUND", "Account not found", "The requested account does not exist")
    return result
