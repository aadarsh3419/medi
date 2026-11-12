from __future__ import annotations


class PaymentError(Exception):
    def __init__(
        self, status: int, code: str, title: str, detail: str, retryable: bool = False
    ) -> None:
        super().__init__(detail)
        self.status = status
        self.code = code
        self.title = title
        self.detail = detail
        self.retryable = retryable
