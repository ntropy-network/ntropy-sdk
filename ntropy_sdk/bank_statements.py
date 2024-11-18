import asyncio
from datetime import date, datetime
from enum import Enum
from io import IOBase
import time
from typing import List, Optional, TYPE_CHECKING, Union
import uuid

import aiohttp
from pydantic import BaseModel, Field, NonNegativeFloat

from ntropy_sdk.paging import PagedResponse
from ntropy_sdk.transactions import LocationInput, TransactionInput
from ntropy_sdk.utils import EntryType
from ntropy_sdk.v2.bank_statements import StatementInfo
from ntropy_sdk.v2.errors import (
    NtropyBankStatementError,
    NtropyDatasourceError,
    NtropyTimeoutError,
)

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs, ExtraKwargsAsync, SDK
    from ntropy_sdk.async_.sdk import AsyncSDK
    from ntropy_sdk.async_.paging import PagedResponse as PagedResponseAsync
    from typing_extensions import Unpack


class BankStatementJobStatus(str, Enum):
    PROCESSING = "processing"
    COMPLETED = "completed"
    ERROR = "error"


class BankStatementFile(BaseModel):
    no_pages: int
    size: Optional[int]


class BankStatementError(BaseModel):
    code: str
    message: str


class BankStatementJob(BaseModel):
    id: str
    name: Optional[str]
    status: BankStatementJobStatus
    created_at: datetime
    file: BankStatementFile
    request_id: Optional[str] = None
    error: Optional[BankStatementError] = None

    def is_completed(self):
        return self.status == BankStatementJobStatus.COMPLETED

    def is_error(self):
        return self.status == BankStatementJobStatus.ERROR

    class Config:
        extra = "allow"


class BankStatementTransaction(BaseModel):
    date: date
    entry_type: EntryType
    amount: NonNegativeFloat
    running_balance: Optional[float]
    currency: str = Field(
        description="The currency of the transaction in ISO 4217 format"
    )
    description: str
    id: str = Field(
        description="A generated unique identifier for the transaction", min_length=1
    )

    def to_transaction_input(
        self,
        *,
        account_holder_id: Optional[str] = None,
        location: Optional[LocationInput] = None,
    ) -> TransactionInput:
        return TransactionInput(
            id=self.id,
            description=self.description,
            date=self.date,
            amount=self.amount,
            entry_type=self.entry_type,
            currency=self.currency,
            account_holder_id=account_holder_id,
            location=location,
        )


class BankStatementAccount(BaseModel):
    number: Optional[str]
    opening_balance: Optional[float]
    closing_balance: Optional[float]
    start_date: Optional[date]
    end_date: Optional[date]
    is_balance_reconciled: Optional[bool]
    total_incoming: Optional[float]
    total_outgoing: Optional[float]
    transactions: List[BankStatementTransaction]
    request_id: Optional[str] = None


class BankStatementResults(BaseModel):
    accounts: List[BankStatementAccount]


class BankStatementsResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def list(
        self,
        *,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        status: Optional[BankStatementJobStatus] = None,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> PagedResponse[BankStatementJob]:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url="/v3/bank_statements",
            params={
                "created_before": created_before,
                "created_after": created_after,
                "cursor": cursor,
                "limit": limit,
                "status": status.value if status else None,
            },
            payload=None,
            **extra_kwargs,
        )
        page = PagedResponse[BankStatementJob](
            **resp.json(),
            request_id=resp.headers.get("x-request-id", request_id),
            _resource=self,
            _extra_kwargs=extra_kwargs,
        )
        for b in page.data:
            b.request_id = request_id
        return page

    def create(
        self,
        file: Union[IOBase, bytes],
        *,
        filename: Optional[str] = None,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> BankStatementJob:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/bank_statements",
            payload=None,
            files={
                "file": file if filename is None else (filename, file),
            },
            **extra_kwargs,
        )
        return BankStatementJob(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def get(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]") -> BankStatementJob:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/bank_statements/{id}",
            payload=None,
            **extra_kwargs,
        )
        return BankStatementJob(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def results(
        self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]"
    ) -> BankStatementResults:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/bank_statements/{id}/results",
            payload=None,
            **extra_kwargs,
        )
        return BankStatementResults(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def verify(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]") -> StatementInfo:
        """Waits for and returns preliminary statement information from the
        first page of the PDF. This may not always be consistent with the
        final results."""
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/bank_statements/{id}/verify",
            payload=None,
            **extra_kwargs,
        )
        return StatementInfo(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def wait_for_results(
        self,
        id: str,
        *,
        timeout: int = 10 * 60 * 60,
        poll_interval: int = 10,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> "BankStatementResults":
        """Continuously polls the status of this job, blocking until the job either succeeds
        or fails. If the job is successful, returns the results. Otherwise, raises an
        `NtropyBankStatementError` on a bank statement processing error or `NtropyTimeoutError`
        if the `timeout` is exceeded."""

        finish_statuses = [
            BankStatementJobStatus.COMPLETED,
            BankStatementJobStatus.ERROR,
        ]
        start_time = time.monotonic()
        stmt = None
        while time.monotonic() - start_time < timeout:
            stmt = self._sdk.bank_statements.get(id=id)
            if stmt.status in finish_statuses:
                break
            time.sleep(poll_interval)

        if stmt and stmt.status not in finish_statuses:
            raise NtropyTimeoutError()
        if stmt.is_error():
            assert stmt.error is not None
            raise NtropyBankStatementError(
                id=stmt.id, code=stmt.error.code, message=stmt.error.message
            )
        return self._sdk.bank_statements.results(id=id, **extra_kwargs)

    def delete(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]"):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="DELETE",
            url=f"/v3/bank_statements/{id}",
            payload=None,
            **extra_kwargs,
        )


class BankStatementsResourceAsync:
    def __init__(self, sdk: "AsyncSDK"):
        self._sdk = sdk

    async def list(
        self,
        *,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        status: Optional[BankStatementJobStatus] = None,
        **extra_kwargs: "Unpack[ExtraKwargsAsync]",
    ) -> PagedResponseAsync[BankStatementJob]:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = await self._sdk.retry_ratelimited_request(
            method="GET",
            url="/v3/bank_statements",
            params={
                "created_before": created_before,
                "created_after": created_after,
                "cursor": cursor,
                "limit": limit,
                "status": status.value if status else None,
            },
            payload=None,
            **extra_kwargs,
        )
        async with resp:
            page = PagedResponseAsync[BankStatementJob](
                **await resp.json(),
                request_id=resp.headers.get("x-request-id", request_id),
                _resource=self,
                _extra_kwargs=extra_kwargs,
            )
        for b in page.data:
            b.request_id = request_id
        return page

    async def create(
        self,
        file: Union[IOBase, bytes],
        *,
        filename: Optional[str] = None,
        **extra_kwargs: "Unpack[ExtraKwargsAsync]",
    ) -> BankStatementJob:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        data = {"file": file}
        if filename is not None:
            data = aiohttp.FormData()
            data.add_field("file", file, filename=filename)
        resp = await self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/bank_statements",
            payload=None,
            data=data,
            **extra_kwargs,
        )
        async with resp:
            return BankStatementJob(
                **await resp.json(),
                request_id=resp.headers.get("x-request-id", request_id),
            )

    async def get(
        self, id: str, **extra_kwargs: "Unpack[ExtraKwargsAsync]"
    ) -> BankStatementJob:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = await self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/bank_statements/{id}",
            payload=None,
            **extra_kwargs,
        )
        async with resp:
            return BankStatementJob(
                **await resp.json(),
                request_id=resp.headers.get("x-request-id", request_id),
            )

    async def results(
        self, id: str, **extra_kwargs: "Unpack[ExtraKwargsAsync]"
    ) -> BankStatementResults:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = await self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/bank_statements/{id}/results",
            payload=None,
            **extra_kwargs,
        )
        async with resp:
            return BankStatementResults(
                **await resp.json(),
                request_id=resp.headers.get("x-request-id", request_id),
            )

    async def verify(
        self, id: str, **extra_kwargs: "Unpack[ExtraKwargsAsync]"
    ) -> StatementInfo:
        """Waits for and returns preliminary statement information from the
        first page of the PDF. This may not always be consistent with the
        final results."""
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = await self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/bank_statements/{id}/verify",
            payload=None,
            **extra_kwargs,
        )
        async with resp:
            return StatementInfo(
                **await resp.json(),
                request_id=resp.headers.get("x-request-id", request_id),
            )

    async def wait_for_results(
        self,
        id: str,
        *,
        timeout: int = 10 * 60 * 60,
        poll_interval: int = 10,
        **extra_kwargs: "Unpack[ExtraKwargsAsync]",
    ) -> "BankStatementResults":
        """Continuously polls the status of this job, blocking until the job either succeeds
        or fails. If the job is successful, returns the results. Otherwise, raises an
        `NtropyBankStatementError` on a bank statement processing error or `NtropyTimeoutError`
        if the `timeout` is exceeded."""

        finish_statuses = [
            BankStatementJobStatus.COMPLETED,
            BankStatementJobStatus.ERROR,
        ]
        start_time = time.monotonic()
        stmt = None
        while time.monotonic() - start_time < timeout:
            stmt = await self.get(id=id)
            if stmt.status in finish_statuses:
                break
            await asyncio.sleep(poll_interval)

        if stmt and stmt.status not in finish_statuses:
            raise NtropyTimeoutError()
        if stmt.is_error():
            assert stmt.error is not None
            raise NtropyBankStatementError(
                id=stmt.id, code=stmt.error.code, message=stmt.error.message
            )
        return await self.results(id=id, **extra_kwargs)

    async def delete(self, id: str, **extra_kwargs: "Unpack[ExtraKwargsAsync]"):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        await self._sdk.retry_ratelimited_request(
            method="DELETE",
            url=f"/v3/bank_statements/{id}",
            payload=None,
            **extra_kwargs,
        )
