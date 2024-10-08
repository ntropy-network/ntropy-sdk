from datetime import date, datetime
from enum import Enum
from io import IOBase
import time
from typing import TYPE_CHECKING, List, Optional, Union
import uuid

from pydantic import BaseModel, Field, NonNegativeFloat

from ntropy_sdk.bank_statements import StatementInfo
from ntropy_sdk.errors import NtropyDatasourceError
from ntropy_sdk.utils import EntryType

if TYPE_CHECKING:
    from ntropy_sdk.ntropy_sdk import SDK
    from . import ExtraKwargs
    from typing_extensions import Unpack


class BankStatementJobStatus(str, Enum):
    PROCESSING = "processing"
    COMPLETED = "completed"
    ERROR = "error"


class BankStatementFile(BaseModel):
    no_pages: int
    size: Optional[int]


class BankStatementJob(BaseModel):
    id: str
    name: Optional[str]
    status: BankStatementJobStatus
    created_at: datetime
    file: BankStatementFile
    request_id: Optional[str] = None

    def wait(
        self,
        sdk: "SDK",
        *,
        timeout: int = 4 * 60 * 60,
        poll_interval: int = 10,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> "BankStatementResults":
        """Continuously polls the status of this job, blocking until the job either succeeds
        or fails. If the job is successful, returns the results. Otherwise, raises an
        `NtropyDatasourceError` exception."""

        finish_statuses = [
            BankStatementJobStatus.COMPLETED,
            BankStatementJobStatus.ERROR,
        ]
        start_time = time.monotonic()
        while time.monotonic() - start_time < timeout:
            self.status = sdk.v3.bank_statements.get(id=self.id).status
            if self.status in finish_statuses:
                break
            time.sleep(poll_interval)

        if self.status is BankStatementJobStatus.COMPLETED:
            return sdk.v3.bank_statements.results(id=self.id, **extra_kwargs)
        else:
            raise NtropyDatasourceError()

    def overview(self, sdk: "SDK") -> StatementInfo:
        """Convenience function for `sdk.v3.bank_statements.statement_info`."""
        return sdk.v3.bank_statements.overview(id=self.id)

    class Config:
        extra = "allow"


class BankStatementTransaction(BaseModel):
    date: date
    entry_type: EntryType
    amount: NonNegativeFloat
    running_balance: Optional[float]
    iso_currency_code: str = Field(
        description="The currency of the transaction in ISO 4217 format"
    )
    description: str
    transaction_id: str = Field(
        description="A generated unique identifier for the transaction", min_length=1
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


class BankStatementResults(BankStatementJob):
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
    ) -> List[BankStatementJob]:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            "GET",
            "/v3/bank_statements",
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
        return [
            BankStatementJob(**j, request_id=request_id) for j in resp.json()["data"]
        ]

    def upload_pdf(
        self,
        *,
        file: Union[IOBase, bytes],
        filename: Optional[str] = None,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> BankStatementJob:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            "POST",
            "/v3/bank_statements",
            payload=None,
            files={
                "file": file if filename is None else (filename, file),
            },
            **extra_kwargs,
        )
        return BankStatementJob(**resp.json(), request_id=request_id)

    def get(
        self, *, id: str, **extra_kwargs: "Unpack[ExtraKwargs]"
    ) -> BankStatementJob:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            "GET",
            f"/v3/bank_statements/{id}",
            payload=None,
            **extra_kwargs,
        )
        return BankStatementJob(**resp.json(), request_id=request_id)

    def results(
        self, *, id: str, **extra_kwargs: "Unpack[ExtraKwargs]"
    ) -> BankStatementResults:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            "GET",
            f"/v3/bank_statements/{id}/results",
            payload=None,
            **extra_kwargs,
        )
        return BankStatementResults(**resp.json(), request_id=request_id)

    def overview(
        self, *, id: str, **extra_kwargs: "Unpack[ExtraKwargs]"
    ) -> StatementInfo:
        """Waits for and returns preliminary statement information from the
        first page of the PDF. This may not always be consistent with the
        final results."""
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            "GET",
            f"/v3/bank_statements/{id}/overview",
            payload=None,
            **extra_kwargs,
        )
        return StatementInfo(**resp.json(), request_id=request_id)
