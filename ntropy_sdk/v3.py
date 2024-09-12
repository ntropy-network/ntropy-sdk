from datetime import datetime, date
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


class V3:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk
        self.bank_statements = BankStatementsResource(sdk)


class BankStatementJobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"


class BankStatementFile(BaseModel):
    no_pages: int
    size: Optional[int]


class BankStatementJob(BaseModel):
    id: str
    name: Optional[str]
    status: BankStatementJobStatus
    created_at: datetime
    file: BankStatementFile
    request_id: str

    def wait(
        self,
        sdk: "SDK",
        timeout: int = 4 * 60 * 60,
        poll_interval: int = 10,
    ) -> "BankStatementResults":
        """Continuously polls the status of this job, blocking until the job either succeeds
        or fails. If the job is successful, returns the results. Otherwise, raises an
        `NtropyDatasourceError` exception."""
        finish_statuses = [BankStatementJobStatus.PROCESSED, BankStatementJobStatus.FAILED]
        start_time = time.monotonic()
        while time.monotonic() - start_time < timeout:
            self.status = sdk.v3.bank_statements.get(self.id).status
            if self.status in finish_statuses:
                break
            time.sleep(poll_interval)

        if self.status is BankStatementJobStatus.PROCESSED:
            return sdk.v3.bank_statements.results(self.id)
        else:
            raise NtropyDatasourceError()

    def statement_info(self, sdk: "SDK") -> StatementInfo:
        """Convenience function for `sdk.v3.bank_statements.statement_info`."""
        return sdk.v3.bank_statements.statement_info(self.id)

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
    request_id: str


class BankStatementsResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def upload_pdf(
        self,
        file: Union[IOBase, bytes],
        filename: Optional[str] = None,
    ) -> BankStatementJob:
        request_id = uuid.uuid4().hex
        resp = self._sdk.retry_ratelimited_request(
            "POST",
            "/v3/bank-statements",
            payload=None,
            files={
                "file": file if filename is None else (filename, file),
            },
            request_id=request_id,
        )
        return BankStatementJob(**resp.json(), request_id=request_id)

    def get(self, id: str) -> BankStatementJob:
        request_id = uuid.uuid4().hex
        resp = self._sdk.retry_ratelimited_request(
            "GET",
            f"/v3/bank-statements/{id}",
            payload=None,
            request_id=request_id,
        )
        return BankStatementJob(**resp.json(), request_id=request_id)

    def results(self, id: str) -> BankStatementResults:
        request_id = uuid.uuid4().hex
        resp = self._sdk.retry_ratelimited_request(
            "GET",
            f"/v3/bank-statements/{id}/results",
            payload=None,
            request_id=request_id,
        )
        return BankStatementResults(**resp.json(), request_id=request_id)

    def statement_info(self, id: str) -> StatementInfo:
        """Waits for and returns preliminary statement information from the
        first page of the PDF. This may not always be consistent with the
        final results."""
        request_id = uuid.uuid4().hex
        resp = self._sdk.retry_ratelimited_request(
            "GET",
            f"/v3/bank-statements/{id}/statement-info",
            payload=None,
            request_id=request_id,
        )
        return StatementInfo(**resp.json(), request_id=request_id)
