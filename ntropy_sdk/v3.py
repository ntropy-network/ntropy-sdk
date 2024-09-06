from datetime import datetime, date
from enum import Enum
from io import IOBase
import time
from typing import TYPE_CHECKING, Optional, Union
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
    size: int | None


class BankStatementJob(BaseModel):
    id: str
    name: str | None
    status: BankStatementJobStatus
    created_at: datetime
    file: BankStatementFile

    def wait(
        self,
        sdk: "SDK",
        timeout: int = 4 * 60 * 60,
        poll_interval: int = 10,
    ) -> "BankStatementResults":
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
    transactions: list[BankStatementTransaction]


class BankStatementResults(BankStatementJob):
    accounts: list[BankStatementAccount]


class BankStatementsResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def upload_pdf(
        self,
        file: Union[IOBase, bytes],
        filename: Optional[str] = None,
    ) -> BankStatementJob:
        resp = self._sdk.retry_ratelimited_request(
            "POST",
            "/v3/bank-statements",
            payload=None,
            files={
                "file": file if filename is None else (filename, file),
            },
        )
        return BankStatementJob(**resp.json())

    def get(self, id: str) -> BankStatementJob:
        resp = self._sdk.retry_ratelimited_request(
            "GET",
            f"/v3/bank-statements/{id}",
            payload=None,
        )
        return BankStatementJob(**resp.json())

    def results(self, id: str) -> BankStatementResults:
        resp = self._sdk.retry_ratelimited_request(
            "GET",
            f"/v3/bank-statements/{id}/results",
            payload=None,
        )
        return BankStatementResults(**resp.json())

    def statement_info(self, id: str) -> StatementInfo:
        resp = self._sdk.retry_ratelimited_request(
            "GET",
            f"/v3/bank-statements/{id}/statement-info",
            payload=None,
        )
        return StatementInfo(**resp.json())
