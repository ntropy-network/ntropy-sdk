from datetime import datetime
from enum import Enum
import time
from typing import List, Optional, TYPE_CHECKING
import uuid

from pydantic import BaseModel, Field

from ntropy_sdk.v2 import NtropyBatchError
from ntropy_sdk.utils import pydantic_json
from ntropy_sdk.paging import PagedResponse
from ntropy_sdk.transactions import (
    EnrichedTransaction,
    EnrichmentInput,
    TransactionInput,
)

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs, NtropyTimeoutError
    from ntropy_sdk import SDK
    from typing_extensions import Unpack


class BatchStatus(str, Enum):
    PROCESSING = "processing"
    COMPLETED = "completed"
    ERROR = "error"


class Batch(BaseModel):
    """
    The `Batch` object represents the status and progress of an asynchronous batch enrichment job.
    """

    id: str = Field(description="A unique identifier for the batch.")
    status: BatchStatus = Field(description="The current status of the batch.")
    created_at: datetime = Field(
        description="The timestamp of when the batch was created."
    )
    updated_at: datetime = Field(
        description="The timestamp of when the batch was last updated."
    )
    progress: int = Field(description="The number of transactions processed so far.")
    total: int = Field(description="The total number of transactions in the batch.")
    request_id: Optional[str] = None

    def is_completed(self):
        return self.status == BatchStatus.COMPLETED

    def is_error(self):
        return self.status == BatchStatus.ERROR


class EnrichmentResult(BaseModel):
    transactions: List[EnrichedTransaction]


class BatchResult(BaseModel):
    """
    The `BatchResult` object represents the result of a batch enrichment job, including its status and
    enriched transactions.
    """

    id: str = Field(description="A unique identifier for the batch.")
    total: int = Field(
        description="The total number of transactions in the batch result."
    )
    status: BatchStatus = Field(description="The current status of the batch job.")
    results: EnrichmentResult = Field(
        description="A list of enriched transactions resulting from the enrichment of this batch."
    )
    request_id: Optional[str] = None


class BatchesResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def list(
        self,
        *,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        status: Optional[BatchStatus] = None,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> PagedResponse[Batch]:
        """List all batches"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url="/v3/batches",
            params={
                "created_before": created_before,
                "created_after": created_after,
                "cursor": cursor,
                "limit": limit,
                "status": status.value if status else None,
            },
            **extra_kwargs,
        )
        page = PagedResponse[Batch](
            **resp.json(),
            request_id=resp.headers.get("x-request-id", request_id),
            _resource=self,
            _extra_kwargs=extra_kwargs,
        )
        for t in page.data:
            t.request_id = request_id
        return page

    def get(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]") -> Batch:
        """Retrieve a batch"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/batches/{id}",
            **extra_kwargs,
        )
        return Batch(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def create(
        self,
        transactions: List[TransactionInput],
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> Batch:
        """Submit a batch of transactions for enrichment"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/batches",
            payload_json_str=pydantic_json(EnrichmentInput(transactions=transactions)),
            **extra_kwargs,
        )
        return Batch(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def results(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]") -> BatchResult:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/batches/{id}/results",
            **extra_kwargs,
        )
        return BatchResult(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def wait_for_results(
        self,
        id: str,
        *,
        timeout: int = 10 * 60 * 60,
        poll_interval: int = 10,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> "BatchResult":
        """Continuously polls the status of this batch, blocking until the batch
        either succeeds or fails. Raises `NtropyTimeoutError` if the `timeout` is exceeded or `NtropyBatchError`
        if the batch contains errors."""

        finish_statuses = [BatchStatus.COMPLETED, BatchStatus.ERROR]
        start_time = time.monotonic()

        batch = None
        while time.monotonic() - start_time < timeout:
            batch = self._sdk.batches.get(id=id)
            if batch.status in finish_statuses:
                break
            time.sleep(poll_interval)

        if batch and batch.status not in finish_statuses:
            raise NtropyTimeoutError()
        if batch.is_error():
            raise NtropyBatchError("Some transactions contain errors", id=batch.id)
        return self._sdk.batches.results(id=id, **extra_kwargs)
