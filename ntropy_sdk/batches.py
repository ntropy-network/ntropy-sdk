import time
import uuid
from datetime import datetime
from enum import Enum
from typing import List, Optional, TYPE_CHECKING, Callable

from pydantic import BaseModel, Field

from ntropy_sdk.paging import PagedResponse
from ntropy_sdk.transactions import (
    EnrichedTransaction,
)
from ntropy_sdk.utils import DEFAULT_WITH_PROGRESS
from ntropy_sdk.v2 import NtropyBatchError

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
    operation: str = Field(description="Operation for the batch")
    status: BatchStatus = Field(description="The current status of the batch.")
    created_at: datetime = Field(
        description="The timestamp of when the batch was created."
    )
    updated_at: datetime = Field(
        description="The timestamp of when the batch was last updated."
    )
    progress: int = Field(description="The number of requests processed so far.")
    total: int = Field(description="The total number of requests in the batch.")
    request_id: Optional[str] = None

    def is_completed(self):
        return self.status == BatchStatus.COMPLETED

    def is_error(self):
        return self.status == BatchStatus.ERROR


class BatchResult(BaseModel):
    """
    The `BatchResult` object represents the result of a batch enrichment job
    """

    id: str = Field(description="A unique identifier for the batch.")
    total: int = Field(
        description="The total number of transactions in the batch result."
    )
    status: BatchStatus = Field(description="The current status of the batch job.")
    results: List[EnrichedTransaction] = Field(
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
        operation: str,
        data: List[dict],
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
            payload={
                "operation": operation,
                "data": data,
            },
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

    def _wait(
        self,
        *,
        id: str,
        poll_interval: int,
        timeout: int,
        stop_fn: Callable[[Batch], bool],
    ) -> Batch:
        start_time = time.monotonic()
        batch = None
        while time.monotonic() - start_time < timeout:
            batch = self._sdk.batches.get(id=id)
            if stop_fn(batch):
                break
            time.sleep(poll_interval)
        return batch

    def _wait_with_progress(
        self,
        *,
        id: str,
        poll_interval: int,
        timeout: int,
        stop_fn: Callable[[Batch], bool],
    ) -> Batch:
        from tqdm.auto import tqdm

        start_time = time.monotonic()

        total_set = False
        with tqdm() as p:
            while time.monotonic() - start_time < timeout:
                batch = self._sdk.batches.get(id=id)
                if not total_set:
                    p.total = batch.total
                p.desc = batch.status
                p.update(batch.progress - p.n)

                if stop_fn(batch):
                    break
                time.sleep(poll_interval)
        return batch

    def wait_for_results(
        self,
        id: str,
        *,
        timeout: int = 10 * 60 * 60,
        poll_interval: int = 10,
        with_progress: bool = DEFAULT_WITH_PROGRESS,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> "BatchResult":
        """Continuously polls the status of this batch, blocking until the batch
        either succeeds or fails. Raises `NtropyTimeoutError` if the `timeout` is exceeded or `NtropyBatchError`
        if the batch encountered an error during processing."""

        finish_statuses = [BatchStatus.COMPLETED, BatchStatus.ERROR]
        stop_fn = lambda b: b.status in finish_statuses
        if with_progress:
            batch = self._wait_with_progress(
                id=id, poll_interval=poll_interval, timeout=timeout, stop_fn=stop_fn
            )
        else:
            batch = self._wait(
                id=id, poll_interval=poll_interval, timeout=timeout, stop_fn=stop_fn
            )

        if batch and batch.status not in finish_statuses:
            raise NtropyTimeoutError()
        if batch.is_error():
            raise NtropyBatchError("Batch terminated with an error", id=batch.id)
        return self._sdk.batches.results(id=id, **extra_kwargs)
