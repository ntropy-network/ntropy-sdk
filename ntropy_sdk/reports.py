import uuid
from datetime import datetime
from typing import Optional, TYPE_CHECKING

from pydantic import BaseModel

from ntropy_sdk.paging import PagedResponse

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs
    from ntropy_sdk import SDK
    from typing_extensions import Unpack


class Report(BaseModel):
    id: str
    created_at: datetime
    status: str
    rejection_reason: Optional[str]

    transaction_id: str
    description: str
    fields: list[str]


class ReportResponse(Report):
    request_id: Optional[str] = None


class ReportsResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def create(
        self,
        *,
        transaction_id: str,
        description: str,
        fields: list[str],
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> ReportResponse:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/reports",
            payload={
                "transaction_id": transaction_id,
                "description": description,
                "fields": fields,
            },
            **extra_kwargs,
        )
        return ReportResponse(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def get(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]") -> Report:
        """Retrieve a report"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/reports/{id}",
            **extra_kwargs,
        )
        return ReportResponse(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def list(
        self,
        *,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        status: str = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> PagedResponse[ReportResponse]:
        """List all reports"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url="/v3/reports",
            params={
                "created_before": created_before,
                "created_after": created_after,
                "status": status,
                "cursor": cursor,
                "limit": limit,
            },
            **extra_kwargs,
        )
        page = PagedResponse[ReportResponse](
            **resp.json(),
            request_id=resp.headers.get("x-request-id", request_id),
            _resource=self,
            _extra_kwargs=extra_kwargs,
        )
        for t in page.data:
            t.request_id = request_id
        return page

    def delete(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]"):
        """Delete a report"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="DELETE",
            url=f"/v3/reports/{id}",
            **extra_kwargs,
        )
