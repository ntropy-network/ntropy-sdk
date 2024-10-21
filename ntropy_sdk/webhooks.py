from datetime import datetime
from typing import List, Literal, Optional, TYPE_CHECKING, Union
import uuid

from pydantic import BaseModel, Field

from ntropy_sdk.paging import PagedResponse

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs
    from ntropy_sdk import SDK
    from typing_extensions import Unpack


class _Unset:
    pass


UNSET = _Unset()

WebhookEventType = Literal[
    "reports.resolved",
    "reports.rejected",
    "reports.pending",
    "bank_statements.processing",
    "bank_statements.processed",
    "bank_statements.failed",
    "batches.completed",
    "batches.error",
]


class Webhook(BaseModel):
    id: str = Field(
        description="A generated unique identifier for the webhook",
    )
    created_at: datetime = Field(
        description="The date and time when the webhook was created.",
    )
    url: str = Field(
        description="The URL of the webhook",
    )
    events: List[WebhookEventType] = Field(
        description="A list of events that this webhook subscribes to",
    )
    token: Optional[str] = Field(
        description="A secret string used to authenticate the webhook. This "
        "value will be included in the `X-Ntropy-Token` header when sending "
        "requests to the webhook",
    )
    enabled: bool = Field(
        description="Whether the webhook is enabled or not.",
    )
    request_id: Optional[str] = None

    def delete(self, sdk: "SDK", **extra_kwargs: "Unpack[ExtraKwargs]"):
        return sdk.webhooks.delete(self.id, **extra_kwargs)


class WebhooksResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def list(
        self,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> PagedResponse[Webhook]:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url="/v3/webhooks",
            **extra_kwargs,
        )
        page = PagedResponse[Webhook](
            **resp.json(),
            request_id=request_id,
            _resource=self,
            _extra_kwargs=extra_kwargs,
        )
        for w in page.data:
            w.request_id = request_id
        return page

    def create(
        self,
        *,
        url: str,
        events: List[WebhookEventType],
        token: Optional[str],
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> Webhook:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/webhooks",
            payload={
                "url": url,
                "events": events,
                "token": token,
            },
            **extra_kwargs,
        )
        return Webhook(**resp.json(), request_id=request_id)

    def get(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]") -> Webhook:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/webhooks/{id}",
            **extra_kwargs,
        )
        return Webhook(**resp.json(), request_id=request_id)

    def delete(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]"):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="DELETE",
            url=f"/v3/webhooks/{id}",
            **extra_kwargs,
        )

    def patch(
        self,
        id: str,
        *,
        url: Union[str, _Unset] = UNSET,
        events: Union[List[WebhookEventType], _Unset] = UNSET,
        token: Union[str, None, _Unset] = UNSET,
        enabled: Union[bool, _Unset] = UNSET,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        payload = {}
        if url is not UNSET:
            payload["url"] = url
        if events is not UNSET:
            payload["events"] = events
        if token is not UNSET:
            payload["token"] = token
        if enabled is not UNSET:
            payload["enabled"] = enabled

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="PATCH",
            url=f"/v3/webhooks/{id}",
            payload=payload,
            **extra_kwargs,
        )
