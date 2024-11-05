from typing import TYPE_CHECKING, Any, Dict, List, Optional
import uuid

from pydantic import BaseModel, Field


if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs
    from ntropy_sdk import SDK
    from typing_extensions import Unpack


Rule = Dict[str, Any]
Rules = List[Dict[str, Any]]


class TopLevelRule(BaseModel):
    id: str = Field(
        description="A generated unique identifier for the top level rule",
    )
    request_id: Optional[str] = None

    class Config:
        extra = "allow"


class RulesResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def create(
        self,
        rule: Rule,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> TopLevelRule:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/rules",
            payload=rule,
            **extra_kwargs,
        )
        return TopLevelRule(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def get(
        self,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> List[TopLevelRule]:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url="/v3/rules",
            **extra_kwargs,
        )
        return [TopLevelRule(**r) for r in resp.json()]

    def replace(
        self,
        rules: Rules,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/rules/replace",
            payload=rules,
            **extra_kwargs,
        )

    def patch(
        self,
        id: str,
        rule: Rule,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="PATCH",
            url=f"/v3/rules/{id}",
            payload=rule,
            **extra_kwargs,
        )
        return TopLevelRule(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def delete(
        self,
        id: str,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="DELETE",
            url=f"/v3/rules/{id}",
            **extra_kwargs,
        )
