from typing import TYPE_CHECKING, Any, Dict, List
import uuid


if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs
    from ntropy_sdk import SDK
    from typing_extensions import Unpack


Rule = Dict[str, Any]
Rules = List[Dict[str, Any]]


class RulesResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def create(
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
            url="/v3/rules",
            payload=rules,
            **extra_kwargs,
        )

    def get(
        self,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> Rules:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url="/v3/rules",
            **extra_kwargs,
        )
        return resp.json()

    def append(
        self,
        rule: Rule,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/rules/append",
            payload=rule,
            **extra_kwargs,
        )

    def patch(
        self,
        index: int,
        rule: Rule,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="PATCH",
            url=f"/v3/rules/{index}",
            payload=rule,
            **extra_kwargs,
        )

    def delete(
        self,
        index: int,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="DELETE",
            url=f"/v3/rules/{index}",
            **extra_kwargs,
        )
