from typing import TYPE_CHECKING
import uuid

from ntropy_sdk.account_holders import AccountHolderType

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs
    from ntropy_sdk import SDK
    from typing_extensions import Unpack


class CategoriesResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def get(
        self,
        account_holder_type: AccountHolderType,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> dict:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/categories/{account_holder_type.value}",
            **extra_kwargs,
        )
        return resp.json()

    def set(
        self,
        account_holder_type: AccountHolderType,
        categories: dict,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/categories/{account_holder_type.value}",
            json=categories,
            **extra_kwargs,
        )

    def reset(
        self,
        account_holder_type: AccountHolderType,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/categories/{account_holder_type.value}/reset",
            **extra_kwargs,
        )
