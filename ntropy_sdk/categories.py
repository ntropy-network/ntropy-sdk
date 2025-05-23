import sys
from typing import TYPE_CHECKING, Union
import uuid

from ntropy_sdk.account_holders import AccountHolderType

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs, ExtraKwargsAsync, SDK
    from ntropy_sdk.async_.sdk import AsyncSDK
    from typing_extensions import Unpack, deprecated
elif sys.version_info >= (3, 13):
    from warnings import deprecated
else:
    def deprecated(msg):
        return lambda f: f


class CategoriesResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def get(
        self,
        category_id: Union[AccountHolderType, str],
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> dict:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        if isinstance(category_id, AccountHolderType):
            category_id = category_id.value
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/categories/{category_id}",
            **extra_kwargs,
        )
        return resp.json()

    def set(
        self,
        category_id: Union[AccountHolderType, str],
        categories: dict,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        if isinstance(category_id, AccountHolderType):
            category_id = category_id.value
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/categories/{category_id}",
            payload=categories,
            **extra_kwargs,
        )
        return resp.json()

    @deprecated("Use the delete method instead")
    def reset(
        self,
        category_id: Union[AccountHolderType, str],
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        return self.delete(category_id, **extra_kwargs)

    def delete(
        self,
        category_id: Union[AccountHolderType, str],
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        if isinstance(category_id, AccountHolderType):
            category_id = category_id.value
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/categories/{category_id}/reset",
            **extra_kwargs,
        )
        return resp.json()


class CategoriesResourceAsync:
    def __init__(self, sdk: "AsyncSDK"):
        self._sdk = sdk

    async def get(
        self,
        category_id: Union[AccountHolderType, str],
        **extra_kwargs: "Unpack[ExtraKwargsAsync]",
    ) -> dict:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        if isinstance(category_id, AccountHolderType):
            category_id = category_id.value
        resp = await self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/categories/{category_id}",
            **extra_kwargs,
        )
        async with resp:
            return await resp.json()

    async def set(
        self,
        category_id: Union[AccountHolderType, str],
        categories: dict,
        **extra_kwargs: "Unpack[ExtraKwargsAsync]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        if isinstance(category_id, AccountHolderType):
            category_id = category_id.value
        resp = await self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/categories/{category_id}",
            payload=categories,
            **extra_kwargs,
        )
        async with resp:
            return await resp.json()

    @deprecated("Use the delete method instead")
    async def reset(
        self,
        category_id: Union[AccountHolderType, str],
        **extra_kwargs: "Unpack[ExtraKwargsAsync]",
    ):
        return await self.delete(category_id, **extra_kwargs)

    async def delete(
        self,
        category_id: Union[AccountHolderType, str],
        **extra_kwargs: "Unpack[ExtraKwargsAsync]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        if isinstance(category_id, AccountHolderType):
            category_id = category_id.value
        resp = await self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/categories/{category_id}/reset",
            **extra_kwargs,
        )
        async with resp:
            return await resp.json()
