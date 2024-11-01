import uuid
from datetime import datetime
from enum import Enum
from typing import Optional, TYPE_CHECKING, Union

from pydantic import BaseModel, Field

from ntropy_sdk.paging import PagedResponse
from ntropy_sdk.transactions import RecurrenceGroup, RecurrenceGroups
from ntropy_sdk.utils import pydantic_json

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs
    from ntropy_sdk import SDK
    from typing_extensions import Unpack


class AccountHolderType(str, Enum):
    consumer = "consumer"
    business = "business"


class AccountHolder(BaseModel):
    id: str = Field(
        description="The unique ID of the account holder of the transaction",
        min_length=1,
    )
    type: AccountHolderType = Field(
        description="The type of the account holder. ",
    )
    name: Optional[str] = Field(
        default=None,
        description="The name of the account holder",
    )
    request_id: Optional[str] = None


class AccountHoldersResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def list(
        self,
        *,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> PagedResponse[AccountHolder]:
        """List all account holders"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url="/v3/account_holders",
            params={
                "created_before": created_before,
                "created_after": created_after,
                "cursor": cursor,
                "limit": limit,
            },
            **extra_kwargs,
        )
        page = PagedResponse[AccountHolder](
            **resp.json(),
            request_id=resp.headers.get("x-request-id", request_id),
            _resource=self,
            _extra_kwargs=extra_kwargs,
        )
        for t in page.data:
            t.request_id = request_id
        return page

    def get(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]") -> AccountHolder:
        """Retrieve an account holder"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/account_holders/{id}",
            **extra_kwargs,
        )
        return AccountHolder(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def create(
        self,
        id: str,
        type: Union[AccountHolderType, str],
        name: Optional[str] = None,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> AccountHolder:
        """Create an account holder"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/account_holders",
            payload_json_str=pydantic_json(
                AccountHolder(
                    id=id,
                    type=type,
                    name=name,
                )
            ),
            **extra_kwargs,
        )
        return AccountHolder(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def recurring_groups(
        self,
        id: str,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> RecurrenceGroups:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/account_holders/{id}/recurring_groups",
            **extra_kwargs,
        )
        return RecurrenceGroups(
            groups=[RecurrenceGroup(**r) for r in resp.json()],
            request_id=resp.headers.get("x-request-id", request_id),
        )

    def delete(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]"):
        """Retrieve an account holder"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="DELETE",
            url=f"/v3/account_holders/{id}",
            **extra_kwargs,
        )
