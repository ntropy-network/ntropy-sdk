import uuid
from typing import Optional, TYPE_CHECKING, List

from pydantic import BaseModel

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs
    from ntropy_sdk import SDK
    from typing_extensions import Unpack


class Entity(BaseModel):
    id: Optional[str]
    name: Optional[str]
    website: Optional[str]
    logo: Optional[str]
    mccs: List[int]


class EntityResponse(Entity):
    request_id: Optional[str] = None


class EntitiesResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def get(
        self,
        id: str,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> EntityResponse:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/entities/{id}",
            **extra_kwargs,
        )
        return EntityResponse(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def resolve(
        self,
        name: Optional[str] = None,
        website: Optional[str] = None,
        location: Optional[str] = None,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> EntityResponse:
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/entities/resolve",
            payload={
                "name": name,
                "website": website,
                "location": location,
            },
        )
        return EntityResponse(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )
