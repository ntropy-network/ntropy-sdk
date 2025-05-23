__version__ = "5.2.0"

from typing import TYPE_CHECKING, Optional


if TYPE_CHECKING:
    from typing_extensions import TypedDict
    import aiohttp
    import requests

    class ExtraKwargsBase(TypedDict, total=False):
        request_id: Optional[str]
        api_key: Optional[str]
        retries: int
        timeout: int
        retry_on_unhandled_exception: bool
        extra_headers: Optional[dict]

    class ExtraKwargs(ExtraKwargsBase, total=False):
        session: Optional[requests.Session]

    class ExtraKwargsAsync(ExtraKwargsBase, total=False):
        session: Optional[aiohttp.ClientSession]


from .sdk import SDK
from .v2.errors import (
    NtropyError,
    NtropyBatchError,
    NtropyDatasourceError,
    NtropyTimeoutError,
    NtropyHTTPError,
    NtropyValidationError,
    NtropyQuotaExceededError,
    NtropyNotSupportedError,
    NtropyResourceOccupiedError,
    NtropyServerConnectionError,
    NtropyRateLimitError,
    NtropyNotFoundError,
    NtropyNotAuthorizedError,
    NtropyValueError,
    NtropyRuntimeError,
)


__all__ = (
    "SDK",
    "NtropyError",
    "NtropyBatchError",
    "NtropyDatasourceError",
    "NtropyTimeoutError",
    "NtropyHTTPError",
    "NtropyValidationError",
    "NtropyQuotaExceededError",
    "NtropyNotSupportedError",
    "NtropyResourceOccupiedError",
    "NtropyServerConnectionError",
    "NtropyRateLimitError",
    "NtropyNotFoundError",
    "NtropyNotAuthorizedError",
    "NtropyValueError",
    "NtropyRuntimeError",
)
