from .version import VERSION

__version__ = VERSION

from typing import TYPE_CHECKING, Optional
import requests


if TYPE_CHECKING:
    from typing_extensions import TypedDict

    class ExtraKwargs(TypedDict, total=False):
        request_id: Optional[str]
        api_key: Optional[str]
        session: Optional[requests.Session]
        retries: int
        timeout: int
        retry_on_unhandled_exception: bool
        extra_headers: Optional[dict]


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

from .transactions import (
    TransactionInput, LocationInput
)

from .account_holders import AccountHolder
