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
        retries: Optional[int]
        timeout: Optional[int]
        retry_on_unhandled_exception: Optional[int]
        extra_headers: Optional[int]


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
