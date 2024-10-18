from .version import VERSION

__version__ = VERSION

from typing import TYPE_CHECKING, Optional
import requests


if TYPE_CHECKING:
    from ntropy_sdk.v2.ntropy_sdk import SDK
    from typing_extensions import TypedDict

    class ExtraKwargs(TypedDict, total=False):
        request_id: Optional[str]
        api_key: Optional[str]
        session: Optional[requests.Session]


from .sdk import SDK

__all__ = ["SDK"]
