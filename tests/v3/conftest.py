import os

import pytest as pytest

from ntropy_sdk import SDK
from tests import API_KEY


@pytest.fixture
def sdk():
    sdk = SDK(API_KEY)

    url = os.environ.get("NTROPY_API_URL")
    if url is not None:
        sdk.base_url = url

    return sdk
