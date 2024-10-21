import os

import pytest as pytest

from ntropy_sdk import SDK


@pytest.fixture
def sdk(api_key):
    sdk = SDK(api_key)

    url = os.environ.get("NTROPY_API_URL")
    if url is not None:
        sdk.base_url = url

    return sdk
