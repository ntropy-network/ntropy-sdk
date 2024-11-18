import os

import pytest
import pytest_asyncio

from ntropy_sdk import SDK
from ntropy_sdk.async_.sdk import AsyncSDK


@pytest.fixture
def sdk(api_key):
    sdk = SDK(api_key)

    url = os.environ.get("NTROPY_API_URL")
    if url is not None:
        sdk.base_url = url

    return sdk


@pytest_asyncio.fixture
async def async_sdk(api_key):
    async with AsyncSDK(api_key) as sdk:
        url = os.environ.get("NTROPY_API_URL")
        if url is not None:
            sdk.base_url = url
        yield sdk
