import os

import pytest


@pytest.fixture()
def api_key():
    key = os.environ.get("NTROPY_API_KEY")

    if not key:
        raise RuntimeError("Environment variable NTROPY_API_KEY is not defined")

    return key


@pytest.fixture()
def recurrence_api_key():
    key = os.environ.get("NTROPY_RECURRENCE_API_KEY")

    if not key:
        raise RuntimeError("Environment variable NTROPY_RECURRENCE_API_KEY is not defined")

    return key
