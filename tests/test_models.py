from ntropy_sdk import SDK, Transaction
from ntropy_sdk.models import CustomTransactionClassifier
from tests import API_KEY
import os
import uuid
import pytest
import sklearn
import time
import pandas as pd


tx_supermarket = Transaction(
    amount=102.04,
    description="TARGET T- 5800 20th St 11/30/19 17:32",
    entry_type="debit",
    date="2012-12-10",
    iso_currency_code="USD",
    account_holder_type="business",
    mcc=5432,
)
tx_cloud = Transaction(
    amount=24.56,
    description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
    entry_type="debit",
    date="2012-12-10",
    account_holder_type="business",
    iso_currency_code="USD",
    mcc=1234,
)
tx_supermarket2 = Transaction(
    amount=101.04,
    description="TARGET T- 5800 20th St 11/30/19 17:32",
    entry_type="debit",
    date="2012-12-10",
    iso_currency_code="USD",
    account_holder_type="business",
    mcc=5432,
    label="supermarket",
)


@pytest.fixture
def sdk():
    sdk = SDK(API_KEY)

    url = os.environ.get("NTROPY_API_URL")
    if url is not None:
        sdk.base_url = url

    return sdk


def test_model_status(sdk):
    model_name = f"test_{str(uuid.uuid4())[:20]}"
    model = CustomTransactionClassifier(model_name=model_name, sdk=sdk)

    model.fit([tx_supermarket, tx_cloud] * 5, ["supermarket", "cloud"] * 5)
    status = model.get_status()

    assert (
        status["name"] == model_name
        and status["progress"] == 100
        and status["status"] == "ready"
    )
