import sys
import tempfile
import pytest
import csv
import pandas as pd

from tests import API_KEY

from ntropy_sdk import SDK
from ntropy_sdk.benchmark import main


TRANSACTIONS = [
    {
        "": "0",
        "account_id": "6039c4ac1c63e9c7",
        "description": "AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        "amount": "2687",
        "entry_type": "debit",
        "iso_currency_code": "USD",
        "labels": "cloud computing - infrastructure",
        "source": "foo"
    },
    {
        "": "1",
        "account_id": "601343505fd633",
        "description": "TARGET T- 5800 20th St 11/30/19 17:32",
        "amount": "22.5",
        "entry_type": "debit",
        "iso_currency_code": "USD",
        "labels": "goods - department stores",
        "source": "foo"
    },
]


@pytest.fixture
def data_set_file():
    with tempfile.NamedTemporaryFile() as f:
        pd.DataFrame(TRANSACTIONS).to_csv(f)

        yield f.name


@pytest.fixture
def sdk():
    return SDK(API_KEY)


def test_enrich_dataframe(sdk, data_set_file):
    with open(data_set_file) as f:
        df = pd.read_csv(f)
        df['iso_currency_code'] = 'USD'
        df['account_holder_id'] = '1'
        df['account_holder_type'] = 'business'
        del df['labels']

        sdk.enrich_dataframe(df)


def test_command_line(data_set_file):
    with tempfile.NamedTemporaryFile() as output_file:
        sys.argv = [
            "ntropy-benchmark",
            "--api-key", API_KEY,
            "--api-url", "https://api.ntropy.network",
            "--in-csv-file", data_set_file,
            "--out-csv-file", output_file.name,
            "--hardcoded-field", '{"account_holder_type": "business", "iso_currency_code":"USD", "account_holder_id": "1"}',
            "--poll-interval", "1",
            "--ground-truth-label-field", "labels",
            "--field-mapping", '{"labels": "predicted_labels"}',
            "--max-batch-size", "200",
        ]

        main()

        result = pd.read_csv(output_file)

        assert result.shape[0] == len(TRANSACTIONS)
