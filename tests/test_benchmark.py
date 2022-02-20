import uuid
import tempfile
import pytest
import pandas as pd

from tests import API_KEY

from ntropy_sdk import SDK, AccountHolder


TRANSACTIONS = [
    {
        "": "0",
        "account_holder_id": str(uuid.uuid4()),
        "account_holder_type": "business",
        "description": "AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        "date": "2021-12-12",
        "amount": "2687",
        "entry_type": "debit",
        "iso_currency_code": "USD",
        "labels": "cloud computing - infrastructure",
        "source": "foo",
    },
    {
        "": "1",
        "account_holder_id": str(uuid.uuid4()),
        "account_holder_type": "consumer",
        "date": "2021-12-12",
        "description": "TARGET T- 5800 20th St 11/30/19 17:32",
        "amount": "22.5",
        "entry_type": "debit",
        "iso_currency_code": "USD",
        "labels": "goods - department stores",
        "source": "foo",
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

    account_holders = {}

    def create_account_holder(row):
        if row["account_holder_id"] not in account_holders:
            account_holders[row["account_holder_id"]] = True
            sdk.create_account_holder(
                AccountHolder(
                    id=row["account_holder_id"],
                    type=row["account_holder_type"],
                    name=row.get("account_holder_name"),
                    industry=row.get("account_holder_industry"),
                    website=row.get("account_holder_website"),
                )
            )

    df.apply(create_account_holder, axis=1)

    del df["labels"]

    sdk.add_transactions(df)
