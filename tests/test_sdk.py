import os
import pytest
import uuid
import pandas as pd

from tests import API_KEY
from ntropy_sdk import (
    SDK,
    Transaction,
    LabeledTransaction,
    EnrichedTransaction,
    AccountHolder,
    Batch,
    Model,
)
from ntropy_sdk.ntropy_sdk import ACCOUNT_HOLDER_TYPES


@pytest.fixture
def sdk():
    sdk = SDK(API_KEY)

    url = os.environ.get("NTROPY_API_URL")
    if url is not None:
        sdk.base_url = url

    return sdk


def test_account_holder_type():
    def create_account_holder(account_holder_type):
        return AccountHolder(
            id=str(uuid.uuid4()),
            type=account_holder_type,
            industry="fintech",
            website="ntropy.com",
        )

    for t in ACCOUNT_HOLDER_TYPES:
        account_holder = create_account_holder(t)
        assert account_holder.type == t
    with pytest.raises(ValueError):
        create_account_holder("not_valid")


def test_get_account_holder(sdk):
    account_holder = AccountHolder(
        id=str(uuid.uuid4()),
        type="business",
        industry="fintech",
        website="ntropy.com",
    )

    sdk.create_account_holder(account_holder)
    account_holder2 = sdk.get_account_holder(account_holder.id)

    assert isinstance(account_holder2, AccountHolder)
    assert account_holder2.type == "business"
    assert account_holder2.industry == "fintech"
    assert account_holder2.website == "ntropy.com"


def test_account_holder_type_or_id(sdk):
    id_tx = Transaction(
        amount=24.56,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        iso_currency_code="USD",
        mcc=5432,
    )
    enriched = sdk.add_transactions([id_tx])[0]
    assert "missing account holder information" not in enriched.labels

    type_tx = Transaction(
        amount=24.56,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        account_holder_type="business",
        iso_currency_code="USD",
        mcc=5432,
    )
    enriched = sdk.add_transactions([type_tx])[0]
    assert "missing account holder information" not in enriched.labels

    invalid_tx = Transaction(
        amount=24.56,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        iso_currency_code="USD",
        mcc=5432,
    )

    enriched = sdk.add_transactions([invalid_tx])[0]
    assert "missing account holder information" in enriched.labels


def test_account_holder_type_or_id_pandas(sdk):
    account_holder = AccountHolder(
        id=str(uuid.uuid4()), type="business", industry="fintech", website="ntropy.com"
    )
    sdk.create_account_holder(account_holder)

    df = pd.DataFrame(
        data={
            "amount": [26],
            "description": ["TARGET T- 5800 20th St 11/30/19 17:32"],
            "entry_type": ["debit"],
            "date": ["2012-12-10"],
            "account_holder_id": [account_holder.id],
            "iso_currency_code": ["USD"],
        }
    )
    enriched = sdk.add_transactions(df)
    assert "missing account holder information" not in enriched.labels[0]

    df = pd.DataFrame(
        {
            "amount": [27],
            "description": ["TARGET T- 5800 20th St 11/30/19 17:32"],
            "entry_type": ["debit"],
            "date": ["2012-12-10"],
            "account_holder_type": ["business"],
            "iso_currency_code": ["USD"],
        }
    )
    enriched = sdk.add_transactions(df)
    assert "missing account holder information" not in enriched.labels[0]

    df = pd.DataFrame(
        {
            "amount": [28],
            "description": ["TARGET T- 5800 20th St 11/30/19 17:32"],
            "entry_type": ["debit"],
            "date": ["2012-12-10"],
            "iso_currency_code": ["USD"],
        }
    )
    enriched = sdk.add_transactions(df)
    assert "missing account holder information" in enriched.labels[0]


def test_bad_date():
    def create_tx(date):
        return Transaction(
            amount=24.56,
            description="TARGET T- 5800 20th St 11/30/19 17:32",
            entry_type="debit",
            date=date,
            account_holder_id="1",
            iso_currency_code="USD",
        )

    assert isinstance(create_tx("2021-12-13"), Transaction)

    with pytest.raises(ValueError):
        create_tx("bad date")

    with pytest.raises(ValueError):
        create_tx("")

    with pytest.raises(ValueError):
        create_tx(None)


def test_fields():
    tx = Transaction(
        amount=24.56,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        iso_currency_code="USD",
        transaction_id="one-two-three",
        mcc=5432,
    )

    assert tx.to_dict() == {
        "amount": 24.56,
        "description": "TARGET T- 5800 20th St 11/30/19 17:32",
        "entry_type": "debit",
        "date": "2012-12-10",
        "iso_currency_code": "USD",
        "transaction_id": "one-two-three",
        "mcc": 5432,
        "account_holder_id": "1",
    }

    with pytest.raises(ValueError):
        tx = Transaction(
            amount=float("nan"),
            description="TARGET T- 5800 20th St 11/30/19 17:32",
            entry_type="debit",
            date="2012-12-10",
            account_holder_id="1",
            iso_currency_code="USD",
            transaction_id="one-two-three",
            mcc=5432,
        )


def test_enrich_huge_batch(sdk):
    account_holder = AccountHolder(
        id=str(uuid.uuid4()), type="business", industry="fintech", website="ntropy.com"
    )
    sdk.create_account_holder(account_holder)

    tx = Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id=account_holder.id,
        iso_currency_code="USD",
    )

    txs = [tx] * 10
    sdk.MAX_BATCH_SIZE = 4

    enriched_txs = sdk.add_transactions(txs, labeling=False)

    assert len(enriched_txs) == len(txs)

    for i, enriched_tx in enumerate(enriched_txs):
        assert isinstance(enriched_tx, EnrichedTransaction)
        assert enriched_tx.merchant is not None
        assert enriched_tx.transaction_id == txs[i].transaction_id


def test_report(sdk):
    account_holder = AccountHolder(
        id=str(uuid.uuid4()), type="business", industry="fintech", website="ntropy.com"
    )
    sdk.create_account_holder(account_holder)

    tx = Transaction(
        amount=24.56,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id=account_holder.id,
        iso_currency_code="USD",
    )
    enriched_tx = sdk.add_transactions([tx])[0]

    enriched_tx.report(website="ww2.target.com")
    enriched_tx.report(unplanned_kwarg="bar")


def test_hierarchy(sdk):
    for account_holder_type in ["business", "consumer", "freelance", "unknown"]:
        h = sdk.get_labels(account_holder_type)
        assert isinstance(h, dict)


def test_chart_of_accounts(sdk):
    coa = sdk.get_chart_of_accounts()
    assert isinstance(coa, dict)


def test_transaction_zero_amount():
    vals = {
        "description": "foo",
        "date": "2021-12-12",
        "entry_type": "debit",
        "account_holder_id": "1",
        "country": "US",
        "iso_currency_code": "USD",
    }

    Transaction(amount=0, **vals)
    Transaction(amount=1, **vals)

    with pytest.raises(ValueError):
        Transaction(amount=-1, **vals)


def test_transaction_entry_type():
    for et in ["incoming", "outgoing", "debit", "credit"]:
        Transaction(
            amount=1.0,
            description="foo",
            date="2012-12-10",
            entry_type=et,
            account_holder_id="bar",
            iso_currency_code="USD",
            country="US",
        )

    with pytest.raises(ValueError):
        Transaction(
            amount=1.0,
            description="foo",
            date="2012-12-10",
            entry_type="bar",
            account_holder_id="bar",
            iso_currency_code="bla",
            country="FOO",
        )


def test_readme():
    readme_file = open(
        os.path.join(os.path.dirname(__file__), "..", "README.md")
    ).read()
    readme_data = readme_file.split("```python")[1].split("```")[0]
    exec(readme_data, globals())


def test_add_transactions_async(sdk):
    tx = Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_type="business",
        iso_currency_code="USD",
    )

    batch = sdk.add_transactions_async([tx])
    assert batch.batch_id and len(batch.batch_id) > 0

    enriched = batch.wait()
    assert enriched[0].merchant == "Amazon Web Services"


def test_add_transactions_async_df(sdk):
    df = pd.DataFrame(
        data={
            "amount": [26],
            "description": ["TARGET T- 5800 20th St 11/30/19 17:32"],
            "entry_type": ["debit"],
            "date": ["2012-12-10"],
            "account_holder_type": ["business"],
            "iso_currency_code": ["USD"],
        }
    )
    batch = sdk.add_transactions_async(df)
    enriched = batch.wait()
    assert enriched[0].merchant == "Target"


def test_batch(sdk):
    tx = Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_type="business",
        iso_currency_code="USD",
    )

    batch = sdk.add_transactions_async([tx] * 10)
    resp, status = batch.poll()
    assert status == "started" and resp["total"] == 10

    batch.wait()

    resp, status = batch.poll()
    assert status == "finished" and resp[0].merchant == "Amazon Web Services"

    batch = Batch(sdk=sdk, batch_id=batch.batch_id)
    resp, status = batch.poll()
    assert status == "finished" and resp[0].merchant == "Amazon Web Services"


def test_train_custom_model(sdk):
    tx_cloud = LabeledTransaction(
        amount=102.04,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        iso_currency_code="USD",
        account_holder_type="business",
        mcc=5432,
        label="supermarket",
    )
    tx_supermarket = LabeledTransaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_type="business",
        iso_currency_code="USD",
        label="cloud",
    )
    model_name = f"test_{str(uuid.uuid4())[:20]}"

    model = sdk.train_custom_model([tx_cloud] * 10 + [tx_supermarket] * 10, model_name)
    _, status, _ = model.poll()
    assert status in ["enriching", "training", "queued"] and model.is_synced()

    m = Model(sdk=sdk, model_name=model_name, poll_interval=1)
    _, status, _ = model.poll()
    assert status in ["enriching", "training", "queued"] and m.is_synced()

    m.wait()
    _, status, _ = model.poll()
    assert status == "ready"

    e = sdk.add_transactions(
        [
            Transaction(
                amount=110.2,
                description="TARGET T- 5800 20th St 11/30/19 17:32",
                entry_type="debit",
                date="2012-12-10",
                account_holder_id="1",
                iso_currency_code="USD",
                transaction_id="one-two-three",
                mcc=5432,
            )
        ],
        model_name=model_name,
    )[0]

    assert "supermarket" in e.labels


def test_train_custom_model_df(sdk):
    txs = [
        {
            "amount": 102.04,
            "description": "TARGET T- 5800 20th St 11/30/19 17:32",
            "entry_type": "debit",
            "date": "2012-12-10",
            "iso_currency_code": "USD",
            "account_holder_type": "business",
            "mcc": 5432,
            "label": "supermarket",
        },
        {
            "amount": 24.56,
            "description": "AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
            "entry_type": "debit",
            "date": "2012-12-10",
            "account_holder_type": "business",
            "iso_currency_code": "USD",
            "label": "cloud",
            "mcc": 1234,
        },
    ] * 10

    model_name = f"test_{str(uuid.uuid4())[:20]}"
    df = pd.DataFrame(txs)

    model = sdk.train_custom_model(df, model_name)
    _, status, _ = model.poll()
    assert status in ["enriching", "training", "queued"] and model.is_synced()

    m = Model(sdk=sdk, model_name=model_name, poll_interval=1)
    _, status, _ = model.poll()
    assert status in ["enriching", "training", "queued"] and m.is_synced()

    m.wait()
    _, status, _ = model.poll()
    assert status == "ready"

    e = sdk.add_transactions(
        [
            Transaction(
                amount=110.2,
                description="TARGET T- 5800 20th St 11/30/19 17:32",
                entry_type="debit",
                date="2012-12-10",
                account_holder_id="1",
                iso_currency_code="USD",
                transaction_id="one-two-three",
                mcc=5432,
            )
        ],
        model_name=model_name,
    )[0]

    assert "supermarket" in e.labels
