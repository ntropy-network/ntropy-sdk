import os
import pytest
import uuid

from tests import API_KEY
from ntropy_sdk import (
    SDK,
    Transaction,
    EnrichedTransaction,
    AccountHolder,
)
from ntropy_sdk.ntropy_sdk import ACCOUNT_HOLDER_TYPES


@pytest.fixture
def sdk():
    sdk = SDK(API_KEY)

    if url := os.environ.get("NTROPY_API_URL"):
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

    testcases = [
        (0, True, True),
        (0, False, False),
        (-1, True, True),
        (-1, False, True),
        (1, False, False),
        (1, True, False),
    ]
    for amount, enabled, should_raise in testcases:
        print("Testcase:", amount, enabled, should_raise)

        if enabled:
            Transaction.enable_zero_amount_check()
        else:
            Transaction.disable_zero_amount_check()

        if should_raise:
            with pytest.raises(ValueError):
                Transaction(amount=amount, **vals)
        else:
            Transaction(
                amount=amount,
                **vals,
            )

    Transaction.enable_zero_amount_check()


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
