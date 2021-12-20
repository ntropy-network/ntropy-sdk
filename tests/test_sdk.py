import os
import pytest

from tests import API_KEY
from ntropy_sdk import SDK, Transaction, EnrichedTransaction
from ntropy_sdk.ntropy_sdk import ACCOUNT_HOLDER_TYPES


@pytest.fixture
def sdk():
    sdk = SDK(API_KEY)

    if url := os.environ.get("NTROPY_API_URL"):
        sdk.base_url = url

    return sdk


def test_account_holder_type():
    def create_tx(account_holder_type):
        return Transaction(
            amount=24.56,
            description="TARGET T- 5800 20th St 11/30/19 17:32",
            entry_type="debit",
            date="2012-12-10",
            account_holder_id="1",
            account_holder_type=account_holder_type,
            iso_currency_code="USD",
        )

    for t in ACCOUNT_HOLDER_TYPES + ["not_valid"]:
        if t in ACCOUNT_HOLDER_TYPES:
            tx = create_tx(t)
            assert tx.account_holder_type == t
        else:
            with pytest.raises(ValueError):
                create_tx(t)


def test_fileds():
    tx = Transaction(
        amount=24.56,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        account_holder_type="consumer",
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
        "account_holder": {
            "id": "1",
            "type": "consumer",
        },
    }


def test_enrich(sdk):
    consumer_tx = Transaction(
        amount=24.56,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        account_holder_type="consumer",
        iso_currency_code="USD",
    )
    enriched_tx = sdk.enrich(consumer_tx)
    assert len(enriched_tx.labels) > 0

    business_tx = Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        account_holder_type="business",
        iso_currency_code="USD",
    )
    enriched_tx = sdk.enrich(business_tx, latency_optimized=True)
    assert len(enriched_tx.labels) > 0

    enriched_tx = sdk.enrich(business_tx, labeling=False)
    assert enriched_tx.labels is None


def test_enrich_batch(sdk):
    tx = Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        account_holder_type="business",
        iso_currency_code="USD",
    )

    txs = [tx] * 10

    batch = sdk.enrich_batch(txs, labeling=False)
    result = batch.wait()

    assert len(result.transactions) == len(txs)

    for enriched_tx in result.transactions:
        assert isinstance(enriched_tx, EnrichedTransaction)
        assert enriched_tx.merchant is not None


def test_enrich_huge_batch(sdk):
    tx = Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        account_holder_type="business",
        iso_currency_code="USD",
    )

    txs = [tx] * 10
    sdk.MAX_BATCH_SIZE = 4

    batch = sdk.enrich_batch(txs, labeling=False)
    result = batch.wait_with_progress()

    assert len(result.transactions) == len(txs)

    for i, enriched_tx in enumerate(result.transactions):
        assert isinstance(enriched_tx, EnrichedTransaction)
        assert enriched_tx.merchant is not None
        assert enriched_tx.transaction_id == txs[i].transaction_id


def test_report(sdk):
    consumer_tx = Transaction(
        amount=24.56,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        account_holder_type="consumer",
        iso_currency_code="USD",
    )
    enriched_tx = sdk.enrich(consumer_tx)

    enriched_tx.report(website="ww2.target.com")

    with pytest.raises(ValueError):
        enriched_tx.report(not_supported=True)


def test_transaction_zero_amount():
    vals = {
        "description": "foo",
        "entry_type": "debit",
        "account_holder_id": "1",
        "account_holder_type": "business",
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
        t = Transaction(
            amount=1.0,
            description="foo",
            entry_type=et,
            account_holder_id="bar",
            account_holder_type="business",
            iso_currency_code="USD",
            country="US",
        )

    with pytest.raises(ValueError):
        Transaction(
            amount=1.0,
            description="foo",
            entry_type="bar",
            account_holder_id="bar",
            account_holder_type="business",
            iso_currency_code="bla",
            country="FOO",
        )
