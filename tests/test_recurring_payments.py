import os
import pytest
from ntropy_sdk import SDK, Transaction
from tests import API_KEY


@pytest.fixture
def sdk():
    sdk = SDK(API_KEY)

    url = os.environ.get("NTROPY_API_URL")
    if url is not None:
        sdk.base_url = url

    return sdk


@pytest.fixture
def recurring_payments_transactions():
    transactions = [
        ("2021-01-01", 17.99, "Netflix"),
        ("2021-02-01", 17.99, "Netflix"),
        ("2021-03-01", 17.99, "Netflix"),
        ("2021-04-01", 17.99, "Netflix"),
        ("2021-01-15", 9.99, "Spotify"),
        ("2021-02-15", 9.99, "Spotify"),
        ("2021-03-15", 9.99, "Spotify"),
        ("2021-03-15", 11.99, "Dropbox"),
        ("2021-01-01", 100.0, "Consolidated Edison"),
        ("2021-02-01", 100.0, "Consolidated Edison"),
        ("2021-03-01", 100.0, "Consolidated Edison"),
        ("2021-01-01", 1000.0, "Rent"),
        ("2021-02-01", 1000.0, "Rent"),
        ("2021-03-01", 1000.0, "Rent"),
    ]

    transactions = [
        Transaction(
            date=tx[0],
            amount=tx[1],
            description=tx[2],
            entry_type="debit",
            iso_currency_code="USD",
            transaction_id=f"tx-{i}",
            account_holder_type="consumer",
            account_holder_id="rec-ah-1",
        )
        for i, tx in enumerate(transactions)
    ]

    return transactions


@pytest.mark.skip(
    reason="Only run this test if have an API key with recurrence and subscriptions enabled"
)
def test_recurring_payments(sdk, recurring_payments_transactions):
    account_holder_id = recurring_payments_transactions[0].account_holder_id
    enriched_txs = sdk.add_transactions(recurring_payments_transactions)
    recurring_payments_groups = sdk.get_recurring_payments(account_holder_id)
    assert len(recurring_payments_groups) == 5
    print(recurring_payments_groups)
    print(recurring_payments_groups[0])
    print(recurring_payments_groups[0].transactions)
    assert recurring_payments_groups.inactive() == []
    print(recurring_payments_groups.inactive())
    assert len(recurring_payments_groups.subscriptions()) == 3
    print(recurring_payments_groups.subscriptions())
    assert len(recurring_payments_groups.recurring_bills()) == 2
