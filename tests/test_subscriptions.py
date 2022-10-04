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
def subscriptions_api_response():
    return [
        {
            "amount": 9.99,
            "total_amount": 49.95,
            "first_payment_date": "2021-01-01",
            "latest_payment_date": "2021-05-01",
            "next_expected_payment_date": "2021-06-01",
            "periodicity": "monthly",
            "type": "subscription",
            "is_essential": False,
            "is_active": True,
            "merchant": "Spotify",
            "transaction_ids": [
                "b6cdb5bb-4dee-435a-84db-5c99fda70e51",
                "b6cdb5bb-4dee-435a-84db-5c99fda70e52",
                "b6cdb5bb-4dee-435a-84db-5c99fda70e53",
                "b6cdb5bb-4dee-435a-84db-5c99fda70e54",
                "b6cdb5bb-4dee-435a-84db-5c99fda70e55",
            ],
        },
        {
            "amount": 100,
            "total_amount": 300,
            "first_payment_date": "2021-01-01",
            "latest_payment_date": "2021-03-01",
            "next_expected_payment_date": "2021-04-01",
            "periodicity": "monthly",
            "type": "recurring",
            "is_essential": True,
            "is_active": True,
            "merchant": "Con Edison",
            "transaction_ids": [
                "b6cdb5bb-4dee-435a-84db-5c99fda70e11",
                "b6cdb5bb-4dee-435a-84db-5c99fda70e12",
                "b6cdb5bb-4dee-435a-84db-5c99fda70e13",
            ],
        },
    ]


@pytest.fixture
def subscription_transactions():

    return [
        Transaction(
            amount=17.99,
            description="Netflix",
            entry_type="debit",
            date="2022-01-01",
            iso_currency_code="USD",
            transaction_id="tx-1",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
        Transaction(
            amount=17.99,
            description="Netflix",
            entry_type="debit",
            date="2022-02-01",
            iso_currency_code="USD",
            transaction_id="tx-2",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
        Transaction(
            amount=17.99,
            description="Netflix",
            entry_type="debit",
            date="2022-03-01",
            iso_currency_code="USD",
            transaction_id="tx-3",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
        Transaction(
            amount=17.99,
            description="Netflix",
            entry_type="debit",
            date="2022-04-01",
            iso_currency_code="USD",
            transaction_id="tx-4",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
        Transaction(
            amount=9.99,
            description="Spotify",
            entry_type="debit",
            date="2022-01-15",
            iso_currency_code="USD",
            transaction_id="tx-5",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
        Transaction(
            amount=9.99,
            description="Spotify",
            entry_type="debit",
            date="2022-02-15",
            iso_currency_code="USD",
            transaction_id="tx-6",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
        Transaction(
            amount=9.99,
            description="Spotify",
            entry_type="debit",
            date="2022-03-15",
            iso_currency_code="USD",
            transaction_id="tx-7",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
        Transaction(
            amount=11.99,
            description="Dropbox",
            entry_type="debit",
            date="2022-03-15",
            iso_currency_code="USD",
            transaction_id="tx-8",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
        Transaction(
            amount=1000,
            description="Rent",
            entry_type="debit",
            date="2022-01-01",
            iso_currency_code="USD",
            transaction_id="tx-9",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
        Transaction(
            amount=1000,
            description="Rent",
            entry_type="debit",
            date="2022-02-01",
            iso_currency_code="USD",
            transaction_id="tx-10",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
        Transaction(
            amount=1000,
            description="Rent",
            entry_type="debit",
            date="2022-03-01",
            iso_currency_code="USD",
            transaction_id="tx-11",
            account_holder_type="consumer",
            account_holder_id="rec-1",
        ),
    ]


@pytest.mark.skip(
    reason="Only run this test if have an API key with recurrence and subscriptions enabled"
)
def test_subscriptions(sdk, subscription_transactions):
    account_holder_id = subscription_transactions[0].account_holder_id
    enriched_txs = sdk.add_transactions(subscription_transactions)
    subscriptions = sdk.get_account_subscriptions(account_holder_id)
    assert len(subscriptions) == 5
    print(subscriptions)
    print(subscriptions[0].transactions)
