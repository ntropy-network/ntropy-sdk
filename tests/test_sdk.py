import pytest

from tests import API_KEY
from ntropy_sdk import SDK, Transaction

def test_enrich():
    sdk = SDK(API_KEY)

    tx = Transaction(
        amount=24.56,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        account_holder_type="business",
        iso_currency_code="USD",
    )

    resp = sdk.enrich(tx)

    print("HELLO")
    print(resp)

    resp = sdk.enrich(tx, latency_optimized=True)
    print(resp)

    txs = [tx, tx, tx]

    resp = sdk.enrich_batch(txs, labeling=False)

    print(resp)

    result = resp.wait()

    print(result.transactions)


def test_enrich_business():
    sdk = SDK(API_KEY)

    tx = Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        account_holder_type="business",
        iso_currency_code="USD",
    )

    resp = sdk.enrich(tx)

    print("HELLO")
    print(resp)

    resp = sdk.enrich(tx, latency_optimized=True)
    print(resp)

    txs = [tx, tx, tx]

    resp = sdk.enrich_batch(txs, labeling=False)

    print(resp)

    result = resp.wait()

    print(result.transactions)


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
