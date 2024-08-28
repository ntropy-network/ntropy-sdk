import os
import uuid
from decimal import Decimal
from unittest.mock import patch

import pandas as pd
import pytest
from requests import Response

from ntropy_sdk import (
    AccountHolder,
    Batch,
    EnrichedTransaction,
    LabeledTransaction,
    Model,
    SDK,
    Transaction,
    NtropyError,
)
from ntropy_sdk.errors import NtropyValueError, NtropyBatchError
from ntropy_sdk.utils import TransactionType
from ntropy_sdk.ntropy_sdk import ACCOUNT_HOLDER_TYPES
from tests import API_KEY


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


def test_dataframe_inplace(sdk):
    df = pd.DataFrame(
        data={
            "amount": [26],
            "description": ["TARGET T- 5800 20th St 11/30/19 17:32"],
            "entry_type": ["debit"],
            "date": ["2012-12-10"],
            "account_holder_id": [str(uuid.uuid4())],
            "account_holder_type": ["consumer"],
            "iso_currency_code": ["USD"],
        }
    )
    original_columns = df.columns
    # inplace = False should NOT change the original dataframe
    enriched = sdk.add_transactions(df, inplace=False)
    assert list(df.columns) == list(original_columns)
    assert "goods" in enriched.labels[0]

    # inplace = True should change the original dataframe
    sdk.add_transactions(df, inplace=True)
    assert "goods" in df.labels[0]


def test_account_holder_type_or_id_iterable(sdk):
    id_tx = Transaction(
        amount=24.56,
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        iso_currency_code="USD",
        mcc=5432,
    )
    enriched = sdk.add_transactions((t for t in [id_tx]))[0]
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
    enriched = sdk.add_transactions((t for t in [type_tx]))[0]
    assert "missing account holder information" not in enriched.labels


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

    enriched_txs = sdk.add_transactions(txs)

    assert len(enriched_txs) == len(txs)

    for i, enriched_tx in enumerate(enriched_txs):
        assert isinstance(enriched_tx, EnrichedTransaction)
        assert enriched_tx.merchant is not None
        assert enriched_tx.transaction_id == txs[i].transaction_id
        assert enriched_tx.parent_tx is txs[i]


# TODO: temporarily disabled until persistence timing is adjusted for reports
# def test_report(sdk):
#     account_holder = AccountHolder(
#         id=str(uuid.uuid4()), type="business", industry="fintech", website="ntropy.com"
#     )
#     sdk.create_account_holder(account_holder)
#
#     tx = Transaction(
#         amount=24.56,
#         description="TARGET T- 5800 20th St 11/30/19 17:32",
#         entry_type="debit",
#         date="2012-12-10",
#         account_holder_id=account_holder.id,
#         iso_currency_code="USD",
#     )
#     enriched_tx = sdk.add_transactions([tx])[0]
#
#     enriched_tx.create_report(website="ww2.target.com")
#     enriched_tx.create_report(unplanned_kwarg="bar")


def test_hierarchy(sdk):
    for account_holder_type in ["business", "consumer", "unknown"]:
        h = sdk.get_labels(account_holder_type)
        assert isinstance(h, dict)


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
    readme_data = readme_data.replace("YOUR-API-KEY", API_KEY)
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
    assert enriched[0].parent_tx is tx


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


def test_add_transactions_async_iterable(sdk):
    tx = Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_type="business",
        iso_currency_code="USD",
    )

    batch = sdk.add_transactions_async((t for t in [tx]))
    assert batch.batch_id and len(batch.batch_id) > 0

    enriched = batch.wait()
    assert enriched[0].merchant == "Amazon Web Services"


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

    if status != "finished":
        # Might've finished already
        assert status == "started" and resp["total"] == 10

    batch.wait()

    resp, status = batch.poll()
    assert status == "finished" and resp[0].merchant == "Amazon Web Services"

    batch = Batch(sdk=sdk, batch_id=batch.batch_id)
    resp, status = batch.poll()
    assert status == "finished" and resp[0].merchant == "Amazon Web Services"


def test_numerical_support():
    tx = Transaction(
        amount=Decimal(24.56),
        description="TARGET T- 5800 20th St 11/30/19 17:32",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id="1",
        iso_currency_code="USD",
        mcc="5432",
    )

    assert isinstance(tx.amount, float)
    assert isinstance(tx.mcc, int)


def test_none_in_txns_error(sdk):
    test_values = [[None]]
    for test_value in test_values:
        try:
            sdk.add_transactions(test_value)
            assert False
        except ValueError as e:
            assert str(e) == "transactions contains a None value"

        try:
            sdk.add_transactions_async(test_value)
            assert False
        except ValueError as e:
            assert str(e) == "transactions contains a None value"


def test_parent_tx(sdk):
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
    assert enriched.parent_tx is id_tx


def test_single_transaction_enrich_error(sdk):
    tx = Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_type="business",
        iso_currency_code="USD",
    )
    try:
        sdk.add_transactions(tx)
        assert False
    except TypeError as e:
        assert str(e) == "transactions must be either a pandas.Dataframe or an iterable"


def test_enriched_fields(sdk):
    tx = Transaction(
        transaction_id="test-enriched-fields",
        amount=12046.15,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2019-12-01",
        account_holder_type="business",
        iso_currency_code="USD",
        country="US",
        # mcc=5432,
    )
    df = pd.DataFrame(
        data={
            "amount": [12046.15],
            "description": [
                "AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15"
            ],
            "entry_type": ["debit"],
            "date": ["2019-12-01"],
            "account_holder_type": ["business"],
            "iso_currency_code": ["USD"],
            "transaction_id": ["test-enriched-fields"],
            # "mcc": [5432],
        }
    )

    enriched_df = sdk.add_transactions(df).iloc[0]
    enriched_list = sdk.add_transactions([tx])[0]

    for enriched in [enriched_df, enriched_list]:
        print(enriched)
        assert "infrastructure" in enriched.labels
        # assert len(enriched.location) > 0
        assert enriched.logo == "https://logos.ntropy.com/aws.amazon.com"
        assert enriched.merchant == "Amazon Web Services"
        assert enriched.merchant_id == str(
            uuid.uuid3(uuid.NAMESPACE_DNS, enriched.website)
        )
        assert enriched.transaction_id == "test-enriched-fields"
        assert enriched.website == "aws.amazon.com"
        # assert enriched.recurrence == "one off"
        assert 0 <= enriched.confidence <= 1
        assert any(enriched.transaction_type == t.value for t in TransactionType)
        # assert 5432 in enriched.mcc


def test_sdk_region():
    ah_id = str(uuid.uuid4())
    tx = Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id=ah_id,
        account_holder_type="consumer",
        iso_currency_code="USD",
    )

    _sdk = SDK(API_KEY)
    assert _sdk.base_url == "https://api.ntropy.com"
    res = _sdk.add_transactions([tx])[0]
    assert res.website is not None

    _sdk = SDK(API_KEY, region="us")
    assert _sdk.base_url == "https://api.ntropy.com"
    res = _sdk.add_transactions([tx])[0]
    assert res.website is not None

    _sdk = SDK(API_KEY, region="eu")
    assert _sdk.base_url == "https://api.eu.ntropy.com"
    res = _sdk.add_transactions([tx])[0]
    assert res.website is not None

    with pytest.raises(ValueError):
        _sdk = SDK(API_KEY, region="atlantida")


@pytest.fixture()
def async_sdk():
    sdk = SDK(API_KEY)
    sdk._make_batch = make_batch

    with patch.object(sdk, "MAX_SYNC_BATCH", 0):
        with patch.object(sdk, "MAX_BATCH_SIZE", 1):
            yield sdk


@pytest.fixture()
def sync_sdk():
    sdk = SDK(API_KEY)
    sdk._make_batch = lambda batch_status, results: results

    with patch.object(sdk, "MAX_SYNC_BATCH", 999999):
        with patch.object(sdk, "MAX_BATCH_SIZE", 1):
            yield sdk


@pytest.fixture()
def input_tx():
    ah_id = str(uuid.uuid4())
    return Transaction(
        amount=24.56,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2012-12-10",
        account_holder_id=ah_id,
        account_holder_type="consumer",
        iso_currency_code="USD",
    )


def make_batch(batch_status, results):
    return {
        "status": batch_status,
        "created_at": "2023-01-01T00:00:00.000000+00:00",
        "updated_at": "2023-01-01T00:00:00.000000+00:00",
        "id": "mock-id",
        "progress": 1,
        "total": 1,
        "results": results,
    }


class MockResponse:
    def __init__(self, status_code, json_data):
        self.status_code = status_code
        self.json_data = json_data

    def json(self):
        return self.json_data

    def raise_for_status(self):
        r = Response()
        r.status_code = self.status_code
        return r.raise_for_status()


def wrap_response(sdk, wrap_meth, responses):
    """
    Mocks a response for `wrap_meth` iterating through `responses` to obtain return values
    """
    orig = sdk.session.request
    responses = iter(responses)

    def fn(meth, *args, **kwargs):
        t = None
        if meth == wrap_meth:
            try:
                t = next(responses)
            except StopIteration:
                pass

            if not t:
                return orig(meth, *args, **kwargs)

            status_code, batch_status, results = t
            return MockResponse(
                status_code,
                sdk._make_batch(batch_status, results),  # noqa
            )
        else:
            return orig(meth, *args, **kwargs)

    return fn


@pytest.mark.parametrize("batch_status", ["finished", "error"])
def test_async_batch_with_err_ignore_raise(async_sdk, input_tx, batch_status):
    async_sdk._raise_on_enrichment_error = False
    responses = wrap_response(
        async_sdk,
        "GET",
        [
            (
                200,
                batch_status,
                [
                    {
                        "transaction_id": "err",
                        "error": "internal_error",
                        "error_details": "internal_error",
                    }
                ],
            ),
        ],
    )

    with patch.object(
        async_sdk.session,
        "request",
        side_effect=responses,
    ) as m:
        res = async_sdk.add_transactions([input_tx] * 2)
        assert m.call_count >= 4
        assert len(res) == 2
        assert res[0].error is not None and "mock-id" in str(res[0].error)
        assert res[1].merchant is not None and res[1].error is None


def test_async_batch_request_err_ignore_raise(async_sdk, input_tx):
    async_sdk._raise_on_enrichment_error = False
    responses = wrap_response(
        async_sdk,
        "GET",
        [
            (
                400,
                "error",
                [
                    {
                        "transaction_id": "err",
                        "error": "internal_error",
                        "error_details": "internal_error",
                    }
                ],
            ),
        ],
    )

    with patch.object(
        async_sdk.session,
        "request",
        side_effect=responses,
    ) as m:
        res = async_sdk.add_transactions([input_tx] * 2)
        assert m.call_count >= 4
        assert len(res) == 2
        assert res[0].error is not None and isinstance(res[0].error, NtropyValueError)
        assert res[1].merchant is not None and res[1].error is None


def test_sync_batch_request_err_ignore_raise(sync_sdk, input_tx):
    sync_sdk._raise_on_enrichment_error = False
    responses = wrap_response(
        sync_sdk,
        "POST",
        [
            (
                400,
                "error",
                [
                    {
                        "transaction_id": "err",
                        "error": "internal_error",
                        "error_details": "internal_error",
                    }
                ],
            ),
        ],
    )

    with patch.object(
        sync_sdk.session,
        "request",
        side_effect=responses,
    ) as m:
        res = sync_sdk.add_transactions([input_tx] * 2)
        assert m.call_count == 2
        assert len(res) == 2
        assert res[0].error is not None and isinstance(res[0].error, NtropyValueError)
        assert res[1].merchant is not None and res[1].error is None


@pytest.mark.parametrize("batch_status", ["finished", "error"])
def test_async_batch_with_err(async_sdk, input_tx, batch_status):
    async_sdk._raise_on_enrichment_error = True
    responses = wrap_response(
        async_sdk,
        "GET",
        [
            (
                200,
                batch_status,
                [
                    {
                        "transaction_id": "err",
                        "error": "internal_error",
                        "error_details": "internal_error",
                    }
                ],
            ),
        ],
    )

    with patch.object(
        async_sdk.session,
        "request",
        side_effect=responses,
    ) as m:
        with pytest.raises(NtropyBatchError) as be:
            async_sdk.add_transactions([input_tx] * 2)
        assert "mock-id" in str(be.value)
        assert m.call_count == 2


def test_async_batch_request_err(async_sdk, input_tx):
    async_sdk._raise_on_enrichment_error = True
    responses = wrap_response(
        async_sdk,
        "GET",
        [
            (
                400,
                "error",
                [
                    {
                        "transaction_id": "err",
                        "error": "internal_error",
                        "error_details": "internal_error",
                    }
                ],
            ),
        ],
    )

    with patch.object(
        async_sdk.session,
        "request",
        side_effect=responses,
    ) as m:
        with pytest.raises(NtropyValueError):
            async_sdk.add_transactions([input_tx] * 2)
        assert m.call_count == 2


def test_sync_batch_request_err(sync_sdk, input_tx):
    sync_sdk._raise_on_enrichment_error = True
    responses = wrap_response(
        sync_sdk,
        "POST",
        [
            (
                400,
                "error",
                [
                    {
                        "transaction_id": "err",
                        "error": "internal_error",
                        "error_details": "internal_error",
                    }
                ],
            ),
        ],
    )

    with patch.object(
        sync_sdk.session,
        "request",
        side_effect=responses,
    ) as m:
        with pytest.raises(NtropyValueError):
            sync_sdk.add_transactions([input_tx] * 2)
        assert m.call_count == 1


def test_mapping(sdk):
    tx = Transaction(
        transaction_id="test-enriched-fields",
        amount=12046.15,
        description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S Crd15",
        entry_type="debit",
        date="2019-12-01",
        account_holder_type="business",
        iso_currency_code="USD",
        country="US",
        mcc=5432,
    )

    mapping = {"merchant": "company"}
    mapping_invalid = {"invalid": "test"}

    enriched_mapping = sdk.add_transactions([tx], mapping=mapping)
    try:
        enriched_mapping_invalid = sdk.add_transactions([tx], mapping=mapping_invalid)
    except KeyError as e:
        assert "invalid mapping" in str(e)

    try:
        enriched_mapping[0].merchant
    except AttributeError as e:
        assert str(e) == "'EnrichedTransaction' object has no attribute 'merchant'"

    assert enriched_mapping[0].company == "Amazon Web Services"
