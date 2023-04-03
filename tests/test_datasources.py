import os

import pytest

from ntropy_sdk.ntropy_sdk import BankStatementRequest


@pytest.fixture()
def bank_statement_sample():
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "bank_statement_sample.jpg"
    )


def test_submit_bank_statement(sdk, bank_statement_sample):
    with open(bank_statement_sample, "rb") as f:
        bs = sdk.add_bank_statement(file=f, filename="bank_statement_sample.jpg")
        r, status = bs.poll()
        assert status == "queued"
        assert bs.bs_id == r["id"]


def test_processed_bank_statement(sdk, bank_statement_sample):
    bsr = BankStatementRequest(
        sdk=sdk,
        filename="file",
        bs_id="d192b263-8332-430c-a5c1-433862eac7ea",
    )

    bs = bsr.wait()
    enriched = sdk.add_transactions(bs.transactions)
    assert len(enriched) > 0
    assert enriched[0].merchant == "Ministrum"
