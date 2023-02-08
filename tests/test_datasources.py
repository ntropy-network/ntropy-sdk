import os

import pytest

from ntropy_sdk.ntropy_sdk import Transaction


@pytest.fixture()
def bank_statement_sample():
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "bank_statement_sample.jpg"
    )


def test_submit_bank_statement(sdk, bank_statement_sample):
    with open(bank_statement_sample, "rb") as f:
        bs = sdk.add_bank_statement(file=f, filename="bank_statement_sample.jpg")
        r, status = bs.poll()
        assert status == "processing"
        assert bs.bs_id == r["id"]
