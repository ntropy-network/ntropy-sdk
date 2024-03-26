import os

import pytest

from ntropy_sdk.errors import NtropyDatasourceError
from ntropy_sdk.ntropy_sdk import BankStatementRequest
from ntropy_sdk.utils import AccountHolderType


@pytest.fixture()
def bank_statement_sample():
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "bank_statement_sample.pdf"
    )


def test_submit_bank_statement(sdk, bank_statement_sample):
    with open(bank_statement_sample, "rb") as f:
        bsr = sdk.add_bank_statement(
            file=f,
            filename="bank_statement_sample.pdf",
            account_type=AccountHolderType.business,
        )
        bs = bsr.poll()
        assert bs.status == "queued"
        assert bs.account_type == AccountHolderType.business
        assert bs.id is not None


def test_processed_bank_statement(sdk, bank_statement_sample):
    bsr = BankStatementRequest(
        sdk=sdk,
        filename="file",
        bs_id="940ee882-cdf1-4770-838b-35e31859b56e",
    )

    df = bsr.wait()
    assert len(df) > 0
    assert df[3].merchant == "RBC"


def test_processed_bank_statement_error(sdk, bank_statement_sample):
    bsr = BankStatementRequest(
        sdk=sdk,
        filename="file",
        bs_id="adcdc5f8-9ce9-46a2-978c-2148a3271d50",
    )

    with pytest.raises(NtropyDatasourceError) as e:
        bsr.wait()
    assert e.value.error_code == 415
    assert "file type not supported" in e.value.error.lower()
