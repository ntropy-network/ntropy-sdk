import os

import pytest


@pytest.fixture()
def payslip_sample():
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "payslip_sample.png"
    )


def test_add_payslip(sdk):
    with open("payslip_sample.png", "rb") as f:
        r = sdk.add_payslip(f, "payslip_sample.png")
        assert r.id is not None
        assert r.filename == "payslip_sample.png"
