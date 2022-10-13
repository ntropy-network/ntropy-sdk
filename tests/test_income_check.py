import pytest
import pandas as pd
from ntropy_sdk.income_check import (
    UNDETERMINED_LABEL,
    IncomeGroup,
    IncomeLabelEnum,
    IncomeReport,
)


@pytest.fixture
def income_api_response():
    return [
        {
            "total_amount": 55266.34,
            "iso_currency_code": "USD",
            "first_payment_date": "2021-03-18",
            "income_type": "salary",
            "latest_payment_date": "2022-08-02",
            "duration": "1 year 4 months 15 day",
            "latest_payment_description": "Tesla PAYROLL",
            "pay_frequency": "bi-weekly",
            "is_active": False,
            "next_expected_payment_date": None,
            "next_expected_payment_amount": None,
            "source": "Tesla Inc.",
            "transaction_ids": [
                "b6cdb5bb-4dee-435a-84db-5c99fda70e50",
                "6245be6b-f982-4970-abf0-fc42854deee9",
            ],
        },
        {
            "total_amount": 625.98,
            "iso_currency_code": "USD",
            "first_payment_date": "2021-06-29",
            "income_type": "rideshare and delivery",
            "latest_payment_date": "2022-08-07",
            "duration": "1 year 1 months 8 day",
            "latest_payment_description": "Uber transfer",
            "pay_frequency": "other",
            "is_active": False,
            "next_expected_payment_date": None,
            "next_expected_payment_amount": None,
            "source": "Uber",
            "transaction_ids": [
                "752ec4e2-2dcc-49a6-9c22-64685dce400d",
                "6c2394b5-5061-4e41-9b9a-409d7f5c4409",
            ],
        },
        {
            "total_amount": 1234.37,
            "iso_currency_code": "USD",
            "first_payment_date": "2021-01-04",
            "income_type": "possible income - please verify",
            "latest_payment_date": "2022-08-01",
            "duration": "1 year 6 months 28 day",
            "latest_payment_description": "Account transfer money",
            "is_active": False,
            "next_expected_payment_date": None,
            "next_expected_payment_amount": None,
            "pay_frequency": "monthly",
            "source": None,
            "transaction_ids": [
                "40356870-aa3a-4866-bead-db7a163f3cf0",
                "2d820606-e12a-425a-a565-1c777189d14e",
            ],
        },
        {
            "total_amount": 45600.00,
            "iso_currency_code": "USD",
            "first_payment_date": "2021-08-01",
            "income_type": "long term rent",
            "latest_payment_date": "2022-08-01",
            "duration": "1 year",
            "latest_payment_description": "Rent transfer to REMAX",
            "is_active": False,
            "next_expected_payment_date": None,
            "next_expected_payment_amount": None,
            "pay_frequency": "yearly",
            "source": None,
            "transaction_ids": [
                "40352870-aa3a-4866-bead-db7a163f3cf0",
                "2d820609-e12a-425a-a565-1c777189d14e",
            ],
        },
    ]


def test_income_check_enum():
    all_labels = [k for k in IncomeLabelEnum]
    passive_labels = IncomeLabelEnum.passive_labels()
    earnings_labels = IncomeLabelEnum.earnings_labels()
    unk_labels = [k for k in IncomeLabelEnum if k.value.label == UNDETERMINED_LABEL]

    assert len(unk_labels) == 1
    assert len(passive_labels.union(earnings_labels)) == len(all_labels)
    assert len(all_labels) == len(set(all_labels))
    assert not set(passive_labels).intersection(set(earnings_labels))


def test_income_group():
    data = dict(
        total_amount=35.2,
        iso_currency_code="USD",
        first_payment_date="2022-08-01",
        latest_payment_date="2022-08-05",
        duration="4 months",
        is_active=False,
        latest_payment_description="KFC",
        pay_frequency="weekly",
        next_expected_payment_date=None,
        next_expected_payment_amount=None,
        income_type=IncomeLabelEnum.freelance.value.label,
        source="Kentucky Fried Chicken",
        transaction_ids=["id1", "id2"],
    )

    assert IncomeGroup.from_dict(data) is not None


def test_income_report(income_api_response):
    income_groups = sorted(
        [IncomeGroup.from_dict(d) for d in income_api_response],
        key=lambda x: float(x.total_amount),
        reverse=True,
    )
    report = IncomeReport(income_groups)
    df = report.to_df()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == len(income_api_response)
    for k in df.columns:
        assert (df[k] == "unknown").sum() == 0
    summary = report.summarize()

    assert summary.total_income == 55266.34 + 625.98 + 1234.37 + 45600.00
    assert summary.main_income_source == "Tesla Inc."
    assert summary.main_income_type == "salary"
    assert summary.earned_income > 0
    assert summary.passive_income > 0
    assert summary.possible_income > 0
    assert len(summary.earned_income_sources) > 0
    assert len(summary.passive_income_sources) == 0
    assert len(summary.possible_income_sources) == 0
    assert summary.passive_income > 0
    assert summary.possible_income > 0
