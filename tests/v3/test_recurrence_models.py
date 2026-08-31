import json

import pytest

from ntropy_sdk.account_holders import (
    AccountHoldersResource,
    AccountHoldersResourceAsync,
)
from ntropy_sdk.transactions import RecurrenceGroup, RecurrencePeriodicity
from ntropy_sdk.utils import pydantic_json


def _semi_monthly_group():
    return {
        "id": "recurrence-group-id",
        "start_date": "2026-06-01",
        "end_date": "2026-08-15",
        "total_amount": 6000,
        "average_amount": 1000,
        "periodicity_in_days": 15,
        "periodicity": "semi-monthly",
        "counterparty": {"type": "organization"},
        "categories": {},
        "transaction_ids": ["transaction-id"],
    }


class _Response:
    headers = {}

    def json(self):
        return [_semi_monthly_group()]


class _SyncSDK:
    def retry_ratelimited_request(self, **kwargs):
        return _Response()


class _AsyncResponse(_Response):
    async def json(self):
        return [_semi_monthly_group()]

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass


class _AsyncSDK:
    async def retry_ratelimited_request(self, **kwargs):
        return _AsyncResponse()


@pytest.mark.parametrize(
    "wire_value, member",
    [
        ("bi-weekly", RecurrencePeriodicity.bi_weekly),
        ("semi-monthly", RecurrencePeriodicity.semi_monthly),
        ("bi-monthly", RecurrencePeriodicity.bi_monthly),
        ("semi-yearly", RecurrencePeriodicity.semi_yearly),
    ],
)
def test_recurrence_periodicity_uses_api_wire_values(wire_value, member):
    assert RecurrencePeriodicity(wire_value) is member
    assert member.value == wire_value


@pytest.mark.parametrize(
    "legacy_value, member",
    [
        ("bi_weekly", RecurrencePeriodicity.bi_weekly),
        ("semi_monthly", RecurrencePeriodicity.semi_monthly),
        ("bi_monthly", RecurrencePeriodicity.bi_monthly),
        ("semi_yearly", RecurrencePeriodicity.semi_yearly),
    ],
)
def test_recurrence_periodicity_accepts_legacy_sdk_values(
    legacy_value, member
):
    assert RecurrencePeriodicity(legacy_value) is member


def test_recurrence_group_parses_and_serializes_semi_monthly():
    group = RecurrenceGroup(**_semi_monthly_group())

    assert group.periodicity is RecurrencePeriodicity.semi_monthly
    assert json.loads(pydantic_json(group))["periodicity"] == "semi-monthly"


def test_sync_recurring_groups_parses_semi_monthly():
    groups = AccountHoldersResource(_SyncSDK()).recurring_groups(
        "account-holder-id"
    )

    assert groups.groups[0].periodicity is RecurrencePeriodicity.semi_monthly


@pytest.mark.asyncio
async def test_async_recurring_groups_parses_semi_monthly():
    groups = await AccountHoldersResourceAsync(_AsyncSDK()).recurring_groups(
        "account-holder-id"
    )

    assert groups.groups[0].periodicity is RecurrencePeriodicity.semi_monthly
