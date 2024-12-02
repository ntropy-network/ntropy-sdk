import pytest
from ntropy_sdk.async_.sdk import AsyncSDK
from ntropy_sdk.v2.errors import NtropyValueError


@pytest.mark.asyncio
async def test_async_pagination(async_sdk: AsyncSDK):
    tx_ids = set()
    it = (await async_sdk.transactions.list(limit=2)).auto_paginate(page_size=2)
    i = 0
    async for tx in it:
        tx_ids.add(tx.id)
        i += 1
        if i == 10:
            break
    assert len(tx_ids) == 10


@pytest.mark.asyncio
async def test_recurrence_groups(async_sdk: AsyncSDK):
    sdk = async_sdk
    try:
        await sdk.account_holders.create(
            id="Xksd9SWd",
            type="consumer",
        )
    except NtropyValueError:
        pass

    for i in range(1, 5):
        await sdk.transactions.create(
            id=f"netflix-{i}",
            description=f"Recurring Debit Purchase Card 1350 #{i} netflix.com Netflix.com CA",
            amount=17.99,
            currency="USD",
            entry_type="outgoing",
            date=f"2021-0{i}-01",
            account_holder_id="Xksd9SWd",
        )

    recurring_groups = await sdk.account_holders.recurring_groups("Xksd9SWd")

    assert recurring_groups.groups[0].counterparty.website == "netflix.com"
    assert recurring_groups.groups[0].periodicity == "monthly"
