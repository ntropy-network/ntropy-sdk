import os
from itertools import islice

from ntropy_sdk import (
    SDK,
    TransactionInput,
    AccountHolder,
    NtropyValueError,
)


def test_pagination(sdk: SDK):
    tx_ids = set()
    it = sdk.transactions.list(limit=2).auto_paginate(page_size=2)
    for tx in islice(it, 10):
        tx_ids.add(tx.id)
    assert len(tx_ids) == 10


def test_readme(api_key):
    readme_file = open(
        os.path.join(os.path.dirname(__file__), "..", "..", "README.md")
    ).read()
    readme_data = readme_file.split("```python")[1].split("```")[0]
    readme_data = readme_data.replace("YOUR-API-KEY", api_key)
    exec(readme_data, globals())


def test_recurrence_groups(sdk):
    try:
        sdk.account_holders.create(
            id="Xksd9SWd",
            type="consumer",
        )
    except NtropyValueError:
        pass

    for i in range(1, 5):
        sdk.transactions.create(
            id=f"netflix-{i}",
            description=f"Recurring Debit Purchase Card 1350 #{i} netflix.com Netflix.com CA",
            amount=17.99,
            currency="USD",
            entry_type="outgoing",
            date=f"2021-0{i}-01",
            account_holder_id="Xksd9SWd",
        )

    recurring_groups = sdk.account_holders.recurring_groups("Xksd9SWd")

    assert recurring_groups.groups[0].counterparty.website == "netflix.com"
    assert recurring_groups.groups[0].periodicity == "monthly"
