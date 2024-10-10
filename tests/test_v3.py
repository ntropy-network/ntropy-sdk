from itertools import islice
from ntropy_sdk import SDK


def test_pagination(sdk: SDK):
    tx_ids = set()
    it = sdk.v3.transactions.list(limit=2).auto_paginate(page_size=2)
    for tx in islice(it, 10):
        tx_ids.add(tx.id)
    assert len(tx_ids) == 10
