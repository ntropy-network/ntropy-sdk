import os
from itertools import islice
from ntropy_sdk import SDK
from .. import API_KEY


def test_pagination(sdk: SDK):
    tx_ids = set()
    it = sdk.transactions.list(limit=2).auto_paginate(page_size=2)
    for tx in islice(it, 10):
        tx_ids.add(tx.id)
    assert len(tx_ids) == 10


def test_readme():
    readme_file = open(
        os.path.join(os.path.dirname(__file__), "..", "..", "README.md")
    ).read()
    readme_data = readme_file.split("```python")[1].split("```")[0]
    readme_data = readme_data.replace("YOUR-API-KEY", API_KEY)
    exec(readme_data, globals())
