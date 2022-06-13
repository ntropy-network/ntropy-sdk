import os
import pytest
import uuid
import pandas as pd

from ntropy_sdk import (
    SDK,
    Transaction,
    EnrichedTransaction,
    AccountHolder,
)
from ntropy_sdk.ntropy_sdk import ACCOUNT_HOLDER_TYPES

sdk = SDK("R6yiREz91PfXPjIfsjHqpVE4VGgjATXVjGrxuv3H")


id_tx = Transaction(
    amount=24.56,
    description="TARGET T- 5800 20th St 11/30/19 17:32",
    entry_type="debit",
    date="2012-12-10",
    account_holder_id="1",
    iso_currency_code="USD",
    mcc=5432,
)
enriched = sdk.add_transactions([id_tx] * 25)

print(enriched[0])
