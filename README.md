# Ntropy SDK

This repository hosts the SDK for the Ntropy API.
The Ntropy API provides transaction enrichment and categorization, ledger and metrics accounting and 


## Installation:

```bash
$ pip install --upgrade 'ntropy-sdk[benchmark]'
```

## Enrichment usage:

You can initialize the SDK by passing an API token to the constructor, or using the environment variable `NTROPY_API_KEY`

```python
import os
import uuid
from datetime import datetime
from ntropy_sdk import SDK, Transaction, AccountHolder

# initialize from environment variable NTROPY_API_KEY
sdk = SDK()

```

The SDK contains methods to explore the available labels and chart of accounts:

```python
for account_holder_type in ['business', 'consumer', 'freelance', 'unknown']:
  print("ACCOUNT HOLDER TYPE:", account_holder_type)
  print(sdk.get_labels(account_holder_type))

print("CHART of ACCOUNTS:", sdk.get_chart_of_accounts())

```

In order to enrich a transaction it must be associated with an account holder. An account holder represents an entity (business, freelancer or consumer) that can be uniquely identified and holds the account associated with the transaction:

- In an outgoing (debit) transaction, the account holder is the sender and the merchant is the receiver.
- In an incoming (credit) transaction, the account holder is the receiver and the merchant is the sender.

Account holders provide important context for understanding transactions.

```python
account_holder = AccountHolder(
    id=str(uuid.uuid4()),
    type="business",
    name="Ntropy Network Inc.",
    industry="fintech",
    website="ntropy.com"
)
sdk.create_account_holder(account_holder)
```

A new transaction can then be associated with the created account holder and enriched. Not all attributes are mandatory, but if known should be provided, to aid the enrichment:

```python
transaction = Transaction(
    amount=12046.15,
    date="2021-12-13",
    description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S: Crd15",
    entry_type="outgoing",
    iso_currency_code="USD",
    account_holder_id=account_holder.id,
    country="US",
)

transaction_list = [transaction]

enriched_transactions = sdk.add_transactions(transaction_list)
print("ENRICHED:", enriched_transactions)
```

The API can be queried for specific stats regarding account holders such as the total amount in transactions in a time interval:

```python

query = account_holder.get_metrics(['amount'], start=datetime.strptime("2021-12-01", "%Y-%m-%d"), end=datetime.strptime("2022-01-01", "%Y-%m-%d"))
print("QUERY:", query['amount'])

```

For bulk data, the API can easily be used along with some popular libraries such as `pandas` to facilitate the process:

```python
import pandas as pd

df = pd.DataFrame(data = [[12046.15, '2021-12-13', "AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S: Crd15", 'outgoing', 'USD', 'US', str(uuid.uuid4()), 'business', "Ntropy Network Inc.", "fintech", "ntropy.com"]], columns = ["amount", "date", "description", "entry_type", "iso_currency_code", "country", "account_holder_id", "account_holder_type", "account_holder_name", "account_holder_industry", "account_holder_website"])

def create_account_holder(row):
    sdk.create_account_holder(
        AccountHolder(
            id=row["account_holder_id"],
            type=row["account_holder_type"],
            name=row.get("account_holder_name"),
            industry=row.get("account_holder_industry"),
            website=row.get("account_holder_website"),
        )
    )

df.groupby("account_holder_id", as_index=False).first().apply(create_account_holder, axis=1)

enriched_df = sdk.add_transactions(df)
print("ENRICHED:", enriched_df)

```

## License:
Free software: MIT license


