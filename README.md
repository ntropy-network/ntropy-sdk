# Ntropy SDK

SDK for the Ntropy API

## Installation:

```bash
$ pip install --upgrade 'ntropy-sdk[benchmark]'
```

## Usage:

```python
import os
import uuid
from datetime import datetime
from ntropy_sdk import SDK, Transaction, AccountHolder

sdk = SDK(os.getenv("NTROPY_API_KEY"))

for account_holder_type in ['business', 'consumer', 'freelance', 'unknown']:
  print("ACCOUNT HOLDER TYPE:", account_holder_type)
  print(sdk.get_labels(account_holder_type))

print("CHART of ACCOUNTS:", sdk.get_chart_of_accounts())

account_holder = AccountHolder(
    id=str(uuid.uuid4()),
    type="business",
    name="Ntropy Network Inc.",
    industry="fintech",
    website="ntropy.com"
)
sdk.create_account_holder(account_holder)

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

query = account_holder.get_metrics(['amount'], start=datetime.strptime("2021-12-01", "%Y-%m-%d"), end=datetime.strptime("2022-01-01", "%Y-%m-%d"))
print("QUERY:", query['amount'])

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

### Programmatic usage for benchmarking:
Assuming you have a CSV file called testset.csv with the following fields set:
* iso_currency_code: The currency of the transaction
* amount: The amount (a positive number)
* entry_type: incoming for money coming into the account, outgoing for money going out of the account
* description: The transaction description string
* account_holder_id: A unique identifier for the account holder
* account_holder_type: A string indicating the type of the account of the transaction (business or consumer)
* (optional) correct_merchant: A string that should match the merchant output of the API. If you combine this with --ground-truth-merchant-field=correct_merchant you will be given an accuracy % for the API.
* (optional) correct_labels: The label that the transaction should be marked as. If you combine this with --ground-truth-label-field=correct_labels you will be given an F1 score for the API.


```bash
$ ntropy-benchmark --api-key=$NTROPY_API_KEY --in-csv-file=testset.csv --out-csv-file=enriched.csv --ground-truth-label-field=correct_labels
```

## License:
Free software: MIT license


