# Ntropy SDK

SDK for the Ntropy API

## Installation:

```bash
$ pip install --upgrade 'ntropy-sdk[benchmark]'
```

## Usage:
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

### Using this as a python library:

```python
import os
import uuid
from datetime import datetime
from ntropy_sdk.ntropy_sdk import SDK, AccountTransaction, AccountHolder

sdk = SDK(os.getenv("NTROPY_API_KEY"))

account_holder = AccountHolder(
    id=str(uuid.uuid4()),
    type="business",
    industry="SaaS",
    website="mycorp.com"
)
sdk.create_account_holder(account_holder)

transaction = AccountTransaction(
    amount=1.0,
    description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S: Crd15",
    date="2021-12-13",
    entry_type="outgoing",
    iso_currency_code="USD",
    country="US",
)

batch = account_holder.enrich_batch([transaction])
enriched_list = batch.wait_with_progress()
print("BATCH", enriched_list.transactions[0].labels)

enriched = account_holder.enrich(transaction)
print("REALTIME:", enriched.labels)

balance = account_holder.get_metrics(['amount'], start=datetime.strptime("2021-12-01", "%Y-%m-%d"), end=datetime.strptime("2022-01-01", "%Y-%m-%d"))
print("BALANCE:", balance['amount'])
```

## License:
Free software: MIT license


