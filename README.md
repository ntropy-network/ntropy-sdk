# Ntropy SDK

This repository hosts the SDK for the Ntropy API.  To use the Ntropy API you require an API key which can be requested at [ntropy.com](https://ntropy.com).

The Ntropy API provides transaction enrichment and categorization, account ledger, metrics and custom model training. The full documentation is available at the [developer portal](https://developers.ntropy.com/).


## Installation

```bash
$ python3 -m pip install --upgrade 'ntropy-sdk'
```

## Quick Start


Enriching your first transaction requires an `SDK` object and an input `Transaction` object. The API key can be set in the environment variable `NTROPY_API_KEY` or in the `SDK` constructor:

```python
from ntropy_sdk import SDK, Transaction

sdk = SDK("YOUR-API-KEY")
tx = Transaction(
    description = "AMAZON WEB SERVICES",
    entry_type = "outgoing",
    amount = 12042.37,
    iso_currency_code = "USD",
    date = "2021-11-01",
    transaction_id = "4yp49x3tbj9mD8DB4fM8DDY6Yxbx8YP14g565Xketw3tFmn",
    country = "US",
    account_holder_id = "id-1",
    account_holder_type = "business",
    account_holder_name = "Robin's Tacos",
)

enriched_tx = sdk.add_transactions([tx])[0]
print(enriched_tx.merchant)
```

The returned `EnrichedTransaction` contains the added information by Ntropy API.  You can consult the Enrichment section of the documentation for more information on the parameters for both `Transaction` and `EnrichedTransaction`.

## Documentation

 You can consult in-depth documentation and examples at the [developer portal](https://developers.ntropy.com/docs/enrichment) and at the [SDK reference](https://developers.ntropy.com/sdk).

## License:
Free software: MIT license


