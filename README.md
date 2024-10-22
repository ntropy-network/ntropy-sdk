# Ntropy SDK

This repository hosts the SDK for the Ntropy API.  To use the Ntropy API you require an API key which can be requested at [ntropy.com](https://ntropy.com).

The Ntropy API provides transaction enrichment and categorization, account ledger and metrics. The full documentation is available at the [developer portal](https://developers.ntropy.com/).


## Installation

```bash
$ python3 -m pip install --upgrade 'ntropy-sdk'
```

## Quick Start


Enriching your first transaction requires an `SDK` object and an input `Transaction` object. The API key can be set in the environment variable `NTROPY_API_KEY` or in the `SDK` constructor:

```python
from ntropy_sdk import SDK, TransactionInput, LocationInput

sdk = SDK("YOUR-API-KEY")
r = sdk.transactions.create([
    TransactionInput(
        id = "4yp49x3tbj9mD8DB4fM8DDY6Yxbx8YP14g565Xketw3tFmn",
        description = "AMAZON WEB SERVICES",
        entry_type = "outgoing",
        amount = 12042.37,
        currency = "USD",
        date = "2021-11-01",
        location = LocationInput(
            country="US"
        ),
    )
])
print(r.transactions[0].entities.counterparty)
```

The returned `EnrichedTransaction` contains the added information by Ntropy API.  You can consult the Enrichment section of the documentation for more information on the parameters for both `Transaction` and `EnrichedTransaction`.

## Documentation

 You can consult in-depth documentation and examples at the [developer portal](https://developers.ntropy.com/docs/enrichment) and at the [SDK reference](https://developers.ntropy.com/sdk).

## License:
Free software: MIT license


