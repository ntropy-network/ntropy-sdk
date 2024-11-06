# Ntropy SDK

This repository hosts the SDK for the Ntropy API. It provides transaction enrichment and categorization, account ledger
and metrics. The full documentation is available at the [developer portal](https://developers.ntropy.com/).

To obtain an API key, you can follow this [guide](https://docs.ntropy.com/onboarding)

## Installation

```bash
$ python3 -m pip install --upgrade 'ntropy-sdk'
```

## Quick Start

Enriching your first transaction requires an `SDK` object and an input `Transaction` object. The API key can be set in
the environment variable `NTROPY_API_KEY` or in the `SDK` constructor:

```python
from ntropy_sdk import SDK

sdk = SDK("YOUR-API-KEY")
r = sdk.transactions.create(
    id="4yp49x3tbj9mD8DB4fM8DDY6Yxbx8YP14g565Xketw3tFmn",
    description="AMAZON WEB SERVICES",
    entry_type="outgoing",
    amount=12042.37,
    currency="USD",
    date="2021-11-01",
    location=dict(
        country="US"
    ),
)
print(r)
```

The returned `EnrichedTransaction` contains the added information by Ntropy API. You can consult the Enrichment section
of the documentation for more information on the parameters for both `Transaction` and `EnrichedTransaction`.

## Documentation

You can consult in-depth documentation and examples at
the [developer portal](https://developers.ntropy.com/docs/enrichment) and at
the [SDK reference](https://developers.ntropy.com/sdk).

## License:
Free software: MIT license


