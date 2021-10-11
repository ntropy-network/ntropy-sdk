# Ntropy SDK

SDK for the Ntropy API

Installation:

```bash
$ pip install --upgrade ntropy-sdk
```

Usage:

```python
import os
from ntropy_sdk.ntropy_sdk import SDK, Transaction

sdk = SDK(os.getenv("NTROPY_API_KEY"))

transaction = Transaction(
    amount=1.0,
    description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S: Crd15",
    is_business=True,
    entry_type="outgoing",
    account_holder_id="1",
    country="US",
)

batch = sdk.enrich_batch([transaction])
enriched_list = batch.wait_with_progress()
print("BATCH", enriched_list.transactions[0].labels)

enriched = sdk.enrich(transaction)
print("REALTIME:", enriched.labels)
```

* Free software: MIT license


