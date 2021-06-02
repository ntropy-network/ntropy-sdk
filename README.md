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
    entry_type="debit",
    entity_id="1",
)

batch = sdk.classify_batch([transaction])
enriched_list = batch.wait_with_progress()
print("BATCH", enriched_list.transactions[0].labels)

enriched = sdk.classify_realtime(transaction)
print("REALTIME:", enriched.labels)
```

* Free software: MIT license


