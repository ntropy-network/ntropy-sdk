import os
from ntropy_sdk.ntropy_sdk import SDK, Transaction, AccountHolderType

sdk = SDK(os.getenv("NTROPY_API_KEY"))

transaction = Transaction(
    amount=1.0,
    description="AMAZON WEB SERVICES AWS.AMAZON.CO WA Ref5543286P25S: Crd15",
    entry_type="outgoing",
    account_holder_id="1",
    account_holder_type=AccountHolderType.business,
    country="US",
)

batch = sdk.enrich_batch([transaction])
enriched_list = batch.wait_with_progress()
print("BATCH", enriched_list.transactions[0].labels)

enriched = sdk.enrich(transaction)
print("REALTIME:", enriched.labels)
