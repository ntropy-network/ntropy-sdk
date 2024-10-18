from typing import TYPE_CHECKING

from ntropy_sdk.account_holders import AccountHoldersResource
from ntropy_sdk.bank_statements import BankStatementsResource
from ntropy_sdk.batches import BatchesResource
from ntropy_sdk.http import HttpClient
from ntropy_sdk.transactions import TransactionsResource

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs
    from typing_extensions import Unpack


class SDK:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key
        self.http_client = HttpClient()
        self.transactions = TransactionsResource(self)
        self.batches = BatchesResource(self)
        self.bank_statements = BankStatementsResource(self)
        self.account_holders = AccountHoldersResource(self)

    def retry_ratelimited_request(
        self,
        *args,
        **kwargs: "Unpack[ExtraKwargs]",
    ):
        kwargs_copy = kwargs.copy()
        if self.api_key and not kwargs_copy.get("api_key"):
            kwargs_copy["api_key"] = self.api_key
        return self.http_client.retry_ratelimited_request(*args, **kwargs_copy)
