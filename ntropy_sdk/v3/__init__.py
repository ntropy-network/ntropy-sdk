from typing import TYPE_CHECKING, Optional
import requests


if TYPE_CHECKING:
    from ntropy_sdk.ntropy_sdk import SDK
    from typing_extensions import TypedDict

    class ExtraKwargs(TypedDict, total=False):
        request_id: Optional[str]
        api_key: Optional[str]
        session: Optional[requests.Session]


from .transactions import TransactionsResource
from .batches import BatchesResource
from .bank_statements import BankStatementsResource
from .account_holders import AccountHoldersResource


class V3:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk
        self.transactions = TransactionsResource(sdk)
        self.batches = BatchesResource(sdk)
        self.bank_statements = BankStatementsResource(sdk)
        self.account_holders = AccountHoldersResource(sdk)
