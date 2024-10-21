from typing import TYPE_CHECKING, Dict, Optional

from ntropy_sdk.account_holders import AccountHoldersResource
from ntropy_sdk.bank_statements import BankStatementsResource
from ntropy_sdk.batches import BatchesResource
from ntropy_sdk.http import HttpClient
from ntropy_sdk.rules import RulesResource
from ntropy_sdk.transactions import TransactionsResource
from ntropy_sdk.v2.ntropy_sdk import DEFAULT_REGION, ALL_REGIONS
from ntropy_sdk.webhooks import WebhooksResource

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs
    from typing_extensions import Unpack


class SDK:
    def __init__(
        self,
        api_key: Optional[str] = None,
        region: str = DEFAULT_REGION,
    ):
        self.base_url = ALL_REGIONS[region]
        self.api_key = api_key
        self.http_client = HttpClient()
        self.transactions = TransactionsResource(self)
        self.batches = BatchesResource(self)
        self.bank_statements = BankStatementsResource(self)
        self.account_holders = AccountHoldersResource(self)
        self.webhooks = WebhooksResource(self)
        self.rules = RulesResource(self)

    def retry_ratelimited_request(
        self,
        *,
        method: str,
        url: str,
        params: Optional[Dict[str, str]] = None,
        payload: Optional[object] = None,
        payload_json_str: Optional[str] = None,
        **kwargs: "Unpack[ExtraKwargs]",
    ):
        kwargs_copy = kwargs.copy()
        if self.api_key and not kwargs_copy.get("api_key"):
            kwargs_copy["api_key"] = self.api_key

        return self.http_client.retry_ratelimited_request(
            method=method,
            url=self.base_url + url,
            params=params,
            payload=payload,
            payload_json_str=payload_json_str,
            **kwargs_copy,
        )
