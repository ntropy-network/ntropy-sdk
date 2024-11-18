from datetime import datetime
from typing import Dict, Optional, TYPE_CHECKING

import aiohttp

from ntropy_sdk.account_holders import AccountHoldersResourceAsync
from ntropy_sdk.bank_statements import BankStatementsResourceAsync
from ntropy_sdk.batches import BatchesResourceAsync
from ntropy_sdk.categories import CategoriesResourceAsync
from ntropy_sdk.entities import EntitiesResourceAsync
from ntropy_sdk.reports import ReportsResourceAsync
from ntropy_sdk.rules import RulesResourceAsync
from ntropy_sdk.transactions import TransactionsResourceAsync
from ntropy_sdk.v2.ntropy_sdk import ALL_REGIONS, DEFAULT_REGION
from ntropy_sdk.webhooks import WebhooksResourceAsync

from .http import HttpClient

if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargsAsync
    from typing_extensions import Unpack, Self


class AsyncSDK:
    def __init__(
        self,
        api_key: Optional[str] = None,
        region: str = DEFAULT_REGION,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.base_url = ALL_REGIONS[region]
        self.api_key = api_key
        self.http_client = HttpClient(session=session)
        self.account_holders = AccountHoldersResourceAsync(self)
        self.batches = BatchesResourceAsync(self)
        self.bank_statements = BankStatementsResourceAsync(self)
        self.categories = CategoriesResourceAsync(self)
        self.entities = EntitiesResourceAsync(self)
        self.webhooks = WebhooksResourceAsync(self)
        self.reports = ReportsResourceAsync(self)
        self.rules = RulesResourceAsync(self)
        self.transactions = TransactionsResourceAsync(self)

    def retry_ratelimited_request(
        self,
        *,
        method: str,
        url: str,
        params: Optional[Dict[str, str | int | datetime | None]] = None,
        payload: Optional[object] = None,
        payload_json_str: Optional[str] = None,
        **kwargs: "Unpack[ExtraKwargsAsync]",
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

    def __aenter__(self) -> "Self":
        return self
