import os
import time
import csv
import uuid
import requests
import logging
import re
import math

from datetime import datetime, date
from typing import Optional
from tqdm.auto import tqdm
from typing import List
from urllib.parse import urlencode
from functools import singledispatchmethod

DEFAULT_TIMEOUT = 10 * 60
DEFAULT_WITH_PROGRESS = True
ACCOUNT_HOLDER_TYPES = ["consumer", "business", "freelance", "unknown"]
COUNTRY_REGEX = re.compile(r"^[A-Z]{2}(-[A-Z0-9]{1,3})?$")
ENV_NTROPY_API_TOKEN = "NTROPY_API_KEY"


class NtropyError(Exception):
    pass


class NtropyBatchError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


def _assert_type(value, name, expected_type):
    if not isinstance(value, expected_type):
        raise TypeError(f"{name} should be of type {expected_type}")

    if expected_type == float or (
        isinstance(expected_type, tuple) and float in expected_type
    ):
        if math.isnan(value):
            raise ValueError(f"{name} value cannot be NaN")

    return True


class Transaction:
    _zero_amount_check = True

    required_fields = [
        "amount",
        "date",
        "description",
        "entry_type",
        "iso_currency_code",
        "transaction_id",
    ]

    fields = [
        "account_holder_id",
        "account_holder_type",
        "transaction_id",
        "amount",
        "date",
        "description",
        "entry_type",
        "iso_currency_code",
        "country",
        "mcc",
    ]

    def __init__(
        self,
        amount,
        date,
        description,
        entry_type,
        iso_currency_code,
        account_holder_id=None,
        account_holder_type=None,
        country=None,
        transaction_id=None,
        mcc=None,
    ):
        if not transaction_id:
            transaction_id = str(uuid.uuid4())
        else:
            _assert_type(transaction_id, "transaction_id", str)

        self.transaction_id = transaction_id

        if account_holder_id is not None:
            _assert_type(account_holder_id, "account_holder_id", str)
        self.account_holder_id = account_holder_id

        if account_holder_type is not None:
            _assert_type(account_holder_type, "account_holder_type", str)
            if account_holder_type not in ACCOUNT_HOLDER_TYPES:
                raise ValueError(
                    f"account_holder_type must be one of {ACCOUNT_HOLDER_TYPES}"
                )
        self.account_holder_type = account_holder_type

        _assert_type(amount, "amount", (int, float))
        if (amount == 0 and self._zero_amount_check) or amount < 0:
            raise ValueError(
                "amount must be a positive number. For negative amounts, change the entry_type field."
            )

        self.amount = amount

        try:
            _assert_type(date, "date", str)
            datetime.strptime(date, "%Y-%m-%d")
        except (ValueError, TypeError):
            raise ValueError("date must be of the format %Y-%m-%d")

        self.date = date

        _assert_type(description, "description", str)
        self.description = description

        _assert_type(entry_type, "entry_type", str)
        if entry_type not in ["debit", "credit", "outgoing", "incoming"]:
            raise ValueError("entry_type nust be one of 'incoming' or 'outgoing'")

        self.entry_type = entry_type

        _assert_type(iso_currency_code, "iso_currency_code", str)
        self.iso_currency_code = iso_currency_code

        if country is not None:
            _assert_type(country, "country", str)
            if not COUNTRY_REGEX.match(country):
                raise ValueError("country should be in ISO-3611-2 format")

        self.country = country

        if mcc:
            _assert_type(mcc, "mcc", int)
            if not (1000 <= mcc <= 9999):
                raise ValueError("mcc must be in the range of 1000-9999")

        self.mcc = mcc

        for field in self.required_fields:
            if getattr(self, field) is None:
                raise ValueError(f"{field} should be set")

    @classmethod
    def disable_zero_amount_check(cls):
        cls._zero_amount_check = False

    @classmethod
    def enable_zero_amount_check(cls):
        cls._zero_amount_check = True

    def __repr__(self):
        return f"Transaction(transaction_id={self.transaction_id}, description={self.description}, amount={self.amount}, entry_type={self.entry_type})"

    @classmethod
    def from_dict(cls, val: dict):
        return cls(**val)

    def to_dict(self):
        tx_dict = {}
        for field in self.fields:
            value = getattr(self, field)
            if value is not None:
                tx_dict[field] = value

        return tx_dict


class AccountHolder:
    def __init__(
        self,
        id: str,
        type: str,
        name: str = None,
        industry: str = None,
        website: str = None,
    ):
        if not id:
            raise ValueError("id must be set")

        if not type:
            raise ValueError("type must be set")

        if type not in ACCOUNT_HOLDER_TYPES:
            raise ValueError("type is not valid")

        self.id = id
        self.type = type
        self.name = name
        self.industry = industry
        self.website = website
        self._sdk = None

    def set_sdk(self, sdk):
        self._sdk = sdk

    def to_dict(self):
        out = {"id": self.id, "type": self.type}
        for key in ("name", "industry", "website"):
            if value := getattr(self, key):
                out[key] = value

        return out

    def get_metrics(self, metrics: List[str], start: date, end: date):
        if not self._sdk:
            raise ValueError(
                "sdk is not set: either call SDK.create_account_holder or set self._sdk first"
            )
        return self._sdk.get_account_holder_metrics(self.id, metrics, start, end)


class EnrichedTransaction:
    def __init__(
        self,
        sdk,
        labels=None,
        location=None,
        logo=None,
        merchant=None,
        person=None,
        transaction_id=None,
        website=None,
        chart_of_accounts=None,
        recurrence=None,
        confidence=None,
        transaction_type=None,
        **kwargs,
    ):
        self.sdk = sdk
        self.labels = labels
        self.location = location
        self.logo = logo
        self.merchant = merchant
        self.person = person
        self.transaction_id = transaction_id
        self.website = website
        self.kwargs = kwargs
        self.chart_of_accounts = chart_of_accounts
        self.recurrence = recurrence
        self.confidence = confidence
        self.transaction_type = transaction_type

    def __repr__(self):
        return f"EnrichedTransaction(transaction_id={self.transaction_id}, merchant={self.merchant}, logo={self.logo}, labels={self.labels})"

    def report(
        self,
        **kwargs,
    ):
        return self.sdk.retry_ratelimited_request(
            "POST", "/v2/report", {"transaction_id": self.transaction_id, **kwargs}
        )

    @classmethod
    def from_dict(cls, sdk, val: dict):
        return cls(sdk, **val)

    def to_dict(self):
        return {
            "labels": self.labels,
            "location": self.location,
            "logo": self.logo,
            "merchant": self.merchant,
            "person": self.person,
            "transaction_id": self.transaction_id,
            "website": self.website,
            "chart_of_accounts": self.chart_of_accounts,
            "recurrence": self.recurrence,
            "confidence": self.confidence,
            "transaction_type": self.transaction_type,
        }


class EnrichedTransactionList(list):
    def __init__(self, transactions: List[EnrichedTransaction]):
        super().__init__(transactions)
        self.transactions = transactions

    def to_csv(self, filepath):
        if not len(self):
            return
        with open(filepath, "w") as fp:
            tx = self[0]
            writer = csv.DictWriter(fp, tx.to_dict().keys())
            writer.writeheader()
            for tx in self:
                writer.writerow(tx.to_dict())

    @classmethod
    def from_list(cls, sdk, vals: list):
        return cls([EnrichedTransaction.from_dict(sdk, val) for val in vals])


class Batch:
    def __init__(
        self,
        sdk,
        batch_id,
        timeout=4 * 60 * 60,
        poll_interval=10,
        num_transactions=0,
    ):
        self.batch_id = batch_id
        self.timeout = time.time() + timeout
        self.poll_interval = poll_interval
        self.sdk = sdk
        self.num_transactions = num_transactions

    def __repr__(self):
        return f"Batch(id={self.batch_id}"

    def poll(self):
        url = f"/v2/transactions/async/{self.batch_id}"

        json_resp = self.sdk.retry_ratelimited_request("GET", url, None).json()
        status, results = json_resp.get("status"), json_resp.get("results", [])

        if status == "finished":
            return EnrichedTransactionList.from_list(self.sdk, results), status

        if status == "error":
            raise NtropyBatchError(f"Batch failed: {results}", errors=results)

        return json_resp, status

    def wait(self, with_progress=DEFAULT_WITH_PROGRESS, poll_interval=None):
        if with_progress:
            return self._wait_with_progress(poll_interval=poll_interval)
        else:
            return self._wait(poll_interval=poll_interval)

    def _wait(self, poll_interval=None):
        if not poll_interval:
            poll_interval = self.poll_interval
        while self.timeout - time.time() > 0:
            resp, status = self.poll()
            if status == "started":
                time.sleep(poll_interval)
                continue
            return resp
        raise NtropyError("Batch wait timeout")

    def _wait_with_progress(self, poll_interval=None):
        if not poll_interval:
            poll_interval = self.poll_interval
        with tqdm(total=self.num_transactions, desc="started") as progress:
            while self.timeout - time.time() > 0:
                resp, status = self.poll()
                if status == "started":
                    diff_n = resp.get("progress", 0) - progress.n
                    progress.update(diff_n)
                    time.sleep(poll_interval)
                    continue
                progress.desc = status
                diff_n = self.num_transactions - progress.n
                progress.update(diff_n)
                return resp
            raise NtropyError("Batch wait timeout")


class SDK:
    MAX_BATCH_SIZE = 100000
    MAX_SYNC_BATCH = 4000
    DEFAULT_MAPPING = {
        "merchant": "merchant",
        "website": "website",
        "labels": "labels",
        "logo": "logo",
        "location": "location",
        "person": "person",
        # the entire enriched transaction object is at _output_tx
    }

    def __init__(
        self,
        token: Optional[str] = None,
        timeout: int = DEFAULT_TIMEOUT,
        with_progress: bool = DEFAULT_WITH_PROGRESS,
    ):
        if not token:
            if ENV_NTROPY_API_TOKEN not in os.environ:
                raise NtropyError(
                    "API Token must be passed as an argument or set in the env. variable NTROPY_API_TOKEN"
                )
            token = os.environ[ENV_NTROPY_API_TOKEN]

        self.base_url = "https://api.ntropy.com"
        self.retries = 10
        self.token = token
        self.session = requests.Session()
        self.logger = logging.getLogger("Ntropy-SDK")
        self._timeout = timeout
        self._with_progress = with_progress

    def retry_ratelimited_request(
        self, method: str, url: str, payload: object, log_level=logging.DEBUG
    ):
        for i in range(self.retries):
            resp = self.session.request(
                method,
                self.base_url + url,
                json=payload,
                headers={"X-API-Key": self.token},
                timeout=self._timeout,
            )
            if resp.status_code == 429:
                try:
                    retry_after = int(resp.headers.get("retry-after", "1"))
                except ValueError:
                    retry_after = 1
                if retry_after <= 0:
                    retry_after = 1

                self.logger.log(
                    log_level, "Retrying in %s seconds due to ratelimit", retry_after
                )
                time.sleep(retry_after)
                continue
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                if e.response.headers.get("content-type") == "application/json":
                    raise NtropyError(e.response.json()) from e
                raise
            return resp
        raise NtropyError(f"Failed to {method} {url} after {self.retries} attempts")

    def create_account_holder(self, account_holder: AccountHolder):
        if not isinstance(account_holder, AccountHolder):
            raise ValueError("account_holder should be of type AccountHolder")

        url = "/v2/account-holder"

        self.retry_ratelimited_request("POST", url, account_holder.to_dict())
        account_holder.set_sdk(self)

    @singledispatchmethod
    def add_transactions(
        self,
        df,
        mapping=None,
        poll_interval=10,
        with_progress=None,
        labeling=True,
        create_account_holders=False,
        model=None,
    ):
        try:
            import pandas as pd
        except ImportError:
            # If here, the input data is not a dataframe, or import would succeed
            raise ValueError(
                f"add_transactions takes either a pandas.DataFrame or a list of Transactions for it's `df` parameter, you supplied a '{type(df)}'"
            )

        if not isinstance(df, pd.DataFrame):
            raise TypeError("Transactions object needs to be a pandas dataframe.")

        if mapping is None:
            mapping = self.DEFAULT_MAPPING.copy()

        required_columns = [
            "amount",
            "date",
            "description",
            "entry_type",
            "iso_currency_code",
            "account_holder_id",
            "account_holder_type",
        ]

        cols = set(df.columns)
        missing_cols = set(required_columns).difference(cols)
        if missing_cols:
            raise KeyError(f"Missing columns {missing_cols}")
        overlapping_cols = set(mapping.values()).intersection(cols)
        if overlapping_cols:
            raise KeyError(
                f"Overlapping columns {overlapping_cols} will be overwritten"
                "- consider overriding the mapping keyword argument, or move the existing columns to another column"
            )

        def to_tx(row):
            return Transaction(
                amount=row["amount"],
                date=row.get("date"),
                description=row.get("description", ""),
                entry_type=row["entry_type"],
                iso_currency_code=row["iso_currency_code"],
                account_holder_id=row["account_holder_id"],
                account_holder_type=row["account_holder_type"],
                country=row.get("country"),
                transaction_id=row.get("transaction_id"),
            )

        txs = df.apply(to_tx, axis=1).to_list()

        df["_output_tx"] = self.add_transactions(
            txs,
            labeling=labeling,
            create_account_holders=create_account_holders,
            poll_interval=poll_interval,
            with_progress=with_progress,
            model=model,
        )

        def get_tx_val(tx, v):
            sentinel = object()
            output = getattr(tx, v, tx.kwargs.get(v, sentinel))
            if output == sentinel:
                raise KeyError(f"invalid mapping: {v} not in {tx}")
            return output

        for k, v in mapping.items():
            df[v] = df["_output_tx"].apply(lambda tx: get_tx_val(tx, k))
        df = df.drop(["_output_tx"], axis=1)
        return df

    @add_transactions.register(list)
    def _(
        self,
        transactions: List[Transaction],
        timeout=4 * 60 * 60,
        poll_interval=10,
        with_progress=None,
        labeling=True,
        create_account_holders=False,
        model=None,
    ):
        if len(transactions) > self.MAX_BATCH_SIZE:
            chunks = [
                transactions[i : (i + self.MAX_BATCH_SIZE)]
                for i in range(0, len(transactions), self.MAX_BATCH_SIZE)
            ]

            arr = []
            for chunk in chunks:
                arr += self._add_transactions(chunk)
                time.sleep(self.MAX_BATCH_SIZE / 1000)

            return arr

        return self._add_transactions(
            transactions,
            timeout,
            poll_interval,
            with_progress,
            labeling,
            create_account_holders,
            model,
        )

    def _add_transactions(
        self,
        transactions: List[Transaction],
        timeout=4 * 60 * 60,
        poll_interval=10,
        with_progress=None,
        labeling=True,
        create_account_holders=False,
        model=None,
    ):
        is_sync = len(transactions) <= self.MAX_SYNC_BATCH

        params = {
            "labeling": labeling,
            "create_account_holders": create_account_holders,
        }
        if model is not None:
            params["model_name"] = model

        params_str = urlencode(params)

        try:

            url = f"/v2/transactions/{'sync' if is_sync else 'async'}?" + params_str

            data = [transaction.to_dict() for transaction in transactions]
            resp = self.retry_ratelimited_request("POST", url, data)

            if is_sync:

                if resp.status_code != 200:
                    raise NtropyBatchError("Batch failed", errors=resp.json())

                return EnrichedTransactionList.from_list(self, resp.json())

            else:

                r = resp.json()
                batch_id = r.get("id", "")

                if not batch_id:
                    raise ValueError("batch_id missing from response")

                with_progress = with_progress or self._with_progress

                return Batch(
                    self,
                    batch_id,
                    timeout=timeout,
                    poll_interval=poll_interval,
                    num_transactions=len(transactions),
                ).wait(with_progress=with_progress)

        except requests.HTTPError as e:
            if e.response.status_code == 404:
                error = e.response.json()
                raise ValueError(f"{error['detail']}: {error['missingIds']}")

            raise

    def get_account_holder(self, account_holder_id: str):
        url = f"/v2/account-holder/{account_holder_id}"
        try:
            response = self.retry_ratelimited_request("GET", url, None).json()
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                error = e.response.json()
                raise ValueError(f"{error['detail']}")
            raise

        return response

    def get_account_holder_metrics(
        self, account_holder_id: str, metrics: List[str], start: date, end: date
    ):
        if not isinstance(account_holder_id, str):
            raise ValueError("account_holder_id should be of type string")

        url = f"/v2/account-holder/{account_holder_id}/query"

        payload = {
            "metrics": metrics,
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
        }

        response = self.retry_ratelimited_request("POST", url, payload)
        return response.json()

    def get_labels(self, account_holder_type: str):
        assert account_holder_type in ACCOUNT_HOLDER_TYPES
        url = f"/v2/labels/hierarchy/{account_holder_type}"
        resp = self.retry_ratelimited_request("GET", url, None)
        return resp.json()

    def get_chart_of_accounts(self):
        url = "/v2/chart-of-accounts"
        resp = self.retry_ratelimited_request("GET", url, None)
        return resp.json()

    def list_models(self):
        url = "/v2/models"
        r = self.retry_ratelimited_request("GET", url, None).json()
        return r
