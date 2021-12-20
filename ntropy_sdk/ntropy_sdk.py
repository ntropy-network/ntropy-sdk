import time
import sys
from datetime import datetime
import uuid
import requests
import logging
import enum
from tqdm.auto import tqdm
from typing import List, Dict
from urllib.parse import urlencode


DEFAULT_TIMEOUT = 10 * 60
ACCOUNT_HOLDER_TYPES = ["consumer", "business", "freelance", "unknown"]


class NtropyError(Exception):
    pass


class NtropyBatchError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class Transaction:
    _zero_amount_check = True

    required_fields = [
        "amount",
        "date",
        "description",
        "entry_type",
        "iso_currency_code",
        "transaction_id",
        "account_holder_id",
    ]

    fields = [
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
        transaction_id=None,
        amount=None,
        date=None,
        description=None,
        entry_type=None,
        iso_currency_code=None,
        country=None,
        mcc=None,
        account_holder_type=None,
        account_holder_id=None,
    ):
        if not transaction_id:
            transaction_id = str(uuid.uuid4())

        if not date:
            date = datetime.now().strftime("%Y-%m-%d")

        self.transaction_id = transaction_id

        if (amount == 0 and self._zero_amount_check) or amount < 0:
            raise ValueError(
                "amount must be a positive number. For negative amounts, change the entry_type field."
            )

        self.amount = amount
        self.date = date
        self.description = description

        if entry_type not in ["debit", "credit", "outgoing", "incoming"]:
            raise ValueError("entry_type nust be one of 'incoming' or 'outgoing'")

        self.entry_type = entry_type
        self.iso_currency_code = iso_currency_code
        self.country = country
        self.mcc = mcc

        if not isinstance(account_holder_id, str):
            raise ValueError("account_holder_id must be a string")

        if account_holder_type not in ACCOUNT_HOLDER_TYPES:
            raise ValueError("account_holder_type must be either consumer, business, freelance or unknown")

        self.account_holder_id = account_holder_id
        self.account_holder_type = account_holder_type

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

    def to_dict(self):
        tx_dict = {}
        for field in self.fields:
            value = getattr(self, field)
            if value is not None:
                tx_dict[field] = value

        account_holder = {
            "id": self.account_holder_id,
            "type": self.account_holder_type,
        }

        tx_dict["account_holder"] = account_holder

        return tx_dict


class EnrichedTransactionList:
    def __init__(self, transactions: List[Transaction]):
        self.transactions = transactions

    @classmethod
    def from_list(cls, sdk, vals: list):
        return cls([EnrichedTransaction.from_dict(sdk, val) for val in vals])


class EnrichedTransaction:
    def __init__(
        self,
        sdk,
        contact=None,
        labels=None,
        location=None,
        logo=None,
        merchant=None,
        person=None,
        transaction_id=None,
        website=None,
        **kwargs,
    ):
        self.sdk = sdk
        self.contact = contact
        self.labels = labels
        self.location = location
        self.logo = logo
        self.merchant = merchant
        self.person = person
        self.transaction_id = transaction_id
        self.website = website
        self.kwargs = kwargs

    def __repr__(self):
        return f"EnrichedTransaction(transaction_id={self.transaction_id}, merchant={self.merchant}, logo={self.logo}, labels={self.labels})"

    def report(
        self,
        **kwargs,
    ):
        supported_fileds = [
            "logo",
            "website",
            "merchant",
            "location",
            "contact",
            "person",
            "labels"
        ]
        excess_fields = set(kwargs.keys()) - set(supported_fileds)
        if excess_fields:
            raise ValueError(f"Unexpected keys supplied to report: {excess_fields}")

        return self.sdk.retry_ratelimited_request(
            "POST", "/v2/report", {
                "transaction_id": self.transaction_id,
                **kwargs
            }
        )

    @classmethod
    def from_dict(cls, sdk, val: dict):
        return cls(sdk, **val)


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
        return f"Batch(id={self.batch_id})"

    def poll(self):
        url = f"/v2/enrich/batch/{self.batch_id}"

        json_resp = self.sdk.retry_ratelimited_request("GET", url, None).json()
        status, results = json_resp.get("status"), json_resp.get("results", [])

        if status == "finished":
            return EnrichedTransactionList.from_list(self.sdk, results), status

        if status == "error":
            raise NtropyBatchError(f"Batch failed: {results}", errors=results)

        return json_resp, status

    def wait(self, poll_interval=None):
        if not poll_interval:
            poll_interval = self.poll_interval
        while self.timeout - time.time() > 0:
            resp, status = self.poll()
            if status == "started":
                time.sleep(poll_interval)
                continue
            return resp
        raise NtropyError("Batch wait timeout")

    def wait_with_progress(self, poll_interval=None):
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


class BatchGroup(Batch):
    def __init__(self, sdk, chunks, timeout=10 * 60 * 60, poll_interval=10):
        self._chunks = chunks
        self._batches = []
        self._results = []
        self._pending_batches = []
        self._sdk = sdk
        self.timeout = time.time() + timeout
        self.poll_interval = poll_interval
        self.num_transactions = sum([len(chunk) for chunk in chunks])
        self._finished_num_transactions = 0

        self._enrich_batches()

    def _enrich_batches(self):
        for chunk in self._chunks:
            self._batches.append(self._sdk._enrich_batch(chunk))
            time.sleep(self._sdk.MAX_BATCH_SIZE / 1000)

        self._pending_batches = self._batches.copy()

    def poll(self):
        if self._pending_batches:
            pending_progress = 0
            for batch in self._pending_batches:
                resp, status = batch.poll()

                if status == "finished":
                    self._pending_batches.remove(batch)
                    self._finished_num_transactions += batch.num_transactions
                    self._results += resp.transactions
                else:
                    pending_progress += resp.get("progress", 0)
                    break

            return {
                "progress": pending_progress + self._finished_num_transactions
            }, "started"
        else:
            return EnrichedTransactionList(self._results), "finished"

    def __repr__(self):
        return f"BatchGroup({repr(self._batches)})"


class SDK:
    MAX_BATCH_SIZE = 100000

    def __init__(self, token: str, timeout: int = DEFAULT_TIMEOUT):
        if not token:
            raise NtropyError("API Token must be set")

        self.base_url = "https://api.ntropy.network"
        self.retries = 10
        self.token = token
        self.session = requests.Session()
        self.logger = logging.getLogger("Ntropy-SDK")
        self._timeout = timeout

    def retry_ratelimited_request(self, method: str, url: str, payload: object):
        for i in range(self.retries):
            resp = self.session.request(
                method,
                self.base_url + url,
                json=payload,
                headers={"X-API-Key": self.token},
                timeout=self._timeout,
            )
            if resp.status_code == 429:
                self.logger.debug("Retrying due to ratelimit")
                try:
                    retry_after = int(resp.headers.get("retry-after", "1"))
                except ValueError:
                    retry_after = 1
                if retry_after <= 0:
                    retry_after = 1
                time.sleep(retry_after)
                continue
            resp.raise_for_status()
            return resp
        raise NtropyError(f"Failed to {method} {url} after {self.retries} attempts")

    def enrich(self, transaction: Transaction, latency_optimized=False, labeling=True):
        if not isinstance(transaction, Transaction):
            raise ValueError("transaction should be of type Transaction")

        params_str = urlencode(
            {"latency_optimized": latency_optimized, "labeling": labeling}
        )
        url = "/v2/enrich?" + params_str

        resp = self.retry_ratelimited_request("POST", url, transaction.to_dict())

        return EnrichedTransaction.from_dict(self, resp.json())

    def enrich_batch(
        self,
        transactions: List[Transaction],
        timeout=4 * 60 * 60,
        poll_interval=10,
        labeling=True,
    ):
        if len(transactions) > self.MAX_BATCH_SIZE:
            chunks = [
                transactions[i:i + self.MAX_BATCH_SIZE]
                for i in range(0, len(transactions), self.MAX_BATCH_SIZE)
            ]

            return BatchGroup(self, chunks)

        return self._enrich_batch(transactions, timeout, poll_interval, labeling)

    def _enrich_batch(
        self,
        transactions: List[Transaction],
        timeout=4 * 60 * 60,
        poll_interval=10,
        labeling=True,
    ):
        url = "/v2/enrich/batch"

        if not labeling:
            url += "?labeling=false"

        resp = self.retry_ratelimited_request(
            "POST", url, [transaction.to_dict() for transaction in transactions]
        )
        batch_id = resp.json().get("id", "")

        if not batch_id:
            raise ValueError("batch_id missing from response")
        return Batch(
            self,
            batch_id,
            timeout=timeout,
            poll_interval=poll_interval,
            num_transactions=len(transactions),
        )

    def get_labels(self, account_holder_type: str):
        assert account_holder_type in ACCOUNT_HOLDER_TYPES
        url = f"/v2/labels/hierarchy/{account_holder_type}"
        resp = self.retry_ratelimited_request("GET", url, None)
        return resp.json()

    def enrich_dataframe(
        self,
        df,
        mapping=None,
        progress=True,
        chunk_size=100000,
        poll_interval=10,
    ):
        from ntropy_sdk.benchmark import enrich_dataframe

        return enrich_dataframe(self, df, mapping, progress, chunk_size, poll_interval)
