import time
from datetime import datetime
import uuid
import requests
import logging
from tqdm.auto import tqdm
from typing import List


class NtropyError(Exception):
    pass


class NtropyBatchError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class Transaction:
    required_fields = [
        "amount",
        "date",
        "description",
        "entry_type",
        "iso_currency_code",
        "transaction_id",
        "entity_id",
    ]

    fields = [
        "transaction_id",
        "amount",
        "date",
        "description",
        "entry_type",
        "iso_currency_code",
        "moto_eci_code",
        "pan_entry_mode_auth",
        "pan_entry_mode_capture",
        "payment_channel",
        "pending",
        "possible_labels",
    ]

    def __init__(
        self,
        transaction_id=None,
        amount=None,
        date=None,
        description=None,
        entry_type=None,
        iso_currency_code="USD",
        moto_eci_code=None,
        pan_entry_mode_auth=None,
        pan_entry_mode_capture=None,
        payment_channel=None,
        pending=None,
        possible_labels=None,
        is_business=False,
        entity_id=None,
    ):
        if not transaction_id:
            transaction_id = str(uuid.uuid4())
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
        self.transaction_id = transaction_id
        if amount < 0:
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
        self.moto_eci_code = moto_eci_code
        self.pan_entry_mode_auth = pan_entry_mode_auth
        self.pan_entry_mode_capture = pan_entry_mode_capture
        self.payment_channel = payment_channel
        self.pending = pending
        self.possible_labels = possible_labels
        if not isinstance(entity_id, str):
            raise ValueError("Entity_id must be a string")
        self.entity_id = entity_id
        self.is_business = is_business

        for field in self.required_fields:
            if getattr(self, field) is None:
                raise ValueError(f"{field} should be set")

    def __repr__(self):
        return f"Transaction(transaction_id={self.transaction_id}, description={self.description}, amount={self.amount}, entry_type={self.entry_type})"

    def to_dict(self, latency_optimized=False):
        t = {"transaction": {}}
        for field in self.fields:
            value = getattr(self, field)
            if value is not None:
                t["transaction"][field] = value
        if self.is_business:
            t["business"] = {"business_id": self.entity_id}
        else:
            t["user"] = {"user_id": self.entity_id}
        if latency_optimized:
            t["latency_optimized"] = True
        return t


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
        rating=None,
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
        self.rating = rating
        self.transaction_id = transaction_id
        self.website = website
        self.kwargs = kwargs

    def __repr__(self):
        return f"EnrichedTransaction(transaction_id={self.transaction_id}, merchant={self.merchant}, logo={self.logo}, labels={self.labels})"

    def report(self):
        return self.sdk.retry_ratelimited_request(
            "POST", "/classifier/report", {"transaction_id": self.transaction_id}
        )

    @classmethod
    def from_dict(cls, sdk, val: dict):
        return cls(sdk, **val)


class Batch:
    def __init__(
        self,
        sdk,
        batch_id,
        is_business,
        timeout=4 * 60 * 60,
        poll_interval=10,
        num_transactions=0,
    ):
        self.batch_id = batch_id
        self.is_business = is_business
        self.timeout = time.time() + timeout
        self.poll_interval = poll_interval
        self.sdk = sdk
        self.num_transactions = num_transactions

    def __repr__(self):
        return f"Batch(id={self.batch_id})"

    def poll(self):
        url = (
            f"/classifier/business/batch/{self.batch_id}"
            if self.is_business
            else f"/classifier/consumer/batch/{self.batch_id}"
        )
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


class SDK:
    def __init__(self, token: str):
        if not token:
            raise NtropyError("API Token must be set")
        self.base_url = "https://api.ntropy.network"
        self.retries = 10
        self.token = token
        self.session = requests.Session()
        self.logger = logging.getLogger("Ntropy-SDK")

    def retry_ratelimited_request(self, method: str, url: str, payload: object):
        for i in range(self.retries):
            resp = self.session.request(
                method,
                self.base_url + url,
                json=payload,
                headers={"X-API-Key": self.token},
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

    def classify_realtime(self, transaction: Transaction, latency_optimized=False):
        if not isinstance(transaction, Transaction):
            raise ValueError("transaction should be of type Transaction")
        url = (
            "/classifier/business"
            if transaction.is_business
            else "/classifier/consumer"
        )
        resp = self.retry_ratelimited_request(
            "POST", url, transaction.to_dict(latency_optimized=latency_optimized)
        )
        return EnrichedTransaction.from_dict(self, resp.json())

    def classify_batch(
        self, transactions: List[Transaction], timeout=4 * 60 * 60, poll_interval=10
    ):
        if len(transactions) > 100000:
            raise ValueError("transactions list must be < 100000")
        is_business = [transaction.is_business for transaction in transactions]
        if not (all(is_business) or all([not a for a in is_business])):
            raise ValueError("transactions should all have the same is_business value")
        url = (
            "/classifier/business/batch"
            if is_business[0]
            else "/classifier/consumer/batch"
        )
        resp = self.retry_ratelimited_request(
            "POST", url, [transaction.to_dict() for transaction in transactions]
        )
        batch_id = resp.json().get("id", "")
        if not batch_id:
            raise ValueError("batch_id missing from response")
        return Batch(
            self,
            batch_id,
            is_business[0],
            timeout=timeout,
            poll_interval=poll_interval,
            num_transactions=len(transactions),
        )
