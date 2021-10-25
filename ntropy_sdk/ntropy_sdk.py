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


class NtropyError(Exception):
    pass


class NtropyBatchError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class AccountHolderType(enum.Enum):
    consumer = "consumer"
    business = "business"


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
    ]

    def __init__(
        self,
        transaction_id=None,
        amount=None,
        date=None,
        description=None,
        entry_type=None,
        iso_currency_code="USD",
        country=None,
        account_holder_type=AccountHolderType.consumer,
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

        if not isinstance(account_holder_id, str):
            raise ValueError("account_holder_id must be a string")

        if not isinstance(account_holder_type, AccountHolderType):
            raise ValueError("account_holder_type must be of type AccountHolderType")

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
            "type": self.account_holder_type.value
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
            "POST", "/v2/report", {"transaction_id": self.transaction_id}
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

    def enrich(
        self, transaction: Transaction, latency_optimized=False, categorization=True
    ):
        if not isinstance(transaction, Transaction):
            raise ValueError("transaction should be of type Transaction")

        params_str = urlencode(
            {"latency_optimized": latency_optimized, "categorization": categorization}
        )
        url = "/v2/enrich?" + params_str

        resp = self.retry_ratelimited_request("POST", url, transaction.to_dict())

        return EnrichedTransaction.from_dict(self, resp.json())

    def enrich_batch(
        self,
        transactions: List[Transaction],
        timeout=4 * 60 * 60,
        poll_interval=10,
        categorization=True,
    ):
        if len(transactions) > 100000:
            raise ValueError("transactions list must be < 100000")

        url = "/v2/enrich/batch"

        if not categorization:
            url += "?categorization=false"

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
        assert account_holder_type in ["business", "consumer"]
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
        if mapping is None:
            mapping = self.default_mapping.copy()

        required_columns = [
            "iso_currency_code",
            "amount",
            "entry_type",
            "description",
            "account_holder_id",
            "account_holder_type",
        ]

        optional_columns = [
            "transaction_id",
            "date",
        ]

        def to_tx(row):
            return Transaction(
                amount=row["amount"],
                date=row.get("date"),
                description=row.get("description", ""),
                entry_type=row["entry_type"],
                iso_currency_code=row["iso_currency_code"],
                transaction_id=row.get("transaction_id"),
                account_holder_id=row["account_holder_id"],
                account_holder_type=AccountHolderType(row["account_holder_type"]),
            )

        cols = set(df.columns)
        missing_cols = set(required_columns).difference(cols)
        if missing_cols:
            raise KeyError(f"Missing columns {missing_cols}")
        overlapping_cols = set(mapping.values()).intersection(cols)
        if overlapping_cols:
            raise KeyError(
                f"Overlapping columns {overlapping_cols} will be overwritten - consider overriding the mapping keyword argument, or move the existing columns to another column"
            )
        df["_input_tx"] = df.apply(to_tx, axis=1)
        chunks = [df[i : i + chunk_size] for i in range(0, df.shape[0], chunk_size)]
        prev_chunks = 0
        outputs = []
        with tqdm(total=df.shape[0], desc="started") as progress:
            for chunk in chunks:
                txs = chunk["_input_tx"]
                b = self.enrich_batch(txs)
                while b.timeout - time.time() > 0:
                    resp, status = b.poll()
                    if status == "started":
                        diff_n = resp.get("progress", 0) - (progress.n - prev_chunks)
                        progress.update(diff_n)
                        time.sleep(poll_interval)
                        continue
                    progress.desc = status
                    diff_n = b.num_transactions - (progress.n - prev_chunks)
                    progress.update(diff_n)
                    df.loc[chunk.index, "_output_tx"] = resp.transactions
                    break
                prev_chunks += b.num_transactions

        def get_tx_val(tx, v):
            sentinel = object()
            output = getattr(tx, v, tx.kwargs.get(v, sentinel))
            if output == sentinel:
                raise KeyError(f"invalid mapping: {v} not in {tx}")
            return output

        for k, v in mapping.items():
            df[v] = df["_output_tx"].apply(lambda tx: get_tx_val(tx, k))
        df = df.drop(["_input_tx", "_output_tx"], axis=1)
        return df

    def _get_nodes(self, x, prefix=""):
        """
        Args:
            x: a tree where internal nodes are dictionaries, and leaves are lists.
            prefix: not meant to be passed. The parent prefix of a label. e.g. given A -> B -> C,
                the parent prefix of C is 'A [sep] B'.
            sep: the separator to use between labels. Could be 'and', '-', or whatever
        Returns:
            All nodes in the hierarchy. Each node is given by a string A [sep] B [sep] C etc.
        """
        res = []
        q = [(x, prefix)]
        while q:
            x, prefix = q.pop()
            if isinstance(x, list):
                res.extend([prefix + k for k in x])
            else:
                for k, v in x.items():
                    res.append(prefix + k)
                    q.append((v, prefix + k + f" - "))
        return list(set(res))

    default_mapping = {
        "merchant": "merchant",
        "website": "website",
        "labels": "labels",
        "logo": "logo",
        "location": "location",
        "person": "person",
        "rating": "rating",
        "contact": "contact",
        # the entire enriched transaction object is at _output_tx
    }

    def _node2branch(self, branch):
        if isinstance(branch, str):
            branch = branch.split(" - ")
        return [" - ".join(branch[: i + 1]) for i in range(len(branch))]

    def benchmark(
        self,
        in_csv_file: str,
        out_csv_file: str,
        drop_fields: List[str] = None,
        hardcode_fields: Dict[str, str] = None,
        ground_truth_merchant_field=None,
        ground_truth_label_field=None,
        mapping=None,
        chunk_size=100000,
        poll_interval=10,
    ):
        try:
            import pandas
        except ImportError:
            print(
                "Pandas not found, please install ntropy-sdk with the benchmark extra (e.g. pip install 'ntropy-sdk[benchamrk]') to use the benchmarking functionality"
            )
            sys.exit(1)
        try:
            import numpy as np
        except ImportError:
            print(
                "Numpy not found, please install ntropy-sdk with the benchmark extra (e.g. pip install 'ntropy-sdk[benchamrk]') to use the benchmarking functionality"
            )
            sys.exit(1)
        try:
            from sklearn.metrics import (
                f1_score,
                accuracy_score,
                precision_recall_fscore_support,
            )
        except ImportError:
            print(
                "Scikit-learn not found, please install ntropy-sdk with the benchmark extra (e.g. pip install 'ntropy-sdk[benchamrk]') to use the benchmarking functionality"
            )
            sys.exit(1)
        default_mapping = self.default_mapping.copy()
        if mapping is not None:
            default_mapping.update(mapping)
        mapping = default_mapping

        df = pandas.read_csv(in_csv_file)
        if drop_fields:
            df = df.drop(drop_fields, axis=1)
        if hardcode_fields:
            for a, b in hardcode_fields.items():
                df[a] = b
        df = self.enrich_dataframe(
            df, mapping=mapping, chunk_size=chunk_size, poll_interval=poll_interval
        )
        if ground_truth_merchant_field:
            correct_merchants = df[ground_truth_merchant_field]
            predicted_merchants = df[mapping["merchant"]]
            accuracy_merchant = np.mean(
                [
                    x == y
                    for x, y in zip(
                        correct_merchants,
                        predicted_merchants,
                    )
                ]
            )
            output = f"Merchant:\n\tAccuracy: {accuracy_merchant:.3f}%"
            print(output)
        if ground_truth_label_field:
            labels_per_type = {
                AccountHolderType.consumer: self._get_nodes(self.get_labels("consumer")),
                AccountHolderType.business: self._get_nodes(self.get_labels("business"))
            }

            correct_labels = df[ground_truth_label_field].to_list()
            account_holder_types = [AccountHolderType(t) for t in df["account_holder_type"].to_list()]
            predicted_labels = df[mapping["labels"]].to_list()
            y_pred = []
            y_true = []
            for x, y, account_holder_type in zip(correct_labels, predicted_labels, account_holder_types):
                nodes = labels_per_type[account_holder_type]
                ground_truth = self._node2branch(x)
                preds = self._node2branch(y)
                for node in nodes:
                    y_true.append(node in ground_truth)
                    y_pred.append(node in preds)
            labeller_accuracy = np.mean(
                [x == " - ".join(y) for x, y in zip(correct_labels, predicted_labels)]
            )
            (
                precision_labeller,
                recall_labeller,
                f1_labeller,
                _,
            ) = precision_recall_fscore_support(
                y_true, y_pred, average="binary", zero_division=0.0
            )

            output = f"Labels:\n\tF1: {f1_labeller:.3f}\n\tPrecision: {precision_labeller:.3f}\n\tRecall: {recall_labeller:.3f}\n\tAccuracy: {labeller_accuracy:.3f}"
            print(output)
        if out_csv_file:
            df.to_csv(out_csv_file)
