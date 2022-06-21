import os
import time
import csv
import uuid
import requests
import logging
import re
import sys

from datetime import datetime, date
from typing import Optional, Union
from tqdm.auto import tqdm
from typing import List
from urllib.parse import urlencode

from ntropy_sdk.utils import singledispatchmethod, assert_type
from ntropy_sdk import __version__


DEFAULT_TIMEOUT = 10 * 60
DEFAULT_WITH_PROGRESS = hasattr(sys, "ps1")
ACCOUNT_HOLDER_TYPES = ["consumer", "business", "freelance", "unknown"]
COUNTRY_REGEX = re.compile(r"^[A-Z]{2}(-[A-Z0-9]{1,3})?$")
ENV_NTROPY_API_TOKEN = "NTROPY_API_KEY"


class NtropyError(Exception):
    pass


class NtropyBatchError(Exception):
    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class Transaction:
    """A financial transaction that can be enriched with the Ntropy SDK."""

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
        amount: Union[int, float],
        date: str,
        description: str,
        entry_type: str,
        iso_currency_code: str,
        account_holder_id: str = None,
        account_holder_type: str = None,
        country: str = None,
        transaction_id: str = None,
        mcc: int = None,
    ):
        """Parameters
        ----------
        amount : int or float
            Amount of the transaction.
        date : str
            Transaction date in ISO-8601 format (i.e. YYYY-MM-DD).
        description : str
            Description text of the transaction.
        entry_type : {"incoming", "outgoing"}
            Either incoming or outgoing depending on the transaction.
        iso_currency_code : str
            Currency of the transaction, in ISO-4217 format (e.g. USD).
        account_holder_id : str, optional
            ID of the account holder; if the account holder does not exist, create
            a new one with the specified account holder type.
        account_holder_type: {"consumer", "business", "freelance", "unknown"}, optional
            Type of the account holder – must be one of consumer, business,
            freelance, or unknown.
        country: str, optional
            Country where the transaction was made, in ISO-3166-2 format (e.g. US).
        transaction_id: str, optional
            Unique identifier of the transaction in your system. If not supplied,
            a random transaction_id is used.
        mcc: int, optional
            The Merchant Category Code of the merchant, according to ISO 18245.
        """

        if not transaction_id:
            transaction_id = str(uuid.uuid4())
        else:
            assert_type(transaction_id, "transaction_id", str)

        self.transaction_id = transaction_id

        if account_holder_id is not None:
            assert_type(account_holder_id, "account_holder_id", str)
        self.account_holder_id = account_holder_id

        if account_holder_type is not None:
            assert_type(account_holder_type, "account_holder_type", str)
            if account_holder_type not in ACCOUNT_HOLDER_TYPES:
                raise ValueError(
                    f"account_holder_type must be one of {ACCOUNT_HOLDER_TYPES}"
                )
        self.account_holder_type = account_holder_type

        assert_type(amount, "amount", (int, float))
        if amount < 0:
            raise ValueError(
                "amount must be a nonnegative number. For negative amounts, change the entry_type field."
            )

        self.amount = amount

        try:
            assert_type(date, "date", str)
            datetime.strptime(date, "%Y-%m-%d")
        except (ValueError, TypeError):
            raise ValueError("date must be of the format %Y-%m-%d")

        self.date = date

        assert_type(description, "description", str)
        self.description = description

        assert_type(entry_type, "entry_type", str)
        if entry_type not in ["debit", "credit", "outgoing", "incoming"]:
            raise ValueError("entry_type nust be one of 'incoming' or 'outgoing'")

        self.entry_type = entry_type

        assert_type(iso_currency_code, "iso_currency_code", str)
        self.iso_currency_code = iso_currency_code

        if country is not None:
            assert_type(country, "country", str)
            if not COUNTRY_REGEX.match(country):
                raise ValueError("country should be in ISO-3611-2 format")

        self.country = country

        if mcc:
            assert_type(mcc, "mcc", int)
            if not (1000 <= mcc <= 9999):
                raise ValueError("mcc must be in the range of 1000-9999")

        self.mcc = mcc

        for field in self.required_fields:
            if getattr(self, field) is None:
                raise ValueError(f"{field} should be set")

    def __repr__(self):
        return f"Transaction(transaction_id={self.transaction_id}, description={self.description}, amount={self.amount}, entry_type={self.entry_type})"

    @classmethod
    def from_dict(cls, val: dict):
        """Constructs a Transaction object from a dictionary of Transaction fields.

        Parameters
        ----------
        val
            A key-value dictionary of Transaction fields.

        Returns
        ------
        Transaction
            A corresponding Transaction object.
        """

        return cls(**val)

    def to_dict(self):
        """Returns a dictionary of non-empty fields for a Transaction.

        Returns
        ------
        dict
            A dictionary of the Transaction's fields.
        """

        tx_dict = {}
        for field in self.fields:
            value = getattr(self, field)
            if value is not None:
                tx_dict[field] = value

        return tx_dict

    @classmethod
    def from_row(cls, row):
        return cls(
            amount=row["amount"],
            date=row.get("date"),
            description=row.get("description", ""),
            entry_type=row["entry_type"],
            iso_currency_code=row["iso_currency_code"],
            account_holder_id=row.get("account_holder_id"),
            account_holder_type=row.get("account_holder_type"),
            mcc=row.get("mcc"),
            country=row.get("country"),
            transaction_id=row.get("transaction_id"),
        )


class AccountHolder:
    """A financial account holder."""

    def __init__(
        self,
        id: str,
        type: str,
        name: str = None,
        industry: str = None,
        website: str = None,
    ):
        """Parameters
        ----------
        id : str
            Unique identifier for the account holder in your system.
        type : {"consumer", "business", "freelance", "unknown"}
            Type of the account holder – must be one of consumer, business,
            freelance, or unknown.
        name : str, optional
            Name of the account holder.
        industry : str, optional
            Industry of the account holder.
        website : str, optional
            Website of the account holder.
        """

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
        """Sets the internal SDK of the account holder.

        Parameters
        ----------
        sdk : SDK
            A SDK to use with the account holder.
        """

        self._sdk = sdk

    def to_dict(self):
        """Returns a dictionary of non-empty fields for an AccountHolder.

        Returns
        -------
        dict
            A dictionary of the account holder's fields.
        """

        out = {"id": self.id, "type": self.type}
        for key in ("name", "industry", "website"):
            value = getattr(self, key, None)
            if value is not None:
                out[key] = value

        return out

    def get_metrics(self, metrics: List[str], start: date, end: date):
        """Returns the result of a metrics query.

        Parameters
        ----------
        metrics : list of str
            A list of metrics to query for.
        start : date
            A start date range.
        end : date
            An end date range.

        Returns
        -------
        dict:
            A JSON object of the query result
        """
        if not self._sdk:
            raise ValueError(
                "sdk is not set: either call SDK.create_account_holder or set self._sdk first"
            )
        return self._sdk.get_account_holder_metrics(self.id, metrics, start, end)


class EnrichedTransaction:
    """An enriched financial transaction."""

    def __init__(
        self,
        sdk,
        labels: List[str] = None,
        location: str = None,
        logo: str = None,
        merchant: str = None,
        merchant_id: str = None,
        person: str = None,
        transaction_id: str = None,
        website: str = None,
        chart_of_accounts: List[str] = None,
        recurrence: str = None,
        confidence: float = None,
        transaction_type: str = None,
        **kwargs,
    ):
        """Parameters
        ----------
        sdk : SDK
            An SDK to use with the EnrichedTransaction.
        labels : list of str, optional
            Label for the transaction.
        location : str, optional
            Location of the merchant.
        logo : str, optional
            A link to the logo of the merchant.
        merchant : str, optional
            The name of the transaction merchant.
        merchant_id : str, optional
            A unique identifier for the merchant.
        person : str, optional
            Name of the person in the transaction.
        transaction_id : str, optional
            Unique transaction identifier.
        website : str, optional
            Website of the merchant.
        chart_of_accounts : list of str, optional
            Label from the standard chart-of-accounts hierarchy.
        recurrence: {"one off", "recurring"}, optional
            Indicates if the Transaction is recurring.
        confidence: float, optional
            A numerical score between 0.0 and 1.0 indicating the confidence
            of the enrichment.
        transaction_type: {"consumer", "business", "freelance", "unknown"}
            Type of the transaction.
        """

        self.sdk = sdk
        self.labels = labels
        self.location = location
        self.logo = logo
        self.merchant = merchant
        self.person = person
        self.transaction_id = transaction_id
        self.website = website
        self.merchant_id = merchant_id
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
        """Reports an incorrectly enriched transaction.

        Parameters
        ----------
        **kwargs
            Keyword arguments for the correct transaction.
        """

        return self.sdk.retry_ratelimited_request(
            "POST", "/v2/report", {"transaction_id": self.transaction_id, **kwargs}
        )

    @classmethod
    def from_dict(cls, sdk, val: dict):
        """Constructs an EnrichedTransaction object from a dictionary of fields.

        Parameters
        ----------
        val : dict
            A key-value dictionary of EnrichedTransaction fields.

        Returns
        -------
        EnrichedTransaction
            A corresponding EnrichedTransaction object.
        """

        return cls(sdk, **val)

    def to_dict(self):
        """Returns a dictionary of non-empty fields for an EnrichedTransaction.

        Returns
        -------
        dict
            A dictionary of the EnrichedTransaction's fields.
        """

        return {
            "labels": self.labels,
            "location": self.location,
            "logo": self.logo,
            "merchant": self.merchant,
            "merchant_id": self.merchant_id,
            "person": self.person,
            "transaction_id": self.transaction_id,
            "website": self.website,
            "chart_of_accounts": self.chart_of_accounts,
            "recurrence": self.recurrence,
            "confidence": self.confidence,
            "transaction_type": self.transaction_type,
        }


class EnrichedTransactionList(list):
    """A list of EnrichedTransaction."""

    def __init__(self, transactions: List[EnrichedTransaction]):
        """Parameters
        ----------
        transactions : list of EnrichedTransaction
            A list of EnrichedTransaction objects.
        """

        super().__init__(transactions)
        self.transactions = transactions

    def to_csv(self, filepath: str):
        """Writes the list of EnrichedTransaction objects to a CSV file.

        Parameters
        ----------
        filepath : str
            Filepath of the CSV to write in.
        """

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
        """Constructs a list of EnrichedTransaction objects from corresponding fields.

        Parameters
        ----------
        vals : list of dict
            A list of dictionaries representing EnrichedTransaction fields.

        Returns
        -------
        EnrichedTransactionList
            A corresponding EnrichedTransactionList object.
        """
        return cls([EnrichedTransaction.from_dict(sdk, val) for val in vals])


class Batch:
    """An enriched batch with a unique identifier."""

    def __init__(
        self,
        sdk,
        batch_id: str,
        timeout: int = 4 * 60 * 60,
        poll_interval: int = 10,
        num_transactions: int = 0,
    ):
        """Parameters
        ----------
        sdk : SDK
            A SDK associated with the batch.
        batch_id : str
            A unique identifier for the batch.
        timeout : int, optional
            A timeout for retrieving the batch result.
        poll_interval : int, optional
            The interval between polling retries.
        num_transactions : int, optional
            The number of transactions in the batch.
        """

        self.batch_id = batch_id
        self.timeout = time.time() + timeout
        self.poll_interval = poll_interval
        self.sdk = sdk
        self.num_transactions = num_transactions

    def __repr__(self):
        return f"Batch(id={self.batch_id})"

    def poll(self):
        """Poll the current batch.

        Returns
        -------
        status : str
            The status of the batch enrichment.
        dict
            The JSON response of the batch poll.
        """

        url = f"/v2/transactions/async/{self.batch_id}"

        json_resp = self.sdk.retry_ratelimited_request("GET", url, None).json()
        status, results = json_resp.get("status"), json_resp.get("results", [])

        if status == "finished":
            return EnrichedTransactionList.from_list(self.sdk, results), status

        if status == "error":
            raise NtropyBatchError(f"Batch failed: {results}", errors=results)

        return json_resp, status

    def wait(self, with_progress: bool = DEFAULT_WITH_PROGRESS, poll_interval=None):
        """Retrieve the current batch enrichment.

        Parameters
        ----------
        with_progress : bool
            True if the batch enrichment is displayed with a progress bar,
            False otherwise. By default, progress is displayed only in interactive
            mode.
        poll_interval : bool
            The interval between polling retries. If not specified, defaults to
            the batch's poll_interval.

        Returns
        -------
        status : str
            The status of the batch enrichment.
        dict
            The JSON response of the batch poll.
        """

        if with_progress:
            return self._wait_with_progress(poll_interval=poll_interval)
        else:
            return self._wait(poll_interval=poll_interval)

    def _wait(self, poll_interval=None):
        """Retrieve the current batch enrichment without progress updates."""

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
        """Retrieve the current batch enrichment with progress updates."""

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
    """An Ntropy SDK, associated with an api key."""

    MAX_BATCH_SIZE = 100000
    MAX_SYNC_BATCH = 4000
    DEFAULT_MAPPING = {
        "merchant": "merchant",
        "merchant_id": "merchant_id",
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
        """Parameters
        ----------
        token : str, optional
            The api key for Ntropy SDK. If not supplied, the SDK will use the
            environment variable $NTROPY_API_KEY.
        timeout : int, optional
            The timeout for requests to the Ntropy API.
        with_progress : bool, optional
            True if enrichment should include a progress bar; False otherwise.
        """

        if not token:
            if ENV_NTROPY_API_TOKEN not in os.environ:
                raise NtropyError(
                    f"API Token must be passed as an argument or set in the env. variable {ENV_NTROPY_API_TOKEN}"
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
        """Request an Ntropy API endpoint.

        Parameters
        ----------
        method : str
            The HTTP method to use.
        url : str
            The API url to request.
        payload : object
            The request payload.
        log_level : int, optional
            The logging level for the request.

        Raises
        ------
        NtropyError
            If the request failed after the maximum number of retries.
        """

        for _ in range(self.retries):
            try:
                resp = self.session.request(
                    method,
                    self.base_url + url,
                    json=payload,
                    headers={
                        "X-API-Key": self.token,
                        "User-Agent": f"ntropy-sdk/{__version__}",
                    },
                    timeout=self._timeout,
                )
            except requests.ConnectionError:
                # Rebuild session on connection error and retry
                self.session = requests.Session()
                continue

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
        """Adds an AccountHolder to an SDK.

        Parameters
        ----------
        account_holder : AccountHolder
            The AccountHolder to add.
        """

        if not isinstance(account_holder, AccountHolder):
            raise ValueError("account_holder should be of type AccountHolder")

        url = "/v2/account-holder"

        self.retry_ratelimited_request("POST", url, account_holder.to_dict())
        account_holder.set_sdk(self)

    def df_to_transaction_list(
        self,
        df,
        mapping=None,
        poll_interval: int = 10,
        with_progress: bool = DEFAULT_WITH_PROGRESS,
        labeling: bool = True,
        create_account_holders: bool = True,
        model=None,
        inplace: bool = False,
    ):
        """Enriches a pandas dataframe of Transactions.

        Parameters
        ----------
        df
            The dataframe containing the Transactions. At minimum, the dataframe
            must contain the columns specified in the Transaction class.
        mapping
            TODO: fill out
        poll_interval : int, optional
            The interval between consecutive polling retries.
        with_progress : bool, optional
            True if progress bar should be displayed; False otherwise. By default,
            progress is displayed only in interactive mode.
        labeling: bool, optional
            True if the enriched transactions should be labeled; False otherwise.
        model
            TODO: fill out
        inplace : bool, optional
            Enrich the dataframe inplace.
        """
        try:
            import pandas as pd
        except ImportError:
            # If here, the input data is not a dataframe, or import would succeed
            raise ValueError(
                f"add_transactions takes either a pandas.DataFrame or a list of Transactions for it's `df` parameter, you supplied a '{type(df)}'"
            )

        if not isinstance(df, pd.DataFrame):
            raise TypeError("Transactions object needs to be a pandas dataframe.")

        required_columns = [
            "amount",
            "date",
            "description",
            "entry_type",
            "iso_currency_code",
        ]

        optional_columns = [
            "country",
            "mcc",
            "transaction_id",
            "account_holder_id",
            "account_holder_type",
        ]

        cols = set(df.columns)
        missing_cols = set(required_columns).difference(cols)
        if missing_cols:
            raise KeyError(f"Missing columns {missing_cols}")

        if not inplace:
            # Only copy needed columns
            provided_cols = list(
                set(required_columns + optional_columns).intersection(cols)
            )
            df = df[provided_cols].copy()
        else:
            overlapping_cols = set(mapping.values()).intersection(cols)
            if overlapping_cols:
                raise KeyError(
                    f"Overlapping columns {overlapping_cols} will be overwritten"
                    "- consider using inplace=False, overriding the mapping keyword "
                    "argument, or move the existing columns to another column"
                )

        txs = df.apply(tx_builder, axis=1).to_list()
        return txs

    @singledispatchmethod
    def add_transactions(
        self,
        df,
        mapping=DEFAULT_MAPPING.copy(),
        poll_interval=10,
        with_progress=None,
        labeling=True,
        create_account_holders=True,
        model=None,
        inplace=False,
    ):
        txs = self.df_to_transaction_list(df, mapping, inplace)
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
    def _add_transactions_list(
        self,
        transactions: List[Transaction],
        timeout: int = 4 * 60 * 60,
        poll_interval: int = 10,
        with_progress=DEFAULT_WITH_PROGRESS,
        labeling: bool = True,
        create_account_holders: bool = True,
        model=None,
        inplace: bool = False,
    ):
        """Enriches a list of Transaction objects.

        Parameters
        ----------
        transactions : list of Transaction
            The list containing the Transaction objects.
        timeout : int, optional
            Timeout for enriching the transactions.
        poll_interval : int, optional
            The interval between consecutive polling retries.
        with_progress
            True if progress bar should be displayed; False otherwise. By default,
            progress is displayed only in interactive mode.
        labeling : bool, optional
            True if the enriched transactions should be labeled; False otherwise.
        model
            TODO: fill out
        inplace
            Enrich the dataframe inplace.
        """

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

    @staticmethod
    def _build_params_str(labeling, create_account_holders, model=None):
        params = {
            "labeling": labeling,
            "create_account_holders": create_account_holders,
        }
        if model is not None:
            params["model_name"] = model

        params_str = urlencode(params)
        return params_str

    def _add_transactions(
        self,
        transactions: List[Transaction],
        timeout: int = 4 * 60 * 60,
        poll_interval: int = 10,
        with_progress: bool = DEFAULT_WITH_PROGRESS,
        labeling: bool = True,
        create_account_holders: bool = True,
        model=None,
    ):
        is_sync = len(transactions) <= self.MAX_SYNC_BATCH
        if not is_sync:
            batch = self._add_transactions_async(
                transactions,
                timeout,
                poll_interval,
                labeling,
                create_account_holders,
                model,
            )
            with_progress = with_progress or self._with_progress
            return batch.wait(with_progress=with_progress)

        params_str = self._build_params_str(
            labeling, create_account_holders, model=model
        )

        try:
            data = [transaction.to_dict() for transaction in transactions]
            url = f"/v2/transactions/sync?" + params_str
            resp = self.retry_ratelimited_request("POST", url, data)

            if resp.status_code != 200:
                raise NtropyBatchError("Batch failed", errors=resp.json())

            return EnrichedTransactionList.from_list(self, resp.json())

        except requests.HTTPError as e:
            if e.response.status_code == 404:
                error = e.response.json()
                raise ValueError(f"{error['detail']}")
            raise

    @singledispatchmethod
    def add_transactions_async(
        self,
        df,
        mapping=DEFAULT_MAPPING.copy(),
        poll_interval=10,
        with_progress=None,
        labeling=True,
        create_account_holders=True,
        model=None,
        inplace=False,
    ):
        txs = self.df_to_transaction_list(df, mapping, inplace)
        return self.add_transactions_async(
            txs,
            labeling=labeling,
            create_account_holders=create_account_holders,
            poll_interval=poll_interval,
            with_progress=with_progress,
            model=model,
        )

    @add_transactions_async.register(list)
    def _add_transactions_list_async(
        self,
        transactions: List[Transaction],
        timeout=4 * 60 * 60,
        poll_interval=10,
        with_progress=None,
        labeling=True,
        create_account_holders=True,
        model=None,
    ):
        return self._add_transactions_async(
            transactions,
            timeout,
            poll_interval,
            labeling,
            create_account_holders,
            model,
        )

    def _add_transactions_async(
        self,
        transactions: List[Transaction],
        timeout=4 * 60 * 60,
        poll_interval=10,
        labeling=True,
        create_account_holders=True,
        model=None,
    ):
        params_str = self._build_params_str(
            labeling, create_account_holders, model=model
        )

        try:

            url = f"/v2/transactions/async?" + params_str

            data = [transaction.to_dict() for transaction in transactions]
            resp = self.retry_ratelimited_request("POST", url, data)

            r = resp.json()
            batch_id = r.get("id", "")

            if not batch_id:
                raise ValueError("batch_id missing from response")

            return Batch(
                self,
                batch_id,
                timeout=timeout,
                poll_interval=poll_interval,
                num_transactions=len(transactions),
            )

        except requests.HTTPError as e:
            if e.response.status_code == 404:
                error = e.response.json()
                raise ValueError(f"{error['detail']}")

            raise

    def get_account_holder(self, account_holder_id: str):
        """Returns an AccountHolder object from an id

        Parameters
        ----------
        account_holder_id : str
            A unique identifier for the account holder.

        Returns
        -------
        AccountHolder
            The AccountHolder corresponding to the id.
        """

        url = f"/v2/account-holder/{account_holder_id}"
        try:
            response = self.retry_ratelimited_request("GET", url, None).json()
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                error = e.response.json()
                raise ValueError(f"{error['detail']}")
            raise

        return AccountHolder(**response)

    def get_account_holder_metrics(
        self, account_holder_id: str, metrics: List[str], start: date, end: date
    ):
        """Returns the result of a metrics query for a specific account holder.

        Parameters
        ----------
        account_holder_id : str
            The unique identifier for the account holder.
        metrics : list of str
            A list of metrics to query for.
        start : date
            A start date range.
        end : date
            An end date range.

        Returns
        -------
        dict:
            A JSON object of the query result
        """

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
        """Returns a hierarchy of possible labels for a specific type.

        Parameters
        ----------
        account_holder_type : {"consumer", "business", "freelance", "unknown"}
            The account holder type.

        Returns
        -------
        dict
            A hierarchy of labels for the given account holder type.
        """

        assert account_holder_type in ACCOUNT_HOLDER_TYPES
        url = f"/v2/labels/hierarchy/{account_holder_type}"
        resp = self.retry_ratelimited_request("GET", url, None)
        return resp.json()

    def get_chart_of_accounts(self):
        """Returns all available chart of accounts.

        Returns
        -------
        dict
            A hierarchy of possible chart of accounts.
        """

        url = "/v2/chart-of-accounts"
        resp = self.retry_ratelimited_request("GET", url, None)
        return resp.json()
