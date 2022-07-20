import os
import time
import csv
import uuid
import requests
import logging
import re
import sys

from datetime import datetime, date
from typing import Optional, Union, Callable, Any, ClassVar
from tqdm.auto import tqdm
from typing import List
from urllib.parse import urlencode
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter

from ntropy_sdk.utils import (
    RecurrenceType,
    TransactionType,
    dict_to_str,
    singledispatchmethod,
    assert_type,
    AccountHolderType,
    validate_date,
    EntryType,
)
from ntropy_sdk import __version__
from pydantic import BaseModel, Field, Extra, validator

DEFAULT_TIMEOUT = 10 * 60
DEFAULT_RETRIES = 10
DEFAULT_WITH_PROGRESS = hasattr(sys, "ps1")
ACCOUNT_HOLDER_TYPES = ["consumer", "business", "freelance", "unknown"]
COUNTRY_REGEX = r"^[A-Z]{2}(-[A-Z0-9]{1,3})?$"
ENV_NTROPY_API_TOKEN = "NTROPY_API_KEY"


class NtropyError(Exception):
    """An expected error returned from the server-side"""

    pass


class NtropyBatchError(Exception):
    """One or more errors in one or more transactions of a submitted transaction batch"""

    def __init__(self, message, errors=None):
        super().__init__(message)
        self.errors = errors


class Transaction(BaseModel):
    """A financial transaction that can be enriched with the Ntropy SDK."""

    _required_fields: ClassVar[List[str]] = [
        "amount",
        "description",
        "entry_type",
        "iso_currency_code",
        "date",
    ]

    _fields: ClassVar[List[str]] = [
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

    amount: float = Field(ge=0, description="Amount of the transaction.")
    entry_type: EntryType = Field(
        description="Either incoming or outgoing depending on the transaction."
    )
    iso_currency_code: str = Field(
        description="Currency of the transaction, in ISO-4217 format (e.g. USD)."
    )
    description: str = Field(description="Description text of the transaction.")

    _date_validator = validator("date", pre=True, allow_reuse=True)(validate_date)
    date: str = Field(
        description="Transaction date in ISO-8601 format (i.e. YYYY-MM-DD)."
    )
    account_holder_id: Optional[str] = Field(
        min_length=1,
        description="ID of the account holder; if the account holder does not exist, create a new one with the specified account holder type.",
    )
    account_holder_type: Optional[AccountHolderType] = Field(
        description="Type of the account holder – must be one of consumer, business, freelance, or unknown."
    )
    country: Optional[str] = Field(
        regex=COUNTRY_REGEX,
        description="Country where the transaction was made, in ISO-3166-2 format (e.g. US).",
    )
    transaction_id: Optional[str] = Field(
        description="Unique identifier of the transaction in your system. If not supplied, a random transaction_id is used.",
        min_length=1,
    )
    mcc: Optional[int] = Field(
        ge=1000,
        le=9999,
        description="The Merchant Category Code of the merchant, according to ISO 18245.",
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not kwargs.get("transaction_id"):
            self.transaction_id = str(uuid.uuid4())

    def __repr__(self):
        return f"Transaction({dict_to_str(self.to_dict())})"

    def __str__(self):
        return repr(self)

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

    @classmethod
    def from_row(cls, row):
        """Constructs a Transaction object from a pandas.Series containing Transaction fields.

        Parameters
        ----------
        val
            A pandas.Series containing Transaction fields

        Returns
        ------
        Transaction
            A corresponding Transaction object.
        """
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

    def to_dict(self):
        """Returns a dictionary of non-empty fields for a Transaction.

        Returns
        ------
        dict
            A dictionary of the Transaction's fields.
        """
        return self.dict(exclude_none=True)

    class Config:
        use_enum_values = True


class LabeledTransaction(Transaction):
    """Represents a base Transaction object with an associated label for custom model training tasks. All other fields are the same as Transaction."""

    label: str = Field(description="Ground truth label for a transaction.")

    _required_fields: ClassVar[List[str]] = [
        "amount",
        "description",
        "entry_type",
        "iso_currency_code",
        "date",
    ]

    _fields: ClassVar[List[str]] = [
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
        "label",
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __repr__(self):
        return f"LabeledTransaction({dict_to_str(self.to_dict())})"

    @classmethod
    def from_row(cls, row):
        """Constructs a LabeledTransaction object from a pandas.Series containing LabeledTransaction fields.

        Parameters
        ----------
        val
            A pandas.Series containing LabeledTransaction fields

        Returns
        ------
        LabeledTransaction
            A corresponding LabeledTransaction object.
        """
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
            label=row.get("label"),
        )

    @classmethod
    def from_dict(cls, val: dict):
        """Constructs a LabeledTransaction object from a dictionary of LabeledTransaction fields.

        Parameters
        ----------
        val
            A key-value dictionary of LabeledTransaction fields.

        Returns
        ------
        LabeledTransaction
            A corresponding LabeledTransaction object.
        """

        return cls(**val)

    def to_dict(self):
        """Returns a dictionary of non-empty fields for a LabeledTransaction.

        Returns
        ------
        dict
            A dictionary of the LabeledTransaction's fields.
        """
        return self.dict(exclude_none=True)


class AccountHolder(BaseModel):
    """A financial account holder."""

    id: str = Field(
        description="Unique identifier for the account holder in your system."
    )
    type: AccountHolderType = Field(
        description="Type of the account holder – must be one of consumer, business, freelance, or unknown."
    )
    name: Optional[str] = Field(description="Name of the account holder.")
    industry: Optional[str] = Field(description="Industry of the account holder.")
    website: Optional[str] = Field(description="Website of the account holder.")
    _sdk = None

    def __repr__(self):
        return f"AccountHolder({dict_to_str(self.to_dict())})"

    def __str__(self):
        return repr(self)

    def set_sdk(self, sdk):
        """Sets the internal SDK reference used by this account holder object

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

        return self.dict(exclude_none=True)

    def get_metrics(self, metrics: List[str], start: date, end: date):
        """Returns the result of a metrics query.

        Parameters
        ----------
        metrics : List[str]
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

    class Config:
        use_enum_values = True
        extra = "allow"


class EnrichedTransaction(BaseModel):
    """An enriched financial transaction."""

    sdk: "SDK" = Field(description="An SDK to use with the EnrichedTransaction.")
    labels: Optional[List[str]] = Field(description="Label for the transaction.")
    location: Optional[str] = Field(description="Location of the merchant.")
    logo: Optional[str] = Field(description="A link to the logo of the merchant.")
    merchant: Optional[str] = Field(description="The name of the transaction merchant.")
    merchant_id: Optional[str] = Field(
        description="A unique identifier for the merchant."
    )
    person: Optional[str] = Field(description="Name of the person in the transaction.")
    transaction_id: Optional[str] = Field(description="Unique transaction identifier.")
    website: Optional[str] = Field(description="Website of the merchant.")
    chart_of_accounts: Optional[List[str]] = Field(
        description="Label from the standard chart-of-accounts hierarchy."
    )
    recurrence: Optional[RecurrenceType] = Field(
        description="Indicates if the Transaction is recurring."
    )
    confidence: Optional[float] = Field(
        ge=0.0,
        le=1.0,
        description="A numerical score between 0.0 and 1.0 indicating the confidence",
    )
    transaction_type: Optional[TransactionType] = Field(
        description="Type of the transaction."
    )
    predicted_mcc: Optional[List[int]] = Field(
        description="A list of MCC (Merchant Category Code of the merchant, according to ISO 18245) predicted by the model."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.kwargs = kwargs

    def __repr__(self):
        return f"EnrichedTransaction({dict_to_str(self.to_dict())})"

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

        return cls(sdk=sdk, **val)

    def to_dict(self):
        """Returns a dictionary of non-empty fields for an EnrichedTransaction.

        Returns
        -------
        dict
            A dictionary of the EnrichedTransaction's fields.
        """

        return self.dict(exclude_none=True)

    class Config:
        use_enum_values = True
        arbitrary_types_allowed = True
        extra = "allow"


class EnrichedTransactionList(list):
    """A list of EnrichedTransaction objects."""

    def __init__(self, transactions: List[EnrichedTransaction]):
        """Parameters
        ----------
        transactions : List[EnrichedTransaction]
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
        """Constructs a list of EnrichedTransaction objects from a list of dictionaries containing corresponding fields.

        Parameters
        ----------
        vals : List[dict]
            A list of dictionaries representing EnrichedTransaction fields.

        Returns
        -------
        EnrichedTransactionList
            A corresponding EnrichedTransactionList object.
        """
        return cls([EnrichedTransaction.from_dict(sdk, val) for val in vals])


class Batch(BaseModel):
    """An enriched batch with a unique identifier."""

    sdk: "SDK" = Field(description="A SDK associated with the batch.")
    batch_id: str = Field(description="A unique identifier for the batch.")
    timeout: int = Field(
        4 * 60 * 60, description="A timeout for retrieving the batch result."
    )
    poll_interval: int = Field(10, description="The interval between polling retries.")
    num_transactions: int = Field(
        0, description="The number of transactions in the batch."
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.timeout = time.time() + self.timeout

    def __repr__(self):
        return f"Batch({dict_to_str(self.dict(exclude_none=True))})"

    def poll(self):
        """Polls the current batch status and returns the server response and status attribute.

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
        """Continuously polls the status of this batch, blocking until the batch status is
        "ready" or "error"

        Parameters
        ----------
        with_progress : bool
            If True the batch enrichment is displayed with a progress bar.
            By default, progress is displayed only in interactive
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

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"


class Model(BaseModel):
    """A model reference with an associated name"""

    sdk: "SDK" = Field(description="A SDK associated with the model.")
    model_name: str = Field(description="The name of the model.")
    created_at: Optional[str] = Field(description="The date the model was created.")
    account_holder_type: Optional[AccountHolderType] = Field(description="Type of the account holder – must be one of consumer, business, freelance, or unknown.")
    status: Optional[str] = Field(description="The status of the batch enrichment.")
    progress: Optional[int] = Field(description="The progress from 0 to 100 of the training process")
    timeout: Optional[int] = Field(20 * 60 * 60, description="A timeout for retrieving the batch result.")
    poll_interval: Optional[int] = Field(10, description="The interval between polling retries.")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.account_holder_type = (self.account_holder_type,)
        self.created_at = (self.created_at,)
        self.status = (self.status,)
        self.progress = (self.progress,)

        self.timeout = time.time() + self.timeout

    def __repr__(self):
        return f"Model({dict_to_str(self.dict(exclude_none=True))})"

    def is_synced(self) -> bool:
        """Returns True if the model instance was already synced at least once with the server

        Returns:
            boolean
        """
        return self.status is not None

    def poll(self):
        """Polls the current model status and returns the server response, status and progress attributes.

        Returns
        -------
        json_resp : str
            The JSON response of the model poll
        status : str
            The status of the batch enrichment.
        progress:
            The progress from 0 to 100 of the training process
        """
        url = f"/v2/models/{self.model_name}"

        json_resp = self.sdk.retry_ratelimited_request("GET", url, None).json()
        status = json_resp.get("status")
        progress = json_resp.get("progress")
        created_at = json_resp.get("created_at")
        account_holder_type = json_resp.get("account_holder_type")

        self.status = status
        self.progress = progress
        self.created_at = created_at
        self.account_holder_type = account_holder_type

        return json_resp, status, progress

    def wait(self, with_progress=DEFAULT_WITH_PROGRESS, poll_interval=None):
        """Continuously polls the status of this model, blocking until the model status is
        "ready" or "error"

        Parameters
        ----------
        with_progress : bool
            If True, the model training is displayed with a progress bar.
            By default, progress is displayed only in interactive mode.
        poll_interval : bool
            The interval between polling retries. If not specified, defaults to
            the batch's poll_interval.

        Returns
        -------
        status : str
            The status of the model training.
        dict
            The JSON response of the model training.
        """

        if with_progress:
            return self._wait_with_progress(poll_interval=poll_interval)
        else:
            return self._wait(poll_interval=poll_interval)

    def _wait(self, poll_interval=None):
        if not poll_interval:
            poll_interval = self.poll_interval
        while self.timeout - time.time() > 0:
            resp, status, _ = self.poll()
            if status == "error":
                raise NtropyError("Unexpected model training error")
            if status != "ready":
                time.sleep(poll_interval)
                continue
            return resp
        raise NtropyError("Model training wait timeout")

    def _wait_with_progress(self, poll_interval=None):
        if not poll_interval:
            poll_interval = self.poll_interval
        with tqdm(total=100, desc="started") as progress:
            while self.timeout - time.time() > 0:
                resp, status, pr = self.poll()
                if status == "error":
                    raise NtropyError("Unexpected model training error")
                if status != "ready":
                    diff_n = pr - progress.n
                    progress.update(diff_n)
                    time.sleep(poll_interval)
                    continue
                progress.desc = status
                progress.update(100)
                return resp
            raise NtropyError("Model training wait timeout")

    @staticmethod
    def from_response(sdk: "SDK", response) -> "Model":
        """Creates a model instance from an API response referencing a model

        Parameters
        ----------
        sdk : SDK
            SDK to bind to the model instance
        response : dict
            Server response referencing a model

        Returns
        -------
        Model
            The Model instance referencing the same model as in the response
        """
        name = response.get("name")
        if name is None:
            raise ValueError("Invalid response for creating a model - missing name")

        created_at = response.get("created_at")
        account_holder_type = response.get("account_holder_type")
        status = response.get("status")
        progress = response.get("progress")

        return Model(
            sdk=sdk,
            model_name=name,
            created_at=created_at,
            account_holder_type=account_holder_type,
            status=status,
            progress=progress,
        )

    class Config:
        arbitrary_types_allowed = True
        use_enum_values = True


class SDK:
    """The main Ntropy SDK object that holds the connection to the API server and implements
    the fault-tolerant communication methods. An SDK instance is associated with an API key.
    """

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
        retries: int = DEFAULT_RETRIES,
        retry_on_unhandled_exception: bool = False,
        with_progress: bool = DEFAULT_WITH_PROGRESS,
    ):
        """Parameters
        ----------
        token : str, optional
            The api key for Ntropy SDK. If not supplied, the SDK will use the
            environment variable $NTROPY_API_KEY.
        timeout : int, optional
            The timeout for requests to the Ntropy API.
        retries : int, optional
            The number of retries for a certain request before failing
        retry_on_unhandled_exception : boolean, optional
            Whether to retry or not, when a request returns an unhandled exception (50x status codes)
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
        self.token = token
        self.session = requests.Session()
        self.keep_alive = TCPKeepAliveAdapter()
        self.session.mount("https://", self.keep_alive)
        self.logger = logging.getLogger("Ntropy-SDK")

        self._timeout = timeout
        self._retries = retries
        self._retry_on_unhandled_exception = retry_on_unhandled_exception
        self._with_progress = with_progress

    def retry_ratelimited_request(
        self, method: str, url: str, payload: object, log_level=logging.DEBUG
    ):
        """Executes a request to an endpoint in the Ntropy API (given the `base_url` parameter).
        Catches expected errors and wraps them in NtropyError.
        Retries the request for Rate-Limiting errors or Unexpected Errors (50x)

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

        backoff = 1
        for _ in range(self._retries):
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
                self.session.mount("https://", self.keep_alive)
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
            elif (
                resp.status_code >= 500 and resp.status_code <= 511
            ) and self._retry_on_unhandled_exception:
                time.sleep(backoff)
                backoff = min(backoff * 2, 8)

                self.logger.log(
                    log_level,
                    "Retrying in %s seconds due to unhandled exception in the server side",
                    backoff,
                )

                continue
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                if e.response.headers.get("content-type") == "application/json":
                    raise NtropyError(e.response.json()) from e
                raise
            return resp
        raise NtropyError(f"Failed to {method} {url} after {self._retries} attempts")

    def create_account_holder(self, account_holder: AccountHolder):
        """Creates an AccountHolder for the current user.

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
        mapping: dict = None,
        inplace: bool = False,
        tx_class: Any = Transaction,
    ) -> List[Transaction]:
        """Transforms a dataframe with the expected format to a list of `Transacton` objects

        Parameters
        ----------
        df
            The dataframe containing the Transactions. At minimum, the dataframe
            must contain the columns specified in the Transaction class.
        mapping
            A mapping from the column names of the provided dataframe and the
            expected column names
        inplace : bool, optional
            Enrich the dataframe inplace.
        """
        try:
            import pandas as pd
        except ImportError:
            # If here, the input data is not a dataframe, or import would succeed
            raise ValueError(
                f"add_transactions takes either a pandas.DataFrame or a list of Transactions for its `df` parameter, you supplied a '{type(df)}'"
            )

        if not isinstance(df, pd.DataFrame):
            raise TypeError("Transactions object needs to be a pandas dataframe.")

        if mapping is None:
            mapping = self.DEFAULT_MAPPING

        required_columns = tx_class._required_fields
        optional_columns = tx_class._fields

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

        txs = df.apply(tx_class.from_row, axis=1).to_list()
        return txs

    @singledispatchmethod
    def add_transactions(
        self,
        transactions,
        timeout: int = 4 * 60 * 60,
        poll_interval: int = 10,
        with_progress: bool = DEFAULT_WITH_PROGRESS,
        labeling: bool = True,
        create_account_holders: bool = True,
        model_name: str = None,
        mapping: dict = None,
        inplace: bool = False,
    ):
        """Enriches either a list of Transaction objects or a pandas dataframe synchronously.
        Returns a list of EnrichedTransactions or dataframe with the same order as the provided input.

        Parameters
        ----------
        transactions : List[Transaction], pandas.DataFrame
            A list of Transaction objects or a pandas DataFrame with the required
            columns.
        timeout : int, optional
            Timeout for enriching the transactions.
        poll_interval : int, optional
            The interval between consecutive polling retries.
        with_progress : bool, optional
            True if progress bar should be displayed; False otherwise. By default,
            progress is displayed only in interactive mode.
        labeling : bool, optional
            True if the enriched transactions should be labeled; False otherwise.
        model_name: str, optional
            Name of the custom model to use for labeling the transaction. If
            provided, replaces the default labeler
        mapping : dict, optional
            A mapping from the column names of the provided dataframe and the
            expected column names. Note: this only applies to DataFrame enrichment.
        inplace : bool, optional
            Enrich the dataframe inplace. Note: this only applies to DataFrame enrichment.

        Returns
        -------
        List[EnrichedTransaction], pandas.DataFrame
            A list of EnrichedTransaction objects or a corresponding pandas DataFrame.
        """
        if mapping is None:
            mapping = self.DEFAULT_MAPPING

        txs = self.df_to_transaction_list(transactions, mapping, inplace)
        transactions["_output_tx"] = self.add_transactions(
            txs,
            labeling=labeling,
            create_account_holders=create_account_holders,
            timeout=timeout,
            poll_interval=poll_interval,
            with_progress=with_progress,
            model_name=model_name,
        )

        def get_tx_val(tx, v):
            sentinel = object()
            output = getattr(tx, v, tx.kwargs.get(v, sentinel))
            if output == sentinel:
                raise KeyError(f"invalid mapping: {v} not in {tx}")
            return output

        for k, v in mapping.items():
            transactions[v] = transactions["_output_tx"].apply(
                lambda tx: get_tx_val(tx, k)
            )
        transactions = transactions.drop(["_output_tx"], axis=1)
        return transactions

    @add_transactions.register(list)
    def _add_transactions_list(
        self,
        transactions,
        timeout: int = 4 * 60 * 60,
        poll_interval=10,
        with_progress=DEFAULT_WITH_PROGRESS,
        labeling=True,
        create_account_holders=True,
        model_name=None,
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
            model_name,
        )

    @staticmethod
    def _build_params_str(
        labeling: bool, create_account_holders: bool, model_name: str = None
    ) -> str:
        params = {
            "labeling": labeling,
            "create_account_holders": create_account_holders,
        }
        if model_name is not None:
            params["model_name"] = model_name

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
        model_name: str = None,
    ) -> EnrichedTransactionList:
        is_sync = len(transactions) <= self.MAX_SYNC_BATCH
        if not is_sync:
            batch = self._add_transactions_async(
                transactions,
                timeout,
                poll_interval,
                labeling,
                create_account_holders,
                model_name,
            )
            with_progress = with_progress or self._with_progress
            return batch.wait(with_progress=with_progress)

        params_str = self._build_params_str(
            labeling, create_account_holders, model_name=model_name
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
        transactions,
        timeout: int = 4 * 60 * 60,
        poll_interval: int = 10,
        labeling: bool = True,
        create_account_holders: bool = True,
        model_name: str = None,
        mapping: dict = None,
        inplace: bool = False,
    ):
        """Enriches either a list of Transaction objects or a pandas dataframe asynchronously.
        Returns a list of EnrichedTransactions or dataframe with the same order as the provided input.

        Parameters
        ----------
        transactions : List[Transaction], pandas.DataFrame
            A list of Transaction objects or a pandas DataFrame with the required
            columns.
        timeout : int, optional
            Timeout for enriching the transactions.
        poll_interval : int, optional
            The interval between consecutive polling retries.
        labeling : bool, optional
            True if the enriched transactions should be labeled; False otherwise.
        model_name: str, optional
            Name of the custom model to use for labeling the transaction. If
            provided, replaces the default labeler
        mapping : dict, optional
            A mapping from the column names of the provided dataframe and the
            expected column names. Note: this only applies to DataFrame enrichment.
        inplace : bool, optional
            Enrich the dataframe inplace. Note: this only applies to DataFrame enrichment.

        Returns
        -------
        Batch
            A Batch object that can be polled and awaited.
        """
        if mapping is None:
            mapping = self.DEFAULT_MAPPING

        txs = self.df_to_transaction_list(transactions, mapping, inplace)
        return self.add_transactions_async(
            txs,
            timeout=timeout,
            labeling=labeling,
            create_account_holders=create_account_holders,
            poll_interval=poll_interval,
            model_name=model_name,
        )

    @add_transactions_async.register(list)
    def _add_transactions_async_list(
        self,
        transactions: List[Transaction],
        timeout=4 * 60 * 60,
        poll_interval=10,
        labeling=True,
        create_account_holders=True,
        model_name=None,
    ):
        return self._add_transactions_async(
            transactions,
            timeout,
            poll_interval,
            labeling,
            create_account_holders,
            model_name,
        )

    def _add_transactions_async(
        self,
        transactions: List[Transaction],
        timeout=4 * 60 * 60,
        poll_interval=10,
        labeling=True,
        create_account_holders=True,
        model_name=None,
    ) -> Batch:
        params_str = self._build_params_str(
            labeling, create_account_holders, model_name=model_name
        )

        try:

            url = "/v2/transactions/async?" + params_str

            data = [transaction.to_dict() for transaction in transactions]
            resp = self.retry_ratelimited_request("POST", url, data)

            r = resp.json()
            batch_id = r.get("id", "")

            if not batch_id:
                raise ValueError("batch_id missing from response")

            return Batch(
                sdk=self,
                batch_id=batch_id,
                timeout=timeout,
                poll_interval=poll_interval,
                num_transactions=len(transactions),
            )

        except requests.HTTPError as e:
            if e.response.status_code == 404:
                error = e.response.json()
                raise ValueError(f"{error['detail']}")

            raise

    def get_account_holder(self, account_holder_id: str) -> AccountHolder:
        """Returns an AccountHolder object for the account holder with the provided id

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
    ) -> dict:
        """Returns the result of a metrics query for a specific account holder.

        Parameters
        ----------
        account_holder_id : str
            The unique identifier for the account holder.
        metrics : List[str]
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

    def get_labels(self, account_holder_type: str) -> dict:
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

    def get_chart_of_accounts(self) -> dict:
        """Returns all available chart of accounts.

        Returns
        -------
        dict
            A hierarchy of possible chart of accounts.
        """

        url = "/v2/chart-of-accounts"
        resp = self.retry_ratelimited_request("GET", url, None)
        return resp.json()

    @singledispatchmethod
    def train_custom_model(self, transactions, model_name: str) -> Model:
        """Trains a custom model for labeling transactions, using the provided transactions as training data,
        either as a list of LabeledTransactions, or as a dataframe with the Transactions attributes and a label column.
        The model is associated with the provided name. Returns a Model instance that can be polled or waited for
        while the training is running, and can be used in enrichment after ready.

        Parameters
        ----------
        transactions : Union[List[LabeledTransaction], pandas.DataFrame]
            Set of input transactions and corresponding new label for training
        model_name : str
            Name to associate to the model

        Returns
        -------
        Model
            Model instance referencing the in-training model
        """
        try:
            import pandas as pd
        except ImportError:
            # If here, the input data is not a dataframe, or import would succeed
            raise ValueError(
                f"train_custom_model takes either a pandas.DataFrame or a list of Transactions for its `df` parameter, you supplied a '{type(transactions)}'"
            )

        transactions = self.df_to_transaction_list(
            transactions,
            mapping=None,
            inplace=True,
            tx_class=LabeledTransaction,
        )

        return self._train_custom_model(transactions, model_name)

    @train_custom_model.register(list)
    def train_custom_model_list(
        self,
        transactions,
        model_name: str,
    ) -> Model:
        return self._train_custom_model(
            transactions,
            model_name,
        )

    def _train_custom_model(self, transactions, model_name: str) -> Model:
        txs = [tx.to_dict() for tx in transactions]

        url = f"/v2/models/{model_name}"
        response = self.retry_ratelimited_request(
            "POST", url, {"transactions": txs}
        ).json()
        return Model.from_response(self, response)

    def get_all_custom_models(self) -> List[Model]:
        """Returns a list of Model objects for all existing custom models previously trained

        Returns
        -------
        List[Model]
            List of all trained models, independently of their status
        """
        url = "/v2/models"
        responses = self.retry_ratelimited_request("GET", url, None).json()
        return [Model.from_response(self, r) for r in responses]

    def get_custom_model(self, model_name: str) -> Model:
        """Returns a specific model referenced by its model name

        Parameters
        ----------
        model_name : str
            Name of the model to query

        Returns
        -------
        Model
            Reference to the queried model
        """
        url = f"/v2/models/{model_name}"
        response = self.retry_ratelimited_request("GET", url, None).json()
        return Model.from_response(self, response)

    class Config:
        keep_untouched = (singledispatchmethod,)


Batch.update_forward_refs()
EnrichedTransaction.update_forward_refs()
Model.update_forward_refs()
