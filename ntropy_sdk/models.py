import logging
import pandas as pd
import time

from typing import Optional
from requests import HTTPError
from urllib.parse import urlencode
from typing import List, Union, Any, Dict

from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.metrics import f1_score
from sklearn.utils.validation import check_is_fitted, check_X_y
from sklearn.utils.multiclass import unique_labels

from ntropy_sdk import SDK, NtropyError, Transaction
from tqdm.auto import tqdm

TransactionList = Union[List[Union[dict, Transaction]], pd.DataFrame]


class BaseModel(BaseEstimator, ClassifierMixin):
    def __init__(
        self,
        name: str,
        sync: bool = True,
        poll_interval: int = 10,
        labels_only: bool = True,
        progress_bar: bool = True,
        sdk: SDK = None,
    ):
        self.name = name
        self.sync = sync
        self.poll_interval = poll_interval
        self.labels_only = labels_only
        self.progress_bar = progress_bar
        self._sdk = sdk
        self.params = {}

        if not self._sdk:
            # attempt initialization from environment variable
            try:
                self._sdk = SDK()
            except NtropyError:
                pass

    @property
    def model_type(self):
        raise NotImplementedError("BaseModel cannot be used to train directly")

    @property
    def sdk(self):
        # Lazy checking for sdk
        if self._sdk is None:
            raise RuntimeError(
                "API SDK is not set. You must either provide SDK on initialization, set the environment variable NTROPY_API_TOKEN or use the model's set_sdk method"
            )
        return self._sdk

    def set_sdk(self, sdk: SDK):
        self._sdk = sdk

    def get_status(self) -> dict:
        status = self.sdk.retry_ratelimited_request(
            "GET", f"/v2/models/{self.name}", None
        ).json()
        return status

    def status(self) -> Dict:
        try:
            status = self.get_status()
        except HTTPError as e:
            if e.response.status_code == 404:
                return False
            raise

        if "status" not in status:
            raise ValueError("Unexpected response from server during fit")
        if status["status"] == "error":
            raise RuntimeError("Model training failed with an internal error")

        return status

    def is_ready(self) -> bool:
        status = self.status()
        return status["status"] == "ready"

    def __sklearn_is_fitted__(self):
        return self.is_ready()

    @staticmethod
    def _process_transactions(txs: TransactionList, as_dict: bool = True) -> List[dict]:
        if isinstance(txs, pd.DataFrame):
            txs = txs.to_dict(orient="records")

        uniform_txs = []
        for tx in txs:
            if not (isinstance(tx, dict) or isinstance(tx, Transaction)):
                raise ValueError(f"Unsupported type for transaction: {type(tx)}")

            if isinstance(tx, Transaction) and as_dict:
                tx = tx.to_dict()
            if isinstance(tx, dict) and not as_dict:
                tx = Transaction.from_dict(tx)

            if as_dict:
                tx = tx.copy()

            uniform_txs.append(tx)
        return uniform_txs

    def fit(self, X: TransactionList, y: List[str], **params) -> "BaseModel":
        url = f"/v2/models/{self.name}"
        self.params = params

        X = self._process_transactions(X)
        for tx, label in zip(X, y):
            tx["label"] = label

        self.sdk.retry_ratelimited_request(
            "POST",
            url,
            payload={
                "transactions": X,
                "model_type": self.model_type,
                "params": params,
            },
            log_level=logging.WARNING,
        ).json()

        if self.sync:
            if self.progress_bar:
                with tqdm(total=100, desc="starting") as pbar:
                    ready = False
                    while not ready:
                        status = self.status()
                        ready = status["status"] == "ready"
                        diff_n = status["progress"] - pbar.n
                        pbar.update(int(diff_n))
                        pbar.desc = status["status"]
                        if ready:
                            break
                        time.sleep(self.poll_interval)
            else:
                status = self.status()
                while not status["status"] == "ready":
                    status = self.status()

                    if status["status"] == "error":
                        # TODO: improve information
                        raise RuntimeError("Unexpected error while training the model")

                    time.sleep(self.poll_interval)

        return self

    def predict(self, X: TransactionList) -> List[str]:
        check_is_fitted(self)

        X = self._process_transactions(X, as_dict=False)
        y = self.sdk.add_transactions(
            X, model=self.name, poll_interval=self.poll_interval
        )

        if self.labels_only:
            return [tx.labels for tx in y]

        return y

    def score(self, X: TransactionList, y: List[str]) -> float:
        y_pred = self.predict(X)
        return f1_score(y, y_pred, average="micro")

    def get_params(self, deep: bool = True) -> dict:
        return {
            "name": self.name,
            "sync": self.sync,
            "poll_interval": self.poll_interval,
            "labels_only": self.labels_only,
            "params": self.params,
        }

    def set_params(self, **parameters: Any) -> "BaseModel":
        for parameter, value in parameters.items():
            setattr(self, parameter, value)
        return self


class CustomTransactionClassifier(BaseModel):
    model_type = "CustomTransactionClassifier"

    def fit(
        self,
        X: TransactionList,
        y: List[str],
        n_epochs: int = 2,
        random_state: int = 42,
    ):
        super().fit(X, y, n_epochs=n_epochs, random_state=random_state)
