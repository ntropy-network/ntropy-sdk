from multiprocessing.sharedctypes import Value
import time
from requests import HTTPError
from urllib.parse import urlencode
from typing import List, Union, Any

from sklearn.base import BaseEstimator
from sklearn.base import ClassifierMixin
from sklearn.metrics import f1_score

from ntropy_sdk import SDK, NtropyError, Transaction


class FewShotClassifier(BaseEstimator, ClassifierMixin):
    def __init__(
        self,
        name: str,
        sync: bool = True,
        poll_interval: int = 10,
        labels_only: bool = True,
        _sdk: SDK = None,
    ):
        self.name = name
        self.sync = sync
        self.poll_interval = poll_interval
        self.labels_only = labels_only
        self._sdk = _sdk

        if not self._sdk:
            # attempt initialization from environment variable
            try:
                self._sdk = SDK()
            except NtropyError:
                pass

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

    def is_ready(self) -> bool:
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

        return status["status"] == "ready"

    @staticmethod
    def _process_transactions(txs: List[Union[Transaction, dict]]) -> List[dict]:
        uniform_txs = []
        for tx in txs:
            if isinstance(tx, Transaction):
                tx = tx.to_dict()     
            elif not isinstance(tx, dict):
                raise ValueError(f"Unsupported type for transaction: {type(tx)}")
            uniform_txs.append(tx)
        return uniform_txs


    def fit(self, X: List[Union[dict, Transaction]], y: List[str]) -> "FewShotClassifier":
        url = f"/v2/models/{self.name}"

        X = self._process_transactions(X)
        for tx, label in zip(X, y):
            tx["label"] = label

        self.sdk.retry_ratelimited_request(
            "POST", url, payload={"transactions": X, "model_type": "FewShotClassifier"}
        ).json()

        if self.sync:
            while not self.is_ready():
                time.sleep(self.poll_interval)

        return self

    def predict(self, X: List[Union[dict, Transaction]]) -> List[str]:
        if not self.is_ready():
            raise ValueError("Model is not ready for predictions yet")

        params_str = urlencode({"model_name": self.name})
        url = f"/v2/transactions/sync?" + params_str

        X = self._process_transactions(X)

        r = self.sdk.retry_ratelimited_request(
            "POST",
            url,
            payload=X,
        )

        y = r.json()

        if "status" in y and y["status"] != 200:
            raise RuntimeError(y["detail"])
        if self.labels_only:
            return [tx["labels"] for tx in y]

        return y

    def score(self, X: List[Union[dict, Transaction]], y: List[str]) -> float:
        y_pred = self.predict(X)
        return f1_score(y, y_pred, average="micro")

    def get_params(self, deep: bool=True) -> dict:
        return {
            "name": self.name,
            "sync": self.sync,
            "refresh_rate": self.poll_interval,
            "labels_only": self.labels_only,
        }

    def set_params(self, **parameters: Any) -> "FewShotClassifier":
        for parameter, value in parameters.items():
            setattr(self, parameter, value)
        return self
