import logging
import pandas as pd
import time

from requests import HTTPError
from typing import List, Union, Any, Dict

from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.metrics import f1_score
from sklearn.exceptions import NotFittedError

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
        """Base wrapper for an Ntropy custom model that implements
        the scikit-learn interfaces of BaseEstimator and ClassifierMixin

        Parameters
        ----------
        name : str
            identifying name of the custom model
        sync : bool, optional
            if True the scikit-learn model will block during training
            until it is complete or errors, by default True
        poll_interval : int, optional
            interval in seconds to use for polling the server when
            listening for model status changes, by default 10
        labels_only : bool, optional
            if True, returns only the labels on the predict method
            so that the interface is scikit-learn compatible, by default True
        progress_bar : bool, optional
            if True uses a progress bar to track progress of model training
        sdk : SDK, optional
            instantiated SDK object, if not provided and not set using
            set_sdk, it will be created by reading the API key from
            environment variables
        """
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
        """Returns a json dictionary containing the status of the model

        Returns
        -------
        dict
        """
        status = self.sdk.retry_ratelimited_request(
            "GET", f"/v2/models/{self.name}", None
        ).json()
        return status

    def status(self) -> Dict:
        """Returns a json dictionary containing the status of model, checking for errors first

        Returns
        -------
        dict

        Raises
        ------
        ValueError
            if status of the model is set to error by the server
        RuntimeError
            if the model fails with an unexpected error
        """
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
        """Checks if the model is ready to be used for inference

        Returns
        -------
        bool
        """
        status = self.status()
        if isinstance(status, bool):
            return False

        return status["status"] == "ready"

    def check_is_fitted(self):
        """Checks if the model has been fitted before

        Raises
        ------
        NotFittedError
        """
        if not self.is_ready():
            raise NotFittedError()

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
                filtered_tx = {}
                for field in Transaction.fields:
                    if field in tx:
                        filtered_tx[field] = tx[field]

                tx = Transaction.from_dict(filtered_tx)

            if as_dict:
                tx = tx.copy()

            uniform_txs.append(tx)
        return uniform_txs

    def fit(self, X: TransactionList, y: List[str], **params) -> "BaseModel":
        """Starts a training process for a custom labeling model given the provided
        input data. The model can be trained using a list of transactions or a
        dataframe with the same transactions

        Parameters
        ----------
        X : Union[List[Union[dict, Transaction]], pd.DataFrame]
            A list of transactions in Transaction or dictionary format, or a dataframe
            containing one transaction per row, and the necessary attributes in columnar format
        y : List[str]
            A list of labels in the same order as the provided transactions

        Returns
        -------
        BaseModel
            self

        Raises
        ------
        RuntimeError
            if the model errors out unexpectedly during training
        """
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
        """Given a sequence of transactions (in list or dataframe format), predicts the
        labels using the trained custom model and returns them in the same order as the
        provided transactions. If labels_only is set to False, returns a list of
        EnrichedTransactions instead

        Parameters
        ----------
        X : Union[List[Union[dict, Transaction]], pd.DataFrame]
            A list of transactions in Transaction or dictionary format, or a dataframe
            containing one transaction per row, and the necessary attributes in columnar format

        Returns
        -------
        Union[List[str], EnrichedTransaction]
            A list of labels or EnrichedTransactions in the same order as the provided
            input transactions
        """
        self.check_is_fitted()

        X = self._process_transactions(X, as_dict=False)
        y = self.sdk.add_transactions(
            X, model=self.name, poll_interval=self.poll_interval
        )

        if self.labels_only:
            return [tx.labels for tx in y]

        return y

    def score(self, X: TransactionList, y: List[str]) -> float:
        """Calculates the micro-averaged F1 score for the trained custom model given the provided
        test set

        Parameters
        ----------
        X : Union[List[Union[dict, Transaction]], pd.DataFrame]
            A list of transactions in Transaction or dictionary format, or a dataframe
            containing one transaction per row, and the necessary attributes in columnar format
        y : List[str]
            A list of labels in the same order as the provided transactions

        Returns
        -------
        float
            micro-averaged F1 score
        """
        y_pred = self.predict(X)
        return f1_score(y, y_pred, average="micro")

    def get_params(self, deep: bool = True) -> dict:
        return {
            "name": self.name,
            "sync": self.sync,
            "poll_interval": self.poll_interval,
            "labels_only": self.labels_only,
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
        return super().fit(X, y, n_epochs=n_epochs, random_state=random_state)
