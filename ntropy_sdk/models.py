from asyncore import poll
import pandas as pd

from typing import List, Union, Any, Optional

from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.metrics import f1_score
from sklearn.exceptions import NotFittedError

from ntropy_sdk import SDK, Transaction, LabeledTransaction, Model

TransactionList = Union[List[Union[dict, Transaction]], pd.DataFrame]


class CustomTransactionClassifier(BaseEstimator, ClassifierMixin):
    def __init__(
        self,
        model_name: str,
        sync: bool = True,
        progress: bool = True,
        labels_only: bool = True,
        sdk: Optional[SDK] = None,
        poll_interval: Optional[int] = None,
        timeout: Optional[int] = None,
    ):
        """
        Parameters
        ----------
        model_name : str
            Unique name that identifies the trained model
        poll_interval : int, optional
            A timeout for retrieving the batch result.
        timeout : int, optional
            The interval between polling retries.
        sync : bool, optional
            if True the scikit-learn model will block during training until it is complete or errors
        progress : bool, optional
            if True displays a progress bar during the training process
        labels_only : bool, optional
            if True, returns only the labels on the predict method so that the interface is scikit-learn compatible
        sdk: SDK, optional
            if provided sets the SDK instance to use for this model's interaction with the API
        """
        self.model_name = model_name
        self.labels_only = labels_only
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.sync = sync
        self.progress = progress
        self._sdk = sdk
        self._model = None

    @property
    def sdk(self):
        # Lazy checking for sdk
        if self._sdk is None:
            raise RuntimeError(
                "API SDK is not set. You must either provide SDK on initialization, set the environment variable NTROPY_API_TOKEN or use the model's set_sdk method"
            )
        return self._sdk

    @property
    def model(self):
        if self._model is None:
            self._model = Model(
                sdk=self.sdk,
                model_name=self.model_name,
                poll_interval=self.poll_interval,
                timeout=self.timeout,
            )
        return self._model

    def set_sdk(self, sdk: SDK):
        self._sdk = sdk

    def get_status(self) -> dict:
        """Returns a json dictionary containing the status of the model

        Returns
        -------
        dict
        """

        return self.model.poll()[0]

    def is_ready(self) -> bool:
        """Checks if the model is ready to be used for inference

        Returns
        -------
        bool
        """

        _, status, _ = self.model.poll()
        return status == "ready"

    def check_is_fitted(self):
        """Checks if the model has been fitted before

        Raises
        ------
        NotFittedError
        """
        if not self.is_ready():
            raise NotFittedError()

    @staticmethod
    def _process_transactions(
        txs: TransactionList, labels: List[str]
    ) -> List[LabeledTransaction]:
        """
        Adds labels to TransactionList object, returns the corresponding list of LabeledTransaction objects
        """

        if isinstance(txs, pd.DataFrame):
            txs = txs.apply(Transaction.from_row, axis=1).to_list()

        uniform_txs = []
        for tx, label in zip(txs, labels):
            if not isinstance(tx, Transaction):
                raise ValueError(f"Unsupported type for transaction: {type(tx)}")

            tx_dict = tx.to_dict()
            tx_dict["label"] = label

            uniform_txs.append(LabeledTransaction.from_dict(tx_dict))

        return uniform_txs

    def fit(
        self, X: TransactionList, y: List[str], **params
    ) -> "CustomTransactionClassifier":
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
        NtropyError
            if the model errors out unexpectedly during training
        """

        X = self._process_transactions(X, y)
        self._model = self.sdk.train_custom_model(
            X,
            self.model_name,
            poll_interval=self.poll_interval,
            timeout=self.timeout,
        )

        if self.sync:
            self.model.wait(with_progress=self.progress)

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

        y = self.sdk.add_transactions(
            X, model_name=self.model_name, poll_interval=self.poll_interval
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
            "model_name": self.model_name,
            "sync": self.sync,
            "poll_interval": self.poll_interval,
            "labels_only": self.labels_only,
        }

    def set_params(self, **parameters: Any) -> "CustomTransactionClassifier":
        for parameter, value in parameters.items():
            setattr(self, parameter, value)
        return self
