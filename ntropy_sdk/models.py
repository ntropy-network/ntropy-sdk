import pandas as pd

from typing import List, Union, Any

from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.metrics import f1_score
from sklearn.exceptions import NotFittedError

from ntropy_sdk import SDK, Transaction, LabeledTransaction, Model
from pydantic import Field

TransactionList = Union[List[Union[dict, Transaction]], pd.DataFrame]


class BaseModel(Model, BaseEstimator, ClassifierMixin):

    sync: bool = Field(
        True,
        description="if True the scikit-learn model will block during training until it is complete or errors",
    )
    labels_only: bool = Field(
        True,
        description="if True, returns only the labels on the predict method so that the interface is scikit-learn compatible",
    )

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

        return super().poll()[0]

    def is_ready(self) -> bool:
        """Checks if the model is ready to be used for inference

        Returns
        -------
        bool
        """

        _, status, _ = super().poll()
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
        NtropyError
            if the model errors out unexpectedly during training
        """

        X = self._process_transactions(X, y)
        self.sdk.train_custom_model(X, self.model_name)

        if self.sync:
            super().wait(with_progress=self.progress)

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

    def set_params(self, **parameters: Any) -> "BaseModel":
        for parameter, value in parameters.items():
            setattr(self, parameter, value)
        return self


class CustomTransactionClassifier:
    def __init__(
        self,
        **kwargs,
    ):
        self.model = BaseModel(**kwargs)
        self.model_type = "CustomTransactionClassifier"

    def fit(
        self,
        X: TransactionList,
        y: List[str],
        n_epochs: int = 2,
        random_state: int = 42,
    ):
        return self.model.fit(X, y, n_epochs=n_epochs, random_state=random_state)

    def score(self, X: TransactionList, y: List[str]) -> float:
        return self.model.score(X, y)

    def get_params(self, deep: bool = True) -> dict:
        return self.model.get_params(deep)

    def set_params(self, **parameters: Any) -> "BaseModel":
        return self.model.set_params(**parameters)

    def get_base_model(self):
        return self.model
