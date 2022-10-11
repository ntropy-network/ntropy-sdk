from typing import List, Union, Optional, Dict, Any
from pydantic import BaseModel
from tabulate import tabulate


class RecurringPaymentsGroup(BaseModel):
    latest_payment_amount: float
    periodicity: str
    merchant: Optional[str]
    website: Optional[str]
    labels: Optional[List[str]]
    logo: Optional[str]
    type: str
    is_essential: bool
    is_active: bool
    first_payment_date: str
    latest_payment_date: str
    next_expected_payment_date: Optional[str]
    latest_payment_description: str
    transaction_ids: List[Union[int, str]]
    transactions: Any  # EnrichedTransactionList
    total_amount: float
    iso_currency_code: Optional[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        return cls(
            periodicity=data.get("periodicity"),
            latest_payment_amount=data.get(
                "latest_payment_amount", data.get("amount", 0)
            ),
            merchant=data.get("merchant"),
            website=data.get("website"),
            logo=data.get("website"),
            labels=data.get("labels", []),
            type=data.get("type"),
            is_essential=data.get("is_essential"),
            is_active=data.get("is_active"),
            first_payment_date=data.get("first_payment_date"),
            latest_payment_date=data.get("latest_payment_date"),
            next_expected_payment_date=data.get("next_expected_payment_date"),
            latest_payment_description=data.get("latest_payment_description", ""),
            transaction_ids=data.get("transaction_ids", []),
            transactions=data.get("transactions", []),
            total_amount=data.get("total_amount", 0),
            iso_currency_code=data.get("iso_currency_code"),
        )

    def to_df(self) -> Any:
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("pandas is not installed")
        return pd.DataFrame(
            self.dict(exclude={"transactions"}).items(), columns=["key", "value"]
        )

    def _repr_df(self) -> Any:
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("pandas is not installed")
        df = self.to_df()
        if df.empty:
            return df
        df = df.fillna("N/A")
        return df

    def _repr_html_(self) -> Union[str, None]:
        # used by ipython/jupyter to render
        try:
            import pandas as pd

            df = self._repr_df()
            if df.empty:
                return self.__repr__()
            return df._repr_html_()
        except ImportError:
            # pandas not installed
            return self.__repr__()

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        try:
            import pandas as pd

            df = self._repr_df()
            if df.empty:
                repr = self.dict(exclude={"transactions"})
                return f"{self.__class__.__name__}({repr})"
            return tabulate(df, showindex=False)
        except ImportError:
            # pandas not installed
            repr = self.dict(exclude={"transactions"})
            return f"{self.__class__.__name__}({repr})"


class RecurringPaymentsGroups(list):
    """A list of RecurringPaymentsGroup objects."""

    def __init__(self, recurring_payments_groups: List[RecurringPaymentsGroup]):
        """Parameters
        ----------
        recurring_payments_groups : List[RecurringPaymentsGroup]
            A list of RecurringPaymentsGroup objects.
        """
        super().__init__(recurring_payments_groups)

    def to_df(self) -> Any:
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("pandas is not installed")
        return pd.DataFrame([rpg.dict(exclude={"transactions"}) for rpg in self])

    def dict(self) -> List[Dict[str, Any]]:
        return [rpg.dict(exclude={"transactions"}) for rpg in self]

    def _repr_df(self) -> Any:
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("pandas is not installed")
        df = self.to_df()
        if df.empty:
            return df
        df = df.fillna("N/A")
        df.insert(0, "# transactions", df.transaction_ids.apply(lambda x: len(x)))
        df = df.drop(columns=["transaction_ids"])
        return df

    def _repr_html_(self) -> Union[str, None]:
        # used by ipython/jupyter to render
        try:
            import pandas as pd

            df = self._repr_df()
            if df.empty:
                return f"{self.__class__.__name__}([])"
            return df._repr_html_()
        except ImportError:
            # pandas not installed
            return self.__repr__()

    def __repr__(self) -> str:
        try:
            import pandas as pd

            df = self._repr_df()
            if df.empty:
                return f"{self.__class__.__name__}([])"
            if "labels" in df.columns:
                df["labels"] = df["labels"].apply(lambda x: "\n".join(x))
            return tabulate(
                df,
                showindex=False,
                headers="keys",
                maxcolwidths=[2, 12, 12, 16, 16, 16, 16, 12, 12, 12, 20, 12],
            )
        except ImportError:
            # pandas not installed
            repr = str(self.dict())
            return f"{self.__class__.__name__}({repr})"

    def essential(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self
                if recurring_payments_group.is_essential
            ]
        )

    def non_essential(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self
                if not recurring_payments_group.is_essential
            ]
        )

    def active(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self
                if recurring_payments_group.is_active
            ]
        )

    def inactive(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self
                if not recurring_payments_group.is_active
            ]
        )

    def subscriptions(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self
                if recurring_payments_group.type == "subscription"
            ]
        )

    def recurring_bills(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self
                if recurring_payments_group.type == "bill"
            ]
        )

    def total_amount(self):
        return round(
            sum(
                [
                    recurring_payments_group.total_amount
                    for recurring_payments_group in self
                ]
            ),
            2,
        )
