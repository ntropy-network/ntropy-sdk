from typing import List, Union, Optional, Dict, Any
import pandas as pd
from pydantic import BaseModel
from tabulate import tabulate


class RecurringPaymentsGroup(BaseModel):
    merchant: Optional[str]
    website: Optional[str]
    labels: Optional[List[str]]
    logo: Optional[str]
    periodicity: str
    latest_payment_amount: float
    type: str
    is_essential: bool
    is_active: bool
    first_payment_date: str
    latest_payment_date: str
    next_expected_payment_date: Optional[str]
    latest_payment_description: str
    transaction_ids: List[Union[int, str]]
    transactions: Any
    total_amount: float
    iso_currency_code: Optional[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        return cls(
            periodicity=data.get('periodicity'),
            latest_payment_amount=data.get('latest_payment_amount', data.get('amount', 0)),
            merchant=data.get('merchant'),
            website=data.get('website'),
            logo=data.get('website'),
            labels=data.get('labels', []),
            type=data.get('type'),
            is_essential=data.get('is_essential'),
            is_active=data.get('is_active'),
            first_payment_date=data.get('first_payment_date'),
            latest_payment_date=data.get('latest_payment_date'),
            next_expected_payment_date=data.get('next_expected_payment_date'),
            latest_payment_description=data.get('latest_payment_description', ''),
            transaction_ids=data.get('transaction_ids', []),
            transactions=data.get('transactions', []),
            total_amount=data.get('total_amount', 0),
            iso_currency_code=data.get('iso_currency_code'),
        )

    def _repr_df(self):
        labels = [
            "latest_payment_amount",
            "merchant",
            "website",
            "labels",
            "type",
            "is_essential",
            "periodicity",
            "is_active",
            "first_payment_date",
            "latest_payment_date",
            "next_expected_payment_date",
            "latest_payment_description",
            "total_amount",
            "iso_currency_code",
        ]
        data = []
        d = vars(self)
        for label in labels:
            data.append({"key": label, "value": d[label]})
        data.insert(0, {"key": "# txs", "value": len(self.transaction_ids)})
        df = pd.DataFrame(data)
        return df

    def _repr_html_(self) -> Union[str, None]:
        df = self._repr_df()
        return df._repr_html_()

    def __repr__(self) -> str:
        df = self._repr_df()
        return tabulate(df, showindex=False)


class RecurringPaymentsGroups(list):
    """A list of RecurringPaymentsGroup objects."""

    def __init__(self, recurring_payments_groups: List[RecurringPaymentsGroup]):
        """Parameters
        ----------
        recurring_payments_groups : List[RecurringPaymentsGroup]
            A list of RecurringPaymentsGroup objects.
        """

        super().__init__(recurring_payments_groups)
        self.list = recurring_payments_groups

    def to_df(self):
        recurring_payments_groups = []
        for rpg in self.list:
            recurring_payments_groups.append(
                {
                    "latest_payment_amount": rpg.latest_payment_amount,
                    "merchant": rpg.merchant,
                    "website": rpg.website,
                    "labels": rpg.labels,
                    "periodicity": rpg.periodicity,
                    "is_active": rpg.is_active,
                    "first_payment_date": rpg.first_payment_date,
                    "latest_payment_date": rpg.latest_payment_date,
                    "next_expected_payment_date": rpg.next_expected_payment_date,
                    "latest_payment_description": rpg.latest_payment_description,
                    "type": rpg.type,
                    "is_essential": rpg.is_essential,
                    "transaction_ids": rpg.transaction_ids,
                    "total_amount": rpg.total_amount,
                    "iso_currency_code": rpg.iso_currency_code,
                }
            )

        df = pd.DataFrame(recurring_payments_groups)
        return df

    def _repr_df(self):
        df = self.to_df()
        df = df.fillna("")
        if df.empty:
            return df
        df.insert(0, "# txs", df["transaction_ids"].apply(lambda x: len(x)))
        df = df.drop(columns=["transaction_ids"])
        return df

    def _repr_html_(self) -> Union[str, None]:
        df = self._repr_df()
        if df.empty:
            return f"{self.__class__.__name__}([])"
        return df._repr_html_()

    def __repr__(self) -> str:
        df = self._repr_df()
        if df.empty:
            return f"{self.__class__.__name__}([])"

        if "labels" in df.columns:
            df["labels"] = df["labels"].apply(lambda x: "\n".join(x))
        return tabulate(
            df,
            showindex=False,
            headers="keys",
            maxcolwidths=[2, 16, 16, 16, 16, 12, 12, 12, 12, 12, 20, 12],
        )

    def essential(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self.list
                if recurring_payments_group.is_essential
            ]
        )

    def non_essential(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self.list
                if not recurring_payments_group.is_essential
            ]
        )

    def active(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self.list
                if recurring_payments_group.is_active
            ]
        )

    def inactive(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self.list
                if not recurring_payments_group.is_active
            ]
        )

    def subscriptions(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self.list
                if recurring_payments_group.type == "subscription"
            ]
        )

    def recurring_bills(self):
        return RecurringPaymentsGroups(
            [
                recurring_payments_group
                for recurring_payments_group in self.list
                if recurring_payments_group.type == "bill"
            ]
        )

    def total_amount(self):
        return round(
            sum(
                [
                    recurring_payments_group.total_amount
                    for recurring_payments_group in self.list
                ]
            ),
            2,
        )
