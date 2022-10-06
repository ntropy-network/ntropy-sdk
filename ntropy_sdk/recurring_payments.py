from typing import List, Union
import pandas as pd
from tabulate import tabulate


class RecurringPaymentsGroup:
    def __init__(self, recurring_payments_dict):
        self.data = recurring_payments_dict

        self.periodicity = (
            self.data["periodicity"] if "periodicity" in self.data else "unknown"
        )
        self.amount = self.data["amount"] if "amount" in self.data else 0
        self.total_amount = (
            self.data["total_amount"] if "total_amount" in self.data else 0
        )
        self.iso_currency_code = (
            self.data["iso_currency_code"]
            if "iso_currency_code" in self.data
            else "unknown"
        )
        self.type = self.data["type"] if "type" in self.data else "unknown"
        self.is_essential = (
            self.data["is_essential"] if "is_essential" in self.data else False
        )
        self.merchant = self.data["merchant"] if "merchant" in self.data else "unknown"
        self.website = self.data["website"] if "website" in self.data else "unknown"
        self.logo = self.data["logo"] if "logo" in self.data else "unknown"
        self.labels = self.data["labels"] if "labels" in self.data else []
        self.first_payment_date = (
            self.data["first_payment_date"]
            if "first_payment_date" in self.data
            else None
        )
        self.latest_payment_date = (
            self.data["latest_payment_date"]
            if "latest_payment_date" in self.data
            else None
        )
        self.latest_payment_description = (
            self.data["latest_payment_description"]
            if "latest_payment_description" in self.data
            else ""
        )
        self.transaction_ids = (
            self.data["transaction_ids"] if "transaction_ids" in self.data else []
        )
        self.transactions = (
            self.data["transactions"] if "transactions" in self.data else []
        )
        self.is_active = self.data["is_active"] if "is_active" in self.data else False
        self.next_expected_payment_date = (
            self.data["next_expected_payment_date"]
            if "next_expected_payment_date" in self.data
            else None
        )
        self.next_expected_payment_amount = (
            self.data["next_expected_payment_amount"]
            if "next_expected_payment_amount" in self.data
            else 0
        )

    def _repr_df(self):
        labels = [
            "amount",
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
                    "amount": rpg.amount,
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
