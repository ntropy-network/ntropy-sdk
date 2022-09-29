from typing import List, Union

import pandas as pd
from tabulate import tabulate

class RecurringPaymentsGroup:
    def __init__(self, recurring_payments_group_dict):
        self.data = recurring_payments_group_dict

        self.periodicity = (
            self.data["periodicity"] if "periodicity" in self.data else "unknown"
        )
        self.amount = self.data["amount"] if "amount" in self.data else 0
        self.total_amount = self.data["total_amount"] if "total_amount" in self.data else 0
        self.type = self.data["type"] if "type" in self.data else 'unknown'
        self.is_essential = self.data["is_essential"] if "is_essential" in self.data else False
        self.merchant = self.data["merchant"] if "merchant" in self.data else "unknown"
        self.website = self.data["website"] if "website" in self.data else "unknown"
        self.logo = self.data["logo"] if "logo" in self.data else "unknown"
        self.labels = (
            self.data["labels"] if "labels" in self.data else []
        )
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
        self.transaction_ids = (
            self.data["transaction_ids"] if "transaction_ids" in self.data else []
        )
        self.transactions = (
            self.data["transactions"] if "transactions" in self.data else EnrichedTransactionList()
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
        ]
        data = []
        d = vars(self)
        for label in labels:
            data.append({"key": label, "value": d[label]})
        df = pd.DataFrame(data)
        return df

    def _repr_html_(self) -> Union[str, None]:
        df = self._repr_df()
        return df._repr_html_()

    def __repr__(self) -> str:
        df = self._repr_df()
        # return df.to_string(index=False)
        return tabulate(df, showindex=False)

    # def __repr__(self) -> str:
    #     return f"{self.__class__.__name__}(periodicity={self.periodicity}, merchant={self.merchant}, amount={self.amount}, is_active={self.is_active})"


class RecurringPaymentsList(list):
    """A list of RecurringPaymentsGroup objects."""

    def __init__(self, recurring_payments_groups: List[RecurringPaymentsGroup]):
        """Parameters
        ----------
        recurring_payments_groups : List[RecurringPaymentsGroup]
            A list of RecurringPaymentsGroup objects.
        """

        super().__init__(recurring_payments_groups)
        self.recurring_payments_groups = recurring_payments_groups

    def to_df(self):
        rpgs = []
        for rpg in self.recurring_payments_groups:
            rpgs.append(
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
                    "type": rpg.type,
                    "is_essential": rpg.is_essential,
                    "transaction_ids": rpg.transaction_ids,
                }
            )

        df = pd.DataFrame(rpgs)
        return df

    def _repr_df(self):
        df = self.to_df()
        df = df.fillna('')
        if df.empty:
            return f"{self.__class__.__name__}([])"
        df.insert(0, '#txs', df['transaction_ids'].apply(lambda x: len(x)))
        df = df.drop(columns=['transaction_ids'])
        return df

    def _repr_html_(self) -> Union[str, None]:
        df = self._repr_df()
        return df._repr_html_()

    def __repr__(self) -> str:
        df = self._repr_df()
        df['labels'] = df['labels'].apply(lambda x: '\n'.join(x))
        return tabulate(df, showindex=False, headers="keys", maxcolwidths=[2, 16, 16, 16, 16, 12, 12, 12, 12, 12, 20, 12])
        # return df.to_string(max_rows=10, max_cols=20, max_colwidth=30, col_space={'next_expected_payment_date': 3}, justify='left', index=False)

    def essential(self):
        return RecurringPaymentsList([rpg for rpg in self.recurring_payments_groups if rpg.is_essential])

    def non_essential(self):
        return RecurringPaymentsList([rpg for rpg in self.recurring_payments_groups if not rpg.is_essential])

    def active(self):
        return RecurringPaymentsList([rpg for rpg in self.recurring_payments_groups if rpg.is_active])

    def inactive(self):
        return RecurringPaymentsList([rpg for rpg in self.recurring_payments_groups if not rpg.is_active])

    def subscriptions(self):
        return RecurringPaymentsList([rpg for rpg in self.recurring_payments_groups if rpg.type == 'subscription'])

    def recurring_bills(self):
        return RecurringPaymentsList([rpg for rpg in self.recurring_payments_groups if rpg.type == 'bill'])

    def total_amount(self):
        return round(sum([rpg.total_amount for rpg in self.recurring_payments_groups]),2)
