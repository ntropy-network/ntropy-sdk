class RecurringPaymentsReport:
    def __init__(self, recurring_payments_dict):
        self.data = recurring_payments_dict
        self.recurring_payments_groups = sorted(
            [RecurringPaymentsGroup(d) for d in self.data],
            key=lambda x: x.first_payment_date,
            reverse=True,
        )

    def get_recurring_payments_groups(self):
        return self.recurring_payments_groups

    def get_essential_recurring_payments_groups(self):
        return [rpg for rpg in self.recurring_payments_groups if rpg.is_essential]

    def get_non_essential_recurring_payments_groups(self):
        return [rpg for rpg in self.recurring_payments_groups if not rpg.is_essential]

    def get_active_recurring_payments_groups(self):
        return [rpg for rpg in self.recurring_payments_groups if rpg.is_active]

    def get_subscriptions(self):
        return [rpg for rpg in self.recurring_payments_groups if rpg.type == 'subscription']

    def get_active_subscriptions(self):
        return [rpg for rpg in self.recurring_payments_groups if rpg.type == 'subscription' and rpg.is_active]

    def get_recurring_bills(self):
        return [rpg for rpg in self.recurring_payments_groups if rpg.type == 'bill']

    def get_active_recurring_bills(self):
        return [rpg for rpg in self.recurring_payments_groups if rpg.type == 'bill' and rpg.is_active]


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

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(periodicity={self.periodicity},merchant={self.merchant}, amount={self.amount}, is_active={self.is_active})"
