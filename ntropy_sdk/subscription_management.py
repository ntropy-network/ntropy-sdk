class SubscriptionReport:
    def __init__(self, subscription_management_dict):
        self.data = subscription_management_dict
        self.subscription_groups = sorted(
            [SubscriptionGroup(d) for d in self.data],
            key=lambda x: x.first_payment_date,
            reverse=True,
        )

    def get_subscription_groups(self):
        return self.subscription_groups


class SubscriptionGroup:
    def __init__(self, subscription_group_dict):
        self.data = subscription_group_dict

        self.periodicity = (
            self.data["periodicity"] if "periodicity" in self.data else "unknown"
        )
        self.price = self.data["price"] if "price" in self.data else 0
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
        return f"{self.__class__.__name__}(periodicity={self.periodicity},merchant={self.merchant}, price={self.price}, is_active={self.is_active})"
