INCOME_HIERARCHY = {
    "earned": ["salary", "freelance", "rideshare & delivery", "donations"],
    "passive": [
        "child support",
        "social security",
        "unemployment insurance",
        "government benefits",
        "long term rent",
        "short term rent",
        "ecommerce",
        "donations",
        "interest and dividends",
        "investment",
        "retirement funds",
    ],
}
INCOME_CLASSES = list(INCOME_HIERARCHY.keys())
INCOME_LABELS = [
    label for class_labels in list(INCOME_HIERARCHY.values()) for label in class_labels
]

UNDETERMINED_LABEL = "possible income - please verify"


class IncomeReport:
    def __init__(self, income_report_dict):
        self.data = income_report_dict
        self.income_groups = sorted(
            [IncomeGroup(d) for d in self.data], key=lambda x: x.amount, reverse=True
        )

    def get_income_groups(self):
        return self.igs

    def get_main_income_type(self):
        sources = {k: 0 for k in INCOME_LABELS}

        for ig in self.income_groups:
            sources[ig.income_type] += 1

        return max(sources)

    def get_income_types(self):
        sources = []
        for ig in self.income_groups:
            sources.append(ig.income_type)

        return list(set(sources))

    def get_class_summary(self, income_class):
        if income_class not in INCOME_CLASSES:
            raise RuntimeError("Unsupported income class")

        igs = []
        for ig in self.income_groups:
            if ig.income_type in INCOME_HIERARCHY[income_class]:
                igs.append(ig)

        return IncomeClassSummary(igs)

    def get_undetermined_income(self):
        igs = []
        for ig in self.income_groups:
            if ig.income_type == UNDETERMINED_LABEL:
                igs.append(ig)
        igs = sorted(igs, key=lambda x: x.amount, reverse=True)
        return igs


class IncomeGroup:
    def __init__(self, income_group_dict):
        self.data = income_group_dict

        self.amount = self.data["amount"] if "amount" in self.data else 0
        self.date_of_first_payment = (
            self.data["start_date"] if "start_date" in self.data else None
        )
        self.date_of_last_payment = (
            self.data["end_date"] if "end_date" in self.data else None
        )
        self.income_type = (
            self.data["income_type"] if "income_type" in self.data else "unknown"
        )
        self.source = self.data["source"] if "source" in self.data else "unknown"
        self.transaction_ids = (
            self.data["transaction_ids"] if "transaction_ids" in self.data else []
        )
        self.pay_frequency = (
            self.data["periodicity"] if "periodicity" in self.data else "unknown"
        )


class IncomeClassSummary:
    def __init__(self, income_groups):
        self.income_groups = income_groups

        self.amount = sum([ig.amount for ig in self.income_groups])
        self.sources = list(set([ig.source for ig in self.income_groups]))

    def __repr__(self) -> str:
        return f"{self.__class__}(amount={self.amount},sources={self.sources})"

    def __str__(self) -> str:
        return self.__repr__()
