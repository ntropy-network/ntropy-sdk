INCOME_HIERARCHY = {
    "wages": [
        "wages - salary",
        "wages - child support",
        "wages - freelance",
        "wages - rideshare and delivery",
    ],
    "government": [
        "government - social security",
        "government - unemployment insurance",
        "government - other benefits",
    ],
    "assets": ["assets - property", "assets - airbnb"],
    "enterprise": [
        "enterprise - ecommerce",
        "enterprise - donations",
        "enterprise - art",
        "enterprise - other",
    ],
    "financial": [
        "financial - interest and dividends"
        "financial - investment"
        "financial - retirement funds"
    ],
}
INCOME_CLASSES = list(INCOME_HIERARCHY.keys())

UNDETERMINED_LABELS = [
    "incorrect entry type or refunds and corrective transactions",
    "not enough information",
]


class IncomeReport:
    def __init__(self, income_report_dict):
        self.data = income_report_dict
        self.income_groups = sorted(
            [IncomeGroup(d) for d in self.data], key=lambda x: x.amount, reverse=True
        )

    def income_groups(self):
        return self.igs

    def get_main_income_source(self):
        sources = {k: 0 for k in INCOME_CLASSES}

        for ig in self.income_groups:
            for s in sources:
                if s in ig.income_type:
                    sources[s] += ig.amount

        return max(sources)

    def get_income_sources(self):
        sources = []
        for ig in self.income_groups:
            for s in INCOME_CLASSES:
                if s in ig.income_type:
                    sources.append(s)

        return list(set(sources))

    def get_class_summary(self, income_class):
        if income_class not in INCOME_CLASSES:
            raise RuntimeError("Unsupported income_class")

        igs = []
        for ig in self.income_groups:
            for s in INCOME_CLASSES:
                if s in ig.income_type:
                    igs.append(ig)

        return IncomeClassSummary(igs)

    def get_undetermined_income(self):
        igs = []
        for ig in self.income_groups:
            for s in UNDETERMINED_LABELS:
                if s in ig.income_type:
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
