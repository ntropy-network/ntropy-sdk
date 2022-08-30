import pandas as pd
from typing import Any, Dict, List, Optional, Type, TypeVar, Union
from pydantic import BaseModel


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


class IncomeGroup(BaseModel):
    amount: float
    date_of_first_payment: Optional[str]
    date_of_last_payment: Optional[str]
    income_type: str
    source: str
    transaction_ids: List[Union[int, str]]
    pay_frequency: str

    @classmethod
    def from_dict(cls, income_group: Dict[str, Any]):
        return cls(
            amount=income_group["amount"] if "amount" in income_group else 0,
            date_of_first_payment=(
                income_group["first_date"] if "first_date" in income_group else None
            ),
            date_of_last_payment=(
                income_group["last_date"] if "last_date" in income_group else None
            ),
            income_type=(
                income_group["income_type"]
                if "income_type" in income_group
                else "unknown"
            ),
            source=income_group["source"] if "source" in income_group else "unknown",
            transaction_ids=(
                income_group["transaction_ids"]
                if "transaction_ids" in income_group
                else []
            ),
            pay_frequency=(
                income_group["periodicity"]
                if "periodicity" in income_group
                else "unknown"
            ),
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


class IncomeReport(BaseModel):
    income_groups: List[IncomeGroup]

    @classmethod
    def from_dics(cls, income_report: List[Dict[str, Any]]):
        income_groups = sorted(
            [IncomeGroup.from_dict(d) for d in income_report],
            key=lambda x: float(x.amount),
            reverse=True,
        )
        return cls(income_groups=income_groups)

    def __repr__(self) -> str:
        with pd.option_context("expand_frame_repr", False):
            return str(
                pd.DataFrame(
                    [ig.dict(exclude={"transaction_ids"}) for ig in self.income_groups]
                )
            )

    def get_income_groups(self):
        return self.income_groups

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
