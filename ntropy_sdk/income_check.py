from enum import Enum
import pandas as pd
from typing import Any, Dict, List, Optional, Set, Union
from pydantic import BaseModel


UNDETERMINED_LABEL = "not enough information"


class IncomeLabel(BaseModel):
    label: str
    is_passive: bool


class IncomeLabelEnum(Enum):
    undetermined_label = IncomeLabel(label=UNDETERMINED_LABEL, is_passive=False)
    child_support = IncomeLabel(label="child support", is_passive=True)
    donations = IncomeLabel(label="donations", is_passive=True)
    ecommerce = IncomeLabel(label="ecommerce", is_passive=True)
    freelance = IncomeLabel(label="freelance", is_passive=False)
    government_benefits = IncomeLabel(label="government benefits", is_passive=True)
    interest_and_dividends = IncomeLabel(
        label="interest and dividends", is_passive=True
    )
    investment = IncomeLabel(label="investment", is_passive=True)
    long_term_rent = IncomeLabel(label="long term rent", is_passive=True)
    retirement_funds = IncomeLabel(label="retirement funds", is_passive=True)
    rideshare_and_delivery = IncomeLabel(
        label="rideshare and delivery", is_passive=False
    )
    salary = IncomeLabel(label="salary", is_passive=False)
    short_term_rent = IncomeLabel(label="short term rent", is_passive=True)
    social_security = IncomeLabel(label="social security", is_passive=True)
    unemployment_insurance = IncomeLabel(
        label="unemployment insurance", is_passive=True
    )

    # @classmethod
    # def passive_labels(cls) -> Set[str]:
    #     res = []
    #     for label in cls:
    #         if label.value.is_passive:
    #             res.append(label.value.label)
    #     return res

    @classmethod
    def passive_labels(cls) -> Set[str]:
        if not hasattr(cls, "_passive_labels"):
            setattr(
                cls,
                "_passive_labels",
                set([k.value.label for k in cls if k.value.is_passive]),
            )
        return getattr(cls, "_passive_labels")

    @classmethod
    def earnings_labels(cls) -> Set[str]:
        if not hasattr(cls, "_earnings_labels"):
            setattr(
                cls,
                "_earnings_labels",
                set([k.value.label for k in cls if not k.value.is_passive]),
            )
        return getattr(cls, "_earnings_labels")


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


class IncomeSummary(BaseModel):
    total_amount: float
    undetermined_amount: float
    passive_income_sources: List[str]
    passive_income_amount: float
    earned_income_sources: List[str]
    earned_income_amount: float

    @classmethod
    def from_igs(cls, igs: List[IncomeGroup]):
        return cls(
            total_amount=sum([ig.amount for ig in igs]),
            undetermined_sources=list(
                set([ig.source for ig in igs if ig.income_type == UNDETERMINED_LABEL])
            ),
            undetermined_amount=sum(
                [ig.amount for ig in igs if ig.income_type == UNDETERMINED_LABEL]
            ),
            passive_income_sources=list(
                set(
                    [
                        ig.source
                        for ig in igs
                        if ig.income_type in IncomeLabelEnum.passive_labels()
                    ]
                )
            ),
            passive_income_amount=sum(
                [
                    ig.amount
                    for ig in igs
                    if ig.income_type in IncomeLabelEnum.passive_labels()
                ]
            ),
            earned_income_sources=list(
                set(
                    [
                        ig.source
                        for ig in igs
                        if ig.income_type in IncomeLabelEnum.earnings_labels()
                    ]
                )
            ),
            earned_income_amount=sum(
                [
                    ig.amount
                    for ig in igs
                    if ig.income_type in IncomeLabelEnum.earnings_labels()
                ]
            ),
        )


class IncomeReport(BaseModel):
    income_groups: List[IncomeGroup]

    @classmethod
    def from_dicts(cls, income_report: List[Dict[str, Any]]):
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

    def get_report(self):
        sources = []
        for ig in self.income_groups:
            sources.append(ig.income_type)

        return list(set(sources))

    def summarize(self):
        return IncomeSummary.from_igs(self.income_groups)
