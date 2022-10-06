from enum import Enum
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Set, Union


UNDETERMINED_LABEL = "possible income - please verify"


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
    total_amount: float
    iso_currency_code: str
    income_type: str
    source: Optional[str]
    first_payment_date: str
    latest_payment_date: str
    duration: str
    latest_payment_description: str
    pay_frequency: Optional[str]
    next_expected_payment_date: Optional[str]
    next_expected_payment_amount: Optional[str]
    transaction_ids: List[Union[int, str]]

    @classmethod
    def from_dict(cls, income_group: Dict[str, Any]):
        return cls(
            total_amount=income_group["total_amount"],
            iso_currency_code=income_group["iso_currency_code"],
            source=income_group["source"],
            income_type=income_group["income_type"],
            first_payment_date=income_group["first_payment_date"],
            latest_payment_date=income_group["latest_payment_date"],
            duration=income_group["duration"],
            latest_payment_description=income_group["latest_payment_description"],
            pay_frequency=income_group["pay_frequency"],
            next_expected_payment_date=income_group["next_expected_payment_date"],
            next_expected_payment_amount=income_group["next_expected_payment_amount"],
            transaction_ids=income_group["transaction_ids"],
        )


class IncomeSummary(BaseModel):
    main_income_source: str
    main_income_type: str
    total_income: float
    earned_income: float
    passive_income: float
    possible_income: float
    earned_income_sources: List[str]
    passive_income_sources: List[str]
    possible_income_sources: List[str]

    @classmethod
    def from_income_groups(cls, income_groups: List[IncomeGroup]):
        igs = income_groups
        total_amount = sum([ig.total_amount for ig in igs])
        undetermined_sources = [
            ig.source
            for ig in igs
            if ig.source is not None and ig.income_type == UNDETERMINED_LABEL
        ]
        undetermined_amount = sum(
            [ig.total_amount for ig in igs if ig.income_type == UNDETERMINED_LABEL]
        )
        passive_income_sources = [
            ig.source
            for ig in igs
            if ig.source is not None
            and ig.income_type in IncomeLabelEnum.passive_labels()
            and not ig.income_type == UNDETERMINED_LABEL
        ]
        passive_income_amount = sum(
            [
                ig.total_amount
                for ig in igs
                if ig.income_type in IncomeLabelEnum.passive_labels()
                and not ig.income_type == UNDETERMINED_LABEL
            ]
        )
        earned_income_sources = [
            ig.source
            for ig in igs
            if ig.source is not None
            and ig.income_type in IncomeLabelEnum.earnings_labels()
            and not ig.income_type == UNDETERMINED_LABEL
        ]
        earned_income_amount = [
            ig.total_amount
            for ig in igs
            if ig.income_type in IncomeLabelEnum.earnings_labels()
            and not ig.income_type == UNDETERMINED_LABEL
        ]
        income_types = [ig.income_type for ig in igs]
        amounts = [ig.total_amount for ig in igs]
        sources = [ig.source for ig in igs]
        if len(sources) > 0:
            main_income_source = max(list(zip(sources, amounts)), key=lambda z: z[1])[0]
            main_income_type = max(
                list(zip(income_types, amounts)), key=lambda z: z[1]
            )[0]
            if main_income_source is None:
                main_income_source = "N/A"
        else:
            main_income_source = "N/A"
            main_income_type = "N/A"
        return cls(
            total_income=round(total_amount, 2),
            main_income_source=main_income_source,
            main_income_type=main_income_type,
            earned_income=round(sum(earned_income_amount), 2),
            passive_income=round(passive_income_amount, 2),
            possible_income=round(undetermined_amount, 2),
            earned_income_sources=sorted(set(earned_income_sources)),
            passive_income_sources=sorted(set(passive_income_sources)),
            possible_income_sources=sorted(set(undetermined_sources)),
        )


class IncomeReport(BaseModel):
    income_groups: List[IncomeGroup]

    @classmethod
    def from_dicts(cls, income_report: List[Dict[str, Any]]):
        income_groups = sorted(
            [IncomeGroup.from_dict(d) for d in income_report],
            key=lambda x: float(x.total_amount),
            reverse=True,
        )
        return cls(income_groups=income_groups)

    def to_df(self) -> Any:
        try:
            import pandas as pd
        except ImportError:
            raise RuntimeError("pandas is not installed")
        return pd.DataFrame(self.to_json())

    def to_json(self) -> List[Dict[str, Any]]:
        return [ig.dict() for ig in self.income_groups]

    def summarize(self) -> IncomeSummary:
        return IncomeSummary.from_income_groups(self.income_groups)

    def __repr__(self) -> str:
        return str([ig.dict(exclude={"transaction_ids"}) for ig in self.income_groups])
