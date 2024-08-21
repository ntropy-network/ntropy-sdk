from typing import List, Optional
from pydantic import BaseModel
from datetime import date


from ntropy_sdk.utils import AccountHolderType


class Address(BaseModel):
    street: Optional[str]
    postcode: Optional[str]
    city: Optional[str]
    state: Optional[str]
    country: Optional[str]

    class Config:
        extra = "allow"


class AccountHolder(BaseModel):
    type: Optional[AccountHolderType]
    name: Optional[str]
    address: Optional[Address]

    class Config:
        use_enum_values = True
        extra = "allow"


class Account(BaseModel):
    type: Optional[str]
    number: Optional[str]
    opening_balance: Optional[float]
    closing_balance: Optional[float]
    iso_currency_code: Optional[str]

    class Config:
        extra = "allow"


class StatementInfo(BaseModel):
    institution: Optional[str]
    start_date: Optional[date]
    end_date: Optional[date]
    account_holder: Optional[AccountHolder]
    accounts: Optional[List[Account]]

    class Config:
        extra = "allow"
