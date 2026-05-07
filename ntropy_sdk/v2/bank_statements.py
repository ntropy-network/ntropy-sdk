from typing import List, Optional
from pydantic import BaseModel
from datetime import date


from ntropy_sdk.utils import AccountHolderType


class Address(BaseModel):
    street: Optional[str] = None
    postcode: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None

    class Config:
        extra = "allow"


class AccountHolder(BaseModel):
    type: Optional[AccountHolderType] = None
    name: Optional[str] = None
    address: Optional[Address] = None

    class Config:
        use_enum_values = True
        extra = "allow"


class Account(BaseModel):
    type: Optional[str] = None
    number: Optional[str] = None
    opening_balance: Optional[float] = None
    closing_balance: Optional[float] = None
    iso_currency_code: Optional[str] = None

    class Config:
        extra = "allow"


class StatementInfo(BaseModel):
    institution: Optional[str] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    account_holder: Optional[AccountHolder] = None
    accounts: Optional[List[Account]] = None
    request_id: str

    class Config:
        extra = "allow"
