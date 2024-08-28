import math
from datetime import datetime, date
from typing import Union
from enum import Enum


class AccountHolderType(Enum):
    consumer = "consumer"
    business = "business"
    unknown = "unknown"


class EntryType(Enum):
    incoming = "incoming"
    outgoing = "outgoing"
    credit = "credit"
    debit = "debit"


class TransactionType(Enum):
    business = "business"
    consumer = "consumer"
    unknown = "unknown"


class RecurrenceType(str, Enum):
    recurring = "recurring"
    subscription = "subscription"
    one_off = "one off"
    possible_recurring = "possible recurring"


def assert_type(value, name, expected_type):
    if not isinstance(value, expected_type):
        raise TypeError(f"{name} should be of type {expected_type}")

    if expected_type == float or (
        isinstance(expected_type, tuple) and float in expected_type
    ):
        if math.isnan(value):
            raise ValueError(f"{name} value cannot be NaN")

    return True


def validate_date(value: Union[str, date, datetime]) -> Union[date, datetime]:
    if isinstance(value, str):
        datetime.strptime(value, "%Y-%m-%d")
    elif not isinstance(value, (date, datetime)):
        raise ValueError(f"Received incorrect type: {type(value)} for date field.")

    return value


def dict_to_str(dict):
    return ", ".join(f"{k}={v}" for k, v in dict.items())
