from datetime import date as dt_date, date, datetime
import enum
from typing import List, Optional, TYPE_CHECKING
import uuid

from pydantic import BaseModel, Field, NonNegativeFloat

from ntropy_sdk.utils import EntryType, PYDANTIC_V2, pydantic_json
from ntropy_sdk.paging import PagedResponse
from ntropy_sdk.async_.paging import PagedResponse as PagedResponseAsync

PYDANTIC_PATTERN = "pattern" if PYDANTIC_V2 else "regex"
MAX_SYNC_BATCH = 1000
MAX_ASYNC_BATCH = 24960


if TYPE_CHECKING:
    from ntropy_sdk import ExtraKwargs, ExtraKwargsAsync, SDK
    from ntropy_sdk.async_.sdk import AsyncSDK
    from typing_extensions import Unpack


class LocationInput(BaseModel):
    """
    Location of where the transaction has taken place. This can greatly improve entity identification, especially
    under ambiguity.
    """

    raw_address: Optional[str] = Field(
        None, description="An unstructured string containing the address"
    )
    country: Optional[str] = Field(
        None,
        description="The country where the transaction was made in ISO 3166-2 format",
        **{PYDANTIC_PATTERN: r"[A-Z]{2}(-[A-Z0-9]{1,3})?"},  # type: ignore
    )


class _TransactionBase(BaseModel):
    id: str = Field(description="A unique identifier of the transaction", min_length=1)

    description: str = Field(
        description="The description string of the transaction",
        min_length=0,
        max_length=1024,
    )

    date: dt_date = Field(
        description="The date that the transaction was posted. Uses ISO 8601 format (YYYY-MM-DD)"
    )

    amount: NonNegativeFloat = Field(
        description="The amount of the transaction. Must be a positive value."
    )

    entry_type: EntryType = Field(
        description="The direction of the flow of the money from the perspective of the "
        "account holder. `outgoing` to represent money leaving the account, such as purchases or fees, "
        "while `incoming` represents money entering the account, such as income or refunds.",
    )
    currency: str = Field(
        description="The currency of the transaction in ISO 4217 format"
    )


class TransactionInput(_TransactionBase):
    account_holder_id: Optional[str] = Field(
        default=None,
        description="The id of the account holder. Unsetting it will disable categorization.",
    )

    location: Optional[LocationInput] = None


# Enriched


class Entity(BaseModel):
    """
    The `Entity` object represents an entity that can be involved in a transaction, such as an organization or a person.
    Two different entities (with different ID's) can have different websites or different names.
    """

    id: Optional[str] = Field(
        default=None, description="The unique UUID identifier of the entity"
    )
    name: Optional[str] = Field(default=None, description="The name of the entity")
    website: Optional[str] = Field(
        default=None, description="The website URL of the entity"
    )
    logo: Optional[str] = Field(default=None, description="Logo's URL")
    mccs: List[int] = Field(
        default=[],
        description="A list of [Merchant Category Codes](https://en.wikipedia.org/wiki/Merchant_category_code)",
    )


class LocationStructured(BaseModel):
    street: Optional[str] = Field(
        None, description="The street name and number of the location"
    )
    city: Optional[str] = Field(
        None, description="The city where the location is situated"
    )
    state: Optional[str] = Field(
        None, description="The state or region of the location"
    )
    postcode: Optional[str] = Field(
        None, description="The postal code or ZIP code of the location"
    )
    country_code: Optional[str] = Field(
        None, description="The country code of the location in ISO 3166-2 format"
    )
    country: Optional[str] = Field(None, description="The full name of the country")
    latitude: Optional[float] = Field(
        None, description="The latitude coordinate of the location"
    )
    longitude: Optional[float] = Field(
        None, description="The longitude coordinate of the location"
    )
    google_maps_url: Optional[str] = Field(
        None, description="A URL link to view the location on Google Maps"
    )
    apple_maps_url: Optional[str] = Field(
        None, description="A URL link to view the location on Apple Maps"
    )
    store_number: Optional[str] = Field(
        None,
        description="A unique identifier for a specific store or branch, if applicable",
    )
    house_number: Optional[str] = Field(
        None, description="The house number if, applicable"
    )


class Location(BaseModel):
    raw_address: Optional[str] = Field(
        default=None, description="An unstructured string containing the address"
    )

    structured: Optional[LocationStructured] = Field(
        default=None, description="When raw is set, a structured representation of it."
    )


class CounterpartyType(str, enum.Enum):
    PERSON = "person"
    ORGANIZATION = "organization"


class Counterparty(Entity):
    type: CounterpartyType


class Intermediary(Entity):
    ...


class Entities(BaseModel):
    """
    Entities found by identity identification
    """

    counterparty: Optional[Counterparty] = None
    intermediaries: List[Intermediary] = []


class CategoryConfidence(str, enum.Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    UNKNOWN = "unknown"


class AccountingCategory(str, enum.Enum):
    OPERATIONAL_EXPENSES = "operational expenses"
    COST_OF_GOODS_SOLD = "cost of goods sold"
    REVENUE = "revenue"
    FINANCING = "financing"
    TAXES = "taxes"


class Categories(BaseModel):
    general: Optional[str] = Field(
        None,
        description="The category of the transaction. You can view the set of valid labels in your hierarchy.",
    )
    accounting: Optional[AccountingCategory] = Field(
        default=None,
        description="The corresponding accounting category. Only available for business transactions.",
    )


class RecurrenceType(str, enum.Enum):
    recurring = "recurring"
    subscription = "subscription"
    one_off = "one off"


class RecurrencePeriodicity(str, enum.Enum):
    daily = "daily"
    weekly = "weekly"
    bi_weekly = "bi_weekly"
    monthly = "monthly"
    bi_monthly = "bi_monthly"
    quarterly = "quarterly"
    semi_yearly = "semi_yearly"
    yearly = "yearly"
    other = "other"


class RecurrenceGroup(BaseModel):
    id: str = Field(description="A unique UUID identifier for the group")
    start_date: date = Field(
        description="The date of the oldest transaction in the group"
    )
    end_date: date = Field(
        description="The date of the most recent transaction in the group"
    )
    total_amount: float = Field(
        description="The sum of all transaction amounts in this group"
    )
    average_amount: float = Field(
        description="The average amount per transaction in this group"
    )
    periodicity_in_days: float = Field(
        description="The estimated number of days between transactions in this group",
    )
    periodicity: RecurrencePeriodicity = Field(
        description="A human-readable description of the transaction frequency"
    )
    counterparty: Counterparty = Field(description="Counterparty of the transactions")
    categories: Categories = Field(
        description="Categories of the transactions in the recurrence group"
    )
    transaction_ids: List[str] = Field(description="Transactions in the group")


class RecurrenceGroups(BaseModel):
    groups: List[RecurrenceGroup]
    request_id: Optional[str]


class Recurrence(BaseModel):
    """
    The `Recurrence` object represents the recurrence pattern of a transaction. It provides information about
    whether a transaction is a one-time event or a part of a recurring series.
    """

    type: RecurrenceType = Field(
        description="Whether the transaction is a one-time transfer `one-off`, regularly with varying pricing "
        "`recurring` or with fixed pricing `subscription`",
    )
    group_id: str = Field(description="ID of recurrence group")


class TransactionErrorCode(str, enum.Enum):
    ACCOUNT_HOLDER_NOT_FOUND = "account_holder_not_found"
    INTERNAL_ERROR = "internal_error"


class TransactionError(BaseModel):
    code: TransactionErrorCode
    message: str


class _EnrichedTransactionBase(BaseModel):
    entities: Optional[Entities] = None
    categories: Optional[Categories] = None
    location: Optional[Location] = None
    error: Optional[TransactionError] = None

    created_at: datetime = Field(
        ...,
        description="Date of creation of the transaction",
    )


class EnrichedTransaction(_EnrichedTransactionBase):
    id: str = Field(
        description="A unique identifier for the transaction. If two transactions are submitted with the same `id` "
        "the most recent one will replace the previous one.",
        min_length=1,
    )


class EnrichedTransactionResponse(EnrichedTransaction):
    request_id: Optional[str] = None


class Transaction(_EnrichedTransactionBase, _TransactionBase):
    account_holder_id: Optional[str] = Field(
        None,
        description="The unique ID of the account holder of the transaction",
        min_length=1,
    )

    request_id: Optional[str] = None


class TransactionsResource:
    def __init__(self, sdk: "SDK"):
        self._sdk = sdk

    def list(
        self,
        *,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        account_holder_id: Optional[str] = None,
        dataset_id: Optional[int] = None,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> PagedResponse[Transaction]:
        """List all transactions"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url="/v3/transactions",
            params={
                "created_before": created_before,
                "created_after": created_after,
                "cursor": cursor,
                "limit": limit,
                "account_holder_id": account_holder_id,
                "dataset_id": dataset_id,
            },
            payload=None,
            **extra_kwargs,
        )
        extra_kwargs["created_after"] = created_after
        extra_kwargs["account_holder_id"] = account_holder_id
        extra_kwargs["dataset_id"] = dataset_id
        page = PagedResponse[Transaction](
            **resp.json(),
            request_id=resp.headers.get("x-request-id", request_id),
            _resource=self,
            _request_kwargs=extra_kwargs,
        )
        for t in page.data:
            t.request_id = request_id
        return page

    def get(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]") -> Transaction:
        """Retrieve a transaction"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/transactions/{id}",
            payload=None,
            **extra_kwargs,
        )
        return Transaction(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def create(
        self,
        id: str,
        description: str,
        date: str,
        amount: float,
        entry_type: str,
        currency: str,
        account_holder_id: Optional[str] = None,
        location: Optional[dict] = None,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/transactions",
            payload_json_str=pydantic_json(
                TransactionInput(
                    id=id,
                    description=description,
                    date=date,
                    amount=amount,
                    entry_type=entry_type,
                    currency=currency,
                    account_holder_id=account_holder_id,
                    location=location,
                )
            ),
            **extra_kwargs,
        )
        return EnrichedTransactionResponse(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def assign(
        self,
        transaction_id: str,
        account_holder_id: str,
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> Transaction:
        """Assign a transaction to an account holder"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/transactions/{transaction_id}/assign",
            payload={
                "account_holder_id": account_holder_id,
            },
            **extra_kwargs,
        )
        return Transaction(
            **resp.json(), request_id=resp.headers.get("x-request-id", request_id)
        )

    def delete(self, id: str, **extra_kwargs: "Unpack[ExtraKwargs]"):
        """Delete a transaction"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        self._sdk.retry_ratelimited_request(
            method="DELETE",
            url=f"/v3/transactions/{id}",
            payload=None,
            **extra_kwargs,
        )


class TransactionsResourceAsync:
    def __init__(self, sdk: "AsyncSDK"):
        self._sdk = sdk

    async def list(
        self,
        *,
        created_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        cursor: Optional[str] = None,
        limit: Optional[int] = None,
        account_holder_id: Optional[str] = None,
        dataset_id: Optional[int] = None,
        **extra_kwargs: "Unpack[ExtraKwargsAsync]",
    ) -> PagedResponseAsync[Transaction]:
        """List all transactions"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = await self._sdk.retry_ratelimited_request(
            method="GET",
            url="/v3/transactions",
            params={
                "created_before": created_before,
                "created_after": created_after,
                "cursor": cursor,
                "limit": limit,
                "account_holder_id": account_holder_id,
                "dataset_id": dataset_id,
            },
            payload=None,
            **extra_kwargs,
        )
        async with resp:
            extra_kwargs["created_after"] = created_after
            extra_kwargs["account_holder_id"] = account_holder_id
            extra_kwargs["dataset_id"] = dataset_id
            page = PagedResponseAsync[Transaction](
                **await resp.json(),
                request_id=resp.headers.get("x-request-id", request_id),
                _resource=self,
                _request_kwargs=extra_kwargs,
            )
        for t in page.data:
            t.request_id = request_id
        return page

    async def get(
        self, id: str, **extra_kwargs: "Unpack[ExtraKwargsAsync]"
    ) -> Transaction:
        """Retrieve a transaction"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = await self._sdk.retry_ratelimited_request(
            method="GET",
            url=f"/v3/transactions/{id}",
            payload=None,
            **extra_kwargs,
        )
        async with resp:
            return Transaction(
                **await resp.json(),
                request_id=resp.headers.get("x-request-id", request_id),
            )

    async def create(
        self,
        id: str,
        description: str,
        date: str,
        amount: float,
        entry_type: str,
        currency: str,
        account_holder_id: Optional[str] = None,
        location: Optional[dict] = None,
        **extra_kwargs: "Unpack[ExtraKwargsAsync]",
    ):
        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = await self._sdk.retry_ratelimited_request(
            method="POST",
            url="/v3/transactions",
            payload_json_str=pydantic_json(
                TransactionInput(
                    id=id,
                    description=description,
                    date=date,
                    amount=amount,
                    entry_type=entry_type,
                    currency=currency,
                    account_holder_id=account_holder_id,
                    location=location,
                )
            ),
            **extra_kwargs,
        )
        async with resp:
            return EnrichedTransactionResponse(
                **await resp.json(),
                request_id=resp.headers.get("x-request-id", request_id),
            )

    async def assign(
        self,
        transaction_id: str,
        account_holder_id: str,
        **extra_kwargs: "Unpack[ExtraKwargsAsync]",
    ) -> Transaction:
        """Assign a transaction to an account holder"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        resp = await self._sdk.retry_ratelimited_request(
            method="POST",
            url=f"/v3/transactions/{transaction_id}/assign",
            payload={
                "account_holder_id": account_holder_id,
            },
            **extra_kwargs,
        )
        async with resp:
            return Transaction(
                **await resp.json(),
                request_id=resp.headers.get("x-request-id", request_id),
            )

    async def delete(self, id: str, **extra_kwargs: "Unpack[ExtraKwargsAsync]"):
        """Delete a transaction"""

        request_id = extra_kwargs.get("request_id")
        if request_id is None:
            request_id = uuid.uuid4().hex
            extra_kwargs["request_id"] = request_id
        await self._sdk.retry_ratelimited_request(
            method="DELETE",
            url=f"/v3/transactions/{id}",
            payload=None,
            **extra_kwargs,
        )
