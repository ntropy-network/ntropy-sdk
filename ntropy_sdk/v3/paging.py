from dataclasses import dataclass
from typing import (
    Any,
    Generic,
    Iterator,
    List,
    Optional,
    Protocol,
    TYPE_CHECKING,
    TypeVar,
)

from pydantic import PrivateAttr

from ntropy_sdk.utils import PYDANTIC_V2

if TYPE_CHECKING:
    from . import ExtraKwargs
    from pydantic import BaseModel as GenericModel
    from typing_extensions import Unpack

elif PYDANTIC_V2:
    from pydantic import BaseModel as GenericModel
else:
    from pydantic.generics import GenericModel

T = TypeVar("T")


class ListableResource(Protocol[T]):
    def list(
        self,
        *,
        cursor: str,
        limit: Optional[int],
        **extra_kwargs: "Unpack[ExtraKwargs]",
    ) -> "PagedResponse[T]": ...


class PagedResponse(GenericModel, Generic[T]):
    next_cursor: Optional[str]
    data: List[T]
    request_id: Optional[str] = None
    _resource: Optional[ListableResource[T]] = PrivateAttr(None)
    if TYPE_CHECKING:
        _extra_kwargs: Optional["ExtraKwargs"] = PrivateAttr(None)
        pass
    else:
        _extra_kwargs: Any = PrivateAttr()  # pydantic v1 complains about ExtraKwargs

        def __init__(
            self,
            *,
            _resource: Optional[ListableResource[T]] = None,
            _extra_kwargs: Optional["ExtraKwargs"] = None,
            **data,
        ):
            super().__init__(**data)
            self._resource = _resource
            self._extra_kwargs = _extra_kwargs

    def auto_paginate(
        self,
        *,
        page_size: Optional[int] = None,
    ) -> "AutoPaginate[T]":
        if self._resource is None:
            raise ValueError("self._resource is None")
        return AutoPaginate(
            _first_page=self,
            _resource=self._resource,
            _page_size=page_size,
        )


@dataclass
class AutoPaginate(Generic[T]):
    _first_page: PagedResponse
    _resource: ListableResource[T]
    _page_size: Optional[int]

    def __iter__(self) -> "AutoPaginateIterator[T]":
        return AutoPaginateIterator(
            current_iter=iter(self._first_page.data),
            page_size=self._page_size,
            next_cursor=self._first_page.next_cursor,
            _resource=self._resource,
            _extra_kwargs=self._first_page._extra_kwargs or {},
        )


@dataclass
class AutoPaginateIterator(Generic[T]):
    current_iter: Iterator[T]
    next_cursor: Optional[str]
    page_size: Optional[int]
    _resource: ListableResource[T]
    _extra_kwargs: "ExtraKwargs"

    def __next__(self) -> T:
        try:
            return next(self.current_iter)
        except StopIteration:
            if self.next_cursor is None:
                raise StopIteration
            next_page = self._resource.list(
                cursor=self.next_cursor,
                limit=self.page_size,
                **self._extra_kwargs,
            )
            self.current_iter = iter(next_page.data)
            self.next_cursor = next_page.next_cursor
            return next(self.current_iter)
