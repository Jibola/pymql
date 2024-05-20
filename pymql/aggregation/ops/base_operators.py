from __future__ import annotations
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any


class AggOps(str, Enum):
    addFields = "$addFields"
    project = "$project"
    match = "$match"
    group = "$group"
    search = "$search"
    vectorSearch = "$vectorSearch"
    reduce = "$reduce"
    map = "$map"
    filter = "$filter"

    def __repr__(self) -> str:
        return self.value


class AggregateOperator(ABC):
    _operator: AggOps = None

    def __init__(self):
        self._next: AggregateOperator | None = None
        self._prev: AggregateOperator | None = None
        self._parent: AggregateOperator | None = None

    @abstractmethod
    def as_pyobj(self) -> dict[str, Any]:
        raise NotImplementedError

    @property
    def next(self) -> AggregateOperator:
        return self._next

    @next.setter
    def next(self, value: AggregateOperator) -> None:
        self._next = value
        value._prev = self
    
    def eval(self) -> dict[str, Any]:
        return self.as_pyobj()
