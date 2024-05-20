from __future__ import annotations

from abc import ABCMeta, abstractmethod
from datetime import datetime
from decimal import Decimal
from typing import Any, cast, Callable, Generic, TypeVar

T = TypeVar("T", bound="MqlValue")
K = TypeVar("K", bound="MqlValue")
V = TypeVar("V", bound="MqlValue")

__DNE = object()


class MqlValue(metaclass=ABCMeta):
    expression: Any
    base: Any

    def _expr(self, other: MqlValue, cmp: str) -> MqlExpression:
        if not isinstance(other, MqlValue):
            other = of(other)
        return MqlExpression({cmp: [self.expression, other.expression]})

    # @abstractmethod
    def equals(self, a: object) -> bool:
        self.expression == a.expression

    # MqlBoolean
    # @abstractmethod
    def eq(self, other: MqlValue) -> MqlBoolean:
        return self._expr(other, "$eq")

    # @abstractmethod
    def ne(self, other: MqlValue) -> MqlBoolean:
        return self._expr(other, "$ne")

    # @abstractmethod
    def gt(self, other: MqlValue) -> MqlBoolean:
        return self._expr(other, "$gt")

    # @abstractmethod
    def gte(self, other: MqlValue) -> MqlBoolean:
        return self._expr(other, "$gte")

    # @abstractmethod
    def lt(self, other: MqlValue) -> MqlBoolean:
        return self._expr(other, "$lt")

    # @abstractmethod
    def lte(self, other: MqlValue) -> MqlBoolean:
        return self._expr(other, "$lte")
    
    def mod(self, other: MqlValue) -> MqlBoolean:
        return self._expr(other, "$mod")
    
    def add(self, other: MqlValue) -> MqlBoolean:
        return self._expr(other, "$add")
    
    def multiply(self, other: MqlValue) -> MqlBoolean:
        return self._expr(other, "$multiply")

    # @abstractmethod
    def is_boolean_or(self, other: MqlBoolean) -> MqlBoolean:
        raise NotImplementedError

    # @abstractmethod
    def is_number_or(self, other: MqlNumber) -> MqlNumber:
        raise NotImplementedError

    # @abstractmethod
    def is_integer_or(self, other: MqlInteger) -> MqlInteger:
        raise NotImplementedError

    # @abstractmethod
    def is_string_or(self, other: MqlString) -> MqlString:
        raise NotImplementedError

    # @abstractmethod
    def is_date_or(self, other: MqlDate) -> MqlDate:
        raise NotImplementedError

    # @abstractmethod
    def is_array_or(self, other: MqlArray[T]) -> MqlArray[T]:
        raise NotImplementedError

    # @abstractmethod
    def is_document_or(self, other: MqlDocument) -> MqlDocument:
        raise NotImplementedError

    # @abstractmethod
    def is_map_or(self, other: MqlMap[K, V]) -> MqlMap[K, V]:
        raise NotImplementedError

    # @abstractmethod
    def as_string(self) -> MqlString:
        raise NotImplementedError

    # T => BaseModel=MqlValue
    # @abstractmethod
    def pass_to(self, f: Callable[[MqlValue], T]) -> T:
        raise NotImplementedError

    # Needs typing
    # @abstractmethod
    def switch_on(self, f):
        raise NotImplementedError

    # @abstractmethod
    def eval(self) -> Any:
        if getattr(self, "_eval", None) is not None:
            return self._eval()
        else:
            return self.expression


class MqlBoolean(MqlValue):
    def __init__(self, expression: bool):
        self.expression = bool(expression)


class MqlNumber(MqlValue):
    def __init__(self, expression: float | int):
        self.expression = float(expression) if expression % 1 else int(expression)


class MqlInteger(MqlValue):
    def __init__(self, expression: int):
        self.expression = int(expression)


class MqlString(MqlValue):
    def __init__(self, expression: str):
        self.expression = str(expression)


class MqlDate(MqlValue):
    def __init__(self, expression: str | datetime):
        self.expression = datetime(expression)


class MqlArray(MqlValue, Generic[T]):
    def __init__(self, expression: list[T]):
        self.expression = expression

    def eval(self) -> list[T.base]:
        return [x.eval() for x in self.expression]


class MqlDocument(MqlValue):
    def __init__(self, expression: Any):
        self.expression = expression


class MqlMap(MqlValue, Generic[K, V]):
    def __init__(self, expression: dict[K, V]):
        self.expression = expression

    def eval(self) -> Any:
        return {k: v.eval() for k, v in self.expression.items()}


class MqlExpression(MqlValue):
    def __init__(self, expression: Any):
        self.expression = expression

    def eval(self) -> Any:
        return self.expression


def of(value: Any) -> MqlValue:
    match value:
        case MqlValue():
            return value
        case bool() | MqlBoolean():
            return MqlBoolean(value)
        case int() | MqlInteger():
            return MqlInteger(value)
        case float() | Decimal() | MqlNumber():
            return MqlNumber(value)
        case str() | MqlString():
            return MqlNumber(value) if value.isnumeric() else MqlString(value)
        case datetime() | MqlDate():
            return MqlDate(value)
        case list() | MqlArray():
            mql_type = MqlValue
            if value:
                if isinstance(value[0], MqlValue):
                    mql_list = cast(list[MqlValue], value)
                else:
                    mql_list = [of(x) for x in value]
                    mql_type = type(mql_list[0])
            return MqlArray[mql_type](mql_list)
        case dict() | MqlMap():
            return MqlMap[str, MqlValue]({k: of(v) for k, v in value.items()})
        # Unimplemented
        # case document() | MqlDocument():
        #     return MqlDocument(value)
        case _:
            return value
            # raise "Unresolvable Expression"
