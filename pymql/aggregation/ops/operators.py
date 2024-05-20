from __future__ import annotations
from typing import Any, Callable, TypeVar

from aggregation.ops.base_operators import AggregateOperator, AggOps
from mql.mql_value import MqlValue, MqlExpression, of

MqlModel = TypeVar("MqlModel", bound="MqlValue")


class AddFields(AggregateOperator):
    """

    Args:
        AggregateOperator (_type_): _description_

    Returns:
        _type_: _description_
    """

    _operator = AggOps.addFields

    def __init__(self, **kwargs: dict[str, MqlValue]) -> None:
        self._values: dict[str, MqlValue] = kwargs
        super().__init__()

    @staticmethod
    def from_lambda(f: Callable[[MqlModel], MqlExpression]) -> AddFields:
        return AddFields(f(MqlModel))

    def as_pyobj(self) -> dict[str, Any]:
        return {self._operator: {k: v.eval() for k, v in self._values.items()}}


class Project(AggregateOperator):
    """_summary_

    Args:
        AggregateOperator (_type_): _description_

    Returns:
        _type_: _description_
    """

    _operator = AggOps.project

    def __init__(self, **kwargs: dict[str, MqlValue]) -> None:
        self._values: dict[str, MqlValue] = kwargs
        super().__init__()

    def as_pyobj(self) -> dict[str, Any]:
        return {self._operator: {k: v.eval() for k, v in self._values.items()}}


class Match(AggregateOperator):
    """https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/

    Args:
        AggregateOperator (_type_): _description_
    """

    _operator = AggOps.match

    def __init__(self, query: MqlExpression) -> None:
        self._query = query
        super().__init__()

    def as_pyobj(self) -> dict[str, Any]:
        return {self._operator: self._query.eval()}


class Map(AggregateOperator):
    """https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/

    Args:
        AggregateOperator (_type_): _description_
    """

    _operator = AggOps.map

    def __init__(self, input: MqlValue, _in: MqlExpression) -> None:
        self._input = of(input)
        self._in = of(_in)
        super().__init__()

    def as_pyobj(self) -> dict[str, Any]:
        return {
            self._operator: {
                "input": self._input.eval(),
                "in": self._in.eval(),
            }
        }


class Reduce(Map):
    """https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/

    Args:
        AggregateOperator (_type_): _description_
    """

    _operator = AggOps.reduce

    def __init__(
        self, input: MqlValue, initialValue: MqlValue, _in: MqlExpression
    ) -> None:
        self._initialValue = of(initialValue)
        super().__init__(input, _in)

    def as_pyobj(self) -> dict[str, Any]:
        return {
            self._operator: {
                "input": self._input.eval(),
                "initialValue": self._initialValue.eval(),
                "in": self._in.eval(),
            }
        }


class Group(AggregateOperator):
    """https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/

    Args:
        AggregateOperator (_type_): _description_
    """

    _operator = AggOps.group

    def __init__(
        self, _id: str | dict[str, Any] | None, **kwargs: dict[str, MqlExpression]
    ) -> None:
        self._values = kwargs
        self._id = _id
        super().__init__()

    def as_pyobj(self) -> dict[str, Any]:
        return {
            self._operator: {
                "_id": self._id,
                **{k: v.eval() for k, v in self._values.items()},
            }
        }


class Filter(AggregateOperator):
    """https://www.mongodb.com/docs/manual/reference/operator/aggregation/filter/

    Args:
        AggregateOperator (_type_): _description_
    """

    _operator = AggOps.filter

    def __init__(self, input: MqlValue, cond: MqlExpression) -> None:
        self._input = of(input)
        self._cond = of(cond)
        super().__init__()

    def as_pyobj(self) -> dict[str, Any]:
        return {
            self._operator: {
                "input": self._input.eval(),
                "cond": self._cond.eval(),
            }
        }
