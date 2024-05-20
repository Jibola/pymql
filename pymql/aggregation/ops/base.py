from __future__ import annotations

from typing import Any, Callable, Generic, Self, TypeVar

from mql.mql_value import MqlValue, MqlExpression
from aggregation.ops.operators import AddFields, Project, Match, Group
from aggregation.ops.base_operators import AggregateOperator


MqlModel = TypeVar("MqlModel", bound="MqlValue")


class Aggregate:
    def __init__(self):
        self._pipeline_head: AggregateOperator | None = None
        self._pipeline_tail: AggregateOperator | None = None
        self._config: dict = {}

    def as_pyobj(self) -> list[dict[str, Any]]:
        op = self._pipeline_head
        expression = []
        while op:
            expression.append(op.as_pyobj())
            op = op.next
        return expression

    def _extend(self, op: AggregateOperator) -> Self:
        if not self._pipeline_head:
            self._pipeline_head = op
            self._pipeline_tail = op
        else:
            self._pipeline_tail.next = op
            self._pipeline_tail = self._pipeline_tail.next
        return self

    def add_fields(self, **kwargs: dict[str, MqlValue]) -> Self:
        return self._extend(AddFields(**kwargs))

    def project(self, **kwargs: dict[str, MqlValue]) -> Self:
        return self._extend(Project(**kwargs))

    def match(self, query: MqlExpression) -> Self:
        return self._extend(Match(query))

    def group(
        self, _id: str | dict[str, Any] | None, **kwargs: dict[str, MqlExpression]
    ) -> Self:
        return self._extend(Group(_id=_id, **kwargs))


class TypedAggregate(Aggregate, Generic[MqlModel]):
    _model: MqlModel

    def __init__(self):
        super().__init__()

    def add_field(self, f: Callable[[MqlModel], MqlExpression]) -> Self:
        return self._extend(f(MqlModel))
