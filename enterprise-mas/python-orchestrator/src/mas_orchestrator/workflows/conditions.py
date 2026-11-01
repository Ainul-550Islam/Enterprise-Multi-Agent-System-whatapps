"""Conditional branching.

Workflow branches are chosen by evaluating an expression against state. The
design constraint is that the expression language must not become a code
execution surface: an agent-authored condition that can call arbitrary functions
is a sandbox escape with extra steps.

So conditions are **data, not code**. A condition is a tree of comparisons over
state variables, evaluated by :func:`evaluate`. There is no string that gets
parsed into code, no attribute access, and no traversal into arbitrary objects —
a path is a dotted name into the variables mapping and nothing more.

The operators are total: a missing variable compares as ``None`` rather than
raising, because a condition in a workflow is a routing decision and a routing
decision that throws is a workflow that cannot proceed.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from mas_orchestrator.contracts.errors import InvalidRequestError


class Operator(StrEnum):
    """A comparison."""

    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    CONTAINS = "contains"
    TRUTHY = "truthy"
    FALSY = "falsy"
    EXISTS = "exists"


class Combinator(StrEnum):
    """How several conditions combine."""

    AND = "and"
    OR = "or"
    NOT = "not"


@dataclass(frozen=True, slots=True)
class Condition:
    """One comparison against state.

    Attributes:
        path: A dotted path into the variables, e.g. ``"result.status"``.
        operator: The comparison.
        value: The right-hand side, unused for ``TRUTHY``/``FALSY``/``EXISTS``.
    """

    path: str
    operator: Operator
    value: Any = None

    def __post_init__(self) -> None:
        if not self.path.strip():
            raise InvalidRequestError("a condition requires a path", details={"field": "path"})
        # A dotted path only. Rejecting subscripts and brackets keeps the
        # evaluator from growing into an expression language.
        for char in self.path:
            if char in "[](){}":
                raise InvalidRequestError(
                    "condition path must be a dotted name",
                    details={"path": self.path},
                )


@dataclass(frozen=True, slots=True)
class ConditionGroup:
    """Several conditions combined.

    Attributes:
        combinator: How to combine.
        conditions: Members. ``NOT`` requires exactly one.
    """

    combinator: Combinator
    conditions: tuple[Condition | ConditionGroup, ...]

    def __post_init__(self) -> None:
        if not self.conditions:
            raise InvalidRequestError(
                "a condition group must contain at least one condition",
                details={"combinator": self.combinator.value},
            )
        if self.combinator is Combinator.NOT and len(self.conditions) != 1:
            raise InvalidRequestError(
                "NOT takes exactly one condition",
                details={"count": len(self.conditions)},
            )


Expression = Condition | ConditionGroup


def resolve_path(variables: dict[str, Any], path: str) -> Any:
    """Reads a dotted path out of the variables.

    Returns ``None`` for anything missing rather than raising: a branch that
    depends on a variable that has not been set yet is simply false.
    """
    current: Any = variables
    for part in path.split("."):
        if not part:
            return None
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
        if current is None:
            return None
    return current


def _compare(left: Any, operator: Operator, right: Any) -> bool:
    """Applies one operator, tolerating incomparable operands.

    ``EXISTS`` is handled by the caller, which needs the pre-comparison value
    to distinguish "present" from "present and falsy".
    """
    try:
        if operator is Operator.EQ:
            return bool(left == right)
        if operator is Operator.NE:
            return bool(left != right)
        if operator is Operator.GT:
            return bool(left > right)
        if operator is Operator.GTE:
            return bool(left >= right)
        if operator is Operator.LT:
            return bool(left < right)
        if operator is Operator.LTE:
            return bool(left <= right)
        if operator is Operator.IN:
            return bool(left in right)
        if operator is Operator.CONTAINS:
            return bool(right in left)
        if operator is Operator.TRUTHY:
            return bool(left)
        if operator is Operator.FALSY:
            return not bool(left)
    except TypeError:
        # Incomparable types make the condition false, not the workflow fail.
        return False
    return False


def evaluate(expression: Expression, variables: dict[str, Any]) -> bool:
    """Evaluates a condition tree against state.

    Args:
        expression: The condition or group.
        variables: Current workflow variables.

    Returns:
        The boolean result.
    """
    if isinstance(expression, Condition):
        left = resolve_path(variables, expression.path)
        if expression.operator is Operator.EXISTS:
            return left is not None
        return bool(_compare(left, expression.operator, expression.value))

    results = [evaluate(member, variables) for member in expression.conditions]
    if expression.combinator is Combinator.AND:
        return all(results)
    if expression.combinator is Combinator.OR:
        return any(results)
    return not results[0]


def select_branch(
    *,
    expression: Expression,
    variables: dict[str, Any],
    branches: dict[str, str],
    default: str | None = None,
) -> str | None:
    """Picks the successor node for a condition node.

    Args:
        expression: The condition to evaluate.
        variables: Current state.
        branches: Named successors. ``"true"`` and ``"false"`` are used when
            present, so a boolean branch reads naturally.
        default: Fallback when neither named branch matches.

    Returns:
        The successor node id, or ``None`` if there is no match and no default.
    """
    outcome = evaluate(expression, variables)
    key = "true" if outcome else "false"
    if key in branches:
        return branches[key]
    return default
