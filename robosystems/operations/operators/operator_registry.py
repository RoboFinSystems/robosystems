"""Operator registry for the unified Operator protocol.

Simple registration and lookup. Operators register via the @register_operator
decorator and are looked up by type string.
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import logger
from robosystems.operations.operators.base import Operator

_OPERATORS: dict[str, type[Operator]] = {}
_adapter_operators_loaded: list[bool] = []


def register_operator(operator_type: str):
  """Decorator to register a new-style Operator.

  Example::

      @register_operator("cypher")
      class CypherOperator(Operator):
          spec = OperatorSpec(...)
          async def run(self, ctx): ...
  """

  def decorator(cls: type[Operator]) -> type[Operator]:
    if operator_type in _OPERATORS:
      logger.warning(
        f"Overriding existing operator registration '{operator_type}': "
        f"{_OPERATORS[operator_type].__name__} -> {cls.__name__}"
      )
    _OPERATORS[operator_type] = cls
    logger.info(f"Registered operator '{operator_type}': {cls.__name__}")
    return cls

  return decorator


def get_operator(operator_type: str) -> Operator:
  """Instantiate and return an operator by type.

  Raises:
      KeyError: If operator_type is not registered.
  """
  cls = _OPERATORS.get(operator_type)
  if cls is None:
    registered = ", ".join(_OPERATORS.keys()) or "(none)"
    raise KeyError(
      f"Operator '{operator_type}' not registered. Available: {registered}"
    )
  return cls()


def get_operator_class(operator_type: str) -> type[Operator] | None:
  """Get the operator class without instantiation."""
  return _OPERATORS.get(operator_type)


def list_operators() -> dict[str, dict[str, Any]]:
  """List all registered operators with their spec metadata."""
  return {
    operator_type: {
      "name": cls.spec.name,
      "description": cls.spec.description,
      "capabilities": [c.value for c in cls.spec.capabilities],
      "supported_modes": [m.value for m in cls.spec.supported_modes],
      "requires_credits": cls.spec.requires_credits,
      "version": cls.spec.version,
      "graph_scope": _serialize_scope(cls.spec.graph_scope),
    }
    for operator_type, cls in _OPERATORS.items()
  }


def _serialize_scope(scope: Any) -> dict[str, str] | None:
  """Serialize a GraphScope for API responses."""
  if scope is None:
    return None
  result: dict[str, str] = {}
  if scope.shared_repo is not None:
    result["shared_repo"] = scope.shared_repo
  if scope.schema_extension is not None:
    result["schema_extension"] = scope.schema_extension
  return result or None


def is_registered(operator_type: str) -> bool:
  """Check if an operator type is registered."""
  return operator_type in _OPERATORS


def load_adapter_operators() -> None:
  """Load operators from enabled adapters.

  Each adapter can expose a get_operator_components() function that
  returns {"operator_types": [...]} after importing its operator modules
  (which triggers @register_operator side effects).

  Called once from operations/operators/__init__.py.
  """
  if _adapter_operators_loaded:
    return
  _adapter_operators_loaded.append(True)

  # Future adapters register operators here:
  #
  # from robosystems.config import env
  #
  # if env.SEC_PIPELINE_ENABLED:
  #   from robosystems.adapters.sec.operators import get_operator_components
  #   get_operator_components()  # triggers @register_operator side effects


def clear_registry() -> None:
  """Clear all registrations. For testing only."""
  _OPERATORS.clear()
  _adapter_operators_loaded.clear()
