"""Operator registry keyed by type string. Registration is an import side
effect of `@register_operator` (see `implementations/__init__.py`)."""

from __future__ import annotations

from typing import Any

from robosystems.logger import logger
from robosystems.operations.operators.base import Operator

_OPERATORS: dict[str, type[Operator]] = {}
_adapter_operators_loaded: list[bool] = []

# Former names. A registry key is public surface (URL segment, queued and
# stored `operator_type`), so a rename keeps the old key resolving here.
_ALIASES: dict[str, str] = {
  # The analyst shipped as `cypher` when Cypher was its only tool.
  "cypher": "analyst",
}


def register_operator(operator_type: str):
  """Re-registering a type replaces the previous class (with a warning)."""

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


def resolve_operator_type(operator_type: str) -> str:
  """Map a former name to its canonical registry key; unknown names pass through."""
  if operator_type in _OPERATORS:
    return operator_type
  return _ALIASES.get(operator_type, operator_type)


def get_operator(operator_type: str) -> Operator:
  """Fresh instance of a registered operator (name or alias); KeyError if none."""
  cls = _OPERATORS.get(resolve_operator_type(operator_type))
  if cls is None:
    registered = ", ".join(_OPERATORS.keys()) or "(none)"
    raise KeyError(
      f"Operator '{operator_type}' not registered. Available: {registered}"
    )
  return cls()


def get_operator_class(operator_type: str) -> type[Operator] | None:
  return _OPERATORS.get(resolve_operator_type(operator_type))


def list_operators() -> dict[str, dict[str, Any]]:
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
  if scope is None:
    return None
  result: dict[str, str] = {}
  if scope.shared_repo is not None:
    result["shared_repo"] = scope.shared_repo
  if scope.schema_extension is not None:
    result["schema_extension"] = scope.schema_extension
  return result or None


def is_registered(operator_type: str) -> bool:
  return resolve_operator_type(operator_type) in _OPERATORS


def load_adapter_operators() -> None:
  """Import operator modules contributed by enabled adapters. Idempotent."""
  if _adapter_operators_loaded:
    return
  _adapter_operators_loaded.append(True)
  # Adapters call their `get_operator_components()` here to fire their
  # `@register_operator` side effects; none contribute operators today.


def clear_registry() -> None:
  """For testing only."""
  _OPERATORS.clear()
  _adapter_operators_loaded.clear()
