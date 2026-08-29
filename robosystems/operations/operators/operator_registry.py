"""Registry of operator implementations, keyed by type string.

Registration is an import side effect of `@register_operator`, so a module
defining an operator has to be imported before the registry can find it — see
`implementations/__init__.py`.
"""

from __future__ import annotations

from typing import Any

from robosystems.logger import logger
from robosystems.operations.operators.base import Operator

_OPERATORS: dict[str, type[Operator]] = {}
_adapter_operators_loaded: list[bool] = []

# Former names. An operator's registry key is public surface — the URL
# segment, the `operator_type` in queued tasks and stored operations, and
# whatever client code hardcoded — so a rename keeps the old key resolving
# here rather than breaking those. A direct registration always beats an
# alias, and the listing shows canonical names only.
_ALIASES: dict[str, str] = {
  # The analyst shipped as `cypher` when Cypher was its only tool.
  "cypher": "analyst",
}


def register_operator(operator_type: str):
  """Register an Operator class under `operator_type`.

  Re-registering a type replaces the previous class and logs a warning.

  Example::

      @register_operator("analyst")
      class AnalystOperator(Operator):
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


def resolve_operator_type(operator_type: str) -> str:
  """Map a former name to its canonical registry key; unknown names pass through."""
  if operator_type in _OPERATORS:
    return operator_type
  return _ALIASES.get(operator_type, operator_type)


def get_operator(operator_type: str) -> Operator:
  """Instantiate a registered operator (by canonical name or alias), or raise KeyError.

  A fresh instance per call — operators are stateless, and their per-run state
  lives on the `OperatorContext` the adapter injects.
  """
  cls = _OPERATORS.get(resolve_operator_type(operator_type))
  if cls is None:
    registered = ", ".join(_OPERATORS.keys()) or "(none)"
    raise KeyError(
      f"Operator '{operator_type}' not registered. Available: {registered}"
    )
  return cls()


def get_operator_class(operator_type: str) -> type[Operator] | None:
  """Get the operator class without instantiation."""
  return _OPERATORS.get(resolve_operator_type(operator_type))


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
  """Check if an operator type (canonical or alias) is registered."""
  return resolve_operator_type(operator_type) in _OPERATORS


def load_adapter_operators() -> None:
  """Import operator modules contributed by enabled adapters.

  Each adapter exposes a `get_operator_components()` that imports its operator
  modules, which is what fires their `@register_operator` side effects. Called
  once from `operations/operators/__init__.py`; idempotent thereafter.
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
