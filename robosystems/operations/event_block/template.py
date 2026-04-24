"""Transaction template DSL — {{ expr }} interpolation.

Phase 3 ships immediate postings only. Supported expressions:
  {{ event.amount }}               — event.amount (int, cents)
  {{ event.metadata.foo }}         — nested metadata field
  {{ event.metadata.foo }} / 2     — integer division
  {{ handler.metadata.bar }}       — handler metadata field

Rules:
- All {{ ... }} tokens are resolved before arithmetic.
- Arithmetic: only  `/ N`  trailing suffix is supported (integer division).
  Future: `* N`, `+ N`, `- N` can be added here without touching callers.
- Resolution errors raise TemplateInterpolationError with a descriptive message.
- int results are returned as int; string results as str.
"""

from __future__ import annotations

import re

_TOKEN_RE = re.compile(r"\{\{\s*([^}]+?)\s*\}\}")
_DIV_RE = re.compile(r"^(.*?)\s*/\s*(\d+)$")


class TemplateInterpolationError(Exception):
  """Raised when a {{ expr }} reference cannot be resolved."""


def _resolve_path(path: str, context: dict) -> object:
  """Walk a dotted path like 'event.metadata.amount' into context."""
  parts = path.strip().split(".")
  node: object = context
  for part in parts:
    if not isinstance(node, dict):
      raise TemplateInterpolationError(
        f"Cannot traverse '{part}' in '{path}': parent is not a dict"
      )
    if part not in node:
      raise TemplateInterpolationError(
        f"Field '{part}' not found while resolving '{path}'. "
        f"Available keys: {sorted(node.keys())}"
      )
    node = node[part]
  return node


def interpolate(expression: str, context: dict) -> int | str:
  """Resolve a template expression to a value.

  Args:
      expression: Raw DSL string, e.g. '{{ event.amount }}' or
                  '{{ event.amount }} / 12'.
      context:    Dict with keys 'event' and 'handler', each a dict of
                  the corresponding field values.

  Returns:
      int for amount-like fields; str for string fields.

  Raises:
      TemplateInterpolationError: on missing fields, type errors, divide-by-zero.
  """
  # Check for trailing division: "{{ expr }} / N"
  div_match = _DIV_RE.match(expression)
  if div_match:
    inner_expr = div_match.group(1).strip()
    divisor = int(div_match.group(2))
    if divisor == 0:
      raise TemplateInterpolationError("Division by zero in template expression")
    base = interpolate(inner_expr, context)
    if not isinstance(base, int):
      raise TemplateInterpolationError(
        f"Division applied to non-integer value '{base}' in expression: {expression}"
      )
    return base // divisor

  # Replace all {{ ... }} tokens with resolved values
  tokens = _TOKEN_RE.findall(expression)
  if not tokens:
    # No tokens — return the literal expression unchanged
    return expression

  if len(tokens) == 1 and _TOKEN_RE.fullmatch(expression):
    # Single token that is the whole expression — return typed value
    path = tokens[0]
    return _resolve_path(path, context)

  # Multiple tokens or tokens mixed with literal text → stringify all
  def _replace(m: re.Match) -> str:
    return str(_resolve_path(m.group(1), context))

  return _TOKEN_RE.sub(_replace, expression)


def build_event_context(event) -> dict:
  """Build the 'event' half of the interpolation context from an Event row."""
  return {
    "id": event.id,
    "event_type": event.event_type,
    "event_category": event.event_category,
    "agent_id": event.agent_id,
    "resource_type": event.resource_type,
    "resource_element_id": event.resource_element_id,
    "occurred_at": str(event.occurred_at),
    "effective_at": str(event.effective_at) if event.effective_at else None,
    "source": event.source,
    "external_id": event.external_id,
    "amount": event.amount,
    "currency": event.currency,
    "description": event.description,
    "metadata": dict(event.metadata_ or {}),
  }


def build_handler_context(handler) -> dict:
  """Build the 'handler' half of the interpolation context."""
  return {
    "id": handler.id,
    "name": handler.name,
    "event_type": handler.event_type,
    "origin": handler.origin,
    "priority": handler.priority,
    "metadata": dict(handler.metadata_ or {}),
  }
