"""`pydantic_type`: Strawberry's Pydantic decorator, with the model's class
docstring as the GraphQL type description (Strawberry only propagates field
descriptions). `description=""` suppresses it.
"""

from __future__ import annotations

import inspect
from typing import Any

import strawberry


def _own_docstring(model: type[Any]) -> str | None:
  """The model's own `__doc__`; `inspect.getdoc` would inherit `BaseModel`'s."""
  raw = model.__dict__.get("__doc__")
  if not isinstance(raw, str):
    return None
  cleaned = inspect.cleandoc(raw)
  return cleaned or None


def pydantic_type(
  model: type[Any],
  *,
  description: str | None = None,
  **kwargs: Any,
) -> Any:
  """Drop-in for `strawberry.experimental.pydantic.type`."""
  if description is None:
    description = _own_docstring(model)
  return strawberry.experimental.pydantic.type(
    model=model, description=description, **kwargs
  )
