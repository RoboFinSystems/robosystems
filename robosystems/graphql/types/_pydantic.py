"""Helper for the Strawberry-Pydantic decorator that auto-fills the
GraphQL type description from the Pydantic class docstring.

Strawberry's ``experimental.pydantic.type`` decorator already propagates
field-level ``Field(description=...)`` to the generated GraphQL fields.
What it does not do automatically is pull the Pydantic class docstring
through as the GraphQL **type** description — that requires an explicit
``description=`` kwarg on the decorator.

This wrapper closes that gap. When ``description`` is omitted, the
Pydantic model's class docstring (cleaned by ``inspect.getdoc``) becomes
the GraphQL type description. Pass ``description="..."`` explicitly to
override; pass ``description=""`` (empty string) to suppress.

Usage::

    from robosystems.graphql.types._pydantic import pydantic_type

    @pydantic_type(model=PydanticReportResponse, all_fields=True)
    class Report:
      pass
"""

from __future__ import annotations

import inspect
from typing import Any

import strawberry


def _own_docstring(model: type[Any]) -> str | None:
  """Return the model's own ``__doc__`` if it has one — never an
  inherited docstring.

  ``inspect.getdoc`` walks the MRO and falls back to ``BaseModel``'s
  long internals docstring when the model itself defines no docstring.
  We only want descriptions the model author wrote, so check
  ``__dict__`` directly.
  """
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
  """Drop-in replacement for ``strawberry.experimental.pydantic.type``
  that auto-fills ``description`` from the Pydantic model's class
  docstring when one isn't explicitly supplied.

  Only the model's own docstring is used — inherited docstrings (like
  ``BaseModel``'s internal-mechanics description) are ignored.
  """
  if description is None:
    description = _own_docstring(model)
  return strawberry.experimental.pydantic.type(
    model=model, description=description, **kwargs
  )
