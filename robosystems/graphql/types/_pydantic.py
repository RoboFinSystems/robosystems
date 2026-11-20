"""`pydantic_type`: Strawberry's Pydantic decorator, with the model's class
docstring as the GraphQL type description (Strawberry only propagates field
descriptions; `description=""` suppresses it), and a model's wide integers
(`Cents`) derived as `BigInt` where Strawberry would derive `Int`.
"""

from __future__ import annotations

import annotationlib
import inspect
import types
from typing import Annotated, Any, Union, get_args, get_origin

import strawberry

from robosystems.graphql.scalars import BigInt
from robosystems.models.api.common import WideInteger


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
  derive = strawberry.experimental.pydantic.type(
    model=model, description=description, **kwargs
  )
  wide = _wide_fields(model)
  if not wide:
    return derive

  def decorate(cls: type[Any]) -> Any:
    # A field the class declares itself keeps its declaration.
    cls.__annotations__ = {
      **wide,
      **annotationlib.get_annotations(cls, format=annotationlib.Format.FORWARDREF),
    }
    return derive(cls)

  return decorate


def _wide_fields(model: type[Any]) -> dict[str, Any]:
  """The model's fields that hold a wide integer, with the type each takes."""
  wide: dict[str, Any] = {}
  for name, field in getattr(model, "model_fields", {}).items():
    # Pydantic lifts a field's own `Annotated` metadata off the annotation
    # and leaves it in place inside a union or a list.
    if any(isinstance(m, WideInteger) for m in field.metadata):
      wide[name] = BigInt
      continue
    widened = _widen(field.annotation)
    if widened is not field.annotation:
      wide[name] = widened
  return wide


def _widen(annotation: Any) -> Any:
  origin = get_origin(annotation)
  if origin is Annotated:
    base, *metadata = get_args(annotation)
    if any(isinstance(m, WideInteger) for m in metadata):
      return BigInt
    return annotation
  if origin in (Union, types.UnionType):
    members = get_args(annotation)
    widened = [_widen(member) for member in members]
    if all(new is old for new, old in zip(widened, members, strict=True)):
      return annotation
    result = widened[0]
    for member in widened[1:]:
      result = result | member
    return result
  if origin is list:
    (member,) = get_args(annotation)
    widened_member = _widen(member)
    return annotation if widened_member is member else list[widened_member]
  return annotation
