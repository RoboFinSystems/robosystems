"""Management dimension commands: add, rename and retire members.

Members are never deleted: lines reference them, and a retired member stays on
the lines that carry it.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.dimensions import (
  CreateDimensionRequest,
  DimensionResponse,
  UpdateDimensionRequest,
)
from robosystems.models.extensions.dimension import Dimension
from robosystems.models.extensions.roboledger.dimension_junctions import (
  line_item_dimensions,
)
from robosystems.operations.locking import lock_by_id
from robosystems.operations.roboledger.line_dimensions import (
  RESERVED_DIMENSION_TYPES,
  DimensionNotFoundError,
)
from robosystems.operations.roboledger.reads.dimensions import (
  dimension_to_response,
)


class DuplicateDimensionError(Exception):
  def __init__(self, dimension_type: str, value: str, retired_id: str | None) -> None:
    super().__init__(
      f"The {dimension_type} axis already has a member with value {value!r}."
      + (
        f" It is retired ({retired_id}); bring it back with update-dimension "
        "is_active=true."
        if retired_id
        else ""
      )
    )
    self.dimension_type = dimension_type
    self.value = value


def _slug(name: str) -> str:
  return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def create_dimension(
  session: Session, body: CreateDimensionRequest, created_by: str
) -> DimensionResponse:
  if body.dimension_type in RESERVED_DIMENSION_TYPES:
    raise ValueError(
      f"The {body.dimension_type} axis is reserved for forecasts; ledger lines "
      "take department, class, location, project or an axis of your own."
    )
  value = body.value or _slug(body.name)
  if not value:
    raise ValueError(
      f"{body.name!r} makes no key of letters or digits; give the member a value."
    )
  held = session.execute(
    select(Dimension).where(
      Dimension.dimension_type == body.dimension_type, Dimension.value == value
    )
  ).scalar_one_or_none()
  if held is not None:
    raise DuplicateDimensionError(
      body.dimension_type, value, None if held.is_active else str(held.id)
    )
  now = datetime.now(UTC)
  dimension = Dimension(
    dimension_type=body.dimension_type,
    name=body.name,
    value=value,
    metadata_={"source": "native", "created_by": created_by},
    is_active=True,
    created_at=now,
    updated_at=now,
  )
  try:
    with session.begin_nested():
      session.add(dimension)
      session.flush()
  except IntegrityError as exc:
    # Only the race a concurrent create of the same key loses.
    constraint = getattr(getattr(exc.orig, "diag", None), "constraint_name", None)
    if constraint != "uq_dimension_type_value":
      raise
    raise DuplicateDimensionError(body.dimension_type, value, None) from exc
  session.commit()
  return dimension_to_response(dimension)


def update_dimension(
  session: Session, body: UpdateDimensionRequest, created_by: str
) -> DimensionResponse:
  dimension = lock_by_id(
    session,
    Dimension,
    body.dimension_id,
    f"Dimension {body.dimension_id} is being written by another process. "
    "Retry in a moment.",
  )
  if dimension is None or dimension.dimension_type in RESERVED_DIMENSION_TYPES:
    raise DimensionNotFoundError(body.dimension_id)
  source = (dimension.metadata_ or {}).get("source") or "native"
  if body.name is not None and body.name != dimension.name:
    if source != "native":
      raise ValueError(
        f"Dimension member {dimension.id} came from {source}, and its name "
        f"follows {source}; rename it there."
      )
    dimension.name = body.name
  if body.is_active is not None:
    dimension.is_active = body.is_active
  dimension.updated_at = datetime.now(UTC)
  session.commit()
  line_count = session.execute(
    select(func.count()).where(line_item_dimensions.c.dimension_id == dimension.id)
  ).scalar_one()
  return dimension_to_response(dimension, int(line_count))
