"""Management dimension reads: the graph's members, by axis."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.dimensions import (
  DimensionListResponse,
  DimensionResponse,
)
from robosystems.models.extensions.dimension import Dimension
from robosystems.models.extensions.roboledger.dimension_junctions import (
  line_item_dimensions,
)
from robosystems.operations.roboledger.line_dimensions import (
  RESERVED_DIMENSION_TYPES,
)


def dimension_to_response(
  dimension: Dimension, line_count: int = 0
) -> DimensionResponse:
  return DimensionResponse(
    id=str(dimension.id),
    dimension_type=dimension.dimension_type,
    name=dimension.name,
    value=dimension.value,
    source=str((dimension.metadata_ or {}).get("source") or "native"),
    is_active=bool(dimension.is_active),
    line_count=line_count,
    created_at=dimension.created_at,
  )


def list_dimensions(
  session: Session,
  *,
  dimension_type: str | None = None,
  include_inactive: bool = False,
) -> DimensionListResponse:
  """The members lines can carry, by axis then name, each with how many lines
  carry it. A forecast's scenario members tag facts and are left out."""
  counts = (
    select(
      line_item_dimensions.c.dimension_id,
      func.count().label("n"),
    )
    .group_by(line_item_dimensions.c.dimension_id)
    .subquery()
  )
  query = (
    select(Dimension, func.coalesce(counts.c.n, 0))
    .outerjoin(counts, counts.c.dimension_id == Dimension.id)
    .where(Dimension.dimension_type.not_in(RESERVED_DIMENSION_TYPES))
    .order_by(Dimension.dimension_type, Dimension.name, Dimension.id)
  )
  if dimension_type is not None:
    query = query.where(Dimension.dimension_type == dimension_type)
  if not include_inactive:
    query = query.where(Dimension.is_active.is_(True))
  members = [
    dimension_to_response(dimension, int(n))
    for dimension, n in session.execute(query).all()
  ]
  return DimensionListResponse(
    dimensions=members,
    types=list(dict.fromkeys(m.dimension_type for m in members)),
  )
