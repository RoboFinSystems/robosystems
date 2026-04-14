"""Strawberry types shared across all extensions GraphQL resolvers."""

from __future__ import annotations

import strawberry

from robosystems.models.api.common import PaginationInfo as PydanticPaginationInfo


@strawberry.experimental.pydantic.type(model=PydanticPaginationInfo, all_fields=True)
class PaginationInfo:
  """Standard pagination envelope used by every list-returning query."""
