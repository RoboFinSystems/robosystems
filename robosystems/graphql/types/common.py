"""Strawberry types shared across all extensions GraphQL resolvers."""

from __future__ import annotations

from robosystems.graphql.types._pydantic import pydantic_type
from robosystems.models.api.common import PaginationInfo as PydanticPaginationInfo


@pydantic_type(model=PydanticPaginationInfo, all_fields=True)
class PaginationInfo:
  """Standard pagination envelope used by every list-returning query."""
