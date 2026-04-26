"""Strawberry types for investor (roboinvestor) extensions queries.

Wraps Pydantic response models from `robosystems.models.api.extensions.investor`.
Most types use `strawberry.experimental.pydantic.type(all_fields=True)`;
`Security` + `SecurityList` are hand-written because `Security.terms: dict`
needs the Strawberry `JSON` scalar (the pydantic decorator doesn't know how
to map an untyped `dict` field).
"""

from __future__ import annotations

from datetime import datetime

import strawberry
from strawberry.scalars import JSON

from robosystems.graphql.types.common import PaginationInfo
from robosystems.models.api.extensions.investor import (
  EntityLite as PydanticEntityLite,
)
from robosystems.models.api.extensions.investor import (
  HoldingResponse as PydanticHoldingResponse,
)
from robosystems.models.api.extensions.investor import (
  HoldingSecuritySummary as PydanticHoldingSecuritySummary,
)
from robosystems.models.api.extensions.investor import (
  HoldingsListResponse as PydanticHoldingsListResponse,
)
from robosystems.models.api.extensions.investor import (
  PortfolioBlockEnvelope as PydanticPortfolioBlockEnvelope,
)
from robosystems.models.api.extensions.investor import (
  PortfolioListResponse as PydanticPortfolioListResponse,
)
from robosystems.models.api.extensions.investor import (
  PortfolioResponse as PydanticPortfolioResponse,
)
from robosystems.models.api.extensions.investor import (
  PositionBlock as PydanticPositionBlock,
)
from robosystems.models.api.extensions.investor import (
  PositionListResponse as PydanticPositionListResponse,
)
from robosystems.models.api.extensions.investor import (
  PositionResponse as PydanticPositionResponse,
)
from robosystems.models.api.extensions.investor import (
  SecurityListResponse as PydanticSecurityListResponse,
)
from robosystems.models.api.extensions.investor import (
  SecurityLite as PydanticSecurityLite,
)
from robosystems.models.api.extensions.investor import (
  SecurityResponse as PydanticSecurityResponse,
)

# ── Portfolios ────────────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticPortfolioResponse, all_fields=True)
class Portfolio:
  """An investment portfolio."""


@strawberry.experimental.pydantic.type(
  model=PydanticPortfolioListResponse, all_fields=True
)
class PortfolioList:
  """Paginated list of portfolios."""


# ── Securities (hand-written: terms: dict needs JSON scalar) ──────────────


@strawberry.type
class Security:
  """An investable security.

  Hand-written because `terms: dict` needs the `JSON` scalar — Strawberry's
  pydantic decorator cannot map an untyped `dict` field.
  """

  id: strawberry.ID
  entity_id: str | None
  entity_name: str | None
  source_graph_id: str | None
  name: str
  security_type: str
  security_subtype: str | None
  terms: JSON
  is_active: bool
  authorized_shares: int | None
  outstanding_shares: int | None
  created_at: datetime
  updated_at: datetime

  @classmethod
  def from_pydantic(cls, row: PydanticSecurityResponse) -> Security:
    return cls(
      id=strawberry.ID(row.id),
      entity_id=row.entity_id,
      entity_name=row.entity_name,
      source_graph_id=row.source_graph_id,
      name=row.name,
      security_type=row.security_type,
      security_subtype=row.security_subtype,
      terms=row.terms,  # dict → JSON scalar on the wire
      is_active=row.is_active,
      authorized_shares=row.authorized_shares,
      outstanding_shares=row.outstanding_shares,
      created_at=row.created_at,
      updated_at=row.updated_at,
    )


@strawberry.type
class SecurityList:
  """Paginated list of securities."""

  securities: list[Security]
  pagination: PaginationInfo

  @classmethod
  def from_pydantic(cls, resp: PydanticSecurityListResponse) -> SecurityList:
    return cls(
      securities=[Security.from_pydantic(s) for s in resp.securities],
      pagination=PaginationInfo.from_pydantic(resp.pagination),
    )


# ── Positions ─────────────────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticPositionResponse, all_fields=True)
class Position:
  """An individual holding — portfolio + security combination."""


@strawberry.experimental.pydantic.type(
  model=PydanticPositionListResponse, all_fields=True
)
class PositionList:
  """Paginated list of positions."""


# ── Holdings (aggregated) ─────────────────────────────────────────────────


@strawberry.experimental.pydantic.type(
  model=PydanticHoldingSecuritySummary, all_fields=True
)
class HoldingSecuritySummary:
  """Per-security line within a holding group."""


@strawberry.experimental.pydantic.type(model=PydanticHoldingResponse, all_fields=True)
class Holding:
  """Positions grouped by entity."""


@strawberry.experimental.pydantic.type(
  model=PydanticHoldingsListResponse, all_fields=True
)
class HoldingsList:
  """Full holdings view for a portfolio."""


# ── Portfolio Block (molecule envelope) ───────────────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticEntityLite, all_fields=True)
class EntityLite:
  """Lightweight entity reference."""


@strawberry.experimental.pydantic.type(model=PydanticSecurityLite, all_fields=True)
class SecurityLite:
  """Lightweight security with issuer and cross-graph reference."""


@strawberry.experimental.pydantic.type(model=PydanticPositionBlock, all_fields=True)
class PositionBlock:
  """A position with its embedded security."""


@strawberry.experimental.pydantic.type(
  model=PydanticPortfolioBlockEnvelope, all_fields=True
)
class PortfolioBlock:
  """Portfolio-centric molecule envelope — portfolio + positions + securities + entities."""
