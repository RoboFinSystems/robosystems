"""Investor (roboinvestor) GraphQL resolvers.

Graph-scoped at `/extensions/{graph_id}/graphql`. Auth + per-graph
access are validated by `get_context` before any resolver runs;
`graph_id` is read from `info.context` via `require_graph_id`.
"""

from __future__ import annotations

from typing import NoReturn

import strawberry
from sqlalchemy.exc import ProgrammingError
from strawberry.types import Info

from robosystems.graphql.context import GraphQLContext
from robosystems.graphql.resolvers._common import (
  open_extensions_session as _open_session,
)
from robosystems.graphql.resolvers._common import (
  validate_pagination as _validate_pagination,
)
from robosystems.graphql.types.investor import (
  HoldingsList,
  PortfolioBlock,
  PortfolioList,
  Position,
  PositionList,
  Security,
  SecurityList,
)
from robosystems.operations.roboinvestor.reads import (
  holdings as reads_holdings,
)
from robosystems.operations.roboinvestor.reads import (
  portfolio_block as reads_portfolio_block,
)
from robosystems.operations.roboinvestor.reads import (
  portfolios as reads_portfolios,
)
from robosystems.operations.roboinvestor.reads import (
  positions as reads_positions,
)
from robosystems.operations.roboinvestor.reads import (
  securities as reads_securities,
)


def _raise_investor_not_initialized() -> NoReturn:
  """Raise a typed GraphQL error for an uninitialized investor module.

  Replaces the previous "swallow ValueError/ProgrammingError → return
  null" pattern. Frontends and agents see a clear failure with the
  `INVESTOR_NOT_INITIALIZED` error code instead of an empty result.

  Uses `raise ... from None` so that when called from inside an
  `except (ValueError, ProgrammingError):` block, Python doesn't set
  `__context__` on the new exception. Strawberry's error serializer
  can otherwise leak the raw `ProgrammingError` (which contains schema
  and table names from the failing SQL statement) through the
  `extensions` field of the GraphQL error response.
  """
  raise strawberry.exceptions.StrawberryGraphQLError(
    message="Investor module not initialized.",
    extensions={"code": "INVESTOR_NOT_INITIALIZED"},
  ) from None


@strawberry.type
class InvestorQuery:
  """Read-only fields for the roboinvestor domain.

  Composed into the top-level `Query` root via multiple inheritance.
  """

  # ── Portfolios ──────────────────────────────────────────────────────────

  @strawberry.field
  def portfolios(
    self,
    info: Info[GraphQLContext, None],
    limit: int = 100,
    offset: int = 0,
  ) -> PortfolioList | None:
    """Paginated list of portfolios."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, "roboinvestor") as session:
        response = reads_portfolios.list_portfolios(session, limit=limit, offset=offset)
    except (ValueError, ProgrammingError):
      _raise_investor_not_initialized()
    return PortfolioList.from_pydantic(response)

  # ── Securities ──────────────────────────────────────────────────────────

  @strawberry.field
  def securities(
    self,
    info: Info[GraphQLContext, None],
    entity_id: str | None = None,
    security_type: str | None = None,
    is_active: bool | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> SecurityList | None:
    """Paginated list of securities."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, "roboinvestor") as session:
        response = reads_securities.list_securities(
          session,
          entity_id=entity_id,
          security_type=security_type,
          is_active=is_active,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      _raise_investor_not_initialized()
    return SecurityList.from_pydantic(response)

  @strawberry.field
  def security(
    self,
    info: Info[GraphQLContext, None],
    security_id: str,
  ) -> Security | None:
    """Single security by id."""
    try:
      with _open_session(info, "roboinvestor") as session:
        response = reads_securities.get_security(session, security_id)
    except (ValueError, ProgrammingError):
      _raise_investor_not_initialized()
    if response is None:
      return None
    return Security.from_pydantic(response)

  # ── Positions ───────────────────────────────────────────────────────────

  @strawberry.field
  def positions(
    self,
    info: Info[GraphQLContext, None],
    portfolio_id: str | None = None,
    security_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> PositionList | None:
    """Paginated list of positions."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, "roboinvestor") as session:
        response = reads_positions.list_positions(
          session,
          portfolio_id=portfolio_id,
          security_id=security_id,
          status=status,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      _raise_investor_not_initialized()
    return PositionList.from_pydantic(response)

  @strawberry.field
  def position(
    self,
    info: Info[GraphQLContext, None],
    position_id: str,
  ) -> Position | None:
    """Single enriched position by id."""
    try:
      with _open_session(info, "roboinvestor") as session:
        response = reads_positions.get_position(session, position_id)
    except (ValueError, ProgrammingError):
      _raise_investor_not_initialized()
    if response is None:
      return None
    return Position.from_pydantic(response)

  # ── Holdings (aggregated) ───────────────────────────────────────────────

  @strawberry.field
  def holdings(
    self,
    info: Info[GraphQLContext, None],
    portfolio_id: str,
  ) -> HoldingsList | None:
    """Portfolio positions grouped by entity."""
    try:
      with _open_session(info, "roboinvestor") as session:
        response = reads_holdings.list_holdings(session, portfolio_id)
    except reads_holdings.PortfolioNotFoundError:
      return None
    except (ValueError, ProgrammingError):
      _raise_investor_not_initialized()
    return HoldingsList.from_pydantic(response)

  # ── Portfolio Block (molecule envelope) ─────────────────────────────────

  @strawberry.field
  def portfolio_block(
    self,
    info: Info[GraphQLContext, None],
    portfolio_id: str,
  ) -> PortfolioBlock | None:
    """Portfolio-centric molecule: portfolio + active positions + securities + entities."""
    try:
      with _open_session(info, "roboinvestor") as session:
        response = reads_portfolio_block.get_portfolio_block(session, portfolio_id)
    except reads_holdings.PortfolioNotFoundError:
      return None
    except (ValueError, ProgrammingError):
      _raise_investor_not_initialized()
    return PortfolioBlock.from_pydantic(response)
