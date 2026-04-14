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

from robosystems.graphql.context import GraphQLContext, require_graph_id, require_user
from robosystems.graphql.types.investor import (
  HoldingsList,
  Portfolio,
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
  portfolios as reads_portfolios,
)
from robosystems.operations.roboinvestor.reads import (
  positions as reads_positions,
)
from robosystems.operations.roboinvestor.reads import (
  securities as reads_securities,
)

_MIN_LIMIT = 1
_MAX_LIMIT = 1000
_MIN_OFFSET = 0


def _validate_pagination(limit: int, offset: int) -> None:
  """Reject out-of-range pagination args at the resolver boundary.

  Mirrors the retired REST `Query(..., ge=N, le=M)` bounds so the new
  GraphQL surface can't be used to issue unbounded list reads.
  """
  if not _MIN_LIMIT <= limit <= _MAX_LIMIT:
    raise strawberry.exceptions.StrawberryGraphQLError(
      message=f"limit must be between {_MIN_LIMIT} and {_MAX_LIMIT}",
      extensions={"code": "INVALID_PAGINATION"},
    )
  if offset < _MIN_OFFSET:
    raise strawberry.exceptions.StrawberryGraphQLError(
      message=f"offset must be >= {_MIN_OFFSET}",
      extensions={"code": "INVALID_PAGINATION"},
    )


def _open_session(info: Info[GraphQLContext, None]):
  """Shared auth + session-open prelude for every investor resolver.

  Auth + graph access were enforced by `get_context` before this is
  ever reached. `graph_id` is read from the request URL via
  `require_graph_id`.
  """
  require_user(info)
  graph_id = require_graph_id(info)
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)


def _raise_investor_not_initialized() -> NoReturn:
  """Raise a typed GraphQL error for an uninitialized investor module.

  Replaces the previous "swallow ValueError/ProgrammingError → return
  null" pattern. Frontends and agents see a clear failure with the
  `INVESTOR_NOT_INITIALIZED` error code instead of an empty result.
  """
  raise strawberry.exceptions.StrawberryGraphQLError(
    message="Investor module not initialized.",
    extensions={"code": "INVESTOR_NOT_INITIALIZED"},
  )


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
      with _open_session(info) as session:
        response = reads_portfolios.list_portfolios(session, limit=limit, offset=offset)
    except (ValueError, ProgrammingError):
      _raise_investor_not_initialized()
    return PortfolioList.from_pydantic(response)

  @strawberry.field
  def portfolio(
    self,
    info: Info[GraphQLContext, None],
    portfolio_id: str,
  ) -> Portfolio | None:
    """Single portfolio by id."""
    try:
      with _open_session(info) as session:
        response = reads_portfolios.get_portfolio(session, portfolio_id)
    except (ValueError, ProgrammingError):
      _raise_investor_not_initialized()
    if response is None:
      return None
    return Portfolio.from_pydantic(response)

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
      with _open_session(info) as session:
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
      with _open_session(info) as session:
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
      with _open_session(info) as session:
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
      with _open_session(info) as session:
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
      with _open_session(info) as session:
        response = reads_holdings.list_holdings(session, portfolio_id)
    except reads_holdings.PortfolioNotFoundError:
      return None
    except (ValueError, ProgrammingError):
      _raise_investor_not_initialized()
    return HoldingsList.from_pydantic(response)
