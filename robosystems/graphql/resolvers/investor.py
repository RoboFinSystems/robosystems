"""Investor (roboinvestor) GraphQL resolvers.

Every field is a thin wrapper that:
1. Authenticates the user and checks graph access
2. Opens `extensions_session(graph_id)`
3. Delegates to `operations/roboinvestor/reads/*.py`
"""

from __future__ import annotations

import strawberry
from sqlalchemy.exc import ProgrammingError
from strawberry.types import Info

from robosystems.graphql.auth import check_graph_access
from robosystems.graphql.context import GraphQLContext, require_user
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


def _open_session(info: Info[GraphQLContext, None], graph_id: str):
  """Shared auth + session-open prelude for every investor resolver."""
  user = require_user(info)
  check_graph_access(user, graph_id)
  from robosystems.db.extensions import extensions_session

  return extensions_session(graph_id)


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
    graph_id: strawberry.ID,
    limit: int = 100,
    offset: int = 0,
  ) -> PortfolioList | None:
    """Paginated list of portfolios."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_portfolios.list_portfolios(session, limit=limit, offset=offset)
    except (ValueError, ProgrammingError):
      return None
    return PortfolioList.from_pydantic(response)

  @strawberry.field
  def portfolio(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    portfolio_id: str,
  ) -> Portfolio | None:
    """Single portfolio by id."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_portfolios.get_portfolio(session, portfolio_id)
    except (ValueError, ProgrammingError):
      return None
    if response is None:
      return None
    return Portfolio.from_pydantic(response)

  # ── Securities ──────────────────────────────────────────────────────────

  @strawberry.field
  def securities(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    entity_id: str | None = None,
    security_type: str | None = None,
    is_active: bool | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> SecurityList | None:
    """Paginated list of securities."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_securities.list_securities(
          session,
          entity_id=entity_id,
          security_type=security_type,
          is_active=is_active,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      return None
    return SecurityList.from_pydantic(response)

  @strawberry.field
  def security(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    security_id: str,
  ) -> Security | None:
    """Single security by id."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_securities.get_security(session, security_id)
    except (ValueError, ProgrammingError):
      return None
    if response is None:
      return None
    return Security.from_pydantic(response)

  # ── Positions ───────────────────────────────────────────────────────────

  @strawberry.field
  def positions(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    portfolio_id: str | None = None,
    security_id: str | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
  ) -> PositionList | None:
    """Paginated list of positions."""
    _validate_pagination(limit, offset)
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_positions.list_positions(
          session,
          portfolio_id=portfolio_id,
          security_id=security_id,
          status=status,
          limit=limit,
          offset=offset,
        )
    except (ValueError, ProgrammingError):
      return None
    return PositionList.from_pydantic(response)

  @strawberry.field
  def position(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    position_id: str,
  ) -> Position | None:
    """Single enriched position by id."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_positions.get_position(session, position_id)
    except (ValueError, ProgrammingError):
      return None
    if response is None:
      return None
    return Position.from_pydantic(response)

  # ── Holdings (aggregated) ───────────────────────────────────────────────

  @strawberry.field
  def holdings(
    self,
    info: Info[GraphQLContext, None],
    graph_id: strawberry.ID,
    portfolio_id: str,
  ) -> HoldingsList | None:
    """Portfolio positions grouped by entity."""
    try:
      with _open_session(info, str(graph_id)) as session:
        response = reads_holdings.list_holdings(session, portfolio_id)
    except reads_holdings.PortfolioNotFoundError:
      return None
    except (ValueError, ProgrammingError):
      return None
    return HoldingsList.from_pydantic(response)
