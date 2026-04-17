"""Investor GraphQL resolver tests.

Covers the 7 investor queries against mocked ops calls, plus the
JSON-scalar serialization for `Security.terms: dict` (the highest-risk
non-pydantic-decorator type in the investor schema).
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from robosystems.graphql.schema import schema
from robosystems.models.api.common import PaginationInfo
from robosystems.models.api.extensions.investor import (
  HoldingResponse,
  HoldingSecuritySummary,
  HoldingsListResponse,
  PortfolioListResponse,
  PortfolioResponse,
  PositionResponse,
  SecurityListResponse,
  SecurityResponse,
)
from robosystems.operations.roboinvestor.reads.holdings import PortfolioNotFoundError

GRAPH_ID = "kg01234567890abcdef"


def _make_user() -> MagicMock:
  user = MagicMock()
  user.id = "usr_test"
  user.email = "test@example.com"
  return user


def _ctx(
  *,
  schema_extensions: tuple[str, ...] = ("roboinvestor",),
  graph_type: str = "entity",
) -> dict:
  """Build a fake Strawberry context with the test graph_id pre-set.

  `schema_extensions` defaults to `("roboinvestor",)` so the per-domain
  feature gate (`require_extension`) passes for investor resolvers.
  """
  return {
    "request": MagicMock(),
    "user": _make_user(),
    "graph_id": GRAPH_ID,
    "schema_extensions": schema_extensions,
    "graph_type": graph_type,
  }


def _page(total: int = 0, limit: int = 100, offset: int = 0) -> PaginationInfo:
  return PaginationInfo(
    total=total, limit=limit, offset=offset, has_more=(offset + limit) < total
  )


@contextmanager
def _patch_session():
  mock_session = MagicMock()
  mock_ctx_mgr = MagicMock()
  mock_ctx_mgr.__enter__ = MagicMock(return_value=mock_session)
  mock_ctx_mgr.__exit__ = MagicMock(return_value=False)

  with patch("robosystems.db.extensions.extensions_session", return_value=mock_ctx_mgr):
    yield mock_session


class TestExtensionGate:
  """Resolver-level `require_extension` check for the investor domain.

  A graph whose `schema_extensions` doesn't include `"roboinvestor"`
  must surface a clean `EXTENSION_NOT_PROVISIONED` error instead of
  falling through to the DB layer with a schema-missing failure.
  """

  def test_entity_graph_without_roboinvestor_extension(self) -> None:
    result = schema.execute_sync(
      "query { portfolios { pagination { total } } }",
      context_value=_ctx(schema_extensions=()),
    )
    assert result.errors is not None
    assert any(
      "roboinvestor is not provisioned" in str(e.message) for e in result.errors
    )

  def test_ledger_only_graph_rejects_investor_queries(self) -> None:
    result = schema.execute_sync(
      "query { portfolios { pagination { total } } }",
      context_value=_ctx(schema_extensions=("roboledger",)),
    )
    assert result.errors is not None
    assert any(
      "EXTENSION_NOT_PROVISIONED" in str((e.extensions or {}).get("code", ""))
      for e in result.errors
    )


class TestPortfoliosResolver:
  def test_returns_paginated_list(self) -> None:
    portfolios = [
      PortfolioResponse(
        id=f"pf_{i}",
        name=f"Fund {i}",
        base_currency="USD",
        created_at=datetime(2024, 1, 1, tzinfo=UTC),
        updated_at=datetime(2024, 1, 1, tzinfo=UTC),
      )
      for i in range(3)
    ]
    resp = PortfolioListResponse(portfolios=portfolios, pagination=_page(total=3))

    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboinvestor.reads.portfolios.list_portfolios",
        return_value=resp,
      ),
    ):
      result = schema.execute_sync(
        """
        query {
          portfolios {
            portfolios { id name baseCurrency }
            pagination { total limit offset hasMore }
          }
        }
        """,
        context_value=_ctx(),
      )

    assert result.errors is None, result.errors
    data = result.data["portfolios"]
    assert len(data["portfolios"]) == 3
    assert data["portfolios"][0]["id"] == "pf_0"
    assert data["pagination"]["total"] == 3
    assert data["pagination"]["hasMore"] is False

  def test_raises_typed_error_on_schema_error(self) -> None:
    """Investor schema errors raise INVESTOR_NOT_INITIALIZED, not null."""
    from sqlalchemy.exc import ProgrammingError

    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboinvestor.reads.portfolios.list_portfolios",
        side_effect=ProgrammingError("stmt", {}, Exception("schema missing")),
      ),
    ):
      result = schema.execute_sync(
        "query { portfolios { portfolios { id } } }",
        context_value=_ctx(),
      )

    assert result.errors is not None
    err = result.errors[0]
    assert err.extensions == {"code": "INVESTOR_NOT_INITIALIZED"}


class TestPortfolioResolver:
  def test_returns_single_portfolio(self) -> None:
    pf = PortfolioResponse(
      id="pf_01",
      name="Main Fund",
      strategy="long_only",
      base_currency="USD",
      created_at=datetime(2024, 1, 1, tzinfo=UTC),
      updated_at=datetime(2024, 1, 1, tzinfo=UTC),
    )

    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboinvestor.reads.portfolios.get_portfolio",
        return_value=pf,
      ),
    ):
      result = schema.execute_sync(
        'query { portfolio(portfolioId: "pf_01") { id name strategy } }',
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data["portfolio"]["id"] == "pf_01"
    assert result.data["portfolio"]["strategy"] == "long_only"

  def test_returns_null_when_missing(self) -> None:
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboinvestor.reads.portfolios.get_portfolio",
        return_value=None,
      ),
    ):
      result = schema.execute_sync(
        'query { portfolio(portfolioId: "pf_x") { id } }',
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data == {"portfolio": None}


class TestSecurityResolver:
  """Highest-risk investor type — hand-written with JSON scalar for `terms`."""

  def test_terms_dict_serializes_through_json_scalar(self) -> None:
    sec = SecurityResponse(
      id="sec_01",
      name="Acme Class A",
      security_type="equity",
      security_subtype="common",
      terms={"voting": True, "par_value": 0.01, "preferences": ["dividend"]},
      is_active=True,
      created_at=datetime(2024, 1, 1, tzinfo=UTC),
      updated_at=datetime(2024, 1, 1, tzinfo=UTC),
    )

    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboinvestor.reads.securities.get_security",
        return_value=sec,
      ),
    ):
      result = schema.execute_sync(
        """
        query {
          security(securityId: "sec_01") {
            id
            name
            securityType
            terms
            isActive
          }
        }
        """,
        context_value=_ctx(),
      )

    assert result.errors is None, result.errors
    security = result.data["security"]
    assert security["id"] == "sec_01"
    # JSON scalar round-trips the dict verbatim
    assert security["terms"] == {
      "voting": True,
      "par_value": 0.01,
      "preferences": ["dividend"],
    }

  def test_securities_list_contains_hand_written_type(self) -> None:
    sec = SecurityResponse(
      id="sec_01",
      name="Stock",
      security_type="equity",
      security_subtype="common",
      terms={},
      is_active=True,
      created_at=datetime(2024, 1, 1, tzinfo=UTC),
      updated_at=datetime(2024, 1, 1, tzinfo=UTC),
    )
    resp = SecurityListResponse(securities=[sec], pagination=_page(total=1))

    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboinvestor.reads.securities.list_securities",
        return_value=resp,
      ),
    ):
      result = schema.execute_sync(
        """
        query {
          securities {
            securities { id name terms }
            pagination { total }
          }
        }
        """,
        context_value=_ctx(),
      )

    assert result.errors is None
    data = result.data["securities"]
    assert len(data["securities"]) == 1
    assert data["securities"][0]["terms"] == {}
    assert data["pagination"]["total"] == 1


class TestPositionResolver:
  def test_position_converts_cents_to_dollars(self) -> None:
    pos = PositionResponse(
      id="pos_01",
      portfolio_id="pf_01",
      security_id="sec_01",
      security_name="Acme",
      entity_name="Acme Corp",
      quantity=100.0,
      quantity_type="shares",
      cost_basis=50000,
      cost_basis_dollars=500.0,
      currency="USD",
      current_value=55000,
      current_value_dollars=550.0,
      status="active",
      created_at=datetime(2024, 6, 1, tzinfo=UTC),
      updated_at=datetime(2024, 6, 1, tzinfo=UTC),
    )

    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboinvestor.reads.positions.get_position",
        return_value=pos,
      ),
    ):
      result = schema.execute_sync(
        """
        query {
          position(positionId: "pos_01") {
            id
            securityName
            entityName
            quantity
            costBasis
            costBasisDollars
            currentValueDollars
            status
          }
        }
        """,
        context_value=_ctx(),
      )

    assert result.errors is None
    p = result.data["position"]
    assert p["costBasis"] == 50000
    assert p["costBasisDollars"] == 500.0
    assert p["currentValueDollars"] == 550.0


class TestHoldingsResolver:
  def test_null_when_portfolio_missing(self) -> None:
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboinvestor.reads.holdings.list_holdings",
        side_effect=PortfolioNotFoundError("pf_missing"),
      ),
    ):
      result = schema.execute_sync(
        'query { holdings(portfolioId: "pf_missing") { totalEntities } }',
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data == {"holdings": None}

  def test_aggregated_holdings_response(self) -> None:
    holdings = HoldingsListResponse(
      holdings=[
        HoldingResponse(
          entity_id="ent_01",
          entity_name="Acme Corp",
          source_graph_id="kg_source_1",
          securities=[
            HoldingSecuritySummary(
              security_id="sec_01",
              security_name="Acme A",
              security_type="equity",
              quantity=100.0,
              quantity_type="shares",
              cost_basis_dollars=500.0,
              current_value_dollars=550.0,
            ),
          ],
          total_cost_basis_dollars=500.0,
          total_current_value_dollars=550.0,
          position_count=1,
        ),
      ],
      total_entities=1,
      total_positions=1,
    )

    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboinvestor.reads.holdings.list_holdings",
        return_value=holdings,
      ),
    ):
      result = schema.execute_sync(
        """
        query {
          holdings(portfolioId: "pf_01") {
            totalEntities
            totalPositions
            holdings {
              entityId
              entityName
              sourceGraphId
              positionCount
              securities {
                securityId
                securityName
                costBasisDollars
                currentValueDollars
              }
            }
          }
        }
        """,
        context_value=_ctx(),
      )

    assert result.errors is None, result.errors
    data = result.data["holdings"]
    assert data["totalEntities"] == 1
    assert data["holdings"][0]["entityName"] == "Acme Corp"
    assert data["holdings"][0]["sourceGraphId"] == "kg_source_1"
    assert len(data["holdings"][0]["securities"]) == 1
    assert data["holdings"][0]["securities"][0]["costBasisDollars"] == 500.0
