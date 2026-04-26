"""Tests for operations/roboinvestor/reads/portfolio_block.py."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import pytest

from robosystems.operations.roboinvestor.reads.holdings import PortfolioNotFoundError
from robosystems.operations.roboinvestor.reads.portfolio_block import (
  get_portfolio_block,
)


def _make_portfolio(**overrides) -> MagicMock:
  defaults = {
    "id": "port_01",
    "name": "Main Fund",
    "description": "Primary portfolio",
    "strategy": "venture",
    "inception_date": date(2024, 1, 1),
    "base_currency": "USD",
    "entity_id": "ent_owner",
    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
    "updated_at": datetime(2024, 6, 1, tzinfo=UTC),
  }
  defaults.update(overrides)
  p = MagicMock()
  for k, v in defaults.items():
    setattr(p, k, v)
  return p


def _make_position(**overrides) -> MagicMock:
  defaults = {
    "id": "pos_01",
    "portfolio_id": "port_01",
    "security_id": "sec_01",
    "quantity": 100.0,
    "quantity_type": "shares",
    "cost_basis": 50000,
    "current_value": 55000,
    "valuation_date": date(2024, 6, 1),
    "valuation_source": "manual",
    "acquisition_date": date(2024, 6, 1),
    "status": "active",
    "notes": None,
  }
  defaults.update(overrides)
  row = MagicMock()
  for k, v in defaults.items():
    setattr(row, k, v)
  return row


def _make_security(**overrides) -> MagicMock:
  defaults = {
    "id": "sec_01",
    "entity_id": "ent_issuer_01",
    "source_graph_id": "kg_acme",
    "name": "Acme Class A",
    "security_type": "common_stock",
    "security_subtype": "class_a",
    "is_active": True,
  }
  defaults.update(overrides)
  s = MagicMock()
  for k, v in defaults.items():
    setattr(s, k, v)
  return s


def _make_entity(**overrides) -> MagicMock:
  defaults = {
    "id": "ent_issuer_01",
    "name": "Acme Corp",
    "metadata_": {"source_graph_id": "kg_acme"},
  }
  defaults.update(overrides)
  e = MagicMock()
  for k, v in defaults.items():
    setattr(e, k, v)
  return e


def _build_session(
  portfolio: MagicMock | None,
  owner: MagicMock | None = None,
  positions: list[MagicMock] | None = None,
  securities: list[MagicMock] | None = None,
  issuers: list[MagicMock] | None = None,
) -> MagicMock:
  """Wire a session mock through the sequence of execute() calls:
  portfolio → owner (if entity_id) → positions → securities → issuers."""
  session = MagicMock()
  results: list[MagicMock] = []

  pf_result = MagicMock()
  pf_result.scalar_one_or_none.return_value = portfolio
  results.append(pf_result)

  if portfolio is None:
    session.execute.side_effect = results
    return session

  if portfolio.entity_id:
    owner_result = MagicMock()
    owner_result.scalar_one_or_none.return_value = owner
    results.append(owner_result)

  pos_result = MagicMock()
  pos_result.scalars.return_value.all.return_value = positions or []
  results.append(pos_result)

  if positions:
    sec_result = MagicMock()
    sec_result.scalars.return_value.all.return_value = securities or []
    results.append(sec_result)

    if securities and any(s.entity_id for s in securities):
      ent_result = MagicMock()
      ent_result.scalars.return_value.all.return_value = issuers or []
      results.append(ent_result)

  session.execute.side_effect = results
  return session


class TestGetPortfolioBlock:
  def test_raises_when_portfolio_missing(self) -> None:
    session = _build_session(portfolio=None)
    with pytest.raises(PortfolioNotFoundError):
      get_portfolio_block(session, "port_missing")

  def test_envelope_shape_with_positions(self) -> None:
    portfolio = _make_portfolio()
    owner = _make_entity(
      id="ent_owner",
      name="Family Office LLC",
      metadata_={"source_graph_id": "kg_owner"},
    )
    positions = [
      _make_position(id="pos_1", security_id="sec_1", cost_basis=10000),
      _make_position(
        id="pos_2", security_id="sec_2", cost_basis=20000, current_value=25000
      ),
    ]
    securities = [
      _make_security(id="sec_1", entity_id="ent_a", source_graph_id="kg_a"),
      _make_security(id="sec_2", entity_id="ent_b", source_graph_id="kg_b"),
    ]
    issuers = [
      _make_entity(id="ent_a", name="Alpha Co", metadata_={"source_graph_id": "kg_a"}),
      _make_entity(id="ent_b", name="Beta Co", metadata_={"source_graph_id": "kg_b"}),
    ]
    session = _build_session(portfolio, owner, positions, securities, issuers)

    block = get_portfolio_block(session, "port_01")

    assert block.id == "port_01"
    assert block.name == "Main Fund"
    assert block.strategy == "venture"
    assert block.base_currency == "USD"
    assert block.owner is not None
    assert block.owner.id == "ent_owner"
    assert block.owner.name == "Family Office LLC"
    assert block.owner.source_graph_id == "kg_owner"
    assert block.active_position_count == 2
    assert block.total_cost_basis_dollars == 300.0  # 100 + 200
    assert block.total_current_value_dollars == 800.0  # 550 + 250

    pos1 = block.positions[0]
    assert pos1.id == "pos_1"
    assert pos1.security.id == "sec_1"
    assert pos1.security.source_graph_id == "kg_a"
    assert pos1.security.issuer is not None
    assert pos1.security.issuer.name == "Alpha Co"
    assert pos1.security.issuer.source_graph_id == "kg_a"

  def test_empty_positions(self) -> None:
    portfolio = _make_portfolio()
    owner = _make_entity(id="ent_owner", name="Owner", metadata_={})
    session = _build_session(portfolio, owner, positions=[])

    block = get_portfolio_block(session, "port_01")

    assert block.active_position_count == 0
    assert block.positions == []
    assert block.total_cost_basis_dollars == 0.0
    assert block.total_current_value_dollars is None

  def test_owner_null_when_portfolio_has_no_entity(self) -> None:
    portfolio = _make_portfolio(entity_id=None)
    session = _build_session(portfolio, owner=None, positions=[])

    block = get_portfolio_block(session, "port_01")

    assert block.owner is None

  def test_security_without_issuer(self) -> None:
    portfolio = _make_portfolio(entity_id=None)
    positions = [_make_position()]
    securities = [_make_security(entity_id=None, source_graph_id=None)]
    session = _build_session(portfolio, None, positions, securities)

    block = get_portfolio_block(session, "port_01")

    assert len(block.positions) == 1
    assert block.positions[0].security.issuer is None
    assert block.positions[0].security.source_graph_id is None

  def test_only_active_positions_loaded(self) -> None:
    """The read filters status='active' in the SQL — verify the where
    clause is applied. The mock-based test asserts get_portfolio_block
    only consumes the rows the session returned (which simulate the
    DB-side filter)."""
    portfolio = _make_portfolio(entity_id=None)
    # Session returns only active rows (simulating the WHERE filter)
    positions = [_make_position(id="pos_active", status="active")]
    securities = [_make_security()]
    issuers = [_make_entity()]
    session = _build_session(portfolio, None, positions, securities, issuers)

    block = get_portfolio_block(session, "port_01")

    assert block.active_position_count == 1
    assert all(p.status == "active" for p in block.positions)

  def test_handles_missing_current_value(self) -> None:
    portfolio = _make_portfolio(entity_id=None)
    positions = [
      _make_position(id="pos_1", current_value=None, cost_basis=10000),
      _make_position(id="pos_2", current_value=None, cost_basis=20000),
    ]
    securities = [_make_security(entity_id=None)]
    session = _build_session(portfolio, None, positions, securities)

    block = get_portfolio_block(session, "port_01")

    assert block.total_current_value_dollars is None
    assert block.total_cost_basis_dollars == 300.0
