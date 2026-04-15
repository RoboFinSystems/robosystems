"""Tests for operations/roboinvestor/reads/holdings.py."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import pytest

from robosystems.operations.roboinvestor.reads.holdings import (
  PortfolioNotFoundError,
  list_holdings,
)


def _make_portfolio() -> MagicMock:
  p = MagicMock()
  p.id = "pf_01"
  return p


def _make_position(**overrides) -> MagicMock:
  defaults = {
    "id": "pos_01",
    "portfolio_id": "pf_01",
    "security_id": "sec_01",
    "quantity": 100.0,
    "quantity_type": "shares",
    "cost_basis": 50000,  # $500.00
    "current_value": 55000,
    "status": "active",
    "created_at": datetime(2024, 6, 1, tzinfo=UTC),
    "acquisition_date": date(2024, 6, 1),
  }
  defaults.update(overrides)
  row = MagicMock()
  for k, v in defaults.items():
    setattr(row, k, v)
  return row


def _make_security(**overrides) -> MagicMock:
  defaults = {
    "id": "sec_01",
    "entity_id": "ent_01",
    "name": "Acme Class A",
    "security_type": "equity",
  }
  defaults.update(overrides)
  s = MagicMock()
  for k, v in defaults.items():
    setattr(s, k, v)
  return s


def _make_entity(**overrides) -> MagicMock:
  defaults = {
    "id": "ent_01",
    "name": "Acme Corp",
    "metadata_": {"source_graph_id": "kg_source_1"},
  }
  defaults.update(overrides)
  e = MagicMock()
  for k, v in defaults.items():
    setattr(e, k, v)
  return e


def _build_session(
  portfolio: MagicMock | None,
  positions: list[MagicMock] | None = None,
  securities: list[MagicMock] | None = None,
  entities: list[MagicMock] | None = None,
) -> MagicMock:
  """Wire a session mock through the sequence of execute() calls the
  list_holdings function makes: portfolio → positions → securities →
  entities. Later stages are skipped when earlier stages short-circuit."""
  session = MagicMock()
  results: list[MagicMock] = []

  pf_result = MagicMock()
  pf_result.scalar_one_or_none.return_value = portfolio
  results.append(pf_result)

  if portfolio is None:
    session.execute.side_effect = results
    return session

  pos_result = MagicMock()
  pos_result.scalars.return_value.all.return_value = positions or []
  results.append(pos_result)

  if not positions:
    session.execute.side_effect = results
    return session

  sec_result = MagicMock()
  sec_result.scalars.return_value.all.return_value = securities or []
  results.append(sec_result)

  ent_result = MagicMock()
  ent_result.scalars.return_value.all.return_value = entities or []
  results.append(ent_result)

  session.execute.side_effect = results
  return session


class TestListHoldings:
  def test_raises_when_portfolio_missing(self) -> None:
    session = _build_session(portfolio=None)
    with pytest.raises(PortfolioNotFoundError):
      list_holdings(session, "pf_missing")

  def test_empty_when_no_active_positions(self) -> None:
    session = _build_session(portfolio=_make_portfolio(), positions=[])
    result = list_holdings(session, "pf_01")
    assert result.holdings == []
    assert result.total_entities == 0
    assert result.total_positions == 0

  def test_groups_positions_by_entity(self) -> None:
    positions = [
      _make_position(id="pos_1", security_id="sec_1", cost_basis=10000),
      _make_position(id="pos_2", security_id="sec_2", cost_basis=20000),
      _make_position(id="pos_3", security_id="sec_3", cost_basis=30000),
    ]
    securities = [
      _make_security(id="sec_1", entity_id="ent_1", name="Acme A"),
      _make_security(id="sec_2", entity_id="ent_1", name="Acme B"),
      _make_security(id="sec_3", entity_id="ent_2", name="Beta"),
    ]
    entities = [
      _make_entity(id="ent_1", name="Acme Corp"),
      _make_entity(id="ent_2", name="Beta Co"),
    ]
    session = _build_session(_make_portfolio(), positions, securities, entities)

    result = list_holdings(session, "pf_01")

    assert result.total_entities == 2
    assert result.total_positions == 3
    # Holdings sorted by entity name — Acme first
    assert result.holdings[0].entity_name == "Acme Corp"
    assert result.holdings[0].position_count == 2
    assert result.holdings[0].total_cost_basis_dollars == 300.0  # 100 + 200
    assert len(result.holdings[0].securities) == 2
    assert result.holdings[1].entity_name == "Beta Co"
    assert result.holdings[1].position_count == 1
    assert result.holdings[1].total_cost_basis_dollars == 300.0  # 300

  def test_handles_missing_current_value(self) -> None:
    positions = [_make_position(current_value=None)]
    securities = [_make_security()]
    entities = [_make_entity()]
    session = _build_session(_make_portfolio(), positions, securities, entities)

    result = list_holdings(session, "pf_01")

    # total_current_value_dollars is None because no position had a value
    assert result.holdings[0].total_current_value_dollars is None
    assert result.holdings[0].securities[0].current_value_dollars is None

  def test_preserves_source_graph_id_from_metadata(self) -> None:
    positions = [_make_position()]
    securities = [_make_security()]
    entities = [_make_entity(metadata_={"source_graph_id": "kg_acme"})]
    session = _build_session(_make_portfolio(), positions, securities, entities)

    result = list_holdings(session, "pf_01")
    assert result.holdings[0].source_graph_id == "kg_acme"
