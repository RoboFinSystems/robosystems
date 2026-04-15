"""Tests for operations/roboinvestor/{reads,commands}/portfolios.py."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.investor import CreatePortfolioRequest
from robosystems.operations.roboinvestor.commands.portfolios import (
  PortfolioHasActivePositionsError,
  create_portfolio,
  delete_portfolio,
  update_portfolio,
)
from robosystems.operations.roboinvestor.reads.portfolios import (
  get_portfolio,
  list_portfolios,
  portfolio_to_response,
)


def _make_portfolio(**overrides) -> MagicMock:
  """Build a MagicMock matching the Portfolio row attributes."""
  defaults = {
    "id": "pf_01",
    "name": "Main Fund",
    "description": "Test fund",
    "strategy": "long_only",
    "inception_date": date(2024, 1, 1),
    "base_currency": "USD",
    "created_at": datetime(2024, 1, 1, tzinfo=UTC),
    "updated_at": datetime(2024, 1, 1, tzinfo=UTC),
  }
  defaults.update(overrides)
  row = MagicMock()
  for k, v in defaults.items():
    setattr(row, k, v)
  return row


class TestPortfolioToResponse:
  def test_maps_all_fields(self) -> None:
    row = _make_portfolio(name="Growth Portfolio")
    resp = portfolio_to_response(row)
    assert resp.id == "pf_01"
    assert resp.name == "Growth Portfolio"
    assert resp.strategy == "long_only"
    assert resp.base_currency == "USD"


class TestListPortfolios:
  def test_returns_pagination_and_rows(self) -> None:
    rows = [_make_portfolio(id="pf_1"), _make_portfolio(id="pf_2")]
    session = MagicMock()
    # Two execute() calls: count, then list
    count_result = MagicMock()
    count_result.scalar.return_value = 2
    list_result = MagicMock()
    list_result.scalars.return_value.all.return_value = rows
    session.execute.side_effect = [count_result, list_result]

    result = list_portfolios(session, limit=50, offset=0)

    assert result.pagination.total == 2
    assert result.pagination.limit == 50
    assert len(result.portfolios) == 2
    assert result.portfolios[0].id == "pf_1"
    assert result.portfolios[1].id == "pf_2"

  def test_zero_results(self) -> None:
    session = MagicMock()
    count_result = MagicMock()
    count_result.scalar.return_value = 0
    list_result = MagicMock()
    list_result.scalars.return_value.all.return_value = []
    session.execute.side_effect = [count_result, list_result]

    result = list_portfolios(session)

    assert result.pagination.total == 0
    assert result.portfolios == []


class TestGetPortfolio:
  def test_returns_response_when_found(self) -> None:
    row = _make_portfolio()
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = row

    result = get_portfolio(session, "pf_01")

    assert result is not None
    assert result.id == "pf_01"

  def test_returns_none_when_missing(self) -> None:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    assert get_portfolio(session, "pf_missing") is None


class TestCreatePortfolio:
  def test_adds_flushes_and_returns_response(self) -> None:
    """Patch Portfolio so the added instance has DB-default fields populated.

    In production the id / created_at / updated_at columns get their
    values during `session.flush()`; here we stand in a fully-formed
    mock so `portfolio_to_response` can serialize it.
    """
    session = MagicMock()
    body = CreatePortfolioRequest(
      name="New Fund",
      description="Desc",
      strategy="balanced",
      inception_date=date(2025, 1, 1),
      base_currency="USD",
    )

    fake_row = _make_portfolio(id="pf_new", name="New Fund", strategy="balanced")
    fake_row.created_by = "usr_1"

    with patch(
      "robosystems.operations.roboinvestor.commands.portfolios.Portfolio",
      return_value=fake_row,
    ):
      result = create_portfolio(session, body, created_by="usr_1")

    session.add.assert_called_once_with(fake_row)
    session.flush.assert_called_once()
    assert result.id == "pf_new"
    assert result.name == "New Fund"
    assert result.strategy == "balanced"


class TestUpdatePortfolio:
  def test_applies_updates_and_flushes(self) -> None:
    row = _make_portfolio(name="Original")
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = row

    result = update_portfolio(session, "pf_01", {"name": "Renamed"})

    assert row.name == "Renamed"
    session.flush.assert_called_once()
    assert result is not None and result.name == "Renamed"

  def test_returns_none_when_missing(self) -> None:
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None

    result = update_portfolio(session, "pf_missing", {"name": "X"})

    assert result is None
    session.flush.assert_not_called()


class TestDeletePortfolio:
  def test_deletes_when_no_active_positions(self) -> None:
    row = _make_portfolio()
    session = MagicMock()
    # First execute: portfolio lookup. Second execute: active count.
    pf_result = MagicMock()
    pf_result.scalar_one_or_none.return_value = row
    count_result = MagicMock()
    count_result.scalar.return_value = 0
    session.execute.side_effect = [pf_result, count_result]

    assert delete_portfolio(session, "pf_01") is True
    session.delete.assert_called_once_with(row)

  def test_returns_false_when_missing(self) -> None:
    session = MagicMock()
    pf_result = MagicMock()
    pf_result.scalar_one_or_none.return_value = None
    session.execute.side_effect = [pf_result]

    assert delete_portfolio(session, "pf_missing") is False
    session.delete.assert_not_called()

  def test_raises_when_has_active_positions(self) -> None:
    row = _make_portfolio()
    session = MagicMock()
    pf_result = MagicMock()
    pf_result.scalar_one_or_none.return_value = row
    count_result = MagicMock()
    count_result.scalar.return_value = 3
    session.execute.side_effect = [pf_result, count_result]

    with pytest.raises(PortfolioHasActivePositionsError) as exc_info:
      delete_portfolio(session, "pf_01")

    assert exc_info.value.active_count == 3
    session.delete.assert_not_called()
