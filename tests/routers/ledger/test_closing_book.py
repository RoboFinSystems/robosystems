"""Unit tests for closing book structures endpoint."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.ledger.closing_book import get_closing_book_structures

MODULE = "robosystems.routers.ledger.closing_book"
GRAPH_ID = "kg01234567890abcdef"


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _make_report(report_id="rpt_01", taxonomy_id="tax_reporting_01", status="complete"):
  r = MagicMock()
  r.id = report_id
  r.taxonomy_id = taxonomy_id
  r.generation_status = status
  r.created_at = datetime.now(UTC)
  return r


def _make_structure(struct_id, name, structure_type, is_active=True):
  s = MagicMock()
  s.id = struct_id
  s.name = name
  s.structure_type = structure_type
  s.is_active = is_active
  return s


def _mock_session_ctx(mock_session):
  ctx = MagicMock()
  ctx.__enter__ = MagicMock(return_value=mock_session)
  ctx.__exit__ = MagicMock(return_value=False)
  return ctx


# Simulates a structure row from the statements query
class _StmtRow:
  def __init__(self, id, name, structure_type):
    self.id = id
    self.name = name
    self.structure_type = structure_type


class TestClosingBookStructures:
  @pytest.mark.asyncio
  async def test_full_closing_book(self):
    """Returns all categories when report, mappings, schedules, and entries exist."""
    report = _make_report()
    mapping = _make_structure("struct_map_01", "GAAP Mapping", "coa_mapping")
    schedule = _make_structure(
      "struct_sched_01", "Office Furniture Depreciation", "schedule"
    )

    stmt_rows = [
      _StmtRow("struct_is_01", "Income Statement", "income_statement"),
      _StmtRow("struct_bs_01", "Balance Sheet", "balance_sheet"),
    ]

    mock_session = MagicMock()

    # Mock the four queries in order:
    # 1. Latest report (select Report)
    mock_report_result = MagicMock()
    mock_report_result.scalar_one_or_none.return_value = report

    # 2. Statement structures (text query)
    # 3. Mapping structures (select Structure)
    mock_mappings_result = MagicMock()
    mock_mappings_result.scalars.return_value.all.return_value = [mapping]

    # 4. Schedule structures (select Structure)
    mock_schedules_result = MagicMock()
    mock_schedules_result.scalars.return_value.all.return_value = [schedule]

    # 5. Has posted entries (text query)

    mock_session.execute.side_effect = [
      mock_report_result,  # 1. report query
      stmt_rows,  # 2. statement structures
      mock_mappings_result,  # 3. mappings
      mock_schedules_result,  # 4. schedules
      MagicMock(scalar=MagicMock(return_value=True)),  # 5. has_posted
    ]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_closing_book_structures(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.has_data is True
    # Should have: Statements, Account Rollups, Schedules, Trial Balance, Period Close
    labels = [c.label for c in result.categories]
    assert "Statements" in labels
    assert "Account Rollups" in labels
    assert "Schedules" in labels
    assert "Trial Balance" in labels
    assert "Period Close" in labels

    # Verify statements
    statements = next(c for c in result.categories if c.label == "Statements")
    assert len(statements.items) == 2
    assert statements.items[0].item_type == "statement"
    assert statements.items[0].report_id == "rpt_01"

    # Verify account rollups
    rollups = next(c for c in result.categories if c.label == "Account Rollups")
    assert len(rollups.items) == 1
    assert rollups.items[0].item_type == "account_rollups"

    # Verify schedules
    scheds = next(c for c in result.categories if c.label == "Schedules")
    assert len(scheds.items) == 1
    assert scheds.items[0].name == "Office Furniture Depreciation"

  @pytest.mark.asyncio
  async def test_no_report_omits_statements(self):
    """When no report exists, Statements category is omitted."""
    mock_session = MagicMock()

    mock_report_result = MagicMock()
    mock_report_result.scalar_one_or_none.return_value = None

    mock_mappings = MagicMock()
    mock_mappings.scalars.return_value.all.return_value = []

    mock_schedules = MagicMock()
    mock_schedules.scalars.return_value.all.return_value = []

    mock_session.execute.side_effect = [
      mock_report_result,  # no report
      mock_mappings,  # no mappings
      mock_schedules,  # no schedules
      MagicMock(scalar=MagicMock(return_value=False)),  # no posted entries
    ]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_closing_book_structures(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    labels = [c.label for c in result.categories]
    assert "Statements" not in labels
    assert "Account Rollups" not in labels
    assert "Schedules" not in labels
    assert "Trial Balance" not in labels
    # Period Close always present
    assert "Period Close" in labels
    assert result.has_data is False

  @pytest.mark.asyncio
  async def test_no_posted_entries_omits_trial_balance(self):
    """Trial Balance only appears when posted entries exist."""
    mock_session = MagicMock()

    mock_report_result = MagicMock()
    mock_report_result.scalar_one_or_none.return_value = None

    mock_mappings = MagicMock()
    mock_mappings.scalars.return_value.all.return_value = []

    mock_schedules = MagicMock()
    mock_schedules.scalars.return_value.all.return_value = []

    mock_session.execute.side_effect = [
      mock_report_result,
      mock_mappings,
      mock_schedules,
      MagicMock(scalar=MagicMock(return_value=False)),  # no posted entries
    ]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_closing_book_structures(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    labels = [c.label for c in result.categories]
    assert "Trial Balance" not in labels

  @pytest.mark.asyncio
  async def test_period_close_always_present(self):
    """Period Close category is always included regardless of data state."""
    mock_session = MagicMock()

    mock_report_result = MagicMock()
    mock_report_result.scalar_one_or_none.return_value = None

    mock_mappings = MagicMock()
    mock_mappings.scalars.return_value.all.return_value = []

    mock_schedules = MagicMock()
    mock_schedules.scalars.return_value.all.return_value = []

    mock_session.execute.side_effect = [
      mock_report_result,
      mock_mappings,
      mock_schedules,
      MagicMock(scalar=MagicMock(return_value=False)),
    ]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_closing_book_structures(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    period_close = next(c for c in result.categories if c.label == "Period Close")
    assert len(period_close.items) == 1
    assert period_close.items[0].item_type == "period_close"

  @pytest.mark.asyncio
  async def test_statement_labels_use_display_names(self):
    """Statement items use display labels, not raw structure names."""
    report = _make_report()

    stmt_rows = [
      _StmtRow("struct_is", "some_internal_name", "income_statement"),
      _StmtRow("struct_bs", "some_internal_name", "balance_sheet"),
    ]

    mock_session = MagicMock()

    mock_report_result = MagicMock()
    mock_report_result.scalar_one_or_none.return_value = report

    mock_mappings = MagicMock()
    mock_mappings.scalars.return_value.all.return_value = []

    mock_schedules = MagicMock()
    mock_schedules.scalars.return_value.all.return_value = []

    mock_session.execute.side_effect = [
      mock_report_result,
      stmt_rows,
      mock_mappings,
      mock_schedules,
      MagicMock(scalar=MagicMock(return_value=False)),
    ]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_closing_book_structures(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    statements = next(c for c in result.categories if c.label == "Statements")
    assert statements.items[0].name == "Income Statement"
    assert statements.items[1].name == "Balance Sheet"

  @pytest.mark.asyncio
  async def test_schema_not_found_returns_404(self):
    """When graph schema doesn't exist, returns 404."""
    with patch(f"{MODULE}.extensions_session") as mock_ext_session:
      mock_ext_session.side_effect = ValueError("Invalid graph_id")

      with pytest.raises(HTTPException) as exc_info:
        await get_closing_book_structures(
          graph_id=GRAPH_ID,
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_multiple_schedules_listed(self):
    """Multiple schedules appear as separate items in the Schedules category."""
    mock_session = MagicMock()

    mock_report_result = MagicMock()
    mock_report_result.scalar_one_or_none.return_value = None

    sched1 = _make_structure("sched_01", "Office Furniture Depreciation", "schedule")
    sched2 = _make_structure("sched_02", "Prepaid Insurance Amortization", "schedule")

    mock_mappings = MagicMock()
    mock_mappings.scalars.return_value.all.return_value = []

    mock_schedules = MagicMock()
    mock_schedules.scalars.return_value.all.return_value = [sched1, sched2]

    mock_session.execute.side_effect = [
      mock_report_result,
      mock_mappings,
      mock_schedules,
      MagicMock(scalar=MagicMock(return_value=True)),
    ]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_closing_book_structures(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    scheds = next(c for c in result.categories if c.label == "Schedules")
    assert len(scheds.items) == 2
    assert scheds.items[0].name == "Office Furniture Depreciation"
    assert scheds.items[1].name == "Prepaid Insurance Amortization"
