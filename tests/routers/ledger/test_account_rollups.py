"""Unit tests for ledger account rollups endpoint."""

from collections import namedtuple
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.ledger.account_rollups import get_account_rollups

MODULE = "robosystems.routers.ledger.account_rollups"
GRAPH_ID = "kg01234567890abcdef"
MAPPING_ID = "struct_mapping_01"

# Simulates a row returned by the SQL join query
RollupRow = namedtuple(
  "RollupRow",
  [
    "reporting_element_id",
    "reporting_name",
    "reporting_qname",
    "classification",
    "balance_type",
    "coa_element_id",
    "coa_name",
    "coa_code",
    "total_debits",
    "total_credits",
  ],
)

UnmappedRow = namedtuple("UnmappedRow", ["cnt"])


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _make_mapping_structure(mapping_id=MAPPING_ID, name="GAAP Mapping"):
  s = MagicMock()
  s.id = mapping_id
  s.name = name
  s.structure_type = "coa_mapping"
  s.is_active = True
  return s


def _mock_session_ctx(mock_session):
  """Create a mock context manager wrapping the session."""
  ctx = MagicMock()
  ctx.__enter__ = MagicMock(return_value=mock_session)
  ctx.__exit__ = MagicMock(return_value=False)
  return ctx


class TestAccountRollups:
  @pytest.mark.asyncio
  async def test_returns_grouped_rollups(self):
    """Happy path: two CoA accounts rolling up to one reporting element."""
    rows = [
      RollupRow(
        "elem_cash",
        "Cash and Cash Equivalents",
        "us-gaap:CashAndCashEquivalents",
        "asset",
        "debit",
        "coa_checking",
        "BofA Checking",
        "1010",
        1500000,
        500000,
      ),
      RollupRow(
        "elem_cash",
        "Cash and Cash Equivalents",
        "us-gaap:CashAndCashEquivalents",
        "asset",
        "debit",
        "coa_petty",
        "Petty Cash",
        "1020",
        15000,
        0,
      ),
    ]

    unmapped = UnmappedRow(cnt=3)

    mock_session = MagicMock()
    mock_session.get.return_value = _make_mapping_structure()

    # First execute: main query, second: unmapped count
    mock_unmapped_result = MagicMock()
    mock_unmapped_result.fetchone.return_value = unmapped
    mock_session.execute.side_effect = [rows, mock_unmapped_result]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_account_rollups(
        graph_id=GRAPH_ID,
        mapping_id=MAPPING_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.groups) == 1
    assert result.groups[0].reporting_name == "Cash and Cash Equivalents"
    assert len(result.groups[0].accounts) == 2
    assert result.groups[0].accounts[0].account_name == "BofA Checking"
    assert (
      result.groups[0].accounts[0].net_balance == 10000.0
    )  # (1500000 - 500000) / 100
    assert result.groups[0].accounts[1].account_name == "Petty Cash"
    assert result.groups[0].accounts[1].net_balance == 150.0  # 15000 / 100
    assert result.groups[0].total == 10150.0  # sum of children
    assert result.mapping_id == MAPPING_ID
    assert result.total_mapped == 2
    assert result.total_unmapped == 3

  @pytest.mark.asyncio
  async def test_auto_discovers_mapping(self):
    """When mapping_id is not provided, auto-discovers the first active mapping."""
    mock_session = MagicMock()
    mock_structure = _make_mapping_structure()

    # scalar_one_or_none for auto-discover
    mock_session.execute.return_value.scalar_one_or_none.return_value = mock_structure
    # After auto-discover: main query returns empty, unmapped returns 0
    mock_unmapped = MagicMock()
    mock_unmapped.fetchone.return_value = UnmappedRow(cnt=0)
    mock_session.execute.side_effect = [
      MagicMock(
        scalar_one_or_none=MagicMock(return_value=mock_structure)
      ),  # auto-discover
      [],  # main query
      mock_unmapped,  # unmapped count
    ]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_account_rollups(
        graph_id=GRAPH_ID,
        mapping_id=None,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.mapping_id == MAPPING_ID
    assert result.mapping_name == "GAAP Mapping"
    assert len(result.groups) == 0

  @pytest.mark.asyncio
  async def test_no_mapping_found_returns_empty(self):
    """When no mapping structure exists, returns empty response."""
    mock_session = MagicMock()
    mock_session.execute.return_value.scalar_one_or_none.return_value = None

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_account_rollups(
        graph_id=GRAPH_ID,
        mapping_id=None,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.mapping_id == ""
    assert result.mapping_name == "No mapping found"
    assert len(result.groups) == 0
    assert result.total_mapped == 0
    assert result.total_unmapped == 0

  @pytest.mark.asyncio
  async def test_credit_normal_natural_sign(self):
    """Credit-normal accounts (revenue, liability) negate net_balance."""
    rows = [
      RollupRow(
        "elem_rev",
        "Revenue",
        "us-gaap:Revenues",
        "revenue",
        "credit",
        "coa_sales",
        "Sales Revenue",
        "4000",
        0,
        800000,
      ),
    ]

    mock_session = MagicMock()
    mock_session.get.return_value = _make_mapping_structure()
    mock_unmapped = MagicMock()
    mock_unmapped.fetchone.return_value = UnmappedRow(cnt=0)
    mock_session.execute.side_effect = [rows, mock_unmapped]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_account_rollups(
        graph_id=GRAPH_ID,
        mapping_id=MAPPING_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.groups) == 1
    # Revenue: debits=0, credits=800000 → net = 0-8000 = -8000 → natural_sign negates → 8000
    assert result.groups[0].accounts[0].net_balance == 8000.0
    assert result.groups[0].total == 8000.0

  @pytest.mark.asyncio
  async def test_multiple_classification_groups_sorted(self):
    """Groups are sorted by classification: asset → liability → equity → revenue → expense."""
    rows = [
      RollupRow(
        "elem_rev",
        "Revenue",
        "us-gaap:Revenues",
        "revenue",
        "credit",
        "coa_sales",
        "Sales",
        "4000",
        0,
        500000,
      ),
      RollupRow(
        "elem_cash",
        "Cash",
        "us-gaap:Cash",
        "asset",
        "debit",
        "coa_cash",
        "Cash",
        "1000",
        500000,
        0,
      ),
    ]

    mock_session = MagicMock()
    mock_session.get.return_value = _make_mapping_structure()
    mock_unmapped = MagicMock()
    mock_unmapped.fetchone.return_value = UnmappedRow(cnt=0)
    mock_session.execute.side_effect = [rows, mock_unmapped]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_account_rollups(
        graph_id=GRAPH_ID,
        mapping_id=MAPPING_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.groups) == 2
    # Asset before revenue
    assert result.groups[0].classification == "asset"
    assert result.groups[1].classification == "revenue"

  @pytest.mark.asyncio
  async def test_date_filters_passed(self):
    """Date filters are passed through to the SQL query."""
    mock_session = MagicMock()
    mock_session.get.return_value = _make_mapping_structure()
    mock_unmapped = MagicMock()
    mock_unmapped.fetchone.return_value = UnmappedRow(cnt=0)
    mock_session.execute.side_effect = [[], mock_unmapped]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_account_rollups(
        graph_id=GRAPH_ID,
        mapping_id=MAPPING_ID,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.groups) == 0
    # Main query was called with date params
    call_args = mock_session.execute.call_args_list[0]
    params = (
      call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("params", {})
    )
    assert params.get("start_date") == date(2026, 1, 1)
    assert params.get("end_date") == date(2026, 3, 31)

  @pytest.mark.asyncio
  async def test_mapping_not_found_returns_404(self):
    """When explicit mapping_id doesn't exist, returns 404."""
    mock_session = MagicMock()
    mock_session.get.return_value = None

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      with pytest.raises(HTTPException) as exc_info:
        await get_account_rollups(
          graph_id=GRAPH_ID,
          mapping_id="struct_nonexistent",
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_schema_not_found_returns_404(self):
    """When graph schema doesn't exist, returns 404."""
    with patch(f"{MODULE}.extensions_session") as mock_ext_session:
      mock_ext_session.side_effect = ValueError("Invalid graph_id")

      with pytest.raises(HTTPException) as exc_info:
        await get_account_rollups(
          graph_id=GRAPH_ID,
          mapping_id=MAPPING_ID,
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_empty_mapping_returns_zero_groups(self):
    """When mapping exists but has no associations, returns empty groups."""
    mock_session = MagicMock()
    mock_session.get.return_value = _make_mapping_structure()
    mock_unmapped = MagicMock()
    mock_unmapped.fetchone.return_value = UnmappedRow(cnt=5)
    mock_session.execute.side_effect = [[], mock_unmapped]

    with patch(
      f"{MODULE}.extensions_session", return_value=_mock_session_ctx(mock_session)
    ):
      result = await get_account_rollups(
        graph_id=GRAPH_ID,
        mapping_id=MAPPING_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.groups) == 0
    assert result.total_mapped == 0
    assert result.total_unmapped == 5
