"""Unit tests for ledger trial balance endpoint."""

from collections import namedtuple
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.ledger.trial_balance import get_trial_balance

MODULE = "robosystems.routers.ledger.trial_balance"
GRAPH_ID = "kg01234567890abcdef"

TBRow = namedtuple(
  "TBRow",
  [
    "id",
    "code",
    "name",
    "classification",
    "account_type",
    "total_debits",
    "total_credits",
  ],
)


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


class TestTrialBalance:
  @pytest.mark.asyncio
  async def test_returns_trial_balance(self):
    rows = [
      TBRow("acct_1", "1000", "Cash", "asset", "bank", 100000, 20000),
      TBRow("acct_2", "4000", "Revenue", "revenue", "income", 0, 80000),
    ]

    mock_session = MagicMock()
    mock_session.execute.return_value = rows

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await get_trial_balance(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.rows) == 2
    assert result.rows[0].account_code == "1000"
    assert result.rows[0].total_debits == 1000.0
    assert result.rows[0].total_credits == 200.0
    assert result.rows[0].net_balance == 800.0
    assert result.total_debits == 1000.0
    assert result.total_credits == 1000.0

  @pytest.mark.asyncio
  async def test_empty_trial_balance(self):
    mock_session = MagicMock()
    mock_session.execute.return_value = []

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await get_trial_balance(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.rows) == 0
    assert result.total_debits == 0
    assert result.total_credits == 0

  @pytest.mark.asyncio
  async def test_date_filter_passed(self):
    mock_session = MagicMock()
    mock_session.execute.return_value = []

    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_oltp.return_value.__exit__ = MagicMock(return_value=False)

      result = await get_trial_balance(
        graph_id=GRAPH_ID,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.rows) == 0
    # Verify text query was called with params
    mock_session.execute.assert_called_once()

  @pytest.mark.asyncio
  async def test_schema_not_found_returns_404(self):
    with patch(f"{MODULE}.oltp_session") as mock_oltp:
      mock_oltp.side_effect = ValueError("Invalid graph_id")

      with pytest.raises(HTTPException) as exc_info:
        await get_trial_balance(
          graph_id=GRAPH_ID,
          current_user=_make_user(),
          _rate_limit=None,
        )

      assert exc_info.value.status_code == 404
