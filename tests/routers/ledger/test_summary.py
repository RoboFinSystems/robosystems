"""Unit tests for ledger summary endpoint."""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.ledger.summary import get_summary

MODULE = "robosystems.routers.ledger.summary"
GRAPH_ID = "kg01234567890abcdef"


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


class TestSummary:
  @pytest.mark.asyncio
  async def test_returns_summary(self):
    mock_session = MagicMock()
    # Counts: account, transaction, entry, line_item
    mock_session.execute.side_effect = [
      MagicMock(scalar=MagicMock(return_value=89)),
      MagicMock(scalar=MagicMock(return_value=127)),
      MagicMock(scalar=MagicMock(return_value=127)),
      MagicMock(scalar=MagicMock(return_value=325)),
      MagicMock(one=MagicMock(return_value=(date(2024, 1, 1), date(2025, 6, 30)))),
    ]

    mock_db = MagicMock()
    mock_db.execute.return_value.one.return_value = (2, datetime(2025, 6, 15))

    with (
      patch(f"{MODULE}.extensions_session") as mock_ext_session,
      patch(f"{MODULE}.get_db_session") as mock_get_db,
    ):
      mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)
      mock_get_db.return_value = iter([mock_db])

      result = await get_summary(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.graph_id == GRAPH_ID
    assert result.account_count == 89
    assert result.transaction_count == 127
    assert result.entry_count == 127
    assert result.line_item_count == 325
    assert result.earliest_transaction_date == date(2024, 1, 1)
    assert result.latest_transaction_date == date(2025, 6, 30)
    assert result.connection_count == 2

  @pytest.mark.asyncio
  async def test_empty_ledger(self):
    mock_session = MagicMock()
    mock_session.execute.side_effect = [
      MagicMock(scalar=MagicMock(return_value=0)),
      MagicMock(scalar=MagicMock(return_value=0)),
      MagicMock(scalar=MagicMock(return_value=0)),
      MagicMock(scalar=MagicMock(return_value=0)),
      MagicMock(one=MagicMock(return_value=(None, None))),
    ]

    mock_db = MagicMock()
    mock_db.execute.return_value.one.return_value = (0, None)

    with (
      patch(f"{MODULE}.extensions_session") as mock_ext_session,
      patch(f"{MODULE}.get_db_session") as mock_get_db,
    ):
      mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)
      mock_get_db.return_value = iter([mock_db])

      result = await get_summary(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.account_count == 0
    assert result.earliest_transaction_date is None
    assert result.connection_count == 0
    assert result.last_sync_at is None

  @pytest.mark.asyncio
  async def test_schema_not_found_returns_404(self):
    with patch(f"{MODULE}.extensions_session") as mock_ext_session:
      mock_ext_session.side_effect = ValueError("Invalid graph_id")

      with pytest.raises(HTTPException) as exc_info:
        await get_summary(
          graph_id=GRAPH_ID,
          current_user=_make_user(),
          _rate_limit=None,
        )

      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_platform_db_failure_graceful(self):
    mock_session = MagicMock()
    mock_session.execute.side_effect = [
      MagicMock(scalar=MagicMock(return_value=10)),
      MagicMock(scalar=MagicMock(return_value=5)),
      MagicMock(scalar=MagicMock(return_value=5)),
      MagicMock(scalar=MagicMock(return_value=20)),
      MagicMock(one=MagicMock(return_value=(date(2025, 1, 1), date(2025, 3, 1)))),
    ]

    with (
      patch(f"{MODULE}.extensions_session") as mock_ext_session,
      patch(f"{MODULE}.get_db_session") as mock_get_db,
    ):
      mock_ext_session.return_value.__enter__ = MagicMock(return_value=mock_session)
      mock_ext_session.return_value.__exit__ = MagicMock(return_value=False)
      mock_get_db.side_effect = Exception("Platform DB down")

      result = await get_summary(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.account_count == 10
    assert result.connection_count == 0
    assert result.last_sync_at is None
