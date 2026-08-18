"""Tests for the live-financial-statement route.

Covers the OLTP-backed view operation registered in
`routers/extensions/roboledger/reads.py`. Verifies:
- wiring: window resolution + get_live_financial_statement call shape
- statement_type validation (400 for unknown statement types, with the
  error message pointing users at the graph-backed analysis op)
- CoaMappingNotFoundError → 422 translation
- ProgrammingError (missing schema) → ledger_404 translation
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import ProgrammingError

from robosystems.models.api.extensions.reports import (
  LiveFinancialStatementRequest,
  LiveFinancialStatementResponse,
)
from robosystems.routers.extensions.roboledger.reads import (
  live_financial_statement_op,
)

MODULE = "robosystems.routers.extensions.roboledger.reads"
GRAPH_ID = "kg01234567890abcdef"


def _make_user(user_id: str = "usr_test"):
  u = MagicMock()
  u.id = user_id
  return u


class _FakeCache:
  async def reserve(self, *args, **kwargs):
    return True

  async def release(self, *args, **kwargs):
    return None

  async def get(self, *a, **kw):
    return None

  async def put(self, *a, **kw):
    return None


@pytest.mark.asyncio
class TestLiveFinancialStatementOp:
  @pytest.mark.unit
  async def test_happy_path_wraps_in_envelope(self):
    body = LiveFinancialStatementRequest(
      statement_type="income_statement",
      period_type="quarterly",
    )
    mock_response = LiveFinancialStatementResponse(
      graph_id=GRAPH_ID,
      statement_type="income_statement",
      periods=[],
      facts=[],
      fact_count=0,
      unmapped_count=0,
    )

    # extensions_session is a contextmanager — fake it with a MagicMock supporting __enter__/__exit__.
    cm = MagicMock()
    cm.__enter__.return_value = MagicMock()
    cm.__exit__.return_value = False

    with (
      patch(f"{MODULE}.extensions_session", return_value=cm),
      patch(
        f"{MODULE}.resolve_reporting_window",
        return_value=(date(2026, 1, 1), date(2026, 3, 31)),
      ) as mock_window,
      patch(
        f"{MODULE}.get_live_financial_statement", return_value=mock_response
      ) as mock_fn,
    ):
      envelope = await live_financial_statement_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=MagicMock(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.operation == "live-financial-statement"
    assert envelope.status == "completed"
    assert envelope.result is not None
    assert envelope.result["graph_id"] == GRAPH_ID
    mock_window.assert_called_once()
    mock_fn.assert_called_once()
    call_kwargs = mock_fn.call_args.kwargs
    assert call_kwargs["graph_id"] == GRAPH_ID
    assert call_kwargs["statement_type"] == "income_statement"
    assert call_kwargs["period_start"] == date(2026, 1, 1)

  @pytest.mark.unit
  async def test_invalid_statement_type_raises_400(self):
    body = LiveFinancialStatementRequest(statement_type="not_real")
    with pytest.raises(HTTPException) as exc_info:
      await live_financial_statement_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=MagicMock(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert exc_info.value.status_code == 400
    assert "not_real" in exc_info.value.detail

  @pytest.mark.unit
  async def test_unknown_statement_type_returns_400_with_valid_types(self):
    """Unknown statement types return 400 with the list of valid types
    so callers know what to retry with."""
    body = LiveFinancialStatementRequest(statement_type="bogus_statement")
    with pytest.raises(HTTPException) as exc_info:
      await live_financial_statement_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        _ext=MagicMock(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert exc_info.value.status_code == 400
    assert "Unknown statement_type" in exc_info.value.detail
    assert "income_statement" in exc_info.value.detail
    assert "cash_flow_statement" in exc_info.value.detail

  @pytest.mark.unit
  async def test_missing_mapping_translated_to_422(self):
    """CoaMappingNotFoundError should surface as 422 with the exception message."""
    body = LiveFinancialStatementRequest(statement_type="income_statement")

    cm = MagicMock()
    cm.__enter__.return_value = MagicMock()
    cm.__exit__.return_value = False

    from robosystems.operations.roboledger.reads.reports import (
      CoaMappingNotFoundError,
    )

    with (
      patch(f"{MODULE}.extensions_session", return_value=cm),
      patch(
        f"{MODULE}.resolve_reporting_window",
        return_value=(date(2026, 4, 1), date(2026, 4, 30)),
      ),
      patch(
        f"{MODULE}.get_live_financial_statement",
        side_effect=CoaMappingNotFoundError("run the mapping workflow first"),
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await live_financial_statement_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=MagicMock(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    assert exc_info.value.status_code == 422
    assert "mapping workflow" in exc_info.value.detail

  @pytest.mark.unit
  async def test_missing_ledger_schema_raises_404(self):
    """ProgrammingError (schema doesn't exist) → ledger_404 (404)."""
    body = LiveFinancialStatementRequest(statement_type="income_statement")

    cm = MagicMock()
    cm.__enter__.side_effect = ProgrammingError(
      "stmt", {}, Exception("schema x does not exist")
    )
    cm.__exit__.return_value = False

    with (
      patch(f"{MODULE}.extensions_session", return_value=cm),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await live_financial_statement_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          _ext=MagicMock(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc_info.value.status_code == 404
    assert "Ledger not initialized" in exc_info.value.detail
