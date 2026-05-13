# pyright: reportGeneralTypeIssues=false, reportArgumentType=false
"""Tests for ``change_reporting_style_cmd`` (Phase 2 of §3.2)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.operations.graph.commands.reporting_style import (
  _REQUIRED_STATEMENT_TYPES,
  change_reporting_style_cmd,
)

_GRAPH = "robosystems.models.core.graph.Graph"
_EXT_SESSION = "robosystems.db.extensions.extensions_session"

GRAPH_ID = "kg19e1f9744ea2a8ab4ef7"
DEFAULT_STYLE_ID = "025f5d48-12ce-5d65-b9eb-4f137a10ef06"
SMALL_PRIVATE_STYLE_ID = "921f5214-afd8-565c-8189-50bcc94c0234"


def _make_graph(reporting_style_id: str = DEFAULT_STYLE_ID):
  g = MagicMock()
  g.graph_id = GRAPH_ID
  g.reporting_style_id = reporting_style_id
  return g


def _make_user():
  user = MagicMock()
  user.id = "user_abc"
  return user


def _make_ext_session(
  *,
  style_row: MagicMock | None,
  composed_types: list[str] | None,
):
  """Build a context-manager mock that yields a session whose execute
  returns the configured style row + composition rows."""
  session = MagicMock()

  def execute_side(stmt, params=None):
    sql = str(stmt)
    result = MagicMock()
    if "FROM structures" in sql:
      result.fetchone.return_value = style_row
    elif "FROM reporting_style_networks" in sql:
      if composed_types is None:
        result.fetchall.return_value = []
      else:
        result.fetchall.return_value = [
          MagicMock(statement_type=st) for st in composed_types
        ]
    else:
      result.fetchone.return_value = None
      result.fetchall.return_value = []
    return result

  session.execute.side_effect = execute_side
  cm = MagicMock()
  cm.__enter__.return_value = session
  cm.__exit__.return_value = False
  return cm


@pytest.mark.asyncio
@pytest.mark.unit
class TestChangeReportingStyleCmd:
  async def test_same_target_is_idempotent_noop(self):
    db = MagicMock()
    user = _make_user()
    with patch(f"{_GRAPH}.get_by_id", return_value=_make_graph(DEFAULT_STYLE_ID)):
      result = await change_reporting_style_cmd(GRAPH_ID, DEFAULT_STYLE_ID, user, db)

    assert result["changed"] is False
    assert result["previous_reporting_style_id"] == DEFAULT_STYLE_ID
    assert result["reporting_style_id"] == DEFAULT_STYLE_ID
    # Never touches the platform-DB row when the target is the same.
    db.commit.assert_not_called()

  async def test_unknown_graph_404(self):
    db = MagicMock()
    user = _make_user()
    with patch(f"{_GRAPH}.get_by_id", return_value=None):
      with pytest.raises(HTTPException) as ctx:
        await change_reporting_style_cmd(GRAPH_ID, SMALL_PRIVATE_STYLE_ID, user, db)
    assert ctx.value.status_code == 404

  async def test_unknown_target_style_422(self):
    db = MagicMock()
    user = _make_user()
    ext_cm = _make_ext_session(style_row=None, composed_types=None)
    with (
      patch(f"{_GRAPH}.get_by_id", return_value=_make_graph(DEFAULT_STYLE_ID)),
      patch(_EXT_SESSION, return_value=ext_cm),
    ):
      with pytest.raises(HTTPException) as ctx:
        await change_reporting_style_cmd(GRAPH_ID, "nonexistent-style-id", user, db)
    assert ctx.value.status_code == 422
    assert "not found" in ctx.value.detail.lower()
    db.commit.assert_not_called()

  async def test_wrong_structure_type_422(self):
    db = MagicMock()
    user = _make_user()
    row = MagicMock()
    row.id = "wrong-type-id"
    row.name = "Some Random Structure"
    row.structure_type = "balance_sheet"  # not a Reporting Style
    row.is_active = True
    ext_cm = _make_ext_session(style_row=row, composed_types=None)
    with (
      patch(f"{_GRAPH}.get_by_id", return_value=_make_graph(DEFAULT_STYLE_ID)),
      patch(_EXT_SESSION, return_value=ext_cm),
    ):
      with pytest.raises(HTTPException) as ctx:
        await change_reporting_style_cmd(GRAPH_ID, "wrong-type-id", user, db)
    assert ctx.value.status_code == 422
    assert "structure_type" in ctx.value.detail

  async def test_inactive_target_422(self):
    db = MagicMock()
    user = _make_user()
    row = MagicMock()
    row.id = SMALL_PRIVATE_STYLE_ID
    row.name = "Small Private Company Style"
    row.structure_type = "reporting_style"
    row.is_active = False
    ext_cm = _make_ext_session(style_row=row, composed_types=None)
    with (
      patch(f"{_GRAPH}.get_by_id", return_value=_make_graph(DEFAULT_STYLE_ID)),
      patch(_EXT_SESSION, return_value=ext_cm),
    ):
      with pytest.raises(HTTPException) as ctx:
        await change_reporting_style_cmd(GRAPH_ID, SMALL_PRIVATE_STYLE_ID, user, db)
    assert ctx.value.status_code == 422
    assert "inactive" in ctx.value.detail.lower()

  async def test_incomplete_composition_422(self):
    """Missing one of BS / IS / CF / SE rejects the switch with the
    list of missing statement types in the detail."""
    db = MagicMock()
    user = _make_user()
    row = MagicMock()
    row.id = SMALL_PRIVATE_STYLE_ID
    row.name = "Small Private Company Style"
    row.structure_type = "reporting_style"
    row.is_active = True
    # Only BS + IS composed; CF + SE missing.
    ext_cm = _make_ext_session(
      style_row=row, composed_types=["balance_sheet", "income_statement"]
    )
    with (
      patch(f"{_GRAPH}.get_by_id", return_value=_make_graph(DEFAULT_STYLE_ID)),
      patch(_EXT_SESSION, return_value=ext_cm),
    ):
      with pytest.raises(HTTPException) as ctx:
        await change_reporting_style_cmd(GRAPH_ID, SMALL_PRIVATE_STYLE_ID, user, db)
    assert ctx.value.status_code == 422
    assert "cash_flow_statement" in ctx.value.detail
    assert "equity_statement" in ctx.value.detail
    db.commit.assert_not_called()

  async def test_happy_path_flips_column(self):
    db = MagicMock()
    user = _make_user()
    graph = _make_graph(DEFAULT_STYLE_ID)
    row = MagicMock()
    row.id = SMALL_PRIVATE_STYLE_ID
    row.name = "Small Private Company Style"
    row.structure_type = "reporting_style"
    row.is_active = True
    ext_cm = _make_ext_session(
      style_row=row, composed_types=list(_REQUIRED_STATEMENT_TYPES)
    )
    with (
      patch(f"{_GRAPH}.get_by_id", return_value=graph),
      patch(_EXT_SESSION, return_value=ext_cm),
    ):
      result = await change_reporting_style_cmd(
        GRAPH_ID, SMALL_PRIVATE_STYLE_ID, user, db
      )

    assert result["changed"] is True
    assert result["previous_reporting_style_id"] == DEFAULT_STYLE_ID
    assert result["reporting_style_id"] == SMALL_PRIVATE_STYLE_ID
    # The platform-DB row got flipped.
    assert graph.reporting_style_id == SMALL_PRIVATE_STYLE_ID
    db.commit.assert_called_once()
    db.refresh.assert_called_once_with(graph)

  async def test_legacy_custom_structure_type_accepted(self):
    """Existing tenants whose 3 Style rows weren't promoted by 0008
    (immutability trigger leaves tenant copies as 'custom') can still
    be selected — the picker doesn't filter on structure_type either."""
    db = MagicMock()
    user = _make_user()
    graph = _make_graph(DEFAULT_STYLE_ID)
    row = MagicMock()
    row.id = SMALL_PRIVATE_STYLE_ID
    row.name = "Small Private Company Style — Composition"
    row.structure_type = "custom"  # legacy tenant row
    row.is_active = True
    ext_cm = _make_ext_session(
      style_row=row, composed_types=list(_REQUIRED_STATEMENT_TYPES)
    )
    with (
      patch(f"{_GRAPH}.get_by_id", return_value=graph),
      patch(_EXT_SESSION, return_value=ext_cm),
    ):
      result = await change_reporting_style_cmd(
        GRAPH_ID, SMALL_PRIVATE_STYLE_ID, user, db
      )

    assert result["changed"] is True
    assert graph.reporting_style_id == SMALL_PRIVATE_STYLE_ID
