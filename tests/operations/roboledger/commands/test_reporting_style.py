# pyright: reportGeneralTypeIssues=false, reportArgumentType=false
"""Tests for the entity-level ``change_reporting_style`` command.

Reporting Style moved from the graph (platform DB) to the entity (extensions
DB): the command is now a pure tenant-schema write. Auth (org access +
roboledger provisioned) is enforced by the router/extension gate, so these
tests exercise only the command's own behavior — entity resolution, target
validation, and the flip.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robosystems.models.api.extensions.entity import ChangeReportingStyleRequest
from robosystems.operations.roboledger.commands.reporting_style import (
  _REQUIRED_STATEMENT_TYPES,
  EntityNotFoundError,
  ReportingStyleInvalidError,
  change_reporting_style,
)

DEFAULT_STYLE_ID = "025f5d48-12ce-5d65-b9eb-4f137a10ef06"
SMALL_PRIVATE_STYLE_ID = "921f5214-afd8-565c-8189-50bcc94c0234"
ENTITY_ID = "ent_01H0"


def _make_entity(reporting_style_id: str | None = DEFAULT_STYLE_ID):
  e = MagicMock()
  e.id = ENTITY_ID
  e.reporting_style_id = reporting_style_id
  return e


def _make_session(
  entity,
  *,
  style_row: MagicMock | None = None,
  composed_types: list[str] | None = None,
  primary_entity=None,
):
  """Mock extensions session.

  ``session.get`` yields ``entity`` (the by-id path); ``session.execute`` is
  routed by SQL text — ``structures`` → the style row, ``reporting_style_networks``
  → the composition rows, and a bare ``SELECT`` (the primary-entity lookup) →
  ``primary_entity`` via ``scalar_one_or_none``.
  """
  session = MagicMock()
  session.get.return_value = entity

  def execute_side(stmt, params=None):
    sql = str(stmt)
    result = MagicMock()
    if "reporting_style_networks" in sql:
      result.fetchall.return_value = (
        []
        if composed_types is None
        else [MagicMock(statement_type=st) for st in composed_types]
      )
    elif "FROM structures" in sql:
      result.fetchone.return_value = style_row
    else:
      result.scalar_one_or_none.return_value = primary_entity
    return result

  session.execute.side_effect = execute_side
  return session


def _style_row(
  *,
  block_type: str = "reporting_style",
  is_active: bool = True,
  metadata=None,
):
  row = MagicMock()
  row.id = SMALL_PRIVATE_STYLE_ID
  row.block_type = block_type
  row.is_active = is_active
  row.metadata = metadata
  return row


def _req(reporting_style_id: str, entity_id: str | None = ENTITY_ID):
  return ChangeReportingStyleRequest(
    reporting_style_id=reporting_style_id, entity_id=entity_id
  )


@pytest.mark.unit
class TestChangeReportingStyle:
  def test_missing_target_entity_404(self):
    session = _make_session(None)  # session.get → None
    with pytest.raises(EntityNotFoundError):
      change_reporting_style(session, _req(SMALL_PRIVATE_STYLE_ID))

  def test_no_primary_entity_404(self):
    # No entity_id → primary lookup, which finds nothing.
    session = _make_session(None, primary_entity=None)
    with pytest.raises(EntityNotFoundError):
      change_reporting_style(session, _req(SMALL_PRIVATE_STYLE_ID, entity_id=None))

  def test_same_target_is_idempotent_noop(self):
    entity = _make_entity(DEFAULT_STYLE_ID)
    session = _make_session(entity)
    result = change_reporting_style(session, _req(DEFAULT_STYLE_ID))
    assert result.changed is False
    assert result.previous_reporting_style_id == DEFAULT_STYLE_ID
    assert result.reporting_style_id == DEFAULT_STYLE_ID
    # No validation query, no commit when the target is unchanged.
    session.commit.assert_not_called()

  def test_unknown_target_style_422(self):
    entity = _make_entity(DEFAULT_STYLE_ID)
    session = _make_session(entity, style_row=None)
    with pytest.raises(ReportingStyleInvalidError, match="not found"):
      change_reporting_style(session, _req("nonexistent-style-id"))
    session.commit.assert_not_called()

  def test_wrong_block_type_422(self):
    entity = _make_entity(DEFAULT_STYLE_ID)
    session = _make_session(entity, style_row=_style_row(block_type="balance_sheet"))
    with pytest.raises(ReportingStyleInvalidError, match="block_type"):
      change_reporting_style(session, _req(SMALL_PRIVATE_STYLE_ID))
    session.commit.assert_not_called()

  def test_inactive_target_422(self):
    entity = _make_entity(DEFAULT_STYLE_ID)
    session = _make_session(entity, style_row=_style_row(is_active=False))
    with pytest.raises(ReportingStyleInvalidError, match="inactive"):
      change_reporting_style(session, _req(SMALL_PRIVATE_STYLE_ID))
    session.commit.assert_not_called()

  def test_incomplete_composition_422(self):
    """Missing one of BS / IS / CF / SE rejects the switch, naming the gaps."""
    entity = _make_entity(DEFAULT_STYLE_ID)
    session = _make_session(
      entity,
      style_row=_style_row(),
      composed_types=["balance_sheet", "income_statement"],
    )
    with pytest.raises(ReportingStyleInvalidError) as ctx:
      change_reporting_style(session, _req(SMALL_PRIVATE_STYLE_ID))
    assert "cash_flow_statement" in str(ctx.value)
    assert "equity_statement" in str(ctx.value)
    session.commit.assert_not_called()

  def test_happy_path_flips_entity_column(self):
    entity = _make_entity(DEFAULT_STYLE_ID)
    session = _make_session(
      entity,
      style_row=_style_row(),
      composed_types=list(_REQUIRED_STATEMENT_TYPES),
    )
    result = change_reporting_style(session, _req(SMALL_PRIVATE_STYLE_ID))
    assert result.changed is True
    assert result.entity_id == ENTITY_ID
    assert result.previous_reporting_style_id == DEFAULT_STYLE_ID
    assert result.reporting_style_id == SMALL_PRIVATE_STYLE_ID
    assert entity.reporting_style_id == SMALL_PRIVATE_STYLE_ID
    session.commit.assert_called_once()

  def test_happy_path_returns_reporting_style_code(self):
    entity = _make_entity(DEFAULT_STYLE_ID)
    session = _make_session(
      entity,
      style_row=_style_row(metadata={"reporting_style_code": "BSC-CORP-IS01-CF1"}),
      composed_types=list(_REQUIRED_STATEMENT_TYPES),
    )
    result = change_reporting_style(session, _req(SMALL_PRIVATE_STYLE_ID))
    assert result.reporting_style_code == "BSC-CORP-IS01-CF1"

  def test_reporting_style_code_none_when_unstamped(self):
    entity = _make_entity(DEFAULT_STYLE_ID)
    session = _make_session(
      entity,
      style_row=_style_row(metadata=None),
      composed_types=list(_REQUIRED_STATEMENT_TYPES),
    )
    result = change_reporting_style(session, _req(SMALL_PRIVATE_STYLE_ID))
    assert result.reporting_style_code is None

  def test_previous_style_none_renders_as_null(self):
    entity = _make_entity(reporting_style_id=None)
    session = _make_session(
      entity,
      style_row=_style_row(metadata={"reporting_style_code": "BSC-CORP-IS01-CF1"}),
      composed_types=list(_REQUIRED_STATEMENT_TYPES),
    )
    result = change_reporting_style(session, _req(SMALL_PRIVATE_STYLE_ID))
    assert result.changed is True
    assert result.previous_reporting_style_id is None

  def test_legacy_custom_block_type_accepted(self):
    entity = _make_entity(DEFAULT_STYLE_ID)
    session = _make_session(
      entity,
      style_row=_style_row(block_type="custom"),
      composed_types=list(_REQUIRED_STATEMENT_TYPES),
    )
    result = change_reporting_style(session, _req(SMALL_PRIVATE_STYLE_ID))
    assert result.changed is True
    assert entity.reporting_style_id == SMALL_PRIVATE_STYLE_ID

  def test_defaults_to_primary_entity_when_no_entity_id(self):
    primary = _make_entity(DEFAULT_STYLE_ID)
    session = _make_session(
      None,  # session.get unused on this path
      style_row=_style_row(),
      composed_types=list(_REQUIRED_STATEMENT_TYPES),
      primary_entity=primary,
    )
    result = change_reporting_style(
      session, _req(SMALL_PRIVATE_STYLE_ID, entity_id=None)
    )
    assert result.changed is True
    assert primary.reporting_style_id == SMALL_PRIVATE_STYLE_ID
