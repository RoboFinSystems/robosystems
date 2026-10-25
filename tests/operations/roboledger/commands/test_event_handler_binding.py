"""A person's approval covers exactly the handler they reviewed, and an MCP
change to anything the template can read sends it back for approval."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.event_handler import UpdateEventHandlerRequest
from robosystems.operations.roboledger.commands.event_handler import (
  StaleApprovalError,
  update_event_handler,
)

REVIEWED_AT = datetime(2026, 10, 24, 12, 0, 0, 123456, tzinfo=UTC)


def _handler(**overrides) -> SimpleNamespace:
  base = {
    "id": "evh_dummy",
    "name": "elem_dummy_supplies",
    "description": None,
    "event_category": None,
    "match_source": "mercury",
    "match_agent_type": None,
    "match_resource_type": None,
    "match_metadata_expression": None,
    "priority": 100,
    "is_active": True,
    "transaction_template": {
      "debit": {"element_id": "{{ handler.name }}"},
      "credit": {"element_id": "elem_dummy_cash"},
    },
    "suggested_by": "ai",
    "approved_by": "user_dummy",
    "approved_at": REVIEWED_AT,
    "updated_at": REVIEWED_AT,
    "metadata_": {},
  }
  base.update(overrides)
  return SimpleNamespace(**base)


def _update(handler, surface: str, **fields):
  body = UpdateEventHandlerRequest(event_handler_id=handler.id, **fields)
  with (
    patch("robosystems.operations.locking.lock_by_id", return_value=handler),
    patch(
      "robosystems.operations.roboledger.commands.event_handler.handler_to_response",
      side_effect=lambda h: h,
    ),
  ):
    return update_event_handler(MagicMock(), body, "user_dummy", surface=surface)


@pytest.mark.unit
class TestRenameIsSubstantiveOverMcp:
  def test_mcp_rename_clears_approval(self):
    handler = _handler()
    _update(handler, "mcp", name="elem_dummy_owner_draws")
    assert handler.name == "elem_dummy_owner_draws"
    assert handler.approved_by is None and handler.suggested_by == "ai"

  def test_same_name_is_not_a_change(self):
    handler = _handler()
    _update(handler, "mcp", name=handler.name, description="clearer")
    assert handler.approved_by == "user_dummy"


@pytest.mark.unit
class TestApprovalBindsToTheReviewedVersion:
  def test_ai_suggestion_cannot_be_approved_blind(self):
    handler = _handler(approved_by=None, approved_at=None)
    with pytest.raises(StaleApprovalError):
      _update(handler, "rest", approve=True)
    assert handler.approved_by is None

  def test_changed_handler_is_not_approved(self):
    handler = _handler(
      approved_by=None, approved_at=None, updated_at=REVIEWED_AT + timedelta(seconds=2)
    )
    with pytest.raises(StaleApprovalError):
      _update(handler, "rest", approve=True, expected_updated_at=REVIEWED_AT)
    assert handler.approved_by is None

  def test_reviewed_version_is_approved(self):
    handler = _handler(approved_by=None, approved_at=None)
    _update(handler, "rest", approve=True, expected_updated_at=REVIEWED_AT)
    assert handler.approved_by == "user_dummy"

  def test_millisecond_round_trip_still_matches(self):
    # Browsers keep milliseconds; the stored row keeps microseconds.
    handler = _handler(approved_by=None, approved_at=None)
    truncated = REVIEWED_AT.replace(microsecond=123000)
    _update(handler, "rest", approve=True, expected_updated_at=truncated)
    assert handler.approved_by == "user_dummy"

  def test_human_authored_handler_needs_no_version(self):
    handler = _handler(suggested_by=None, approved_by=None, approved_at=None)
    _update(handler, "rest", approve=True)
    assert handler.approved_by == "user_dummy"
