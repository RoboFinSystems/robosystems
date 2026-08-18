"""Select-then-insert commands translate the unique-key race into the same
typed error their pre-check raises — never a raw IntegrityError 500."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

pytestmark = pytest.mark.unit


def _integrity_error(constraint: str) -> IntegrityError:
  return IntegrityError("INSERT", {}, Exception(f'violates "{constraint}"'))


class TestPublishListMembers:
  def test_racing_duplicate_member_is_already_present(self) -> None:
    from robosystems.models.api.extensions.publish_lists import AddMembersRequest
    from robosystems.operations.roboledger.commands.publish_lists import (
      MembersAlreadyPresentError,
      add_publish_list_members,
    )

    publish_list = MagicMock()
    publish_list.created_by = "usr_owner"
    session = MagicMock()
    # list lookup, then the existing-members check (empty)
    session.execute.return_value.scalar_one_or_none.return_value = publish_list
    session.execute.return_value.scalars.return_value.all.return_value = []
    session.flush.side_effect = _integrity_error("uq_publish_list_members")

    graph = MagicMock()
    graph.graph_id = "kg_target"
    graph.schema_extensions = ["roboledger"]
    platform = MagicMock()
    platform.__enter__.return_value.execute.return_value.scalars.return_value.all.return_value = [
      graph
    ]
    from unittest.mock import patch

    with patch("robosystems.db.platform.SessionFactory", return_value=platform):
      with pytest.raises(MembersAlreadyPresentError):
        add_publish_list_members(
          session,
          "pl_1",
          "kg_current",
          AddMembersRequest(target_graph_ids=["kg_target"]),
          added_by="usr_owner",
        )


class TestEventCreation:
  def test_racing_duplicate_external_id_is_duplicate_event(self) -> None:
    from robosystems.operations.event_block.commands import (
      DuplicateEventError,
      _flush_new_event,
    )

    session = MagicMock()
    session.flush.side_effect = IntegrityError(
      "INSERT",
      {},
      Exception('duplicate key value violates "idx_events_source_external_id"'),
    )
    body = MagicMock()
    body.source = "quickbooks"
    body.external_id = "JournalEntry_42"
    with pytest.raises(DuplicateEventError):
      _flush_new_event(session, MagicMock(), body)

  def test_other_integrity_errors_still_surface(self) -> None:
    from robosystems.operations.event_block.commands import _flush_new_event

    session = MagicMock()
    session.flush.side_effect = _integrity_error("events_agent_id_fkey")
    body = MagicMock()
    body.external_id = "x"
    with pytest.raises(IntegrityError):
      _flush_new_event(session, MagicMock(), body)


class TestFiscalCalendarCreate:
  def test_racing_second_initialize_is_already_initialized(self) -> None:
    from robosystems.operations.roboledger.fiscal_calendar.service import (
      CalendarAlreadyInitializedError,
      FiscalCalendarService,
    )

    service = FiscalCalendarService()
    session = MagicMock()
    session.flush.side_effect = _integrity_error("uq_fiscal_calendar_graph")
    with pytest.MonkeyPatch.context() as mp:
      mp.setattr(service, "get", lambda _s, _g: None)
      with pytest.raises(CalendarAlreadyInitializedError):
        service.get_or_create(session, "kg_x")
