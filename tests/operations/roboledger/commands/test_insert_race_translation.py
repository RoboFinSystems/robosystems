"""Select-then-insert commands translate the unique-key race into the same
typed error their pre-check raises — never a raw IntegrityError 500."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

pytestmark = pytest.mark.unit


def _integrity_error(constraint: str) -> IntegrityError:
  return IntegrityError(
    "INSERT",
    {},
    Exception(f'duplicate key value violates unique constraint "{constraint}"'),
  )


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
    session.flush.side_effect = _integrity_error("uq_publish_list_members_pair")

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
      Exception(
        'duplicate key value violates unique index "idx_events_source_external"'
      ),
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


class TestViolatedConstraint:
  def test_reads_the_driver_diagnostic_first(self) -> None:
    from robosystems.db.integrity import violated_constraint, violates

    orig = MagicMock()
    orig.diag.constraint_name = "uq_publish_list_members_pair"
    exc = IntegrityError("INSERT", {}, orig)
    assert violated_constraint(exc) == "uq_publish_list_members_pair"
    assert violates(exc, "uq_publish_list_members_pair")
    assert not violates(exc, "something_else")

  def test_falls_back_to_the_quoted_name_in_the_message(self) -> None:
    from robosystems.db.integrity import violated_constraint

    exc = IntegrityError(
      "INSERT",
      {},
      Exception('duplicate key value violates unique constraint "uq_x_y"'),
    )
    assert violated_constraint(exc) == "uq_x_y"
    exc = IntegrityError("INSERT", {}, Exception("something unrelated"))
    assert violated_constraint(exc) is None

  def test_an_unrelated_constraint_is_not_reported_as_a_duplicate(self) -> None:
    """The one thing the pre-check translation must not do: call a foreign-key
    or CHECK failure a benign conflict."""
    from robosystems.models.api.extensions.publish_lists import AddMembersRequest
    from robosystems.operations.roboledger.commands.publish_lists import (
      add_publish_list_members,
    )

    publish_list = MagicMock()
    publish_list.created_by = "usr_owner"
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = publish_list
    session.execute.return_value.scalars.return_value.all.return_value = []
    session.flush.side_effect = _integrity_error(
      "publish_list_members_publish_list_id_fkey"
    )

    graph = MagicMock()
    graph.graph_id = "kg_target"
    graph.schema_extensions = ["roboledger"]
    platform = MagicMock()
    platform.__enter__.return_value.execute.return_value.scalars.return_value.all.return_value = [
      graph
    ]
    from unittest.mock import patch

    with patch("robosystems.db.platform.SessionFactory", return_value=platform):
      with pytest.raises(IntegrityError):
        add_publish_list_members(
          session,
          "pl_1",
          "kg_current",
          AddMembersRequest(target_graph_ids=["kg_target"]),
          added_by="usr_owner",
        )
