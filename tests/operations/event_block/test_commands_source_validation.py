"""Tests for `_validate_event_source` — the event-source registry check.

The events-table CHECK constraint was dropped (extensions migration
0025); `create_event_block` now validates `source` against the static
platform set plus the graph's registered platform Connections.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.operations.event_block.commands import (
  _validate_event_source,
  create_event_block,
)

GRAPH_ID = "kg0123456789abcdef"


def _connection(provider: str, source_name: str | None = None) -> SimpleNamespace:
  return SimpleNamespace(id="conn_1", provider=provider, source_name=source_name)


def _patch_platform(connections: list) -> tuple:
  """Patch the platform session factory + Connection lookup used by the
  validator's local imports."""
  session_factory = MagicMock()
  session_factory.return_value.__enter__ = MagicMock(return_value=MagicMock())
  session_factory.return_value.__exit__ = MagicMock(return_value=False)
  get_all = MagicMock(return_value=connections)
  return (
    patch("robosystems.database.SessionFactory", session_factory),
    patch(
      "robosystems.models.core.connection.connection.Connection.get_all_for_graph",
      get_all,
    ),
    session_factory,
    get_all,
  )


class TestValidateEventSource:
  @pytest.mark.parametrize("source", ["manual", "system", "schedule"])
  def test_static_sources_pass_without_platform_lookup(self, source: str) -> None:
    factory_patch, lookup_patch, session_factory, _ = _patch_platform([])
    with factory_patch, lookup_patch:
      _validate_event_source(source, GRAPH_ID)
    session_factory.assert_not_called()

  def test_provider_source_passes_with_matching_connection(self) -> None:
    factory_patch, lookup_patch, _, get_all = _patch_platform(
      [_connection("quickbooks")]
    )
    with factory_patch, lookup_patch:
      _validate_event_source("quickbooks", GRAPH_ID)
    assert get_all.call_args.args[0] == GRAPH_ID

  def test_external_source_name_passes(self) -> None:
    factory_patch, lookup_patch, _, _ = _patch_platform(
      [_connection("external", source_name="salesforce")]
    )
    with factory_patch, lookup_patch:
      _validate_event_source("salesforce", GRAPH_ID)

  def test_provider_source_without_connection_rejected(self) -> None:
    factory_patch, lookup_patch, _, _ = _patch_platform([])
    with factory_patch, lookup_patch:
      with pytest.raises(ValueError, match="Unknown event source"):
        _validate_event_source("quickbooks", GRAPH_ID)

  def test_unknown_source_lists_registered_and_static_values(self) -> None:
    factory_patch, lookup_patch, _, _ = _patch_platform(
      [
        _connection("quickbooks"),
        _connection("external", source_name="salesforce"),
      ]
    )
    with factory_patch, lookup_patch:
      with pytest.raises(ValueError) as exc:
        _validate_event_source("hubspot", GRAPH_ID)
    message = str(exc.value)
    assert "manual" in message
    assert "quickbooks" in message
    assert "salesforce" in message
    assert "provider='external'" in message

  def test_external_connection_without_source_name_never_matches(self) -> None:
    factory_patch, lookup_patch, _, _ = _patch_platform(
      [_connection("external", source_name=None)]
    )
    with factory_patch, lookup_patch:
      with pytest.raises(ValueError, match="Unknown event source"):
        _validate_event_source("salesforce", GRAPH_ID)


class TestCreateEventBlockSourceGate:
  def test_invalid_source_persists_nothing(self) -> None:
    session = MagicMock()
    body = CreateEventBlockRequest(
      event_type="bank_transaction",
      event_category="treasury",
      source="hubspot",
      occurred_at=datetime(2026, 7, 1, tzinfo=UTC),
      metadata={},
      apply_handlers=False,
    )
    factory_patch, lookup_patch, _, _ = _patch_platform([])
    with factory_patch, lookup_patch:
      with pytest.raises(ValueError, match="Unknown event source"):
        create_event_block(session, body, created_by="usr_test", graph_id=GRAPH_ID)
    session.add.assert_not_called()
    session.commit.assert_not_called()

  def test_registered_external_source_captures(self) -> None:
    session = MagicMock()
    body = CreateEventBlockRequest(
      event_type="bank_transaction",
      event_category="treasury",
      source="salesforce",
      external_id="sf-txn-1",
      occurred_at=datetime(2026, 7, 1, tzinfo=UTC),
      metadata={},
      apply_handlers=False,
    )
    added: list = []
    session.add.side_effect = lambda obj: added.append(obj)
    # No prior event with this (source, external_id). A bare MagicMock would
    # return a truthy row from the duplicate probe and trip the conflict guard,
    # which this test is not about.
    session.query.return_value.filter.return_value.first.return_value = None

    def fake_flush():
      for obj in added:
        if getattr(obj, "id", None) is None:
          obj.id = "evt_test"

    session.flush.side_effect = fake_flush

    factory_patch, lookup_patch, _, _ = _patch_platform(
      [_connection("external", source_name="salesforce")]
    )
    with factory_patch, lookup_patch:
      envelope = create_event_block(
        session, body, created_by="usr_test", graph_id=GRAPH_ID
      )

    assert envelope.source == "salesforce"
    assert envelope.status == "captured"


class TestRoutedConnectionMustBeOnTheGraph:
  """`metadata.connection_id` is what `execute-event-block` publishes to.
  It is caller-set at capture and patchable afterwards, and connection ids
  are platform-wide — so both write paths must join it to the calling graph,
  or a later publish would post into another tenant's source-of-truth."""

  @staticmethod
  def _patch_get_by_id(connection):
    session_factory = MagicMock()
    session_factory.return_value.__enter__ = MagicMock(return_value=MagicMock())
    session_factory.return_value.__exit__ = MagicMock(return_value=False)
    return (
      patch("robosystems.database.SessionFactory", session_factory),
      patch(
        "robosystems.models.core.connection.connection.Connection.get_by_id",
        MagicMock(return_value=connection),
      ),
    )

  def test_capture_refuses_a_connection_registered_on_another_graph(self) -> None:
    from robosystems.operations.event_block.commands import (
      ConnectionNotOnGraphError,
      _validate_routed_connection,
    )

    foreign = SimpleNamespace(id="conn_victim", graph_id="kg000000000000ffff")
    factory_p, lookup_p = self._patch_get_by_id(foreign)
    with factory_p, lookup_p:
      with pytest.raises(ConnectionNotOnGraphError):
        _validate_routed_connection({"connection_id": "conn_victim"}, GRAPH_ID)

  def test_capture_accepts_the_graphs_own_connection_and_none_at_all(self) -> None:
    from robosystems.operations.event_block.commands import _validate_routed_connection

    own = SimpleNamespace(id="conn_mine", graph_id=GRAPH_ID)
    factory_p, lookup_p = self._patch_get_by_id(own)
    with factory_p, lookup_p:
      _validate_routed_connection({"connection_id": "conn_mine"}, GRAPH_ID)
    # No routing id → nothing to join; no platform lookup at all.
    factory_p2, lookup_p2 = self._patch_get_by_id(None)
    with factory_p2, lookup_p2 as get_by_id:
      _validate_routed_connection({"posting_date": "2026-05-19"}, GRAPH_ID)
      _validate_routed_connection(None, GRAPH_ID)
    get_by_id.assert_not_called()

  def test_create_event_block_validates_before_persisting(self) -> None:
    from robosystems.operations.event_block.commands import ConnectionNotOnGraphError

    session = MagicMock()
    foreign = SimpleNamespace(id="conn_victim", graph_id="kg000000000000ffff")
    factory_p, lookup_p = self._patch_get_by_id(foreign)
    body = CreateEventBlockRequest(
      event_type="bank_transaction",
      event_category="treasury",
      source="manual",
      occurred_at=datetime(2026, 7, 1, tzinfo=UTC),
      metadata={"connection_id": "conn_victim"},
      apply_handlers=False,
    )
    with factory_p, lookup_p:
      with pytest.raises(ConnectionNotOnGraphError):
        create_event_block(session, body, created_by="usr", graph_id=GRAPH_ID)
    session.add.assert_not_called()

  def test_metadata_patch_cannot_reroute_to_another_graphs_connection(self) -> None:
    from robosystems.models.api.event_block import UpdateEventBlockRequest
    from robosystems.operations.event_block.commands import (
      ConnectionNotOnGraphError,
      update_event_block,
    )

    event = MagicMock()
    event.id = "evt_1"
    event.status = "captured"
    event.metadata_ = {"connection_id": "conn_mine"}
    session = MagicMock()
    session.get.return_value = event
    # The locked read: `.filter(...).order_by(...).populate_existing()
    # .with_for_update().all()` keyed by id afterwards.
    locked_q = session.query.return_value.filter.return_value
    locked_q.order_by.return_value = locked_q
    locked_q.populate_existing.return_value = locked_q
    locked_q.with_for_update.return_value = locked_q
    locked_q.all.return_value = [event]
    foreign = SimpleNamespace(id="conn_victim", graph_id="kg000000000000ffff")
    factory_p, lookup_p = self._patch_get_by_id(foreign)
    with factory_p, lookup_p:
      with pytest.raises(ConnectionNotOnGraphError):
        update_event_block(
          session,
          UpdateEventBlockRequest(
            event_id="evt_1", metadata_patch={"connection_id": "conn_victim"}
          ),
          created_by="usr",
          graph_id=GRAPH_ID,
        )
    assert event.metadata_ == {"connection_id": "conn_mine"}
