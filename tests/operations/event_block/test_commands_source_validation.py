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
