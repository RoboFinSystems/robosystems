"""The response envelope must be built before the session is committed.

`session.commit()` expires every attribute on the instances in the session, so
reading `event.id` afterwards makes the ORM reload the row. When that reload
comes back empty the ORM raises `ObjectDeletedError`, which reaches the client
as a 500 for a write that already committed — the caller is told the write
failed, so a retry duplicates a persisted event.

These tests pin the ordering rather than the symptom: the reload only fails
under conditions a unit test cannot reproduce (it depends on what the session's
connection can still see post-commit), but the ordering that makes the reload
possible at all is exactly what regressed, and it is directly observable.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.operations.event_block.commands import create_event_block

GRAPH_ID = "kg0123456789abcdef"


def _patch_platform() -> tuple:
  """Patch the platform lookups `_validate_event_source` reaches for."""
  session_factory = MagicMock()
  session_factory.return_value.__enter__ = MagicMock(return_value=MagicMock())
  session_factory.return_value.__exit__ = MagicMock(return_value=False)
  return (
    patch("robosystems.database.SessionFactory", session_factory),
    patch(
      "robosystems.models.core.connection.connection.Connection.get_all_for_graph",
      MagicMock(return_value=[]),
    ),
  )


def _capture_only_body() -> CreateEventBlockRequest:
  return CreateEventBlockRequest(
    event_type="bank_transaction",
    event_category="treasury",
    source="manual",
    occurred_at=datetime(2026, 7, 1, tzinfo=UTC),
    metadata={},
    apply_handlers=False,
  )


def _session_recording_order(events: list[str]) -> MagicMock:
  """A session that records commit, and whose duplicate probe finds nothing."""
  session = MagicMock()
  session.query.return_value.filter.return_value.first.return_value = None
  session.commit.side_effect = lambda: events.append("commit")

  added: list = []
  session.add.side_effect = lambda obj: added.append(obj)

  def fake_flush():
    for obj in added:
      if getattr(obj, "id", None) is None:
        obj.id = "evt_test"

  session.flush.side_effect = fake_flush
  return session


def test_envelope_is_built_before_commit() -> None:
  """`_to_envelope` must run while the instance's attributes are still loaded."""
  order: list[str] = []
  session = _session_recording_order(order)

  real_to_envelope = None

  def spy_to_envelope(event, dimension_ids):
    order.append("to_envelope")
    return real_to_envelope(event, dimension_ids)

  import robosystems.operations.event_block.commands as commands_mod

  real_to_envelope = commands_mod._to_envelope

  factory_patch, lookup_patch = _patch_platform()
  with (
    factory_patch,
    lookup_patch,
    patch.object(commands_mod, "_to_envelope", spy_to_envelope),
  ):
    create_event_block(
      session, _capture_only_body(), created_by="usr_test", graph_id=GRAPH_ID
    )

  assert order == ["to_envelope", "commit"], (
    "envelope must be built before commit; building it after means a committed "
    "write can be reported as a 500"
  )


def test_envelope_survives_post_commit_expiry() -> None:
  """The envelope must be complete before commit expires the instance state.

  Commit clears the instance dict, which is what SQLAlchemy's expiry does: the
  values are gone and the next read has to hit the database. Building the
  envelope first means the response never depends on that reload — which is the
  reload that can fail and turn a committed write into a 500.

  Expiry is applied per instance, never by patching the mapped class: mutating
  `Event` would leak into every later test in the session.
  """
  session = MagicMock()
  session.query.return_value.filter.return_value.first.return_value = None

  added: list = []
  session.add.side_effect = lambda obj: added.append(obj)

  def fake_flush():
    for obj in added:
      if getattr(obj, "id", None) is None:
        obj.id = "evt_test"

  session.flush.side_effect = fake_flush
  session.commit.side_effect = lambda: [obj.__dict__.clear() for obj in added]

  factory_patch, lookup_patch = _patch_platform()
  with factory_patch, lookup_patch:
    envelope = create_event_block(
      session, _capture_only_body(), created_by="usr_test", graph_id=GRAPH_ID
    )

  assert envelope.id == "evt_test"
  assert envelope.source == "manual"
