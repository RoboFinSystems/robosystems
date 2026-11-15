"""Unit tests for the shared QB write-back predicate (qb_writeback.py).

Covers the connection resolver and the eligible-entry id helper that both
the close path (`_publish_drafts_to_qb`) and the outbox read
(`list_period_drafts`) share, so the preview can't drift from the write.
Mock-based, mirroring the close-service test style.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  SHADOW_WRITE_POLICY,
  WRITEBACK_EXCLUDED_EVENT_STATUSES,
  WRITEBACK_WRITE_POLICIES,
  WritebackConnection,
  books_keeper,
  publish_posture,
  resolve_writeback_connection,
  select_writeback_eligible_entries,
  shadow_close_for_entity,
  writeback_eligible_entry_ids,
)

GRAPH_ID = "kg01234567890abcdef"
ENTITY = "ent_p"


def _keeper(policy, *, provider="quickbooks", id="conn-live"):
  return SimpleNamespace(id=id, write_policy=policy, provider=provider)


def _platform_session_returning(candidate):
  """Mock platform session whose keeper query resolves to `candidate`."""
  session = MagicMock()
  session.query.return_value.filter.return_value.first.return_value = candidate
  return session


def _platform_session_with_policy(policy):
  """Mock platform session whose keeper query resolves to a row holding
  ``policy``, or to no row when ``policy`` is None."""
  return _platform_session_returning(None if policy is None else _keeper(policy))


class TestShadowClose:
  def test_a_shadow_keeper_shadows_its_entitys_close(self):
    platform = _platform_session_with_policy("shadow")
    assert shadow_close_for_entity(platform, GRAPH_ID, ENTITY) is True

  def test_other_policies_and_no_keeper_do_not(self):
    for policy in ("qb_authoritative", None):
      platform = _platform_session_with_policy(policy)
      assert shadow_close_for_entity(platform, GRAPH_ID, ENTITY) is False

  def test_shadow_is_never_a_writeback_policy(self):
    assert SHADOW_WRITE_POLICY not in WRITEBACK_WRITE_POLICIES

  def test_the_keeper_decides_both_predicates(self):
    """One row answers "does it shadow" and "where does it publish", so
    shadow books never resolve a publish target."""
    session = _platform_session_with_policy("shadow")
    assert publish_posture(session, GRAPH_ID, ENTITY) == (None, True)

  def test_no_entity_has_no_keeper_and_is_never_asked(self):
    platform = _platform_session_with_policy("shadow")
    assert books_keeper(platform, GRAPH_ID, None) is None
    assert not shadow_close_for_entity(platform, GRAPH_ID, None)
    platform.query.assert_not_called()


class TestResolveWritebackConnection:
  def test_returns_writeback_connection_when_candidate_exists(self):
    candidate = _keeper("qb_authoritative", id="conn-123")
    session = _platform_session_returning(candidate)

    result = resolve_writeback_connection(session, GRAPH_ID, ENTITY)

    assert result == WritebackConnection(
      connection_id="conn-123", write_policy="qb_authoritative"
    )

  def test_returns_none_when_no_writeback_connection(self):
    session = _platform_session_returning(None)

    assert resolve_writeback_connection(session, GRAPH_ID, ENTITY) is None

  def test_a_live_row_that_does_not_write_back_is_no_target(self):
    """The policy is read from the live row, never searched for among older
    rows: a `native` live row resolves to no target."""
    session = _platform_session_returning(_keeper("native"))

    assert resolve_writeback_connection(session, GRAPH_ID, ENTITY) is None

  def test_a_ledger_that_cannot_be_written_to_is_no_target(self):
    """A keeper of another provider is read-only whatever its policy says."""
    session = _platform_session_returning(
      _keeper("qb_authoritative", provider="external")
    )

    assert resolve_writeback_connection(session, GRAPH_ID, ENTITY) is None

  def test_coerces_connection_id_to_str(self):
    candidate = _keeper("hybrid", id=42)
    session = _platform_session_returning(candidate)

    result = resolve_writeback_connection(session, GRAPH_ID, ENTITY)

    assert result is not None
    assert result.connection_id == "42"
    assert result.write_policy == "hybrid"


class TestWritebackEligibleEntryIds:
  def test_extracts_str_entry_ids_from_rows(self):
    session = MagicMock()
    rows = [
      (SimpleNamespace(id="e1"), SimpleNamespace(id="ev1")),
      (SimpleNamespace(id="e2"), SimpleNamespace(id="ev2")),
    ]
    session.query.return_value.join.return_value.filter.return_value.all.return_value = rows

    ids = writeback_eligible_entry_ids(
      session, date(2026, 1, 1), date(2026, 1, 31), entity_id="ent_1"
    )

    assert ids == {"e1", "e2"}

  def test_empty_when_no_eligible_entries(self):
    session = MagicMock()
    session.query.return_value.join.return_value.filter.return_value.all.return_value = []

    assert (
      writeback_eligible_entry_ids(
        session, date(2026, 1, 1), date(2026, 1, 31), entity_id="ent_1"
      )
      == set()
    )

  def test_predicate_excludes_retracted_event_statuses(self):
    """Voided / superseded leftovers must not publish. Pinned on the
    shared predicate so the outbox preview and the close write cannot
    drift independently of this check."""
    session = MagicMock()
    session.query.return_value.join.return_value.filter.return_value.all.return_value = []

    select_writeback_eligible_entries(
      session, date(2026, 1, 1), date(2026, 1, 31), entity_id="ent_1"
    )

    predicates = [
      str(arg.compile(compile_kwargs={"literal_binds": True}))
      for arg in session.query.return_value.join.return_value.filter.call_args[0]
    ]
    joined = " ".join(predicates)
    for status in WRITEBACK_EXCLUDED_EVENT_STATUSES:
      assert status in joined, predicates
