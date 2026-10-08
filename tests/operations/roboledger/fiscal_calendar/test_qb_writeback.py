"""Unit tests for the shared QB write-back predicate (qb_writeback.py).

Covers the connection resolver and the eligible-entry id helper that both
the close path (`_publish_drafts_to_qb`) and the outbox read
(`list_period_drafts`) share, so the preview can't drift from the write.
Mock-based, mirroring the close-service test style.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  SHADOW_WRITE_POLICY,
  WRITEBACK_EXCLUDED_EVENT_STATUSES,
  WRITEBACK_WRITE_POLICIES,
  WritebackConnection,
  resolve_writeback_connection,
  select_writeback_eligible_entries,
  shadow_close_for_entity,
  shadow_ledger,
  writeback_eligible_entry_ids,
)

GRAPH_ID = "kg01234567890abcdef"


def _platform_session_returning(candidate):
  """Mock platform session whose Connection query resolves to `candidate`."""
  session = MagicMock()
  session.query.return_value.filter.return_value.order_by.return_value.first.return_value = candidate
  return session


def _platform_session_with_policy(policy):
  """Mock platform session whose write_policy scalar query resolves to
  ``policy``."""
  session = MagicMock()
  chain = session.query.return_value.filter.return_value.order_by.return_value
  chain.limit.return_value.scalar.return_value = policy
  return session


class TestShadowLedger:
  def test_a_shadow_connection_is_a_shadow_ledger(self):
    assert shadow_ledger(_platform_session_with_policy("shadow"), GRAPH_ID) is True

  def test_other_policies_and_no_connection_are_not(self):
    assert (
      shadow_ledger(_platform_session_with_policy("qb_authoritative"), GRAPH_ID)
      is False
    )
    assert shadow_ledger(_platform_session_with_policy(None), GRAPH_ID) is False

  def test_shadow_is_never_a_writeback_policy(self):
    assert SHADOW_WRITE_POLICY not in WRITEBACK_WRITE_POLICIES

  def test_only_the_group_parents_close_shadows(self):
    platform = _platform_session_with_policy("shadow")
    module = "robosystems.operations.roboledger.entity_scope.is_group_parent"
    with patch(module, return_value=True):
      assert shadow_close_for_entity(MagicMock(), platform, GRAPH_ID, "ent_p")
    with patch(module, return_value=False):
      assert not shadow_close_for_entity(MagicMock(), platform, GRAPH_ID, "ent_s")
    assert not shadow_close_for_entity(MagicMock(), platform, GRAPH_ID, None)


class TestResolveWritebackConnection:
  def test_returns_writeback_connection_when_candidate_exists(self):
    candidate = SimpleNamespace(id="conn-123", write_policy="qb_authoritative")
    session = _platform_session_returning(candidate)

    result = resolve_writeback_connection(session, GRAPH_ID)

    assert result == WritebackConnection(
      connection_id="conn-123", write_policy="qb_authoritative"
    )

  def test_returns_none_when_no_writeback_connection(self):
    session = _platform_session_returning(None)

    assert resolve_writeback_connection(session, GRAPH_ID) is None

  def test_coerces_connection_id_to_str(self):
    candidate = SimpleNamespace(id=42, write_policy="hybrid")
    session = _platform_session_returning(candidate)

    result = resolve_writeback_connection(session, GRAPH_ID)

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
