"""Applying QuickBooks' deletions against a real database: an unposted event
is voided, a posted one becomes a reconciling item that only catch-up can
resolve, an entry RoboLedger published is matched by the id QuickBooks gave
it, and an id that never synced is counted, not invented."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from robosystems.adapters.quickbooks.pipeline.cdc import apply_deletions
from robosystems.models.api.extensions.reconciling_items import (
  ResolveReconcilingItemRequest,
)
from robosystems.operations.roboledger.commands.reconciling_items import (
  RestateBlockedError,
  plan_reconciling_item,
  resolve_reconciling_item,
)
from tests.operations.roboledger.commands.test_reconciling_items_db import (
  AMOUNT,
  GRAPH_ID,
  _line_nets,
  _post_synced_event_with_id,
  _seed_elements,
  _seed_periods,
  _skip_platform_db_checks,
  session,
)

__all__ = ["_skip_platform_db_checks", "session"]

pytestmark = pytest.mark.unit

NOW = datetime(2026, 10, 8, 4, 0, tzinfo=UTC)


def _seed(db):
  _seed_elements(db)
  _seed_periods(db)
  posted = _post_synced_event_with_id(db, "Expense_1737")
  unposted = _post_synced_event_with_id(db, "Expense_1738")
  unposted.status = "classified"
  published = _post_synced_event_with_id(db, "Expense_1739")
  published.source = "schedule"
  published.external_id = "sched_1739"
  published.metadata_ = {
    **dict(published.metadata_ or {}),
    "qb_external_id": "900,901",
    "qb_entry_ids": {"e1": "900", "e2": "901"},
  }
  db.flush()
  return posted, unposted, published


def test_deletions_are_applied_by_the_events_status(session):
  posted, unposted, published = _seed(session)

  result = apply_deletions(
    session,
    [
      {"entity": "Purchase", "id": "1737", "last_updated": None},
      {"entity": "Purchase", "id": "1738", "last_updated": None},
      {"entity": "JournalEntry", "id": "901", "last_updated": None},
      {"entity": "Invoice", "id": "never-synced", "last_updated": None},
    ],
    now=NOW,
  )

  assert (result.voided, result.flagged, result.skipped, result.unmatched) == (
    1,
    2,
    0,
    1,
  )
  session.refresh(unposted)
  assert unposted.status == "voided"
  assert unposted.metadata_["void_reason"] == "deleted_in_quickbooks"
  assert unposted.metadata_["source_removed_transaction_ids"] == ["1738"]

  session.refresh(posted)
  assert posted.payload_drift is True and posted.status == "fulfilled"
  accepted = posted.metadata_["drift_payload"]
  assert accepted["entries"] == [] and accepted["source_removed"] is True
  assert accepted["source_removed_transaction_ids"] == ["1737"]
  assert accepted["deleted_upstream"] is True
  assert posted.metadata_["drift_detected_at"] == NOW.isoformat()

  session.refresh(published)
  assert published.payload_drift is True
  assert published.metadata_["drift_payload"]["published_entry_deleted"] is True
  assert published.metadata_["drift_payload"]["source_removed_transaction_ids"] == [
    "901"
  ]


def test_a_deleted_posted_transaction_reverses_through_catch_up_only(session):
  posted, _unposted, _published = _seed(session)
  apply_deletions(
    session, [{"entity": "Purchase", "id": "1737", "last_updated": None}], now=NOW
  )
  session.flush()

  plan = plan_reconciling_item(session, str(posted.id), graph_id=GRAPH_ID)
  assert any("retracted" in blocker for blocker in plan.restate_blockers)
  with pytest.raises(RestateBlockedError):
    resolve_reconciling_item(
      session,
      ResolveReconcilingItemRequest(event_id=str(posted.id), disposition="restate"),
      "user_test",
      graph_id=GRAPH_ID,
    )

  result = resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(event_id=str(posted.id), disposition="catch_up"),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()
  assert result.catch_up is not None
  # The catch-up is the full reversal: every account nets back to zero.
  original = _line_nets(session, str(posted.id))
  reversal = _line_nets(session, result.catch_up.event_id)
  assert original and all(original[k] + reversal.get(k, 0) == 0 for k in original)
  assert sum(abs(v) for v in original.values()) == 2 * AMOUNT


def test_nothing_to_delete_touches_nothing(session):
  posted, _unposted, _published = _seed(session)
  result = apply_deletions(session, [], now=NOW)
  assert (result.voided, result.flagged, result.unmatched) == (0, 0, 0)
  session.refresh(posted)
  assert posted.payload_drift is False
