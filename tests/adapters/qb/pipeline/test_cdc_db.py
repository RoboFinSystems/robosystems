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
  CONNECTION_ID,
  GRAPH_ID,
  QB_TXN,
  _line_nets,
  _post_synced_event_with_id,
  _qb_accounts,
  _qb_expense,
  _qb_item,
  _qb_sync,
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
    # The shape write-back records: the external id the synced copy carries.
    "qb_external_id": "JournalEntry_900,JournalEntry_901",
    "qb_entry_ids": {"e1": "JournalEntry_900", "e2": "JournalEntry_901"},
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


def test_ids_are_matched_per_entity_type(session):
  """QuickBooks ids are unique per entity only: deleting Invoice 900 must not
  touch the published entry QuickBooks holds as JournalEntry 900, and
  deleting Bill 1737 must not touch the Expense the report calls 1737."""
  posted, _unposted, published = _seed(session)

  result = apply_deletions(
    session,
    [
      {"entity": "Invoice", "id": "900", "last_updated": None},
      {"entity": "Bill", "id": "1737", "last_updated": None},
    ],
    now=NOW,
  )

  assert (result.flagged, result.voided, result.unmatched) == (0, 0, 2)
  session.refresh(posted)
  session.refresh(published)
  assert posted.payload_drift is False and published.payload_drift is False


def test_a_label_the_map_does_not_list_is_found_by_suffix_and_learned(session):
  _seed(session)
  odd = _post_synced_event_with_id(session, "Bank Deposit_55")
  session.flush()

  result = apply_deletions(
    session, [{"entity": "Deposit", "id": "55", "last_updated": None}], now=NOW
  )

  assert result.flagged == 1 and result.unmatched == 0
  assert result.observed_labels == {"Deposit": ["Bank Deposit"]}
  session.refresh(odd)
  assert odd.payload_drift is True


def test_a_suffix_match_never_voids_an_unposted_event(session):
  # Ids are unique per type only: Invoice 57 deleted before it synced must
  # not void an unlisted type's unposted transaction 57.
  _seed(session)
  other = _post_synced_event_with_id(session, "Sales Tax Payment_57")
  other.status = "captured"
  session.flush()

  result = apply_deletions(
    session, [{"entity": "Invoice", "id": "57", "last_updated": None}], now=NOW
  )

  assert (result.voided, result.skipped) == (0, 1)
  session.refresh(other)
  assert other.status == "captured"


def test_a_plan_applied_twice_applies_once(session):
  posted, unposted, _published = _seed(session)
  deletions = [
    {"entity": "Purchase", "id": "1737", "last_updated": None},
    {"entity": "Purchase", "id": "1738", "last_updated": None},
  ]
  first = apply_deletions(session, deletions, now=NOW)
  session.flush()
  session.refresh(posted)
  stamp = posted.metadata_["drift_detected_at"]

  second = apply_deletions(session, deletions, now=datetime(2026, 10, 9, tzinfo=UTC))

  assert (first.voided, first.flagged) == (1, 1)
  assert (second.voided, second.flagged, second.already_applied) == (0, 0, 2)
  session.refresh(posted)
  session.refresh(unposted)
  assert posted.metadata_["drift_detected_at"] == stamp
  assert unposted.status == "voided"


def test_a_resolved_deletion_is_never_flagged_again(session):
  posted, _unposted, _published = _seed(session)
  deletions = [{"entity": "Purchase", "id": "1737", "last_updated": None}]
  apply_deletions(session, deletions, now=NOW)
  session.flush()
  resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(event_id=str(posted.id), disposition="catch_up"),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()

  again = apply_deletions(session, deletions, now=datetime(2026, 10, 9, tzinfo=UTC))

  assert again.already_applied == 1 and again.flagged == 0
  session.refresh(posted)
  assert posted.payload_drift is False


# ───────────────────────────────────────────────────────────────────────────
# Voids: QuickBooks keeps a voided transaction and zeroes its amounts
# ───────────────────────────────────────────────────────────────────────────


def _qb_void(sync_token: str) -> dict:
  """The voided expense as the next sync hands it over: the same id, a newer
  SyncToken, and every line zeroed."""
  voided = _qb_expense(0, sync_token)
  for line in voided["line_items"]:
    line["debit_amount"] = 0
    line["credit_amount"] = 0
  return voided


def _load(db, payload: dict):
  from robosystems.operations.extensions.loader import OLTPLoader

  result = OLTPLoader()._capture_transactions_as_events(
    db,
    payload,
    source="quickbooks",
    connection_id=CONNECTION_ID,
    created_by="sync",
    now=NOW,
  )
  db.flush()
  return result


def test_a_void_reaches_a_posted_event_as_a_retraction(session):
  ids = _qb_accounts(session)
  _qb_sync(session, 1000, "0")
  item = _qb_item(session)
  assert item.status in ("committed", "fulfilled") and item.payload_drift is False

  result = _load(session, _qb_void("1"))

  assert result.dropped_empty_transactions == 0
  item = _qb_item(session)
  assert item.payload_drift is True
  accepted = item.metadata_["drift_payload"]
  assert accepted["entries"] == [] and accepted["source_removed"] is True
  assert accepted["source_removed_transaction_ids"] == ["77"]
  assert item.metadata_["qb_sync_token"] == "1"

  # Catch-up reverses it in full, as for a deletion.
  catch_up = resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(event_id=str(item.id), disposition="catch_up"),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()
  assert catch_up.catch_up is not None
  original = _line_nets(session, str(item.id))
  reversal = _line_nets(session, catch_up.catch_up.event_id)
  assert original and all(original[k] + reversal.get(k, 0) == 0 for k in original)
  assert ids


def test_a_void_resynced_is_applied_once(session):
  _qb_accounts(session)
  _qb_sync(session, 1000, "0")
  _load(session, _qb_void("1"))
  stamp = _qb_item(session).metadata_["drift_detected_at"]

  again = _load(session, _qb_void("1"))

  assert again.skipped_same_sync_token == 1
  assert _qb_item(session).metadata_["drift_detected_at"] == stamp


def test_a_void_reaches_an_unposted_event_as_a_void(session):
  _qb_accounts(session)
  _qb_sync(session, 1000, "0")
  item = _qb_item(session)
  item.status = "classified"
  session.flush()

  _load(session, _qb_void("1"))

  item = _qb_item(session)
  assert item.status == "voided"
  assert item.metadata_["void_reason"] == "voided_in_quickbooks"
  assert item.metadata_["source_removed_transaction_ids"] == ["77"]


def test_a_zero_line_transaction_never_seen_is_still_dropped(session):
  from robosystems.models.extensions.roboledger.event import Event

  _qb_accounts(session)

  result = _load(session, _qb_void("0"))

  assert result.dropped_empty_transactions == 1
  assert session.query(Event).filter(Event.external_id == QB_TXN).count() == 0


def test_cdc_plans_a_voided_row_as_a_retraction():
  from robosystems.adapters.quickbooks.pipeline.cdc import plan_cdc

  class _Client:
    def cdc(self, _since, _entities):
      return (
        {
          "Purchase": [
            {
              "Id": "77",
              "TxnDate": "2026-07-09",
              "TotalAmt": 0,
              "PrivateNote": "Voided",
              "MetaData": {"LastUpdatedTime": "2026-10-08T03:00:00Z"},
            },
            {"Id": "78", "TxnDate": "2026-07-09", "TotalAmt": 0},
          ],
        },
        False,
      )

  plan = plan_cdc(_Client(), datetime(2026, 10, 7, tzinfo=UTC), "2026-08-10", now=NOW)

  assert plan.deletions == [
    {
      "entity": "Purchase",
      "id": "77",
      "last_updated": "2026-10-08T03:00:00Z",
      "voided": True,
    }
  ]


def test_a_void_from_cdc_and_from_the_report_applies_once(session):
  _qb_accounts(session)
  _qb_sync(session, 1000, "0")
  _load(session, _qb_void("1"))

  result = apply_deletions(
    session,
    [{"entity": "Purchase", "id": "77", "last_updated": None, "voided": True}],
    now=NOW,
  )

  assert (result.flagged, result.already_applied) == (0, 1)


def test_a_voided_published_entry_is_left_to_the_round_trip_matcher(session):
  """The voided entry keeps its row in the JournalReport, where the matcher
  retracts it; CDC retracting it too would reverse it twice."""
  _posted, _unposted, published = _seed(session)

  result = apply_deletions(
    session,
    [{"entity": "JournalEntry", "id": "901", "last_updated": None, "voided": True}],
    now=NOW,
  )

  assert (result.flagged, result.voided) == (0, 0)
  session.refresh(published)
  assert published.payload_drift is False
