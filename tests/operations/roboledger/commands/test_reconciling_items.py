"""Reconciling-item logic that needs no database.

The behaviour that depends on SQL — the deletes, the JSONB comparison, the
gate's join — is covered against a real database in
``test_reconciling_items_db.py``. What is left here is the request contract
and the shapes the resolver builds, which are worth pinning cheaply.
"""

from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from robosystems.models.api.extensions.reconciling_items import (
  ReconcilingItemDeltaLine,
  ReconcilingItemPlan,
  ResolveReconcilingItemRequest,
)
from robosystems.operations.roboledger.commands.reconciling_items import (
  RECONCILIATION_HISTORY_KEY,
  _accepted_metadata,
  _catch_up_request,
  _net_accepted_lines,
)

pytestmark = pytest.mark.unit


class _Event:
  """The two fields the catch-up builder reads off the flagged event."""

  def __init__(self, external_id: str | None = "Expense_1737"):
    self.id = "evt_original"
    self.external_id = external_id
    self.source = "quickbooks"


def _plan(delta: list[ReconcilingItemDeltaLine]) -> ReconcilingItemPlan:
  return ReconcilingItemPlan(
    event_id="evt_original",
    external_id="Expense_1737",
    source="quickbooks",
    event_type="journal_entry_recorded",
    event_status="fulfilled",
    default_disposition="catch_up",
    delta=delta,
    no_gl_effect=not delta,
  )


class TestRequestContract:
  def test_acknowledge_requires_a_note(self):
    """It is the only record of how the difference was handled — the
    resolution writes no entry to explain itself."""
    with pytest.raises(ValidationError, match="requires a note"):
      ResolveReconcilingItemRequest(event_id="evt_1", disposition="acknowledge")

  def test_whitespace_is_not_a_note(self):
    with pytest.raises(ValidationError, match="requires a note"):
      ResolveReconcilingItemRequest(
        event_id="evt_1", disposition="acknowledge", note="   "
      )

  def test_the_other_dispositions_do_not_need_one(self):
    for disposition in ("restate", "catch_up"):
      body = ResolveReconcilingItemRequest(event_id="evt_1", disposition=disposition)
      assert body.note is None

  def test_disposition_may_be_omitted_to_take_the_default(self):
    body = ResolveReconcilingItemRequest(event_id="evt_1")
    assert body.disposition is None
    # A catch-up is drafted for review by default rather than posted.
    assert body.status == "draft"


class TestDeltaMath:
  def test_lines_net_per_account_debit_positive(self):
    payload = {
      "entries": [
        {
          "line_items": [
            {"element_external_id": "152", "debit_amount": 0, "credit_amount": 5360},
            {"element_external_id": "25", "debit_amount": 5360, "credit_amount": 0},
          ]
        }
      ]
    }
    assert _net_accepted_lines(payload) == {
      (None, "152"): -5360,
      (None, "25"): 5360,
    }

  def test_repeated_accounts_across_entries_net_together(self):
    payload = {
      "entries": [
        {"line_items": [{"element_id": "el_1", "debit_amount": 100}]},
        {"line_items": [{"element_id": "el_1", "credit_amount": 40}]},
      ]
    }
    assert _net_accepted_lines(payload) == {("el_1", None): 60}

  def test_a_flat_payload_is_read_as_a_single_entry(self):
    payload = {
      "posting_date": "2026-07-09",
      "line_items": [
        {"element_id": "el_1", "debit_amount": 100},
        {"element_id": "el_2", "credit_amount": 100},
      ],
    }
    assert _net_accepted_lines(payload) == {("el_1", None): 100, ("el_2", None): -100}


class TestAcceptedMetadata:
  def test_the_accepted_payload_replaces_the_live_one_entirely(self):
    """Carrying anything else forward would differ from every future
    incoming payload and re-raise the item on every sync."""
    live = {
      "entries": ["old"],
      "drift_payload": {"entries": ["new"]},
      "drift_detected_at": "2026-08-20T03:32:00",
      "dispatch_attempts": 3,
    }
    accepted = {"entries": ["new"], "qb_sync_token": "7"}

    result = _accepted_metadata(live=live, accepted=accepted, record={"d": 1})

    assert result["entries"] == ["new"]
    assert "drift_payload" not in result
    assert "drift_detected_at" not in result
    assert "dispatch_attempts" not in result
    assert result[RECONCILIATION_HISTORY_KEY] == [{"d": 1}]

  def test_the_trail_accumulates_rather_than_overwriting(self):
    live = {RECONCILIATION_HISTORY_KEY: [{"disposition": "catch_up"}]}
    result = _accepted_metadata(
      live=live, accepted={}, record={"disposition": "restate"}
    )
    assert [r["disposition"] for r in result[RECONCILIATION_HISTORY_KEY]] == [
      "catch_up",
      "restate",
    ]


class TestCatchUpEntry:
  def _request(self, delta, **kwargs):
    return _catch_up_request(
      event=_Event(),
      plan=_plan(delta),
      posting_date=date(2026, 8, 31),
      status=kwargs.get("status", "draft"),
      note=kwargs.get("note"),
    )

  def test_a_positive_delta_becomes_a_debit_and_a_negative_one_a_credit(self):
    body = self._request(
      [
        ReconcilingItemDeltaLine(
          element_id="el_new", prior_net=0, accepted_net=5360, delta=5360
        ),
        ReconcilingItemDeltaLine(
          element_id="el_old", prior_net=5360, accepted_net=0, delta=-5360
        ),
      ]
    )

    lines = {line["element_id"]: line for line in body.metadata["line_items"]}
    assert lines["el_new"]["debit_amount"] == 5360
    assert lines["el_new"]["credit_amount"] == 0
    assert lines["el_old"]["credit_amount"] == 5360
    assert lines["el_old"]["debit_amount"] == 0

  def test_the_entry_is_pinned_to_the_local_lane(self):
    """Both halves say it: a source that does not publish, and the explicit
    flag. The entry mirrors a change the source system already has."""
    body = self._request(
      [
        ReconcilingItemDeltaLine(
          element_id="el_new", prior_net=0, accepted_net=1, delta=1
        )
      ]
    )

    assert body.source == "system"
    assert body.metadata["publish_to_source"] is False
    # No routing connection, so nothing downstream can pick one up either.
    assert "connection_id" not in body.metadata

  def test_it_points_back_at_the_item_it_resolves(self):
    body = self._request(
      [
        ReconcilingItemDeltaLine(
          element_id="el_new", prior_net=0, accepted_net=1, delta=1
        )
      ]
    )
    assert body.metadata["reconciles_event_id"] == "evt_original"
    assert "Expense_1737" in body.metadata["memo"]

  def test_the_note_rides_on_the_memo(self):
    body = self._request(
      [
        ReconcilingItemDeltaLine(
          element_id="el_new", prior_net=0, accepted_net=1, delta=1
        )
      ],
      note="reclassed to COGS per policy",
    )
    assert "reclassed to COGS per policy" in body.metadata["memo"]

  def test_unmapped_accounts_are_left_out_rather_than_posted_blind(self):
    body = self._request(
      [
        ReconcilingItemDeltaLine(
          element_id=None,
          element_external_id="999",
          prior_net=0,
          accepted_net=100,
          delta=100,
        ),
        ReconcilingItemDeltaLine(
          element_id="el_known", prior_net=0, accepted_net=100, delta=100
        ),
      ]
    )
    assert [line["element_id"] for line in body.metadata["line_items"]] == ["el_known"]
