"""Nothing posts to books a shadow close keeps, except what keeps them equal
to QuickBooks, on real Postgres."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
  ReverseJournalEntryRequest,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
  reverse_journal_entry,
)
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  ShadowLedgerPostingError,
  closed_under_shadow,
)
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef0c"
_WRITEBACK = "robosystems.operations.roboledger.fiscal_calendar.qb_writeback"


@pytest.fixture()
def shadow(two_entities):
  """The parent's QuickBooks connection runs a shadow close; the native
  subsidiary keeps its own books."""
  t = two_entities
  t.session.info["graph_id"] = GRAPH_ID
  t.accounts = {
    entity.id: (
      entity_account(t.session, entity.id, "Cash"),
      entity_account(t.session, entity.id, "Rent"),
    )
    for entity in (t.parent, t.sub)
  }
  t.session.commit()
  with patch(f"{_WRITEBACK}.shadow_ledger", return_value=True):
    yield t


def _entry(t, entity_id, *, status="posted", source=None, mirrors_source=False):
  cash, rent = t.accounts[entity_id]
  return create_journal_entry(
    t.session,
    CreateJournalEntryRequest(
      posting_date=date(2026, 9, 15),
      memo="Rent received",
      status=status,
      source=source,
      line_items=[
        JournalEntryLineItemInput(element_id=cash, debit_amount=50_000),
        JournalEntryLineItemInput(element_id=rent, credit_amount=50_000),
      ],
    ),
    "usr_1",
    entity_id=entity_id,
    mirrors_source=mirrors_source,
  )


def test_an_entry_authored_here_cannot_post_to_shadow_books(shadow):
  with pytest.raises(ShadowLedgerPostingError, match="shadow close"):
    _entry(shadow, shadow.parent.id)
  assert shadow.session.query(Entry).count() == 0


def test_a_draft_is_still_written_for_the_close_to_keep(shadow):
  created = _entry(shadow, shadow.parent.id, status="draft")
  assert created.status == "draft"


def test_synced_history_still_lands(shadow):
  created = _entry(shadow, shadow.parent.id, source="quickbooks")
  assert created.status == "posted"


def test_a_catch_up_that_follows_the_source_still_posts(shadow):
  created = _entry(shadow, shadow.parent.id, mirrors_source=True)
  assert created.status == "posted"


def test_a_native_subsidiary_posts_as_before(shadow):
  created = _entry(shadow, shadow.sub.id)
  assert created.status == "posted"


def test_a_reversal_is_refused_on_shadow_books(shadow):
  synced = _entry(shadow, shadow.parent.id, source="quickbooks")
  shadow.session.commit()

  with pytest.raises(ShadowLedgerPostingError, match="Reversing"):
    reverse_journal_entry(
      shadow.session, ReverseJournalEntryRequest(entry_id=synced.id), "usr_1"
    )
  assert shadow.session.get(Entry, synced.id).status == "posted"


def test_a_session_on_no_graph_is_never_shadow(shadow):
  shadow.session.info.pop("graph_id")
  created = _entry(shadow, shadow.parent.id)
  assert created.status == "posted"


@pytest.mark.parametrize(
  ("receipt", "expected"),
  [
    ({"shadow": True}, True),
    ({"shadow": False}, False),
    ({"version": 1}, None),
  ],
)
def test_a_closed_period_keeps_the_policy_it_closed_under(
  two_entities, receipt, expected
):
  t = two_entities
  t.session.add(
    FiscalPeriod(
      graph_id=GRAPH_ID,
      entity_id=t.parent.id,
      name="2026-09",
      start_date=date(2026, 9, 1),
      end_date=date(2026, 9, 30),
      period_type="month",
      status="closed",
      close_receipt=receipt,
    )
  )
  t.session.commit()

  assert closed_under_shadow(t.session, "2026-09", t.parent.id) is expected
  assert closed_under_shadow(t.session, "2026-10", t.parent.id) is None


def test_the_preview_says_so_before_a_commit_would(shadow):
  from datetime import datetime

  from robosystems.models.api.event_block import CreateEventBlockRequest
  from robosystems.operations.event_block.python_handlers.journal_entry_recorded import (
    JournalEntryRecordedMetadata,
    dispatch_preview,
  )

  cash, rent = shadow.accounts[shadow.parent.id]
  body = CreateEventBlockRequest(
    event_type="journal_entry_recorded",
    event_category="adjustment",
    event_class="economic",
    event_action="transfer",
    source="manual",
    occurred_at=datetime(2026, 9, 15),
    metadata={
      "posting_date": "2026-09-15",
      "memo": "Rent received",
      "status": "posted",
      "line_items": [
        {"element_id": cash, "debit_amount": 50_000},
        {"element_id": rent, "credit_amount": 50_000},
      ],
    },
  )
  preview = dispatch_preview(
    shadow.session,
    body,
    JournalEntryRecordedMetadata.model_validate(body.metadata),
    entity_id=shadow.parent.id,
  )

  assert not preview.would_succeed
  assert any("shadow close" in error for error in preview.validation_errors)
