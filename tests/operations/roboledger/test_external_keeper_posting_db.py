"""Books an external ledger keeps take that ledger's entries and nothing
else, on real Postgres."""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest

from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.operations.roboledger.commands.journal_entries import (
  create_journal_entry,
)
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  MirroredGroupError,
  ShadowLedgerPostingError,
  assert_group_consolidates_here,
  assert_opening_balance_allowed,
)
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef0d"
LEDGER = "netsuite-uk"
_WRITEBACK = "robosystems.operations.roboledger.fiscal_calendar.qb_writeback"


@pytest.fixture()
def mirrored(two_entities):
  """QuickBooks keeps the parent's books and an external ledger the
  subsidiary's, both in shadow."""
  t = two_entities
  t.session.info["graph_id"] = GRAPH_ID
  t.session.info["books_keeper_sources"] = {
    t.parent.id: "quickbooks",
    t.sub.id: LEDGER,
  }
  t.accounts = (
    entity_account(t.session, t.sub.id, "Cash"),
    entity_account(t.session, t.sub.id, "Rent"),
  )
  t.session.commit()
  with patch(f"{_WRITEBACK}.shadow_close_for_entity", return_value=True):
    yield t


def _entry(t, *, source=None, status="posted"):
  cash, rent = t.accounts
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
    entity_id=t.sub.id,
  )


def test_the_ledgers_own_entry_lands_posted(mirrored):
  assert _entry(mirrored, source=LEDGER).status == "posted"


@pytest.mark.parametrize("source", [None, "manual", "netsuite-us", "quickbooks"])
def test_nothing_else_posts_to_the_books_it_keeps(mirrored, source):
  # QuickBooks' own history is exempt wherever it lands; every other source
  # is not this entity's ledger.
  if source == "quickbooks":
    assert _entry(mirrored, source=source).status == "posted"
    return
  with pytest.raises(ShadowLedgerPostingError, match="mirror"):
    _entry(mirrored, source=source)
  assert mirrored.session.query(Entry).count() == 0


def test_the_opening_balance_starts_an_external_mirror_only(mirrored):
  assert_opening_balance_allowed(mirrored.session, mirrored.sub.id)
  with pytest.raises(ShadowLedgerPostingError, match="opening balance"):
    assert_opening_balance_allowed(mirrored.session, mirrored.parent.id)


def test_a_group_mirrored_whole_is_not_consolidated_here(mirrored):
  members = [mirrored.parent.id, mirrored.sub.id]
  with pytest.raises(MirroredGroupError, match="consolidated there"):
    assert_group_consolidates_here(mirrored.session, members)


def test_one_native_member_keeps_consolidation_here(mirrored):
  parent, sub = mirrored.parent.id, mirrored.sub.id
  with patch(
    f"{_WRITEBACK}.shadow_close_for_entity",
    side_effect=lambda _platform, _graph, entity_id: entity_id == parent,
  ):
    mirrored.session.info.pop("books_keepers", None)
    assert_group_consolidates_here(mirrored.session, [parent, sub])
