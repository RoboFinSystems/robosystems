"""The closing book's sidebar against real Postgres: where reconciliations
appear in it."""

from __future__ import annotations

import pytest

from robosystems.operations.roboledger.reads.closing_book import (
  get_closing_book_structures,
)

pytestmark = pytest.mark.unit


def _labels(session) -> list[str]:
  return [c.label for c in get_closing_book_structures(session).categories]


def test_a_ledger_with_books_lists_reconciliations_before_the_trial_balance(
  books, ext_session
):
  response = get_closing_book_structures(ext_session)

  assert _labels(ext_session)[-2:] == ["Reconciliations", "Trial Balance"]
  (item,) = next(c for c in response.categories if c.label == "Reconciliations").items
  assert (item.id, item.item_type) == ("reconciliations", "reconciliations")


def test_a_ledger_with_nothing_posted_has_nothing_to_reconcile(ext_session):
  assert _labels(ext_session) == ["Period Close"]
