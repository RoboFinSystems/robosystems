"""A remap judged on a real Postgres: within its primary statement it is a
reclassification the closed months allow; across one, or onto a concept no
statement presents, a restatement they refuse.

The judgement reads the style's networks (presentation arcs), the rs-gaap
calculation DAG (for synthesized detail the renderer absorbs into a parent)
and the closed-history guard, so it needs the rows, not a mock.
"""

from __future__ import annotations

import pytest

from robosystems.models.extensions import Element
from robosystems.operations.taxonomy_block.reclassification import (
  Reclassification,
  ReclassificationRefusedError,
  judge_remap,
  reporting_style_for_account,
  statement_of_concepts,
)
from tests.closed_books import STYLE, ext_schema_session, reclassification_books

pytestmark = pytest.mark.unit


@pytest.fixture()
def ext_session():
  yield from ext_schema_session()


@pytest.fixture()
def books(ext_session):
  return reclassification_books(ext_session)


def _judge(session, books, account: Element, old: Element, new: Element):
  return judge_remap(
    session,
    account_id=account.id,
    account_taxonomy_id=str(account.taxonomy_id),
    account_qname=account.qname,
    old_target_id=old.id,
    new_target_id=new.id,
  )


def test_a_move_within_the_balance_sheet_is_a_reclassification(ext_session, books):
  judged = _judge(ext_session, books, books["card"], books["debt"], books["other"])

  assert judged == Reclassification(
    statement_type="balance_sheet", closed_periods=("2026-06",)
  )


def test_a_move_across_statements_is_refused_naming_the_months(ext_session, books):
  with pytest.raises(ReclassificationRefusedError) as err:
    _judge(ext_session, books, books["card"], books["debt"], books["rent"])

  message = str(err.value)
  assert "coa:2110 from the balance sheet to the income statement" in message
  assert "a restatement, not a reclassification" in message
  assert "reopen latest-first down to 2026-06" in message
  assert err.value.protected.closed_period_names == ("2026-06",)


def test_a_concept_no_statement_presents_is_refused(ext_session, books):
  with pytest.raises(ReclassificationRefusedError, match="on no primary statement"):
    _judge(ext_session, books, books["card"], books["debt"], books["orphan"])


def test_an_account_with_no_closed_history_is_an_ordinary_write(ext_session, books):
  assert (
    _judge(ext_session, books, books["newer"], books["debt"], books["rent"]) is None
  )


def test_synthesized_detail_sits_where_its_calculation_parent_does(ext_session, books):
  placed = statement_of_concepts(
    ext_session,
    STYLE,
    [books["ppe_gross"].id, books["rent"].id, books["orphan"].id],
  )

  assert placed == {
    books["ppe_gross"].id: "balance_sheet",
    books["rent"].id: "income_statement",
    books["orphan"].id: None,
  }


def test_the_style_is_the_chart_owners(ext_session, books):
  assert reporting_style_for_account(ext_session, str(books["chart"].id)) == STYLE
