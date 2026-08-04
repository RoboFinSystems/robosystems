"""Account rollups read — balance filtering against a real Postgres.

The rollup SQL once carried its `status = 'posted'` and posting-date
predicates on a `LEFT JOIN ... ON` clause, where they filter nothing:
line items from draft or out-of-window entries still landed in the
sums, so the lead schedule disagreed with the trial balance for the
same mapping and window. These tests run the real SQL against a real
Postgres schema — the defect class is invisible to MagicMock sessions.
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Association, Element, Structure, Taxonomy
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.roboledger.reads.account_rollups import (
  get_account_rollups,
)

pytestmark = pytest.mark.unit

JUNE_START = date(2026, 6, 1)
JUNE_END = date(2026, 6, 30)


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_rollup_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))

  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


@pytest.fixture()
def seeded_mapping(ext_session):
  """A CoA mapping with posted, draft, and out-of-window activity.

  - Sales (4000) -> Revenue: $8,500 posted June credit, $412,000 posted
    May credit (out of window), $9,999 draft June credit.
  - Equipment (1500) -> PP&E: mapped, zero activity.
  """
  taxonomy = Taxonomy(name="Mapping", taxonomy_type="mapping")
  ext_session.add(taxonomy)
  ext_session.flush()

  mapping = Structure(
    name="QB to rs-gaap", block_type="coa_mapping", taxonomy_id=taxonomy.id
  )
  ext_session.add(mapping)
  ext_session.flush()

  sales = Element(name="Sales", code="4000", balance_type="credit")
  revenue = Element(name="Revenue", qname="rs-gaap:Revenue", balance_type="credit")
  equipment = Element(name="Equipment", code="1500", balance_type="debit")
  ppe = Element(name="PP&E", qname="rs-gaap:PPE", balance_type="debit")
  ext_session.add_all([sales, revenue, equipment, ppe])
  ext_session.flush()

  ext_session.add_all(
    [
      Association(
        structure_id=mapping.id,
        from_element_id=sales.id,
        to_element_id=revenue.id,
        association_type="mapping",
      ),
      Association(
        structure_id=mapping.id,
        from_element_id=equipment.id,
        to_element_id=ppe.id,
        association_type="mapping",
      ),
    ]
  )

  def _entry_with_line(posting_date: date, status: str, credit_cents: int) -> None:
    entry = Entry(
      posting_date=posting_date,
      status=status,
      created_by="usr_test",
    )
    ext_session.add(entry)
    ext_session.flush()
    ext_session.add(
      LineItem(
        entry_id=entry.id,
        element_id=sales.id,
        debit_amount=0,
        credit_amount=credit_cents,
      )
    )

  _entry_with_line(date(2026, 6, 15), "posted", 850_000)  # $8,500 in window
  _entry_with_line(date(2026, 5, 20), "posted", 41_200_000)  # $412,000 prior
  _entry_with_line(date(2026, 6, 16), "draft", 999_900)  # $9,999 draft
  ext_session.flush()

  return mapping


def _row_by_account(response, account_code: str):
  for group in response.groups:
    for row in group.accounts:
      if row.account_code == account_code:
        return row
  return None


class TestAccountRollupBalanceFiltering:
  def test_window_excludes_drafts_and_prior_periods(self, ext_session, seeded_mapping):
    """June window must report June's posted $8,500 — not $420,500 + drafts."""
    response = get_account_rollups(
      ext_session,
      mapping_id=str(seeded_mapping.id),
      start_date=JUNE_START,
      end_date=JUNE_END,
    )

    sales = _row_by_account(response, "4000")
    assert sales is not None
    assert sales.total_credits == 8_500.0
    assert sales.total_debits == 0.0
    assert sales.net_balance == 8_500.0

  def test_unwindowed_call_still_excludes_drafts(self, ext_session, seeded_mapping):
    """No window: all posted activity sums, drafts still excluded."""
    response = get_account_rollups(ext_session, mapping_id=str(seeded_mapping.id))

    sales = _row_by_account(response, "4000")
    assert sales is not None
    assert sales.total_credits == 420_500.0

  def test_zero_activity_account_stays_visible(self, ext_session, seeded_mapping):
    """The lead schedule shows every mapped account, activity or not."""
    response = get_account_rollups(
      ext_session,
      mapping_id=str(seeded_mapping.id),
      start_date=JUNE_START,
      end_date=JUNE_END,
    )

    equipment = _row_by_account(response, "1500")
    assert equipment is not None
    assert equipment.total_debits == 0.0
    assert equipment.total_credits == 0.0
    assert equipment.net_balance == 0.0

  def test_agrees_with_trial_balance_shape(self, ext_session, seeded_mapping):
    """Same predicate semantics as get_trial_balance for the same window."""
    from robosystems.operations.roboledger.reads.trial_balance import (
      get_trial_balance,
    )

    rollups = get_account_rollups(
      ext_session,
      mapping_id=str(seeded_mapping.id),
      start_date=JUNE_START,
      end_date=JUNE_END,
    )
    tb = get_trial_balance(ext_session, start_date=JUNE_START, end_date=JUNE_END)

    sales_rollup = _row_by_account(rollups, "4000")
    sales_tb = next(r for r in tb.rows if r.account_code == "4000")
    assert sales_rollup.total_credits == sales_tb.total_credits
    assert sales_rollup.total_debits == sales_tb.total_debits
