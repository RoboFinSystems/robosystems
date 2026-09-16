"""The disturb half of the closed-history guard, against a real Postgres.

The reach is a correlated EXISTS over ``line_items`` joined to ``entries`` and
compared to each canonical set's ``period_end`` — a shape a mocked session
cannot exercise, and the one whose boundary (``<=``, landed statuses only,
closed canonical population only) decides whether an ordinary post-close
mapping is wrongly refused or a restatement wrongly allowed.
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import (
  Element,
  FactSet,
  Report,
  Structure,
  Taxonomy,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.taxonomy_block.immutability import (
  ProtectedFactsError,
  assert_history_undisturbed,
  find_protected_fact_sets,
)

pytestmark = pytest.mark.unit

GRAPH = "kg0123456789abcdef01"
JUNE = (date(2026, 6, 1), date(2026, 6, 30))
JULY = (date(2026, 7, 1), date(2026, 7, 31))


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_immut_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session_factory = sessionmaker(bind=engine)
  session = session_factory()
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


def _period(session: Session, name: str, window: tuple[date, date], status: str):
  session.add(
    FiscalPeriod(
      graph_id=GRAPH,
      name=name,
      start_date=window[0],
      end_date=window[1],
      period_type="month",
      status=status,
    )
  )


def _canonical_set(
  session: Session, structure: Structure, window: tuple[date, date], target: Element
) -> FactSet:
  """A close-minted statement set: report_id NULL, scenario NULL, type report."""
  fact_set = FactSet(
    structure_id=structure.id,
    period_start=window[0],
    period_end=window[1],
    factset_type="report",
    entity_id="ent_1",
  )
  fact_set.provenance = {
    "origin": "pivot",
    "mapping_id": "map_1",
    "period": f"{window[0]}/{window[1]}",
  }
  session.add(fact_set)
  session.flush()
  session.add(
    Fact(
      element_id=target.id,
      string_value="stamped",
      fact_type="Nonnumeric",
      value_type="inline",
      period_start=window[0],
      period_end=window[1],
      period_type="duration",
      entity_id="ent_1",
      structure_id=structure.id,
      fact_set_id=fact_set.id,
    )
  )
  return fact_set


def _landed_line(
  session: Session, account: Element, posting_date: date, status: str = "posted"
) -> None:
  entry = Entry(posting_date=posting_date, status=status, created_by="usr_1")
  session.add(entry)
  session.flush()
  session.add(
    LineItem(
      entry_id=entry.id, element_id=account.id, debit_amount=100, credit_amount=0
    )
  )


@pytest.fixture()
def books(ext_session):
  """June closed with a canonical statement set; July open with one too."""
  taxonomy = Taxonomy(name="CoA", taxonomy_type="chart_of_accounts")
  ext_session.add(taxonomy)
  ext_session.flush()
  statement = Structure(
    name="Balance Sheet", block_type="balance_sheet", taxonomy_id=taxonomy.id
  )
  ext_session.add(statement)
  ext_session.flush()
  target = Element(name="rs-gaap:Cash", taxonomy_id=taxonomy.id)
  cash = Element(name="1000 Cash", taxonomy_id=taxonomy.id)
  new_account = Element(name="1010 New Cash", taxonomy_id=taxonomy.id)
  untouched = Element(name="1020 Never Posted", taxonomy_id=taxonomy.id)
  ext_session.add_all([target, cash, new_account, untouched])
  ext_session.flush()

  _period(ext_session, "2026-06", JUNE, "closed")
  _period(ext_session, "2026-07", JULY, "open")
  june = _canonical_set(ext_session, statement, JUNE, target)
  july = _canonical_set(ext_session, statement, JULY, target)

  _landed_line(ext_session, cash, date(2026, 5, 15))  # before June: carries in
  _landed_line(ext_session, new_account, date(2026, 7, 10))  # July only
  ext_session.commit()
  return {
    "taxonomy": taxonomy,
    "statement": statement,
    "target": target,
    "cash": cash,
    "new_account": new_account,
    "untouched": untouched,
    "june": june,
    "july": july,
  }


class TestAccountReach:
  def test_landed_history_before_a_closed_month_reaches_its_stamp(
    self, ext_session, books
  ):
    """`<= period_end`: a May posting is in June's balance, so re-mapping the
    account restates the closed month."""
    protected = find_protected_fact_sets(ext_session, account_ids=[books["cash"].id])
    assert protected.disturbed_fact_set_ids == (books["june"].id,)
    assert protected.closed_period_names == ("2026-06",)
    assert protected.closed_period_fact_set_ids == ()
    assert protected.filed_report_fact_set_ids == ()

  def test_history_only_in_open_months_is_not_reached(self, ext_session, books):
    """The ordinary case: a new account mapped after the last close."""
    protected = find_protected_fact_sets(
      ext_session, account_ids=[books["new_account"].id]
    )
    assert not protected.any

  def test_an_account_with_no_history_is_not_reached(self, ext_session, books):
    protected = find_protected_fact_sets(
      ext_session, account_ids=[books["untouched"].id]
    )
    assert not protected.any

  def test_a_draft_does_not_count_as_history(self, ext_session, books):
    _landed_line(ext_session, books["untouched"], date(2026, 5, 20), status="draft")
    ext_session.commit()
    protected = find_protected_fact_sets(
      ext_session, account_ids=[books["untouched"].id]
    )
    assert not protected.any

  def test_a_reversed_original_still_counts_as_history(self, ext_session, books):
    _landed_line(ext_session, books["untouched"], date(2026, 5, 20), status="reversed")
    ext_session.commit()
    protected = find_protected_fact_sets(
      ext_session, account_ids=[books["untouched"].id]
    )
    assert protected.disturbed_fact_set_ids == (books["june"].id,)

  def test_a_reopened_month_is_no_longer_reached(self, ext_session, books):
    """Reopen flips the period to `closing`; its sets are mutable again."""
    ext_session.query(FiscalPeriod).filter(FiscalPeriod.name == "2026-06").update(
      {"status": "closing"}
    )
    ext_session.commit()
    protected = find_protected_fact_sets(ext_session, account_ids=[books["cash"].id])
    assert not protected.any


class TestDisturbPopulation:
  def test_a_filed_snapshot_is_outside_the_disturb_population(self, ext_session, books):
    """A report is a snapshot by contract and is expected to drift from the
    live render; only the close-minted sets are protected against disturbance."""
    report = Report(
      name="June pack",
      taxonomy_id=books["taxonomy"].id,
      filing_status="filed",
      created_by="usr_1",
    )
    ext_session.add(report)
    ext_session.flush()
    books["june"].report_id = report.id
    ext_session.commit()

    protected = find_protected_fact_sets(ext_session, account_ids=[books["cash"].id])
    assert not protected.any

  def test_a_scenario_set_is_outside_the_disturb_population(self, ext_session, books):
    """A forecast scenario's sets carry `scenario_id` (a Structure FK); they
    never become actuals just because the base month is closed."""
    books["june"].scenario_id = books["statement"].id
    ext_session.commit()

    protected = find_protected_fact_sets(ext_session, account_ids=[books["cash"].id])
    assert not protected.any


class TestSemanticReach:
  def test_a_stamped_target_is_reached(self, ext_session, books):
    """Flipping `balance_type` on an element the stamp holds a fact on re-signs
    that fact at read time."""
    protected = find_protected_fact_sets(
      ext_session, semantic_element_ids=[books["target"].id]
    )
    assert protected.disturbed_fact_set_ids == (books["june"].id,)

  def test_a_pivot_source_is_reached(self, ext_session, books):
    """The same change on a chart account re-signs the pivot it feeds."""
    protected = find_protected_fact_sets(
      ext_session, semantic_element_ids=[books["cash"].id]
    )
    assert protected.disturbed_fact_set_ids == (books["june"].id,)

  def test_an_element_in_neither_role_is_not_reached(self, ext_session, books):
    protected = find_protected_fact_sets(
      ext_session, semantic_element_ids=[books["untouched"].id]
    )
    assert not protected.any


class TestAssertion:
  def test_the_refusal_names_the_months_and_the_order(self, ext_session, books):
    with pytest.raises(ProtectedFactsError) as excinfo:
      assert_history_undisturbed(ext_session, account_ids=[books["cash"].id])
    err = excinfo.value
    assert err.disturbed_count == 1
    assert err.closed_period_count == 0
    assert err.closed_period_names == ("2026-06",)
    assert "refusing to disturb" in str(err)
    assert "reopen latest-first down to 2026-06" in str(err)

  def test_destroy_and_disturb_report_separately(self, ext_session, books):
    """Deleting the target element destroys June's stamp (closed population)
    and, being a stamped target, also disturbs it — both are named."""
    with pytest.raises(ProtectedFactsError) as excinfo:
      assert_history_undisturbed(
        ext_session,
        element_ids=[books["target"].id],
        semantic_element_ids=[books["target"].id],
      )
    err = excinfo.value
    assert err.closed_period_count == 1
    assert err.disturbed_count == 1
    assert "refusing to delete" in str(err)

  def test_nothing_reached_is_silent(self, ext_session, books):
    assert_history_undisturbed(
      ext_session,
      account_ids=[books["new_account"].id],
      semantic_element_ids=[books["untouched"].id],
    )
