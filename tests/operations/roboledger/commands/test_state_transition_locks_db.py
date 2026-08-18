"""DB-backed proof of the remaining roboledger state-transition locks.

Three transitions had the shape `operations/locking.py` exists for — load a
row, branch on a state column, write it back — with nothing held across the
decision: reversing a journal entry, closing or reopening a fiscal period, and
filing a report. Mock sessions can prove a keyword was passed; only a second
real session proves a lock blocks.

Runs against the real extensions database with a throwaway tenant schema, the
same mechanism `create_tenant_schema` uses.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.extensions.journal_entries import (
  ReverseJournalEntryRequest,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands.journal_entries import (
  reverse_journal_entry,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd04"
ENTRY_ID = "je_lock_0001"
PERIOD = "2026-01"


def _tenant_tables():
  return [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]


@pytest.fixture(scope="module")
def tenant():
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  engine = create_engine(url)
  try:
    with engine.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    engine.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")

  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=_tenant_tables(),
      )
      # Migration 0032 creates this per tenant; metadata.create_all builds it
      # from the model, so asserting it exists here also checks the two have
      # not drifted apart.
      present = conn.execute(
        text(
          "SELECT 1 FROM pg_indexes WHERE schemaname = :s "
          "AND indexname = 'uq_entries_one_reversal_per_original'"
        ),
        {"s": GRAPH},
      ).scalar()
      assert present, "the one-reversal-per-entry index is missing from the model"
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.fixture(autouse=True)
def posted_entry(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
    session.execute(
      text("DELETE FROM fiscal_periods WHERE graph_id = :g"), {"g": GRAPH}
    )
    session.add(
      Entry(
        id=ENTRY_ID,
        type="standard",
        status="posted",
        posting_date=date(2026, 1, 15),
        memo="Original",
        posted_at=datetime(2026, 1, 15, tzinfo=UTC),
        created_by="usr_seed",
      )
    )
  yield


class TestReversalLock:
  def test_a_locked_entry_cannot_be_reversed(self, tenant):
    """The race that double-posts: two reversals both read 'posted'."""
    with extensions_session(GRAPH) as holder:
      holder.execute(
        text("SELECT id FROM entries WHERE id = :id FOR UPDATE"), {"id": ENTRY_ID}
      )
      with extensions_session(GRAPH) as reverser:
        with pytest.raises(RowLockedError, match=ENTRY_ID):
          reverse_journal_entry(
            reverser, ReverseJournalEntryRequest(entry_id=ENTRY_ID), "usr_op"
          )

    with extensions_session(GRAPH) as check:
      assert check.get(Entry, ENTRY_ID).status == "posted"
      assert (
        check.execute(
          text("SELECT COUNT(*) FROM entries WHERE reversal_of = :id"),
          {"id": ENTRY_ID},
        ).scalar()
        == 0
      )

  def test_the_database_refuses_a_second_reversal(self, tenant):
    """The lock makes the ordinary race a clean 409; this is the guarantee
    that does not depend on anyone remembering to take it."""
    with extensions_session(GRAPH) as session:
      session.add(
        Entry(
          id="je_rev_a",
          type="reversing",
          status="posted",
          posting_date=date(2026, 1, 16),
          reversal_of=ENTRY_ID,
          created_by="usr_op",
        )
      )

    with pytest.raises(IntegrityError):
      with extensions_session(GRAPH) as session:
        session.add(
          Entry(
            id="je_rev_b",
            type="reversing",
            status="posted",
            posting_date=date(2026, 1, 17),
            reversal_of=ENTRY_ID,
            created_by="usr_op",
          )
        )

  def test_entries_that_are_not_reversals_are_unconstrained(self, tenant):
    """`reversal_of IS NULL` on every ordinary entry — the index is partial
    precisely so those stay unaffected."""
    with extensions_session(GRAPH) as session:
      for i in range(3):
        session.add(
          Entry(
            id=f"je_plain_{i}",
            type="standard",
            status="posted",
            posting_date=date(2026, 1, 20),
            created_by="usr_op",
          )
        )

    with extensions_session(GRAPH) as check:
      assert (
        check.execute(
          text("SELECT COUNT(*) FROM entries WHERE reversal_of IS NULL")
        ).scalar()
        >= 3
      )


class TestPeriodTransitionLock:
  """Two concurrent reopens cannot both retract the same month's statements.

  This does **not** cover reopen-vs-close: `close_period` cannot hold a
  transaction-scoped lock, because its QB pre-publish commits mid-close. That
  gap is recorded in the close's own comment and in the spec, not papered over
  with a test that would imply otherwise.
  """

  @pytest.fixture(autouse=True)
  def closed_period(self, tenant):
    with extensions_session(GRAPH) as session:
      session.add(
        FiscalPeriod(
          graph_id=GRAPH,
          name=PERIOD,
          start_date=date(2026, 1, 1),
          end_date=date(2026, 1, 31),
          period_type="monthly",
          status="closed",
          closed_at=datetime(2026, 2, 1, tzinfo=UTC),
          closed_by="usr_seed",
        )
      )
    yield

  def test_a_locked_period_cannot_be_reopened(self, tenant):
    from robosystems.operations.roboledger.commands.fiscal_calendar import (
      reopen_period,
    )

    with extensions_session(GRAPH) as holder:
      holder.execute(
        text(
          "SELECT name FROM fiscal_periods WHERE graph_id = :g AND name = :n FOR UPDATE"
        ),
        {"g": GRAPH, "n": PERIOD},
      )
      with extensions_session(GRAPH) as reopener:
        with pytest.raises(RowLockedError, match=PERIOD):
          reopen_period(
            reopener,
            MagicMock(),  # platform_db — unreached; the lock blocks first
            GRAPH,
            PERIOD,
            actor_id="usr_op",
            reason="correction",
            note=None,
            service=MagicMock(),
          )

    with extensions_session(GRAPH) as check:
      row = (
        check.query(FiscalPeriod)
        .filter(FiscalPeriod.graph_id == GRAPH, FiscalPeriod.name == PERIOD)
        .one()
      )
      assert row.status == "closed"
      assert row.closed_at is not None


class TestAlreadyReversed:
  """A schedule with `auto_reverse` creates the reversal at generation time and
  leaves the original's status untouched, so `status == 'posted'` passes on an
  entry that already has one. That is a reachable state with no race in it."""

  def test_second_reversal_raises_a_domain_error(self, tenant):
    from robosystems.operations.roboledger.commands.journal_entries import (
      JournalEntryAlreadyReversedError,
    )

    with extensions_session(GRAPH) as session:
      session.add(
        Entry(
          id="je_auto_rev",
          type="reversing",
          status="draft",
          posting_date=date(2026, 2, 1),
          reversal_of=ENTRY_ID,
          provenance="schedule_derived",
          created_by="usr_schedule",
        )
      )

    with extensions_session(GRAPH) as session:
      with pytest.raises(JournalEntryAlreadyReversedError) as exc:
        reverse_journal_entry(
          session, ReverseJournalEntryRequest(entry_id=ENTRY_ID), "usr_op"
        )
      assert exc.value.reversing_entry_id == "je_auto_rev"

    # No second reversal, and the constraint never had to be the one to say so.
    with extensions_session(GRAPH) as check:
      assert (
        check.execute(
          text("SELECT COUNT(*) FROM entries WHERE reversal_of = :id"),
          {"id": ENTRY_ID},
        ).scalar()
        == 1
      )


class TestFileReportLock:
  """`filed_by` and `filed_at` are what an auditor reads; last-writer-wins is
  not good enough for them."""

  REPORT_ID = "rpt_lock_0001"

  @pytest.fixture(autouse=True)
  def draft_report(self, tenant):
    from robosystems.models.extensions import Taxonomy
    from robosystems.models.extensions.roboledger.report import Report

    with extensions_session(GRAPH) as session:
      session.execute(text("DELETE FROM reports"))
      session.execute(text("DELETE FROM taxonomies"))
      session.add(
        Taxonomy(
          id="tax_rpt_0001",
          name="Reporting",
          taxonomy_type="reporting_standard",
          created_by="usr_seed",
        )
      )
      session.flush()
      session.add(
        Report(
          id=self.REPORT_ID,
          taxonomy_id="tax_rpt_0001",
          name="Q1",
          period_start=date(2026, 1, 1),
          period_end=date(2026, 3, 31),
          filing_status="draft",
          generation_status="complete",
          created_by="usr_seed",
        )
      )
    yield

  def test_a_locked_report_cannot_be_filed(self, tenant):
    from robosystems.operations.roboledger.commands.reports import file_report

    with extensions_session(GRAPH) as holder:
      holder.execute(
        text("SELECT id FROM reports WHERE id = :id FOR UPDATE"),
        {"id": self.REPORT_ID},
      )
      with extensions_session(GRAPH) as filer:
        with pytest.raises(RowLockedError, match=self.REPORT_ID):
          file_report(filer, self.REPORT_ID, "usr_op")

    with extensions_session(GRAPH) as check:
      from robosystems.models.extensions.roboledger.report import Report

      row = check.get(Report, self.REPORT_ID)
      assert row.filing_status == "draft"
      assert row.filed_at is None
