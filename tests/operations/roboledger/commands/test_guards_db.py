"""The closed-period gate against a real tenant schema.

The calendar's ``closed_through_period`` closes every month on or before it,
whether or not the month has a ``FiscalPeriod`` row, and the fence is keyed by
the month so a close in progress blocks writers before its row exists.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.locking import RowLockedError, exclusive_period_fence
from robosystems.operations.roboledger.commands._guards import (
  ClosedPeriodError,
  assert_period_not_closed,
  baseline_import,
)
from robosystems.operations.roboledger.commands.reconciling_items import (
  _closed_period_names,
)
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity_on

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd07"


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
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=tables,
      )
      seed_parent_entity_on(conn.execution_options(schema_translate_map={None: GRAPH}))
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.fixture(autouse=True)
def ledger_closed_through_july(tenant):
  """Closed through 2026-07; period rows exist only from 2026-06 on."""
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM fiscal_periods"))
    session.execute(text("DELETE FROM fiscal_calendar"))
    session.add(
      FiscalCalendar(
        entity_id=PARENT_ENTITY_ID, graph_id=GRAPH, closed_through_period="2026-07"
      )
    )
    for name, start, end, status in (
      ("2026-06", date(2026, 6, 1), date(2026, 6, 30), "closed"),
      ("2026-07", date(2026, 7, 1), date(2026, 7, 31), "closed"),
      ("2026-08", date(2026, 8, 1), date(2026, 8, 31), "open"),
    ):
      session.add(
        FiscalPeriod(
          entity_id=PARENT_ENTITY_ID,
          graph_id=GRAPH,
          name=name,
          start_date=start,
          end_date=end,
          period_type="monthly",
          status=status,
        )
      )
  yield


@pytest.mark.parametrize("posting_date", [date(2025, 3, 15), date(2019, 1, 1)])
def test_a_month_before_the_first_row_is_closed(posting_date):
  with extensions_session(GRAPH) as session:
    with pytest.raises(ClosedPeriodError):
      assert_period_not_closed(session, posting_date)


def test_a_closed_row_is_closed():
  with extensions_session(GRAPH) as session:
    with pytest.raises(ClosedPeriodError):
      assert_period_not_closed(session, date(2026, 7, 10))


def test_a_baseline_import_passes_closed_months_and_only_it_does():
  """A source's history onto books that hold nothing posted yet is the books,
  not a change to them: the fence stands aside inside the waiver and nowhere
  else."""
  with extensions_session(GRAPH) as session:
    with baseline_import():
      assert_period_not_closed(session, date(2026, 7, 10), date(2019, 1, 1))
    with pytest.raises(ClosedPeriodError):
      assert_period_not_closed(session, date(2026, 7, 10))


@pytest.mark.parametrize("posting_date", [date(2026, 8, 15), date(2026, 11, 2)])
def test_open_months_pass(posting_date):
  with extensions_session(GRAPH) as session:
    assert_period_not_closed(session, posting_date)


def test_a_close_in_progress_blocks_a_writer_before_its_row_exists():
  with exclusive_period_fence(GRAPH, "2026-09", detail="Period 2026-09 is closing"):
    with extensions_session(GRAPH) as writer:
      with pytest.raises(RowLockedError, match="2026-09"):
        assert_period_not_closed(writer, date(2026, 9, 20))


def test_one_closed_date_among_open_ones_refuses():
  with extensions_session(GRAPH) as session:
    with pytest.raises(ClosedPeriodError):
      assert_period_not_closed(session, date(2026, 8, 1), date(2024, 12, 31))


def test_the_reconciling_item_plan_uses_the_same_rule():
  with extensions_session(GRAPH) as session:
    assert _closed_period_names(
      session,
      [date(2019, 5, 1), date(2026, 7, 3), date(2026, 8, 20)],
      PARENT_ENTITY_ID,
    ) == ["2019-05", "2026-07"]


def test_a_bank_feed_takes_no_new_line_into_a_closed_month():
  from robosystems.adapters.bank_feed.load import in_closed_months

  def line(external_id, day, entity_id=None):
    return {
      "external_id": external_id,
      "occurred_at": f"{day}T00:00:00Z",
      "entity_id": entity_id,
    }

  with extensions_session(GRAPH) as session:
    closed = in_closed_months(
      session,
      [
        line("before_rows", "2025-03-15"),
        line("closed_row", "2026-07-31", PARENT_ENTITY_ID),
        line("open_row", "2026-08-01"),
        # Another entity's books, never closed: the parent's close is not its.
        line("sibling", "2026-07-10", "ent_test_sibling"),
      ],
    )
  assert closed == {"before_rows", "closed_row"}


def test_a_line_with_no_readable_date_is_left_to_capture():
  from robosystems.adapters.bank_feed.load import in_closed_months

  with extensions_session(GRAPH) as session:
    closed = in_closed_months(
      session,
      [
        {"external_id": "undated", "occurred_at": "None", "entity_id": None},
        {"external_id": "closed", "occurred_at": "2026-07-10T00:00:00Z"},
      ],
    )
  assert closed == {"closed"}
