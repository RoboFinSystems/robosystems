"""A report's columns freeze one by one as the calendar closes their periods,
and the open ones are live, on a provisioned tenant with the taxonomy library:
one company, books closed through May, a June-and-July report. June's close
freezes June and leaves July accruing; a July posting reaches the report on
its next read; filing waits for every column; a reopen puts a column back to
live and retracts the close's own statements instead of deleting them; a
regenerate keeps the frozen columns' rows."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
)
from robosystems.models.api.extensions.entity import CreateEntityRequest
from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
  PeriodSpec,
)
from robosystems.models.extensions import Element, Fact, Report
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.entity import create_entity
from robosystems.operations.roboledger.commands.fiscal_calendar import (
  close_period,
  reopen_period,
)
from robosystems.operations.roboledger.commands.report_columns import (
  refresh_report_columns,
)
from robosystems.operations.roboledger.commands.reports import (
  InvalidFilingTransitionError,
  create_report,
  file_report,
  regenerate_report,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  PeriodCloseService,
)
from robosystems.operations.roboledger.reads.reports import get_report_package
from robosystems.operations.roboledger.reports.statement_sets import (
  has_canonical_statement_sets,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)
from robosystems.routers.extensions.roboledger.operations.reports import (
  RegenerateReportOperation,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd63"
_COMMANDS = "robosystems.operations.roboledger.commands.fiscal_calendar"
_BUNDLE = "robosystems.operations.roboledger.commands.reports._stamp_report_bundle"
JUNE = (date(2026, 6, 1), date(2026, 6, 30))
JULY = (date(2026, 7, 1), date(2026, 7, 31))
AUGUST = (date(2026, 8, 1), date(2026, 8, 31))


@pytest.fixture()
def books(request):
  # Each module that uses these books provisions its own tenant: xdist runs
  # files on separate workers, and two on one schema drop it under each other.
  graph = getattr(request.module, "GRAPH", GRAPH)
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  db = create_engine(url)
  try:
    with db.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    db.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")
  with db.begin() as conn:
    conn.execute(text(f"DROP SCHEMA IF EXISTS {graph} CASCADE"))
  try:
    provision_tenant_schema(graph)
    with (
      patch(f"{_COMMANDS}.exclusive_period_fence"),
      patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
      patch(
        "robosystems.operations.roboledger.reads.fiscal_calendar.qb_sync_state",
        return_value=(False, None),
      ),
      patch(_BUNDLE),
      extensions_session(graph) as session,
    ):
      yield _seed(session, graph)
  finally:
    with db.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {graph} CASCADE"))
    db.dispose()


def _seed(session, graph: str) -> dict:
  service = FiscalCalendarService()
  entity_id = create_entity(
    session, CreateEntityRequest(name="Harbor Holdings", entity_type="llc"), "usr_1"
  ).id
  chart = initialize_chart_of_accounts(
    session,
    InitializeChartOfAccountsRequest(template="services", entity_id=entity_id),
    "usr_1",
  ).taxonomy_id
  service.initialize(
    session, graph, closed_through="2026-05", actor_id="usr_1", entity_id=entity_id
  )
  service.ensure_fiscal_periods(
    session,
    graph,
    start_period="2026-05",
    end_period="2026-09",
    closed_through="2026-05",
    entity_id=entity_id,
  )
  session.commit()
  g = {
    "session": session,
    "entity": entity_id,
    "chart": chart,
    "service": service,
    "graph": graph,
  }
  _post(g, date(2026, 6, 10), [("1000", 100_000), ("4000", -100_000)])
  _post(g, date(2026, 7, 15), [("1000", 50_000), ("4000", -50_000)])
  return g


def _account(g, code: str) -> str:
  return (
    g["session"]
    .execute(
      select(Element.id).where(Element.taxonomy_id == g["chart"], Element.code == code)
    )
    .scalar_one()
  )


def _post(g, on: date, lines: list[tuple[str, int]]) -> None:
  session, entity_id = g["session"], g["entity"]
  event = Event(
    entity_id=entity_id,
    event_type="journal_entry_recorded",
    event_category="adjustment",
    occurred_at=datetime(on.year, on.month, on.day, tzinfo=UTC),
    status="fulfilled",
    source="manual",
    created_by="test",
  )
  session.add(event)
  session.flush()
  entry = Entry(
    entity_id=entity_id,
    status="posted",
    posting_date=on,
    triggered_by_event_id=event.id,
    created_by="test",
  )
  session.add(entry)
  session.flush()
  for order, (code, amount) in enumerate(lines, 1):
    session.add(
      LineItem(
        entry_id=entry.id,
        element_id=_account(g, code),
        debit_amount=max(amount, 0),
        credit_amount=max(-amount, 0),
        line_order=order,
      )
    )
  session.commit()


def _close(g, period: str):
  return close_period(
    g["session"],
    MagicMock(),
    g["graph"],
    period,
    actor_id="usr_1",
    allow_stale_sync=False,
    note=None,
    service=g["service"],
    close_service=PeriodCloseService(g["service"]),
    entity_id=g["entity"],
  )


def _reopen(g, period: str):
  return reopen_period(
    g["session"],
    MagicMock(),
    g["graph"],
    period,
    actor_id="usr_1",
    reason="correction",
    note=None,
    service=g["service"],
    entity_id=g["entity"],
  )


def _columns(*windows) -> list[PeriodSpec]:
  return [
    PeriodSpec(start=start, end=end, label=end.strftime("%B")) for start, end in windows
  ]


def _report(g, *windows):
  mapping = find_entity_mapping(g["session"], g["entity"], BOOK_FRAMEWORK)
  windows = windows or (JUNE, JULY)
  return create_report(
    g["session"],
    CreateReportRequest(
      name="Half year",
      mapping_id=str(mapping.id),
      period_start=windows[0][0],
      period_end=windows[-1][1],
      period_type="monthly",
      comparative=False,
      periods=_columns(*windows),
    ),
    graph_id=g["graph"],
    created_by="usr_1",
  )


def _revenue_rows(g, report_id: str) -> dict[date, Fact]:
  """The report's revenue fact per column: the duration fact whose pivot
  summed the revenue account."""
  revenue = _account(g, "4000")
  rows = (
    g["session"]
    .execute(
      select(Fact)
      .join(FactSet, FactSet.id == Fact.fact_set_id)
      .where(FactSet.report_id == report_id, Fact.period_type == "duration")
    )
    .scalars()
    .all()
  )
  by_end: dict[date, Fact] = {}
  for fact in rows:
    sources = (fact.provenance or {}).get("sources") or []
    if any(s["account_id"] == revenue for s in sources):
      by_end.setdefault(fact.period_end, fact)
  return by_end


def _frozen(g, report_id: str) -> dict[date, bool]:
  rows = (
    g["session"]
    .execute(
      text(
        "SELECT period_end, bool_and(frozen_at IS NOT NULL) FROM facts "
        "WHERE fact_set_id IN (SELECT id FROM fact_sets WHERE report_id = :rid) "
        "GROUP BY period_end"
      ),
      {"rid": report_id},
    )
    .all()
  )
  return {row[0]: row[1] for row in rows}


def test_columns_freeze_as_the_calendar_closes_and_open_ones_accrue(books):
  g = books
  report = _report(g)
  session = g["session"]

  # Books closed through May: both columns are live, and every pivoted fact
  # names the accounts that foot to it.
  frozen = _frozen(g, report.id)
  assert frozen[JUNE[1]] is False and frozen[JULY[1]] is False
  revenue = _revenue_rows(g, report.id)
  assert revenue[JUNE[1]].value == 1_000.0
  assert revenue[JULY[1]].value == 500.0
  descriptor = revenue[JUNE[1]].provenance
  assert descriptor["origin"] == "pivot"
  assert [(s["entity_id"], round(s["value"], 2)) for s in descriptor["sources"]] == [
    (g["entity"], 1_000.0)
  ]

  # June closes: the close brings the column up to the books and freezes it
  # there; July stays live. From here June's row is the record, and it
  # carries the hash of the one landed line under its account.
  assert _close(g, "2026-06").statements_stamped
  session.expire_all()
  frozen = _frozen(g, report.id)
  assert frozen[JUNE[1]] is True and frozen[JULY[1]] is False
  revenue = _revenue_rows(g, report.id)
  assert revenue[JUNE[1]].value == 1_000.0
  june_id = revenue[JUNE[1]].id
  descriptor = revenue[JUNE[1]].provenance
  june_lines = (
    session.execute(
      text(
        "SELECT li.id FROM line_items li JOIN entries e ON e.id = li.entry_id "
        "WHERE li.element_id = :account AND e.posting_date BETWEEN :s AND :e "
        "ORDER BY li.id"
      ),
      {"account": _account(g, "4000"), "s": JUNE[0], "e": JUNE[1]},
    )
    .scalars()
    .all()
  )
  assert descriptor["line_count"] == len(june_lines) == 1
  assert (
    descriptor["content_hash"]
    == "sha256:" + hashlib.sha256("\n".join(june_lines).encode()).hexdigest()
  )
  assert revenue[JULY[1]].provenance["content_hash"] is None
  # Every row says how it was made: the subtotals and net income are rollups
  # naming their children, and nothing is left unknown.
  origins = session.execute(
    text(
      "SELECT provenance->>'origin', count(*) FROM facts "
      "WHERE fact_set_id IN (SELECT id FROM fact_sets WHERE report_id = :rid) "
      "GROUP BY 1"
    ),
    {"rid": report.id},
  ).all()
  by_origin = dict(origins)
  assert None not in by_origin, by_origin
  assert by_origin.get("rollup", 0) > 0
  net_income = session.execute(
    text(
      "SELECT f.provenance FROM facts f JOIN elements el ON el.id = f.element_id "
      "WHERE f.fact_set_id IN (SELECT id FROM fact_sets WHERE report_id = :rid) "
      "  AND el.qname = 'rs-gaap:NetIncomeLoss' AND f.period_end = :end LIMIT 1"
    ),
    {"rid": report.id, "end": JUNE[1]},
  ).scalar()
  assert net_income["origin"] == "rollup" and net_income["rule"] == "net_income"
  assert any(c["weight"] == 1.0 for c in net_income["children"])

  # A July posting reaches July on the report's next read, and June's row
  # is the one written at the close.
  generated_at = session.get(Report, report.id).last_generated
  _post(g, date(2026, 7, 20), [("1000", 25_000), ("4000", -25_000)])
  package = get_report_package(session, report.id)
  assert package is not None
  session.expire_all()
  revenue = _revenue_rows(g, report.id)
  assert revenue[JULY[1]].value == 750.0
  assert revenue[JUNE[1]].id == june_id
  refreshed_at = session.get(Report, report.id).last_generated
  assert refreshed_at > generated_at

  # Nothing landed since: a read rewrites nothing.
  assert refresh_report_columns(session, report.id, actor_id="usr_1").rewritten is False
  assert session.get(Report, report.id).last_generated == refreshed_at


def test_filing_waits_for_every_column_and_a_reopen_puts_one_back_to_live(books):
  g = books
  report = _report(g)
  session = g["session"]
  _close(g, "2026-06")

  with pytest.raises(InvalidFilingTransitionError, match="July"):
    file_report(session, report.id, "usr_1", graph_id=GRAPH)

  assert _close(g, "2026-07").statements_stamped
  session.expire_all()
  assert all(_frozen(g, report.id).values())
  july_id = _revenue_rows(g, report.id)[JULY[1]].id

  # Reopening July puts its column back to live and retracts the close's
  # own statements: the month as closed stays, read by nothing current.
  _reopen(g, "2026-07")
  session.expire_all()
  frozen = _frozen(g, report.id)
  assert frozen[JUNE[1]] is True and frozen[JULY[1]] is False
  assert not has_canonical_statement_sets(
    session, period_start=JULY[0], period_end=JULY[1], entity_id=g["entity"]
  )
  retracted = session.execute(
    text(
      "SELECT count(*) FROM fact_sets WHERE report_id IS NULL "
      "AND period_end = :end AND retracted_at IS NOT NULL"
    ),
    {"end": JULY[1]},
  ).scalar()
  assert retracted > 0

  # Re-closing writes July's statements again and freezes the column again.
  assert _close(g, "2026-07").statements_stamped
  session.expire_all()
  assert all(_frozen(g, report.id).values())
  assert has_canonical_statement_sets(
    session, period_start=JULY[0], period_end=JULY[1], entity_id=g["entity"]
  )
  # The column was rewritten at the reopen's first read: a new row.
  assert _revenue_rows(g, report.id)[JULY[1]].id != july_id

  filed = file_report(session, report.id, "usr_1", graph_id=GRAPH)
  assert filed.filing_status == "filed"


def test_regenerate_keeps_the_frozen_columns_rows_and_adds_live_ones(books):
  g = books
  report = _report(g)
  session = g["session"]
  _close(g, "2026-06")
  session.expire_all()
  june_id = _revenue_rows(g, report.id)[JUNE[1]].id

  regenerate_report(
    session,
    RegenerateReportOperation(
      report_id=report.id, periods=_columns(JUNE, JULY, AUGUST)
    ),
    graph_id=GRAPH,
    created_by="usr_1",
  )
  session.expire_all()
  frozen = _frozen(g, report.id)
  assert frozen[JUNE[1]] is True
  assert frozen[JULY[1]] is False
  assert frozen.get(AUGUST[1]) is False
  assert _revenue_rows(g, report.id)[JUNE[1]].id == june_id
  # The sets' envelope followed the columns.
  envelopes = session.execute(
    text(
      "SELECT DISTINCT period_start, period_end, provenance->>'period' "
      "FROM fact_sets WHERE report_id = :rid AND provenance->>'origin' = 'pivot'"
    ),
    {"rid": report.id},
  ).all()
  assert envelopes == [(JUNE[0], AUGUST[1], "2026-06-01/2026-08-31")]
