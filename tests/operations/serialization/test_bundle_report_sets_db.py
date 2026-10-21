"""A report's bundle is built from the report's own statement sets, on the
Structures those sets pinned when the report was generated.

The entity's Reporting Style can change after a report is drafted. The
bundle used to look each statement up through the *current* Style's network
and fall back to the latest set on it, so a report drafted under one Style
and filed after a switch could download with another set's numbers, or with
a statement missing. On a provisioned tenant with the taxonomy library: one
company, one report, a Style switch, the same bundle before and after.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.config.constants import ReportingStyleConstants
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.api.extensions.chart_of_accounts import (
  InitializeChartOfAccountsRequest,
)
from robosystems.models.api.extensions.entity import (
  ChangeReportingStyleRequest,
  CreateEntityRequest,
)
from robosystems.models.api.extensions.reports import CreateReportRequest, PeriodSpec
from robosystems.models.extensions import Element
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.entity import create_entity
from robosystems.operations.roboledger.commands.reporting_style import (
  change_reporting_style,
)
from robosystems.operations.roboledger.commands.reports import create_report
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.reports.network_picker import (
  load_entity_reporting_style,
)
from robosystems.operations.serialization import build_report_bundle
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)

pytestmark = pytest.mark.integration

GRAPH = "kgeeeeeeeeeeeeeeee68"
_BUNDLE = "robosystems.operations.roboledger.commands.reports._stamp_report_bundle"
JUNE = (date(2026, 6, 1), date(2026, 6, 30))

STATEMENTS = (
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
)


@pytest.fixture()
def books():
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
    conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
  try:
    provision_tenant_schema(GRAPH)
    with (
      patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
      patch(_BUNDLE),
      extensions_session(GRAPH) as session,
    ):
      yield _seed(session)
  finally:
    with db.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    db.dispose()


def _seed(session) -> dict:
  entity_id = create_entity(
    session, CreateEntityRequest(name="Harbor Holdings", entity_type="llc"), "usr_1"
  ).id
  chart = initialize_chart_of_accounts(
    session,
    InitializeChartOfAccountsRequest(template="services", entity_id=entity_id),
    "usr_1",
  ).taxonomy_id
  service = FiscalCalendarService()
  service.initialize(
    session, GRAPH, closed_through="2026-05", actor_id="usr_1", entity_id=entity_id
  )
  service.ensure_fiscal_periods(
    session,
    GRAPH,
    start_period="2026-05",
    end_period="2026-07",
    closed_through="2026-05",
    entity_id=entity_id,
  )
  session.commit()
  g = {"session": session, "entity": entity_id, "chart": chart}
  _post(g, date(2026, 6, 10), [("1000", 100_000), ("4000", -100_000)])
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


def _report(g):
  mapping = find_entity_mapping(g["session"], g["entity"], BOOK_FRAMEWORK)
  return create_report(
    g["session"],
    CreateReportRequest(
      name="June",
      mapping_id=str(mapping.id),
      period_start=JUNE[0],
      period_end=JUNE[1],
      period_type="monthly",
      comparative=False,
      periods=[PeriodSpec(start=JUNE[0], end=JUNE[1], label="June")],
    ),
    graph_id=GRAPH,
    created_by="usr_1",
  )


def _bundle(g, report_id: str):
  """``build_report_bundle`` reads the framework pin off the platform Graph
  row, which this throwaway tenant has none of."""
  platform = MagicMock()
  platform.__enter__.return_value.query.return_value.filter.return_value.first.return_value = MagicMock()
  with (
    patch("robosystems.database.platform_session", return_value=platform),
    patch("robosystems.taxonomy.pins.resolve_pin", return_value={"rs-gaap": "v1"}),
  ):
    return build_report_bundle(g["session"], GRAPH, report_id)


def _statement_sets(bundle) -> dict[str, tuple[str, str]]:
  """block_type → (structure id, fact set id) for the bundle's statements."""
  return {
    envelope.block_type: (
      envelope.id,
      envelope.fact_set.id if envelope.fact_set else None,
    )
    for envelope in bundle.ib_envelopes
    if envelope.block_type in STATEMENTS
  }


def test_the_bundle_keeps_its_statements_across_a_style_switch(books) -> None:
  g = books
  report = _report(g)

  before = _statement_sets(_bundle(g, report.id))
  assert set(before) == set(STATEMENTS)

  # The other equity form's Style composes a different equity network.
  current = load_entity_reporting_style(g["session"], g["entity"])
  other = (
    ReportingStyleConstants.DEFAULT_STYLE_ID
    if current != ReportingStyleConstants.DEFAULT_STYLE_ID
    else ReportingStyleConstants.LLC_STYLE_ID
  )
  switched = change_reporting_style(
    g["session"],
    ChangeReportingStyleRequest(reporting_style_id=other, entity_id=g["entity"]),
  )
  assert switched.changed is True

  after = _statement_sets(_bundle(g, report.id))

  assert after == before, (
    "the bundle followed the entity's new Style, not the report's sets"
  )
