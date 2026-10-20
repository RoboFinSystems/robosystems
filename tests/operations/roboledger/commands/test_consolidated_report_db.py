"""A report made for the consolidation entity is the group's consolidated
statements, on a provisioned tenant with the taxonomy library: a parent with a
seventy-percent member it funded in June and the group's consolidation entity,
the group closed through July. The report sums the members and the
consolidating entries as the live consolidated statement does, is named for
the group, is listed on the parent, and stays consolidated on regenerate."""

from __future__ import annotations

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
from robosystems.models.api.extensions.consolidation import (
  ProposeConsolidationEntriesRequest,
)
from robosystems.models.api.extensions.entity import CreateEntityRequest
from robosystems.models.api.extensions.reports import CreateReportRequest
from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
)
from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockElementRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import Element, Entity, Fact
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fact_set import FactSet
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.consolidation import (
  propose_consolidation_entries,
)
from robosystems.operations.roboledger.commands.entity import create_entity
from robosystems.operations.roboledger.commands.fiscal_calendar import close_period
from robosystems.operations.roboledger.commands.reports import (
  create_report,
  regenerate_report,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  create_mapping_association,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  PeriodCloseService,
)
from robosystems.operations.roboledger.reads import consolidation as engine
from robosystems.operations.roboledger.reads.reports import (
  get_live_financial_statement,
  list_reports,
)
from robosystems.operations.roboledger.reports.statement_sets import (
  ConsolidatedPeriodSpansMembershipError,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)
from robosystems.operations.taxonomy_block.commands import update_taxonomy_block
from robosystems.routers.extensions.roboledger.operations.reports import (
  RegenerateReportOperation,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd62"
_COMMANDS = "robosystems.operations.roboledger.commands.fiscal_calendar"
_BUNDLE = "robosystems.operations.roboledger.commands.reports._stamp_report_bundle"
JUNE_20 = date(2026, 6, 20)
JULY = (date(2026, 7, 1), date(2026, 7, 31))
NCI = "rs-gaap:MinorityInterest"


@pytest.fixture()
def group():
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
      patch(f"{_COMMANDS}.exclusive_period_fence"),
      patch("robosystems.operations.extensions.staleness.mark_graph_stale"),
      patch(
        "robosystems.operations.roboledger.reads.fiscal_calendar.qb_sync_state",
        return_value=(False, None),
      ),
      patch(_BUNDLE),
      extensions_session(GRAPH) as session,
    ):
      g = _seed(session)
      _books(g)
      yield g
  finally:
    with db.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    db.dispose()


def _seed(session) -> dict:
  service = FiscalCalendarService()
  ids = {}
  for key, body in (
    ("parent", CreateEntityRequest(name="Harbor Holdings", entity_type="llc")),
    (
      "member",
      CreateEntityRequest(name="Maple Court LLC", entity_type="llc", ownership_pct=70),
    ),
    (
      "adjustments",
      CreateEntityRequest(
        name="Consolidation Adjustments", entity_role="consolidation"
      ),
    ),
  ):
    ids[key] = create_entity(session, body, "usr_1").id
  charts = {}
  for key, template in (
    ("parent", "services"),
    ("member", "services"),
    ("adjustments", "consolidation"),
  ):
    charts[key] = initialize_chart_of_accounts(
      session,
      InitializeChartOfAccountsRequest(template=template, entity_id=ids[key]),
      "usr_1",
    ).taxonomy_id
  _add_investment_account(session, ids["parent"], charts["parent"])
  for key in ("parent", "member"):
    service.initialize(
      session, GRAPH, closed_through="2026-05", actor_id="usr_1", entity_id=ids[key]
    )
    service.ensure_fiscal_periods(
      session,
      GRAPH,
      start_period="2026-05",
      end_period="2026-08",
      closed_through="2026-05",
      entity_id=ids[key],
    )
  # The group's first month is July; June's history is its opening.
  service.initialize(
    session, GRAPH, closed_through=None, actor_id="usr_1", entity_id=ids["adjustments"]
  )
  service.ensure_fiscal_periods(
    session,
    GRAPH,
    start_period="2026-07",
    end_period="2026-08",
    closed_through=None,
    entity_id=ids["adjustments"],
  )
  session.commit()
  return {"ids": ids, "charts": charts, "service": service, "session": session}


def _add_investment_account(session, entity_id: str, chart_id: str) -> None:
  cash = session.execute(
    select(Element).where(Element.taxonomy_id == chart_id, Element.code == "1000")
  ).scalar_one()
  namespace = cash.qname.split(":")[0]
  update_taxonomy_block(
    session,
    UpdateTaxonomyBlockRequest(
      taxonomy_id=chart_id,
      elements_to_add=[
        TaxonomyBlockElementRequest(
          qname=f"{namespace}:1600",
          name="Investment in Maple Court",
          trait="asset",
          balance_type="debit",
          code="1600",
          sub_classification="other_noncurrent_assets",
        )
      ],
    ),
    "usr_1",
  )
  account = session.execute(
    select(Element.id).where(Element.taxonomy_id == chart_id, Element.code == "1600")
  ).scalar_one()
  target = session.execute(
    select(Element.id).where(Element.qname == engine.INVESTMENT_IN_SUBSIDIARIES)
  ).scalar_one()
  mapping = find_entity_mapping(session, entity_id, BOOK_FRAMEWORK)
  create_mapping_association(
    session,
    CreateMappingAssociationOperation(
      mapping_id=str(mapping.id),
      from_element_id=str(account),
      to_element_id=str(target),
    ),
    "usr_1",
  )


def _post(g, key: str, on: date, lines: list[tuple[str, int]], agent: str | None):
  session, entity_id = g["session"], g["ids"][key]
  agent_id = (
    session.execute(
      select(Agent.id).where(
        Agent.agent_type == "self", Agent.external_id == g["ids"][agent]
      )
    ).scalar_one()
    if agent
    else None
  )
  event = Event(
    entity_id=entity_id,
    event_type="journal_entry_recorded",
    event_category="adjustment",
    occurred_at=datetime(on.year, on.month, on.day, tzinfo=UTC),
    status="fulfilled",
    source="manual",
    agent_id=agent_id,
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
    account = session.execute(
      select(Element.id).where(
        Element.taxonomy_id == g["charts"][key], Element.code == code
      )
    ).scalar_one()
    session.add(
      LineItem(
        entry_id=entry.id,
        element_id=account,
        debit_amount=max(amount, 0),
        credit_amount=max(-amount, 0),
        line_order=order,
      )
    )
  session.commit()


def _close(g, key: str, period: str):
  return close_period(
    g["session"],
    MagicMock(),
    GRAPH,
    period,
    actor_id="usr_1",
    allow_stale_sync=False,
    note=None,
    service=g["service"],
    close_service=PeriodCloseService(g["service"]),
    entity_id=g["ids"][key],
  )


def _books(g) -> None:
  """June: the parent puts 7,000 into the member and an outside owner 3,000.
  July: the member earns 1,000. The companies close June and July, the
  group July."""
  _post(g, "parent", JUNE_20, [("1600", 700_000), ("1000", -700_000)], "member")
  _post(g, "member", JUNE_20, [("1000", 700_000), ("3000", -700_000)], "parent")
  _post(g, "member", JUNE_20, [("1000", 300_000), ("3000", -300_000)], None)
  _post(g, "member", date(2026, 7, 15), [("1000", 100_000), ("4000", -100_000)], None)
  for period in ("2026-06", "2026-07"):
    _close(g, "member", period)
    _close(g, "parent", period)
  propose_consolidation_entries(
    g["session"], ProposeConsolidationEntriesRequest(period="2026-07"), "usr_1"
  )
  assert _close(g, "adjustments", "2026-07").statements_stamped


def _report(g, key: str, start: date = JULY[0]):
  mapping = find_entity_mapping(g["session"], g["ids"][key], BOOK_FRAMEWORK)
  return create_report(
    g["session"],
    CreateReportRequest(
      name="July",
      mapping_id=str(mapping.id),
      period_start=start,
      period_end=JULY[1],
      period_type="monthly",
      comparative=False,
    ),
    graph_id=GRAPH,
    created_by="usr_1",
  )


def _facts(g, report_id: str) -> dict[str, float]:
  rows = g["session"].execute(
    select(Element.qname, Fact.value)
    .join(Element, Element.id == Fact.element_id)
    .join(FactSet, FactSet.id == Fact.fact_set_id)
    .where(FactSet.report_id == report_id, Fact.period_end == JULY[1])
    .distinct()
  )
  return {qname: float(value) for qname, value in rows}


def _consolidated(g, statement_type: str) -> dict[str, float]:
  live = get_live_financial_statement(
    g["session"],
    graph_id=GRAPH,
    statement_type=statement_type,
    period_start=JULY[0],
    period_end=JULY[1],
    entity_id=g["ids"]["parent"],
    scope="consolidated",
  )
  july = next(i for i, p in enumerate(live.periods) if p.end == JULY[1])
  return {f.qname: f.values[july] for f in live.facts if not f.is_subtotal}


def test_the_consolidation_entitys_report_is_the_consolidated_group(group):
  report = _report(group, "adjustments")

  assert report.entity_name == "Harbor Holdings (consolidated)"
  facts = _facts(group, report.id)
  # The members' books, not the eliminations alone: the member's revenue is
  # the group's, and the investment in it is eliminated.
  for qname, value in _consolidated(group, "balance_sheet").items():
    assert facts.get(qname, 0.0) * 100 == pytest.approx(value * 100), qname
  assert facts.get(NCI, 0.0) != 0.0
  assert facts.get(engine.INVESTMENT_IN_SUBSIDIARIES, 0.0) == 0.0


def test_the_parent_lists_the_groups_consolidated_reports(group):
  consolidated = _report(group, "adjustments")
  own = _report(group, "parent")

  listed = {
    r.id
    for r in list_reports(group["session"], entity_id=group["ids"]["parent"]).reports
  }
  assert {consolidated.id, own.id} <= listed
  member_listed = {
    r.id
    for r in list_reports(group["session"], entity_id=group["ids"]["member"]).reports
  }
  assert consolidated.id not in member_listed


def test_regenerating_keeps_it_consolidated(group):
  report = _report(group, "adjustments")
  before = _facts(group, report.id)

  regenerated = regenerate_report(
    group["session"],
    RegenerateReportOperation(report_id=report.id),
    graph_id=GRAPH,
    created_by="usr_1",
  )

  assert regenerated.entity_name == "Harbor Holdings (consolidated)"
  assert _facts(group, report.id) == before


def test_a_column_spanning_a_member_joining_is_refused(group):
  """Joined in July, the member is summed for July; a June-to-July column
  would hold part of its time, which a consolidated column cannot state."""
  member = group["session"].get(Entity, group["ids"]["member"])
  member.joined_on = date(2026, 7, 1)
  group["session"].commit()

  with pytest.raises(ConsolidatedPeriodSpansMembershipError):
    _report(group, "adjustments", start=date(2026, 6, 1))
