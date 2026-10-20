"""The group's first close over members with history, on a provisioned tenant
with the taxonomy library: a parent that funded a seventy-percent member in
June, before the consolidation entity's first month, July. What that history
already needed eliminated is the consolidation entity's opening, so the
consolidated equity statement opens on the outside owners' share and rolls
forward; without it the close refuses rather than stamp one that does not."""

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
from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
)
from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockElementRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import Element
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.commands.chart_of_accounts import (
  initialize_chart_of_accounts,
)
from robosystems.operations.roboledger.commands.consolidation import (
  propose_consolidation_entries,
)
from robosystems.operations.roboledger.commands.entity import create_entity
from robosystems.operations.roboledger.commands.fiscal_calendar import close_period
from robosystems.operations.roboledger.commands.taxonomies import (
  create_mapping_association,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  PeriodCloseService,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  CloseGateFailed,
)
from robosystems.operations.roboledger.reads import consolidation as engine
from robosystems.operations.roboledger.reads.reports import (
  get_live_financial_statement,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)
from robosystems.operations.taxonomy_block.commands import update_taxonomy_block

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd61"
_COMMANDS = "robosystems.operations.roboledger.commands.fiscal_calendar"
JUNE_20 = date(2026, 6, 20)
JULY_15 = date(2026, 7, 15)
NCI = "rs-gaap:MinorityInterest"


@pytest.fixture(params=["fresh", "baseline"])
def group(request):
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
      extensions_session(GRAPH) as session,
    ):
      yield _seed(session, baseline=request.param == "baseline")
  finally:
    with db.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    db.dispose()


def _seed(session, *, baseline: bool) -> dict:
  """``baseline`` seeds the consolidation entity's calendar closed through
  June, the way initialize-fiscal-calendar does with a closed_through; else
  it is a fresh calendar whose first month is July."""
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
  closed_through = "2026-06" if baseline else None
  service.initialize(
    session,
    GRAPH,
    closed_through=closed_through,
    actor_id="usr_1",
    entity_id=ids["adjustments"],
  )
  service.ensure_fiscal_periods(
    session,
    GRAPH,
    start_period="2026-05" if baseline else "2026-07",
    end_period="2026-08",
    closed_through=closed_through,
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
  """A posted entry in ``key``'s books, (code, debit-positive cents), naming
  ``agent``'s entity as the counterparty."""
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
  July: the member earns 1,000. Both close June and July."""
  _post(g, "parent", JUNE_20, [("1600", 700_000), ("1000", -700_000)], "member")
  _post(g, "member", JUNE_20, [("1000", 700_000), ("3000", -700_000)], "parent")
  _post(g, "member", JUNE_20, [("1000", 300_000), ("3000", -300_000)], None)
  _post(g, "member", JULY_15, [("1000", 100_000), ("4000", -100_000)], None)
  for period in ("2026-06", "2026-07"):
    _close(g, "member", period)
    _close(g, "parent", period)


def _propose(g, period: str = "2026-07"):
  return propose_consolidation_entries(
    g["session"], ProposeConsolidationEntriesRequest(period=period), "usr_1"
  )


def _equity(g):
  return get_live_financial_statement(
    g["session"],
    graph_id=GRAPH,
    statement_type="equity_statement",
    period_start=date(2026, 7, 1),
    period_end=date(2026, 7, 31),
    entity_id=g["ids"]["parent"],
    scope="consolidated",
  )


def _july(statement, label: str) -> float:
  july = next(i for i, p in enumerate(statement.periods) if p.end == date(2026, 7, 31))
  row = next(
    f for f in statement.facts if f.qname == NCI and label in (f.name or "").lower()
  )
  return row.values[july]


def test_the_first_close_opens_on_what_the_history_needed(group):
  _books(group)
  response = _propose(group)

  opening = [e for e in response.entries if e.type == "opening"]
  assert len(opening) == 1
  assert opening[0].posting_date == date(2026, 6, 30)
  with group["session"].begin_nested():
    preview = engine.preview_consolidation_entries(group["session"], period="2026-07")
  assert preview.up_to_date
  assert {p.adjustment_kind for p in preview.proposals if not p.opening} == {
    "nci_allocation"
  }

  closed = _close(group, "adjustments", "2026-07")
  assert closed.statements_stamped, closed.statement_stamp_note

  equity = _equity(group)
  assert equity.validation.status != "failed", equity.validation.failures
  assert _july(equity, "beginning") == 3000.0
  assert _july(equity, "ending") == 3300.0


def test_the_month_after_holds_the_opening_and_closes(group):
  """August holds July's opening like any posted entry: nothing is owed for
  it, and the group closes August."""
  _books(group)
  _propose(group)
  _close(group, "adjustments", "2026-07")
  _post(group, "member", date(2026, 8, 12), [("1000", 50_000), ("4000", -50_000)], None)
  for key in ("member", "parent"):
    _close(group, key, "2026-08")

  _propose(group, "2026-08")
  with group["session"].begin_nested():
    august = engine.preview_consolidation_entries(group["session"], period="2026-08")
    status = engine.group_close_status(group["session"], GRAPH, "2026-08")
  assert not any(p.opening for p in august.proposals)
  assert august.up_to_date, august.notes
  assert status.pending_adjustments == []
  assert _close(group, "adjustments", "2026-08").statements_stamped


def test_proposing_again_replaces_the_opening(group):
  _books(group)
  _propose(group)
  _propose(group)
  openings = group["session"].execute(
    select(Entry.id).where(
      Entry.entity_id == group["ids"]["adjustments"], Entry.type == "opening"
    )
  )
  assert len(openings.all()) == 1


def test_a_replaced_opening_is_kept_out_of_the_books(group):
  from robosystems.operations.roboledger.commands.consolidation import (
    _record_opening,
  )
  from robosystems.operations.roboledger.reads.consolidation import (
    preview_consolidation_entries,
  )

  _books(group)
  _propose(group)
  session = group["session"]
  adjustments = group["ids"]["adjustments"]
  first = session.execute(
    select(Entry).where(Entry.entity_id == adjustments, Entry.type == "opening")
  ).scalar_one()
  first_id, eve = first.id, first.posting_date

  preview = preview_consolidation_entries(session, period="2026-07")
  replacement = _record_opening(
    session, adjustments, eve, [p for p in preview.proposals if p.opening], "usr_1"
  )
  session.flush()

  assert replacement is not None
  rows = {
    row.id: row
    for row in session.execute(
      select(Entry.id, Entry.status, Entry.supersession).where(
        Entry.entity_id == adjustments, Entry.type == "opening"
      )
    )
  }
  assert rows[first_id].status == "superseded"
  assert rows[first_id].supersession["op"] == "opening_replaced"
  assert rows[first_id].supersession["replacement_entry_ids"] == [replacement.id]
  assert rows[replacement.id].status == "posted"


def test_a_moved_opening_keeps_the_engines_tags(group):
  _books(group)
  _propose(group)
  session = group["session"]
  adjustments = group["ids"]["adjustments"]

  def opening_lines(status: str):
    return sorted(
      (li.element_id, li.debit_amount, li.credit_amount, li.metadata_)
      for li in session.execute(
        select(LineItem)
        .join(Entry, Entry.id == LineItem.entry_id)
        .where(
          Entry.entity_id == adjustments,
          Entry.type == "opening",
          Entry.status == status,
        )
      ).scalars()
    )

  closed = session.execute(
    text("SELECT closed_through_period FROM fiscal_calendar WHERE entity_id = :e"),
    {"e": adjustments},
  ).scalar()
  if closed:
    pytest.skip("The calendar's start only moves before its first close.")
  before = opening_lines("posted")
  assert all(meta.get("adjustment_kind") for *_, meta in before)

  group["service"].change_start(
    session, GRAPH, "2026-06", actor_id="usr_1", entity_id=adjustments
  )
  session.flush()

  assert opening_lines("posted") == before
  assert opening_lines("superseded") == before


def test_without_its_opening_the_group_close_refuses(group, monkeypatch):
  """The catch-up posted inside the month moves the outside owners' share
  with no row for it; the gate read the statements before the drafts posted,
  so the close checks them again and rolls back."""
  monkeypatch.setattr(engine, "first_month_eve", lambda *_a: None)
  monkeypatch.setattr(
    "robosystems.operations.roboledger.commands.consolidation.first_month_eve",
    lambda *_a: None,
  )
  _books(group)
  _propose(group)

  with pytest.raises(CloseGateFailed) as refused:
    _close(group, "adjustments", "2026-07")
  assert refused.value.blockers == ["consolidated_does_not_foot"]
  assert any(
    "Noncontrolling Interest does not roll forward" in f
    for f in refused.value.gate.group_not_footing
  )
