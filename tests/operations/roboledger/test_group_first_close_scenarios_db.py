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

GRAPH = "kgc170000000000000a3"
_COMMANDS = "robosystems.operations.roboledger.commands.fiscal_calendar"
JUNE_20 = date(2026, 6, 20)
JULY_15 = date(2026, 7, 15)
NCI = "rs-gaap:MinorityInterest"


@pytest.fixture(params=["fresh"])
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


# ── Brief F scenarios (review of 3e9e0cc39) ───────────────────────────────

EQ = "rs-gaap:MembersEquity"


def _sql(g, _query, **p):
  return g["session"].execute(text(_query), p)


def _held(g, qname, entity="adjustments", through=None) -> int:
  """Credit-positive balance the entity's posted (and reversed) lines hold on
  accounts mapped to ``qname``."""
  return int(
    _sql(
      g,
      "SELECT COALESCE(SUM(li.credit_amount - li.debit_amount), 0) "
      "FROM line_items li JOIN entries e ON e.id = li.entry_id "
      "JOIN associations a ON a.from_element_id = li.element_id "
      "AND a.association_type = 'mapping' "
      "JOIN elements c ON c.id = a.to_element_id "
      "WHERE e.entity_id = :entity AND e.status IN ('posted','reversed') "
      "AND c.qname = :q AND e.posting_date <= :through",
      entity=g["ids"][entity],
      q=qname,
      through=through or date(2099, 1, 1),
    ).scalar()
  )


def _ledger(g, entity="adjustments"):
  rows = _sql(
    g,
    "SELECT e.posting_date, e.type, e.status, e.adjustment_kind k, el.code, c.qname, "
    "li.debit_amount - li.credit_amount amt, li.metadata->>'adjustment_kind' lk "
    "FROM line_items li JOIN entries e ON e.id = li.entry_id "
    "JOIN elements el ON el.id = li.element_id "
    "LEFT JOIN associations a ON a.from_element_id = li.element_id AND a.association_type='mapping' "
    "LEFT JOIN elements c ON c.id = a.to_element_id "
    "WHERE e.entity_id = :e ORDER BY e.posting_date, e.created_at, li.line_order",
    e=g["ids"][entity],
  ).all()
  for r in rows:
    print(
      "   ",
      r.posting_date,
      r.type,
      r.status,
      r.k or r.lk,
      r.code,
      (r.qname or "").split(":")[-1],
      r.amt,
    )


def _stmt(g, kind, period="2026-07"):
  start, end = engine.month_bounds(period)
  return get_live_financial_statement(
    g["session"],
    graph_id=GRAPH,
    statement_type=kind,
    period_start=start,
    period_end=end,
    entity_id=g["ids"]["parent"],
    scope="consolidated",
  )


def _col(statement, period="2026-07"):
  _s, end = engine.month_bounds(period)
  return next(i for i, p in enumerate(statement.periods) if p.end == end)


def _bs(g, period="2026-07", show=True):
  s = _stmt(g, "balance_sheet", period)
  i = _col(s, period)
  out = {}
  for f in s.facts:
    v = f.values[i]
    if v:
      out[f.qname.split(":")[-1]] = v
  if show:
    print(
      "  BS",
      period,
      s.validation.status if s.validation else None,
      (s.validation.failures if s.validation else None),
    )
    for k, v in out.items():
      print("     ", k, v)
  return s, out


def _eq(g, period="2026-07", show=True):
  s = _stmt(g, "equity_statement", period)
  i = _col(s, period)
  if show:
    print(
      "  EQ",
      period,
      s.validation.status if s.validation else None,
      (s.validation.failures if s.validation else None),
    )
    for f in s.facts:
      if f.values[i]:
        print("     ", f.qname.split(":")[-1], "|", f.name, "|", f.values[i])
  return s


def _preview(g, period="2026-07"):
  with g["session"].begin_nested():
    return engine.preview_consolidation_entries(g["session"], period=period)


def _show(p):
  print("  preview", p.period, "up_to_date", p.up_to_date, "incomplete", p.incomplete)
  for n in p.notes:
    print("    NOTE", n)
  for r in p.residuals:
    print("    RESIDUAL", r.amount, r.explanation)
  for pr in p.proposals:
    print(
      "    ",
      "OPEN" if pr.opening else "    ",
      pr.adjustment_kind,
      pr.posting_date,
      [
        (ln.concept.split(":")[-1], ln.debit_amount - ln.credit_amount)
        for ln in pr.lines
      ],
    )


def _close_members(g, *periods, keys=("member", "parent")):
  for period in periods:
    for k in keys:
      _close(g, k, period)


def _set_pct(g, pct, key="member"):
  _sql(
    g, "UPDATE entities SET ownership_pct = :p WHERE id = :i", p=pct, i=g["ids"][key]
  )
  g["session"].commit()


def _fund(g, parent_amt=700_000, outside_amt=300_000, on=JUNE_20):
  _post(g, "parent", on, [("1600", parent_amt), ("1000", -parent_amt)], "member")
  _post(g, "member", on, [("1000", parent_amt), ("3000", -parent_amt)], "parent")
  if outside_amt:
    _post(g, "member", on, [("1000", outside_amt), ("3000", -outside_amt)], None)


def _first_close(g):
  r = _propose(g)
  p = _preview(g)
  _show(p)
  closed = _close(g, "adjustments", "2026-07")
  print("  stamped", closed.statements_stamped, closed.statement_stamp_note)
  return r, p, closed


def _same_style(g):
  """Give the consolidation entity the parent's Reporting Style."""
  _sql(
    g,
    "UPDATE entities SET reporting_style_id = (SELECT reporting_style_id FROM entities WHERE id = :p) WHERE id = :a",
    p=g["ids"]["parent"],
    a=g["ids"]["adjustments"],
  )
  g["session"].commit()


def _styles(g):
  print(
    "  styles",
    _sql(
      g,
      "SELECT e.name, e.entity_type, e.reporting_style_id, s.metadata->>'retained_earnings_concept' FROM entities e LEFT JOIN structures s ON s.id = e.reporting_style_id",
    ).all(),
  )


def _try_first_close(g):
  try:
    return _first_close(g)
  except Exception as exc:
    print(
      "  CLOSE REFUSED",
      type(exc).__name__,
      getattr(exc, "blockers", None),
      getattr(getattr(exc, "gate", None), "group_not_footing", None),
    )


def _earn(g, amount=100_000, on=JUNE_20, key="member"):
  _post(g, key, on, [("1000", amount), ("4000", -amount)], None)


def _closings(g):
  print(
    "  closing entries:",
    _sql(
      g, "SELECT entity_id, count(*) FROM entries WHERE type='closing' GROUP BY 1"
    ).all(),
  )


def test_s01_loss_before_the_first_month(group):
  _fund(group)
  _post(group, "member", JUNE_20, [("5000", 100_000), ("1000", -100_000)], None)
  _close_members(group, "2026-06", "2026-07")
  _first_close(group)
  _closings(group)
  _ledger(group)
  _s, bs = _bs(group)
  eq = _eq(group)
  print("  NCI held", _held(group, NCI))
  assert _held(group, NCI) == 270_000
  assert eq.validation.status != "failed"


def test_s02_distribution_before_the_first_month(group):
  _fund(group)
  _earn(group)
  _post(
    group, "member", date(2026, 6, 25), [("3000", 35_000), ("1000", -35_000)], "parent"
  )
  _post(group, "member", date(2026, 6, 25), [("3000", 15_000), ("1000", -15_000)], None)
  _post(
    group, "parent", date(2026, 6, 25), [("1000", 35_000), ("1600", -35_000)], "member"
  )
  _close_members(group, "2026-06", "2026-07")
  _first_close(group)
  _s, bs = _bs(group)
  _eq(group)
  print("  NCI held", _held(group, NCI), "expect 315000 = 30% x (10,000+1,000-500)")
  assert _held(group, NCI) == 315_000


def test_s03_wholly_owned_adds_nothing(group):
  _set_pct(group, 100)
  _fund(group, 1_000_000, 0)
  _earn(group)
  _close_members(group, "2026-06", "2026-07")
  r, p, c = _first_close(group)
  _bs(group)
  assert _held(group, NCI) == 0
  assert not any(pr.adjustment_kind == "nci_allocation" for pr in p.proposals)


def test_s04_odd_percentage(group):
  _set_pct(group, 66.67)
  _fund(group, 666_701, 333_300)
  _earn(group, 33_333)
  _close_members(group, "2026-06", "2026-07")
  _first_close(group)
  _bs(group)
  _eq(group)
  cap, res = 1_000_001, 33_333
  expect = (cap - round(cap * 0.6667)) + round(res * (1 - 0.6667))
  print(
    "  NCI held", _held(group, NCI), "expect", expect, "exact", (cap + res) * 0.3333
  )
  assert abs(_held(group, NCI) - (cap + res) * 0.3333) <= 2


def test_s05_member_joins_in_the_first_month(group):
  _fund(group)
  _earn(group)
  _sql(
    group,
    "UPDATE entities SET joined_on = '2026-07-10' WHERE id = :i",
    i=group["ids"]["member"],
  )
  group["session"].commit()
  _close_members(group, "2026-06", "2026-07")
  _try_first_close(group)
  _ledger(group)
  _bs(group)
  _eq(group)
  print("  NCI held", _held(group, NCI), "outside share of net assets 330000")


def test_s06_joined_mid_history_before_the_eve(group):
  _fund(group)
  _earn(group)
  _sql(
    group,
    "UPDATE entities SET joined_on = '2026-06-25' WHERE id = :i",
    i=group["ids"]["member"],
  )
  group["session"].commit()
  _close_members(group, "2026-06", "2026-07")
  try:
    _first_close(group)
  except Exception as exc:
    print(
      "  CLOSE REFUSED",
      type(exc).__name__,
      getattr(exc, "blockers", None),
      getattr(getattr(exc, "gate", None), "group_not_footing", None),
    )
  _ledger(group)
  _bs(group)
  _eq(group)
  print("  NCI held", _held(group, NCI))


def test_s07_first_month_is_the_members_first_month(group):
  _books(group)
  r, p, c = _first_close(group)
  assert not any(
    pr.opening and pr.adjustment_kind == "nci_allocation" for pr in p.proposals
  )
  assert _held(group, NCI) == 330_000


def test_s08_same_style_as_the_parent(group):
  _styles(group)
  _same_style(group)
  _fund(group)
  _earn(group)
  _close_members(group, "2026-06", "2026-07")
  _first_close(group)
  _ledger(group)
  _s, bs = _bs(group)
  _eq(group)
  assert bs.get("MembersEquity") == 700.0


def test_s09_style_differs_month_allocation_vs_opening(group):
  """Same economics two ways: 1,000 earned in July (the month's allocation)
  and 1,000 earned in June (the opening). Where does the 300 leave from?"""
  _books(group)
  _first_close(group)
  _s, bs = _bs(group)
  _eq(group)


from robosystems.models.api.extensions.consolidation import (  # noqa: E402
  RecordDeconsolidationRequest,
  RecordOwnershipChangeRequest,
  SetAllocationOverrideRequest,
)
from robosystems.models.api.extensions.journal_entries import (  # noqa: E402
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.operations.roboledger.commands import consolidation as ccmd  # noqa: E402
from robosystems.operations.roboledger.commands.fiscal_calendar import reopen_period  # noqa: E402
from robosystems.operations.roboledger.commands.journal_entries import (  # noqa: E402
  create_journal_entry,
)


def _add_account(g, key, code, name, trait, balance, qname, sub=None):
  session, entity_id, chart_id = g["session"], g["ids"][key], g["charts"][key]
  cash = session.execute(
    select(Element).where(Element.taxonomy_id == chart_id, Element.code == "1000")
  ).scalar_one()
  namespace = cash.qname.split(":")[0]
  fields = {
    "qname": f"{namespace}:{code}",
    "name": name,
    "trait": trait,
    "balance_type": balance,
    "code": code,
  }
  if sub:
    fields["sub_classification"] = sub
  update_taxonomy_block(
    session,
    UpdateTaxonomyBlockRequest(
      taxonomy_id=chart_id, elements_to_add=[TaxonomyBlockElementRequest(**fields)]
    ),
    "usr_1",
  )
  account = session.execute(
    select(Element.id).where(Element.taxonomy_id == chart_id, Element.code == code)
  ).scalar_one()
  target = session.execute(
    select(Element.id).where(Element.qname == qname)
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
  session.commit()


def _add_entity(g, key, name, pct):
  session = g["session"]
  g["ids"][key] = create_entity(
    session,
    CreateEntityRequest(name=name, entity_type="llc", ownership_pct=pct),
    "usr_1",
  ).id
  g["charts"][key] = initialize_chart_of_accounts(
    session,
    InitializeChartOfAccountsRequest(template="services", entity_id=g["ids"][key]),
    "usr_1",
  ).taxonomy_id
  g["service"].initialize(
    session, GRAPH, closed_through="2026-05", actor_id="usr_1", entity_id=g["ids"][key]
  )
  g["service"].ensure_fiscal_periods(
    session,
    GRAPH,
    start_period="2026-05",
    end_period="2026-08",
    closed_through="2026-05",
    entity_id=g["ids"][key],
  )
  session.commit()
  print(
    "  added",
    key,
    _sql(
      g,
      "SELECT consolidation_method, ownership_pct FROM entities WHERE id=:i",
      i=g["ids"][key],
    ).one(),
  )


def _acct(g, key, code):
  return str(
    g["session"]
    .execute(
      select(Element.id).where(
        Element.taxonomy_id == g["charts"][key], Element.code == code
      )
    )
    .scalar_one()
  )


def _journal(g, on, lines, kind, adjusts, status="posted"):
  return create_journal_entry(
    g["session"],
    CreateJournalEntryRequest(
      posting_date=on,
      memo="review hand entry",
      type="adjusting",
      status=status,
      adjustment_kind=kind,
      adjusts_entity_id=g["ids"][adjusts] if adjusts else None,
      line_items=[
        JournalEntryLineItemInput(
          element_id=_acct(g, "adjustments", code),
          debit_amount=max(a, 0),
          credit_amount=max(-a, 0),
        )
        for code, a in lines
      ],
    ),
    "usr_1",
    entity_id=g["ids"]["adjustments"],
  )


def _reopen(g, key, period):
  return reopen_period(
    g["session"],
    MagicMock(),
    GRAPH,
    period,
    "usr_1",
    "review",
    None,
    g["service"],
    entity_id=g["ids"][key],
  )


def _month(g, period, earn=None, keys=("member", "parent")):
  start, _e = engine.month_bounds(period)
  if earn:
    _earn(g, earn, on=start.replace(day=12))
  _close_members(g, period, keys=keys)
  _propose(g, period)
  p = _preview(g, period)
  _show(p)
  c = _close(g, "adjustments", period)
  print("  ", period, "stamped", c.statements_stamped, "NCI held", _held(g, NCI))
  return p


def _more_periods(g, through="2026-12"):
  for key, eid in g["ids"].items():
    first = "2026-07" if key == "adjustments" else "2026-05"
    ct = _sql(
      g, "SELECT closed_through_period FROM fiscal_calendar WHERE entity_id=:e", e=eid
    ).scalar()
    g["service"].ensure_fiscal_periods(
      g["session"],
      GRAPH,
      start_period=first,
      end_period=through,
      closed_through=ct,
      entity_id=eid,
    )
  g["session"].commit()


def _history(g):
  """June: capital 7,000 + 3,000, the member earns 1,000."""
  _same_style(g)
  _fund(g)
  _earn(g)
  _close_members(g, "2026-06")


def test_l1_later_months_repropose_reopen(group):
  _history(group)
  _close_members(group, "2026-07")
  _earn_july = None
  _propose(group)
  r2 = _propose(group)
  print("  second propose entries", [(e.type, e.posting_date) for e in r2.entries])
  openings = _sql(
    group, "SELECT status, count(*) FROM entries WHERE type='opening' GROUP BY 1"
  ).all()
  print("  openings", openings)
  p = _preview(group)
  _show(p)
  assert p.up_to_date
  assert engine.opening_is_recorded(
    group["session"], group["ids"]["adjustments"], p.proposals
  )
  _close(group, "adjustments", "2026-07")
  assert _held(group, NCI) == 330_000
  _month(group, "2026-08", earn=50_000)
  assert _held(group, NCI) == 345_000
  # reopen both, re-propose, re-close
  _reopen(group, "adjustments", "2026-08")
  _reopen(group, "adjustments", "2026-07")
  p = _preview(group)
  _show(p)
  print("  after reopen: up_to_date", p.up_to_date)
  _propose(group)
  _close(group, "adjustments", "2026-07")
  _propose(group, "2026-08")
  p8 = _preview(group, "2026-08")
  _show(p8)
  _close(group, "adjustments", "2026-08")
  print("  NCI after reopen/reclose", _held(group, NCI))
  _ledger(group)
  assert _held(group, NCI) == 345_000
  _bs(group, "2026-08")
  eq = _eq(group, "2026-08")
  assert eq.validation.status != "failed"


def test_l2_ownership_change_after_the_opening(group):
  _history(group)
  _more_periods(group)
  _close_members(group, "2026-07")
  _propose(group)
  _close(group, "adjustments", "2026-07")
  _month(group, "2026-08", earn=70_000)
  # 1 Sept: the parent buys 10% from the outside owners for 1,300
  _post(
    group, "parent", date(2026, 9, 1), [("1600", 130_000), ("1000", -130_000)], None
  )
  rec = ccmd.record_ownership_change(
    group["session"],
    RecordOwnershipChangeRequest(
      entity_id=group["ids"]["member"],
      period="2026-09",
      ownership_pct=80,
      consideration=130_000,
      basis="SPA",
    ),
    "usr_1",
  )
  print(
    "  change: book",
    rec.book_value,
    "earnings",
    rec.earnings_transferred,
    "premium",
    rec.premium_transferred,
    "equity adj",
    rec.equity_adjustment,
  )
  _month(group, "2026-09", earn=10_000)
  _ledger(group)
  _bs(group, "2026-09")
  eq = _eq(group, "2026-09")
  # net assets 10,000 + 1,000 + 700 + 100 = 11,800; outside 20% = 2,360
  print("  NCI held", _held(group, NCI), "expect 236000")
  assert _held(group, NCI) == 236_000
  assert eq.validation.status != "failed"


def test_l3_deconsolidation_after_the_opening(group):
  _history(group)
  _more_periods(group)
  _close_members(group, "2026-07")
  _propose(group)
  _close(group, "adjustments", "2026-07")
  _month(group, "2026-08", earn=70_000)
  _sql(
    group,
    "UPDATE entities SET left_on = '2026-08-31' WHERE id = :i",
    i=group["ids"]["member"],
  )
  group["session"].commit()
  # the parent sells its 70% for its share of net assets, 8,190: cost 7,000, gain 1,190 (booked on revenue here)
  _post(
    group,
    "parent",
    date(2026, 9, 1),
    [("1000", 819_000), ("1600", -700_000), ("4000", -119_000)],
    "member",
  )
  rec = ccmd.record_deconsolidation(
    group["session"],
    RecordDeconsolidationRequest(
      entity_id=group["ids"]["member"], consideration=819_000, basis="Sale"
    ),
    "usr_1",
  )
  print(
    "  derecognized",
    [(d.concept, d.amount) for d in rec.derecognized],
    "restored",
    rec.earnings_restored,
    "gain_loss",
    rec.gain_loss,
  )
  assert rec.earnings_restored == 170_000
  assert rec.gain_loss == -119_000  # the parent's 70% of 1,700
  _close_members(group, "2026-09", keys=("parent",))
  _propose(group, "2026-09")
  p = _preview(group, "2026-09")
  _show(p)
  _close(group, "adjustments", "2026-09")
  _ledger(group)
  _s, bs = _bs(group, "2026-09")
  _eq(group, "2026-09")
  assert _held(group, NCI) == 0
  assert bs.get("StockholdersEquity") == 1190.0


def test_o1_override_set_for_a_month_before_the_group(group):
  _history(group)
  ccmd.set_allocation_override(
    group["session"],
    SetAllocationOverrideRequest(
      entity_id=group["ids"]["member"],
      period="2026-06",
      amount=50_000,
      basis="Operating agreement: preferred return",
    ),
    "usr_1",
  )
  share = engine.allocation_share(
    group["session"], entity_id=group["ids"]["member"], period="2026-06"
  )
  print(
    "  allocation_share June: pro_rata",
    share.pro_rata_amount,
    "effective",
    share.effective_amount,
    "closed",
    share.closed,
  )
  _close_members(group, "2026-07")
  _first_close(group)
  print(
    "  NCI held",
    _held(group, NCI),
    "(3,000 capital + override 500 = 350000; pro rata 330000)",
  )


def test_m2_two_members(group):
  _same_style(group)
  _add_entity(group, "second", "Birch Row LLC", 60)
  _fund(group)
  _earn(group)
  _post(group, "parent", JUNE_20, [("1600", 300_000), ("1000", -300_000)], "second")
  _post(group, "second", JUNE_20, [("1000", 300_000), ("3000", -300_000)], "parent")
  _post(group, "second", JUNE_20, [("1000", 200_000), ("3000", -200_000)], None)
  _post(group, "second", JUNE_20, [("5000", 40_000), ("1000", -40_000)], None)
  _close_members(group, "2026-06", "2026-07", keys=("member", "second", "parent"))
  _first_close(group)
  _bs(group)
  eq = _eq(group)
  print(
    "  NCI held", _held(group, NCI), "expect 330000 + 40% x 4,600 = 184000 -> 514000"
  )
  assert _held(group, NCI) == 514_000
  assert eq.validation.status != "failed"


def _victim(g, monkeypatch):
  """A group that closed its first month on the code before 3e9e0cc39."""
  _history(g)
  _more_periods(g)
  _close_members(g, "2026-07")
  with monkeypatch.context() as m:
    m.setattr(engine, "_open_on_earlier_results", lambda *a, **k: None)
    _propose(g)
    _close(g, "adjustments", "2026-07")
  assert _held(g, NCI) == 300_000


def test_v2_reopening_back_to_the_first_month_repairs_it(group, monkeypatch):
  _victim(group, monkeypatch)
  _month(group, "2026-08", earn=50_000)
  _reopen(group, "adjustments", "2026-08")
  _reopen(group, "adjustments", "2026-07")
  p = _preview(group)
  _show(p)
  with group["session"].begin_nested():
    status = engine.group_close_status(group["session"], GRAPH, "2026-07")
  print("  July gate", status.pending_adjustments)
  _propose(group)
  _close(group, "adjustments", "2026-07")
  _propose(group, "2026-08")
  _close(group, "adjustments", "2026-08")
  print("  NCI held", _held(group, NCI))
  assert _held(group, NCI) == 345_000


@pytest.mark.parametrize("group", ["fresh", "baseline"], indirect=True)
def test_e1_an_associates_earlier_results(group):
  _same_style(group)
  _add_entity(group, "assoc", "Cedar Partners LLC", 30)
  _add_account(
    group,
    "parent",
    "1700",
    "Investment in Cedar",
    "asset",
    "debit",
    engine.EQUITY_METHOD_INVESTMENT,
    "other_noncurrent_assets",
  )
  _add_account(
    group,
    "parent",
    "4900",
    "Distributions received",
    "revenue",
    "credit",
    "rs-gaap:OtherNonoperatingIncomeExpense",
  )
  _fund(group)
  _post(group, "parent", JUNE_20, [("1700", 300_000), ("1000", -300_000)], "assoc")
  _post(group, "assoc", JUNE_20, [("1000", 300_000), ("3000", -300_000)], "parent")
  _post(group, "assoc", JUNE_20, [("1000", 700_000), ("3000", -700_000)], None)
  _post(group, "assoc", JUNE_20, [("1000", 100_000), ("4000", -100_000)], None)
  d = date(2026, 6, 25)
  _post(group, "assoc", d, [("3000", 6_000), ("1000", -6_000)], "parent")
  _post(group, "assoc", d, [("3000", 14_000), ("1000", -14_000)], None)
  _post(group, "parent", d, [("1000", 6_000), ("4900", -6_000)], "assoc")
  _close_members(group, "2026-06", "2026-07", keys=("member", "assoc", "parent"))
  _propose(group)
  p = _preview(group)
  _show(p)
  with group["session"].begin_nested():
    status = engine.group_close_status(group["session"], GRAPH, "2026-07")
  print("  gate", status)
  # the remedy the note names: a consolidating entry on the eve
  for kind in ("equity_method_pickup", "manual"):
    try:
      with group["session"].begin_nested():
        _journal(
          group, date(2026, 6, 30), [("1700", 24_000), ("3130", -24_000)], kind, "assoc"
        )
      print("  eve entry accepted as", kind)
    except Exception as exc:
      print("  EVE ENTRY REFUSED", kind, type(exc).__name__, str(exc)[:200])
    group["session"].rollback()
  c = _close(group, "adjustments", "2026-07")
  print("  closed, stamped", c.statements_stamped)
  _s, bs = _bs(group)
  print(
    "  equity-method investment on the consolidated balance sheet",
    bs.get("EquityMethodInvestments"),
    "equity method carrying 3,240.00 (3,000 + 300 pickup - 60 received)",
  )
  assert bs.get("EquityMethodInvestments") == 3240.0


def test_r1_the_opening_leaves_the_parents_equity_row(group):
  """The fixture as the commit's own test uses it: an LLC parent, a
  consolidation entity on the corporate default Style."""
  _styles(group)
  _fund(group)
  _earn(group)
  _close_members(group, "2026-06", "2026-07")
  _first_close(group)
  _s, bs = _bs(group)
  eq = _eq(group)
  i = _col(eq)
  members = next(
    f.values[i]
    for f in eq.facts
    if f.qname == EQ and "ending" in (f.name or "").lower()
  )
  print("  balance sheet", bs)
  print(
    "  equity statement Members' Equity ending",
    members,
    "; controlling equity is 700.00",
  )
  assert "RetainedEarningsAccumulatedDeficit" not in bs, bs
  assert bs["MembersEquity"] == 700.0
  assert members == 700.0


def test_r1b_corporate_parent_llc_consolidation_entity(group):
  """The configuration docs/technical/Group-Consolidation.md recommends for a
  group of LLCs under a corporate parent."""
  llc = _sql(
    group,
    "SELECT reporting_style_id FROM entities WHERE id = :p",
    p=group["ids"]["parent"],
  ).scalar()
  corp = _sql(
    group,
    "SELECT reporting_style_id FROM entities WHERE id = :a",
    a=group["ids"]["adjustments"],
  ).scalar()
  _sql(
    group,
    "UPDATE entities SET reporting_style_id = :s, entity_type = 'corporation' WHERE id = :p",
    s=corp,
    p=group["ids"]["parent"],
  )
  _sql(
    group,
    "UPDATE entities SET reporting_style_id = :s, entity_type = 'llc' WHERE id = :a",
    s=llc,
    a=group["ids"]["adjustments"],
  )
  group["session"].commit()
  _styles(group)
  _fund(group)
  _earn(group)
  _close_members(group, "2026-06", "2026-07")
  _try_first_close(group)
  _ledger(group)
  _s, bs = _bs(group)
  _eq(group)
  print("  balance sheet", bs, "; the group's retained earnings are 700.00")
  assert bs.get("RetainedEarningsAccumulatedDeficit") == 700.0


def test_w1_opening_balance_member_first_month_with_its_own_results(group):
  """The member's books begin with an opening balance on 31 May (10,000 of
  equity); it earns 1,000 in June and 500 in July, the group's first month."""
  from robosystems.operations.roboledger.reads.reports import (
    get_consolidation_worksheet,
  )

  _same_style(group)
  session = group["session"]
  entry = Entry(
    entity_id=group["ids"]["member"],
    type="opening",
    status="posted",
    posting_date=date(2026, 5, 31),
    created_by="test",
  )
  session.add(entry)
  session.flush()
  for order, (code, amount) in enumerate(
    [("1000", 1_000_000), ("3000", -1_000_000)], 1
  ):
    session.add(
      LineItem(
        entry_id=entry.id,
        element_id=_acct(group, "member", code),
        debit_amount=max(amount, 0),
        credit_amount=max(-amount, 0),
        line_order=order,
      )
    )
  session.commit()
  _post(
    group, "parent", date(2026, 6, 1), [("1600", 700_000), ("1000", -700_000)], "member"
  )
  _earn(group)
  _earn(group, 50_000, on=JULY_15)
  _close_members(group, "2026-06", "2026-07")
  r, p, c = _first_close(group)
  _ledger(group)
  _s, bs = _bs(group)
  eq = _eq(group)
  cf = _stmt(group, "cash_flow_statement")
  print(
    "  CF",
    cf.validation.status if cf.validation else None,
    cf.validation.failures if cf.validation else None,
  )
  inc = _stmt(group, "income_statement")
  i = _col(inc)
  print(
    "  IS", [(f.qname.split(":")[-1], f.values[i]) for f in inc.facts if f.values[i]]
  )
  for kind in ("balance_sheet", "equity_statement"):
    ws = get_consolidation_worksheet(
      session,
      graph_id=GRAPH,
      statement_type=kind,
      period_start=date(2026, 7, 1),
      period_end=date(2026, 7, 31),
    )
    print("  WS", kind, [c_.label for c_ in ws.columns])
    for row in ws.rows:
      if (
        "MinorityInterest" in row.qname
        or "MembersEquity" in row.qname
        or row.qname.endswith("StockholdersEquity")
      ):
        print("     ", row.qname.split(":")[-1], row.period_role, row.values)
  print("  NCI held", _held(group, NCI), "expect 30% x 11,500 = 345000")
  assert _held(group, NCI) == 345_000
  month = [
    ln.debit_amount - ln.credit_amount
    for pr in p.proposals
    if not pr.opening and pr.adjustment_kind == "nci_allocation"
    for ln in pr.lines
    if ln.concept == NCI
  ]
  assert month == [-15_000]
  assert eq.validation.status != "failed" and bs["MembersEquity"] == 1050.0
