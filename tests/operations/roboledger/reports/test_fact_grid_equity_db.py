"""The equity roll-forward on real Postgres: every equity line becomes a
movement row, cash or not, and the root's balance the day before the earliest
column opens is stored, so beginning balance + changes = ending balance."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.extensions.association import Association
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.element_trait import ElementTrait
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.structure import Structure
from robosystems.models.extensions.taxonomy import Taxonomy
from robosystems.models.extensions.trait import Trait
from robosystems.operations.roboledger.reports.fact_grid import (
  ROLLFORWARD_ROOTS,
  FactRow,
  PeriodSpec,
  _build_rows,
  _facts_to_balance_dict,
  _HierarchyNode,
  _with_opening_balance,
  generate_report_facts,
)
from robosystems.operations.roboledger.reports.guard_rails import validate_report
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity_on

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd12"
FY26 = PeriodSpec(start=date(2026, 1, 1), end=date(2026, 12, 31), label="2026")
FY25 = PeriodSpec(start=date(2025, 1, 1), end=date(2025, 12, 31), label="2025")

SE = "rs-gaap:StockholdersEquity"
MEMBERS = "rs-gaap:MembersEquity"
ISSUED = "rs-gaap:StockIssuedDuringPeriodValueNewIssues"
REPURCHASED = "rs-gaap:StockRepurchasedDuringPeriodValue"
DIVIDENDS = "rs-gaap:Dividends"
OTHER = "rs-gaap:StockholdersEquityOther"
CONTRIBUTED = "rs-gaap:PartnersCapitalAccountContributions"
DISTRIBUTED = "rs-gaap:PartnersCapitalAccountDistributions"
CAPITAL_OTHER = "rs-gaap:PartnersCapitalOther"
LLC_DISTRIBUTIONS = "rs-gaap:DistributionsMadeToLimitedLiabilityCompanyLlcMember"
APIC_SBC = "rs-gaap:AdjustmentsToAdditionalPaidInCapitalSharebasedCompensationRequisiteServicePeriodRecognitionValue"
UNIT_SBC = "rs-gaap:PartnersCapitalAccountUnitBasedCompensation"
NCI = "rs-gaap:MinorityInterest"
NCI_INCOME = "rs-gaap:NetIncomeLossAttributableToNoncontrollingInterest"
NCI_CONTRIBUTED = "rs-gaap:NoncontrollingInterestIncreaseFromSubsidiaryEquityIssuance"
NCI_DISTRIBUTED = (
  "rs-gaap:MinorityInterestDecreaseFromDistributionsToNoncontrollingInterestHolders"
)
NCI_IN = "rs-gaap:ProceedsFromMinorityShareholders"
NCI_OUT = "rs-gaap:PaymentsToMinorityShareholders"


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
    yield _seed()
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _element(qname: str, balance: str, period: str) -> Element:
  return Element(
    qname=qname,
    name=qname.split(":")[1],
    balance_type=balance,
    period_type=period,
    element_type="concept",
    is_abstract=False,
  )


def _seed() -> dict[str, str]:
  """A small chart mapped to rs-gaap: cash, a card, another asset, paid-in
  capital and retained earnings summing to stockholders' equity, members'
  equity, an LLC distributions account mapped to a reducer flow, revenue and
  an expense. Paid-in capital and retained earnings carry their default
  financing flows; members' equity carries the LLC pair."""
  ids: dict[str, str] = {}
  with extensions_session(GRAPH) as session:
    mapping_taxonomy = Taxonomy(name="test", taxonomy_type="mapping")
    calc_taxonomy = Taxonomy(
      name="calc", taxonomy_type="reporting", standard="rs-gaap-calculations"
    )
    session.add_all([mapping_taxonomy, calc_taxonomy])
    session.flush()
    mapping = Structure(
      name="CoA mapping", block_type="coa_mapping", taxonomy_id=mapping_taxonomy.id
    )
    calc = Structure(
      name="BS calc", block_type="balance_sheet", taxonomy_id=calc_taxonomy.id
    )
    session.add_all([mapping, calc])
    session.flush()
    ids["mapping"] = mapping.id

    traits = {
      name: Trait(category="elementsOfFinancialStatements", identifier=name)
      for name in ("asset", "liability", "equity", "revenue", "expense")
    }
    financing = Trait(category="activityType", identifier="financingActivity")
    session.add_all([*traits.values(), financing])
    session.flush()

    concepts = {
      "cash": (_element("rs-gaap:Cash", "debit", "instant"), "asset"),
      "card": (
        _element("rs-gaap:OtherLiabilitiesCurrent", "credit", "instant"),
        "liability",
      ),
      "metals": (
        _element("rs-gaap:OtherAssetsNoncurrent", "debit", "instant"),
        "asset",
      ),
      "apic": (
        _element("rs-gaap:AdditionalPaidInCapital", "credit", "instant"),
        "equity",
      ),
      "re": (
        _element("rs-gaap:RetainedEarningsAccumulatedDeficit", "credit", "instant"),
        "equity",
      ),
      "members": (_element(MEMBERS, "credit", "instant"), "equity"),
      "llc_distributions": (_element(LLC_DISTRIBUTIONS, "credit", "duration"), None),
      "revenue": (_element("rs-gaap:Revenues", "credit", "duration"), "revenue"),
      "expense": (
        _element("rs-gaap:GeneralAndAdministrativeExpense", "debit", "duration"),
        "expense",
      ),
      "stock_comp": (
        _element("rs-gaap:ShareBasedCompensation", "debit", "duration"),
        "expense",
      ),
      "nci": (_element(NCI, "credit", "instant"), "equity"),
      "nci_income": (_element(NCI_INCOME, "debit", "duration"), "expense"),
    }
    session.add_all(c for c, _ in concepts.values())
    session.flush()
    for key, (concept, trait) in concepts.items():
      if trait:
        session.add(
          ElementTrait(
            element_id=concept.id, trait_id=traits[trait].id, is_primary=True
          )
        )
      source = _element(f"coa:{key}", concept.balance_type, concept.period_type)
      session.add(source)
      session.flush()
      session.add(
        Association(
          structure_id=mapping.id,
          from_element_id=source.id,
          to_element_id=concept.id,
          association_type="mapping",
        )
      )
      ids[key] = source.id
      ids[f"{key}_concept"] = concept.id

    se = _element(SE, "credit", "instant")
    ni = _element("rs-gaap:NetIncomeLoss", "credit", "duration")
    movements = [
      _element(ISSUED, "credit", "duration"),
      _element(REPURCHASED, "debit", "duration"),
      _element(DIVIDENDS, "debit", "duration"),
      _element(OTHER, "debit", "duration"),
      _element(CONTRIBUTED, "credit", "duration"),
      _element(DISTRIBUTED, "debit", "duration"),
      _element(CAPITAL_OTHER, "credit", "duration"),
      _element(APIC_SBC, "credit", "duration"),
      _element(UNIT_SBC, "credit", "duration"),
      _element(NCI_CONTRIBUTED, "credit", "duration"),
      _element(NCI_DISTRIBUTED, "debit", "duration"),
    ]
    # The outside owners' flows, tagged on the lines that move the interest.
    for qname in (NCI_IN, NCI_OUT):
      flow = _element(qname, "credit", "duration")
      session.add(flow)
      session.flush()
      ids[qname] = flow.id
    session.add_all([se, ni, *movements])
    session.flush()
    for child in ("apic", "re"):
      session.add(
        Association(
          structure_id=calc.id,
          from_element_id=se.id,
          to_element_id=ids[f"{child}_concept"],
          association_type="calculation",
          weight=1,
        )
      )

    # Default financing flows: (flow, the concept it moves, the direction).
    for qname, concept, weight in (
      ("rs-gaap:ProceedsFromIssuanceOfCommonStock", "apic", 1),
      ("rs-gaap:PaymentsForRepurchaseOfCommonStock", "apic", -1),
      ("rs-gaap:PaymentsOfDividends", "re", -1),
      ("rs-gaap:ProceedsFromContributedCapital", "members", 1),
      ("rs-gaap:PaymentsOfCapitalDistribution", "members", -1),
    ):
      flow = session.execute(
        text("SELECT id FROM elements WHERE qname = :q"), {"q": qname}
      ).scalar()
      if flow is None:
        element = _element(qname, "credit", "duration")
        session.add(element)
        session.flush()
        session.add(
          ElementTrait(element_id=element.id, trait_id=financing.id, is_primary=True)
        )
        flow = element.id
        ids[qname] = flow
      session.add(
        Association(
          structure_id=mapping.id,
          from_element_id=flow,
          to_element_id=ids[f"{concept}_concept"],
          association_type="derivation",
          weight=weight,
        )
      )
  return ids


def _post(ids, posting_date, lines):
  """``lines`` are ``(account key, debit dollars, credit dollars)``, with the
  qname of a flow tag as an optional fourth."""
  with extensions_session(GRAPH) as session:
    entry = Entry(
      entity_id=PARENT_ENTITY_ID,
      type="standard",
      status="posted",
      posting_date=posting_date,
      memo="test",
      created_by="test",
    )
    session.add(entry)
    session.flush()
    for order, (key, debit, credit, *flow) in enumerate(lines, start=1):
      session.add(
        LineItem(
          entry_id=entry.id,
          element_id=ids[key],
          debit_amount=round(debit * 100),
          credit_amount=round(credit * 100),
          flow_element_id=ids[flow[0]] if flow else None,
          line_order=order,
        )
      )


@pytest.fixture(autouse=True)
def empty_ledger(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
  yield


def _facts(ids, periods, close_target="rs-gaap:RetainedEarningsAccumulatedDeficit"):
  with extensions_session(GRAPH) as session:
    return generate_report_facts(
      session, "", ids["mapping"], periods, close_target_qname=close_target
    ).facts


def _by_qname(facts, period: PeriodSpec) -> dict[str, float]:
  out: dict[str, float] = {}
  for f in facts:
    if (f.period_start, f.period_end) == (period.start, period.end):
      out[f.element_qname] = round(out.get(f.element_qname, 0.0) + f.value, 2)
  return out


def _corporate_year(ids) -> None:
  """FY2025: a cash contribution and a metals purchase. FY2026: cash
  revenue, the owner paying the company card (no cash), the card's expense,
  a distribution carved out of the metals (no cash), an expense settled in
  paid-in capital (no counterpart but income), and a cash dividend."""
  _post(ids, date(2025, 6, 1), [("cash", 1000, 0), ("apic", 0, 1000)])
  _post(ids, date(2025, 6, 2), [("metals", 200, 0), ("cash", 0, 200)])
  _post(ids, date(2026, 2, 1), [("cash", 500, 0), ("revenue", 0, 500)])
  _post(ids, date(2026, 3, 1), [("card", 300, 0), ("apic", 0, 300)])
  _post(ids, date(2026, 3, 2), [("expense", 300, 0), ("card", 0, 300)])
  _post(ids, date(2026, 5, 1), [("re", 100, 0), ("metals", 0, 100)])
  _post(ids, date(2026, 6, 1), [("expense", 50, 0), ("apic", 0, 50)])
  _post(ids, date(2026, 7, 1), [("re", 40, 0), ("cash", 0, 40)])


def test_every_equity_line_is_a_movement_cash_or_not(tenant):
  """The owner's card payment is a contribution, the carved-out metals a
  distribution beside the cash dividend, and the expense settled in capital
  is other — and the cash flow's own rows are untouched by any of it."""
  _corporate_year(tenant)
  fy26 = _by_qname(_facts(tenant, [FY26, FY25]), FY26)

  assert fy26[ISSUED] == 300.0
  assert fy26[DIVIDENDS] == -140.0
  assert fy26[OTHER] == 50.0
  assert REPURCHASED not in fy26
  assert fy26["rs-gaap:NetIncomeLoss"] == 150.0
  assert fy26[SE] == 1360.0
  # Only the cash dividend is a financing cash flow.
  assert fy26["rs-gaap:PaymentsOfDividends"] == -40.0
  assert "rs-gaap:ProceedsFromIssuanceOfCommonStock" not in fy26


def test_the_rollforward_reaches_its_closing_balance(tenant):
  """Rendered, the statement opens on the prior column's closing balance and
  the earliest column on the stored opening instant; both articulate."""
  _corporate_year(tenant)
  facts = _facts(tenant, [FY26, FY25])
  periods = [FY26, FY25]
  by_period = [_by_qname(facts, p) for p in periods]

  def row(qname: str, depth: int, *, subtotal: bool = False) -> FactRow:
    return FactRow(
      element_id=next(f.element_id for f in facts if f.element_qname == qname),
      element_qname=qname,
      element_name=qname.split(":")[1],
      classification=None,
      balance_type="credit",
      values=[p.get(qname, 0.0) for p in by_period],
      is_subtotal=subtotal,
      depth=depth,
    )

  rows = [
    row("rs-gaap:NetIncomeLoss", 1),
    row(ISSUED, 1),
    row(DIVIDENDS, 1),
    row(OTHER, 1),
    row(SE, 0, subtotal=True),
  ]
  rows = _with_opening_balance(rows, facts, periods, _hierarchy(rows))

  assert rows[0].period_role == "start"
  assert rows[0].values == [1000.0, 0.0]
  assert rows[-1].period_role == "end"
  assert rows[-1].values == [1360.0, 1000.0]
  result = validate_report("equity_statement", rows, period_labels=["2026", "2025"])
  assert (result.status, result.failures) == ("passed", [])


def test_the_earliest_column_opens_on_a_stored_instant(tenant):
  """A single column has no prior column to open on: its opening balance is
  stored as an instant fact the day before it starts."""
  _corporate_year(tenant)
  facts = _facts(tenant, [FY26])
  opening = [
    f for f in facts if f.element_qname == SE and f.period_end == date(2025, 12, 31)
  ]
  assert [(f.period_start, f.period_type, f.value) for f in opening] == [
    (date(2025, 12, 31), "instant", 1000.0)
  ]


def test_the_capital_account_form_uses_its_own_rows(tenant):
  """On an LLC Style the roll-forward roots at members' equity: an owner's
  non-cash contribution and a distribution through an account mapped to a
  reducer flow land on the capital-account rows, and the root moves by
  exactly them plus net income."""
  _post(tenant, date(2026, 1, 10), [("cash", 500, 0), ("members", 0, 500)])
  _post(tenant, date(2026, 2, 1), [("card", 120, 0), ("members", 0, 120)])
  _post(tenant, date(2026, 3, 1), [("llc_distributions", 70, 0), ("cash", 0, 70)])
  _post(tenant, date(2026, 4, 1), [("cash", 30, 0), ("revenue", 0, 30)])
  facts = _facts(tenant, [FY26], close_target=MEMBERS)
  fy26 = _by_qname(facts, FY26)

  assert fy26[CONTRIBUTED] == 620.0
  assert fy26[DISTRIBUTED] == -70.0
  assert ISSUED not in fy26 and DIVIDENDS not in fy26
  assert fy26[MEMBERS] == 0.0 + 620.0 - 70.0 + 30.0


def test_equity_booked_against_share_based_compensation_is_its_own_row(tenant):
  """A stock award's cost, expensed against paid-in capital, is the
  share-based compensation movement on both forms; equity booked against any
  other expense stays other. Either way the root moves by net income and the
  rows."""
  _post(tenant, date(2026, 2, 1), [("cash", 1000, 0), ("revenue", 0, 1000)])
  _post(tenant, date(2026, 3, 31), [("stock_comp", 250, 0), ("apic", 0, 250)])
  _post(tenant, date(2026, 6, 1), [("expense", 40, 0), ("apic", 0, 40)])
  fy26 = _by_qname(_facts(tenant, [FY26]), FY26)
  assert fy26[APIC_SBC] == 250.0
  assert fy26[OTHER] == 40.0
  assert fy26["rs-gaap:NetIncomeLoss"] == 1000.0 - 250.0 - 40.0
  assert fy26[SE] == 1000.0

  _post(tenant, date(2026, 4, 30), [("stock_comp", 60, 0), ("members", 0, 60)])
  llc = _by_qname(_facts(tenant, [FY26], close_target=MEMBERS), FY26)
  assert llc[UNIT_SBC] == 60.0


def _hierarchy(rows: list[FactRow]) -> list[_HierarchyNode]:
  """The Style's tree over ``rows``: each depth-0 row the root of the depth-1
  rows before it."""
  roots: list[_HierarchyNode] = []
  children: list[_HierarchyNode] = []
  for r in rows:
    node = _HierarchyNode(
      element_id=r.element_id,
      qname=r.element_qname,
      name=r.element_name,
      classification="",
      balance_type=r.balance_type,
      is_abstract=False,
      depth=r.depth,
    )
    if r.depth == 0:
      node.children = children
      roots.append(node)
      children = []
    else:
      children.append(node)
  return roots


def _group_year(ids, *, unexplained: bool = False) -> None:
  """The outside owners contribute 600 and are paid 90, and their share of
  the year's income, 120, is allocated to them; ``unexplained`` adds a line on
  the interest that carries no owner flow."""
  _post(ids, date(2026, 1, 10), [("cash", 600, 0), ("nci", 0, 600, NCI_IN)])
  _post(ids, date(2026, 2, 1), [("cash", 1000, 0), ("revenue", 0, 1000)])
  _post(ids, date(2026, 12, 31), [("nci_income", 120, 0), ("nci", 0, 120)])
  _post(ids, date(2026, 9, 30), [("nci", 90, 0, NCI_OUT), ("cash", 0, 90)])
  if unexplained:
    _post(ids, date(2026, 10, 1), [("cash", 50, 0), ("nci", 0, 50)])


def _rendered(facts, periods) -> list[FactRow]:
  """The capital's roll-forward and the interest's beside it, in the Style's
  order (each root after its rows), opened."""
  by_period = [_by_qname(facts, p) for p in periods]
  layout = [
    ("rs-gaap:NetIncomeLoss", 1),
    (ISSUED, 1),
    (SE, 0),
    (NCI_INCOME, 1),
    (NCI_CONTRIBUTED, 1),
    (NCI_DISTRIBUTED, 1),
    (NCI, 0),
  ]
  ids = {f.element_qname: f.element_id for f in facts}
  rows = [
    FactRow(
      element_id=ids.get(qname, qname),
      element_qname=qname,
      element_name=qname.split(":")[1],
      classification=None,
      balance_type="credit",
      values=[p.get(qname, 0.0) for p in by_period],
      is_subtotal=depth == 0,
      depth=depth,
      position=float(position),
    )
    for position, (qname, depth) in enumerate(layout)
  ]
  return _with_opening_balance(rows, facts, periods, _hierarchy(rows))


def test_the_noncontrolling_interest_rolls_forward_beside_the_capital(tenant):
  """Contributions and distributions read off the owners' flow tags; the
  share of income is the income statement's own line; the interest moves by
  exactly them, and both roll-forwards open on their own balances."""
  _group_year(tenant)
  facts = _facts(tenant, [FY26])
  fy26 = _by_qname(facts, FY26)

  assert fy26[NCI_CONTRIBUTED] == 600.0
  assert fy26[NCI_DISTRIBUTED] == -90.0
  assert fy26[NCI_INCOME] == 120.0
  assert fy26[NCI] == 600.0 + 120.0 - 90.0

  rows = _rendered(facts, [FY26])
  assert [(r.element_qname, r.period_role) for r in rows] == [
    (SE, "start"),
    ("rs-gaap:NetIncomeLoss", None),
    (ISSUED, None),
    (SE, "end"),
    (NCI, "start"),
    (NCI_INCOME, None),
    (NCI_CONTRIBUTED, None),
    (NCI_DISTRIBUTED, None),
    (NCI, "end"),
  ]
  result = validate_report("equity_statement", rows, period_labels=["2026"])
  assert (result.status, result.failures) == ("passed", [])


def test_an_unexplained_movement_of_the_interest_is_not_plugged(tenant):
  """A line on the interest with no owner flow takes no row: the
  roll-forward misses its ending balance by it, and the validator names the
  interest."""
  _group_year(tenant, unexplained=True)
  facts = _facts(tenant, [FY26])
  result = validate_report(
    "equity_statement", _rendered(facts, [FY26]), period_labels=["2026"]
  )
  assert result.status == "failed"
  assert len(result.failures) == 1
  assert "MinorityInterest does not roll forward" in result.failures[0]
  assert "difference: 50.00" in result.failures[0]


def test_an_interest_wound_down_to_nothing_still_rolls_forward(tenant):
  """The outside owners' 500 is paid out in 2026 with no owner flow on the
  line: the interest ends at zero, and its roll-forward stays to open on the
  500 and fail by it rather than vanish beside a capital that ties."""
  _post(tenant, date(2025, 3, 1), [("cash", 1000, 0), ("apic", 0, 1000)])
  _post(tenant, date(2025, 6, 1), [("cash", 500, 0), ("nci", 0, 500, NCI_IN)])
  _post(tenant, date(2026, 6, 1), [("nci", 500, 0), ("cash", 0, 500)])
  facts = _facts(tenant, [FY26])
  ids = {f.element_qname: f.element_id for f in facts}
  hierarchy = [
    _HierarchyNode(
      element_id=ids[root],
      qname=root,
      name=root.split(":")[1],
      classification="",
      balance_type="credit",
      is_abstract=False,
      depth=0,
    )
    for root in (SE, NCI)
  ]
  rows = _build_rows(
    hierarchy,
    [_facts_to_balance_dict(facts, FY26.start, FY26.end)],
    {},
    pre_signed=True,
    keep=ROLLFORWARD_ROOTS,
  )
  rows = _with_opening_balance(rows, facts, [FY26], hierarchy)

  assert [
    (r.element_qname, r.period_role, r.values) for r in rows if r.element_qname == NCI
  ] == [
    (NCI, "start", [500.0]),
    (NCI, "end", [0.0]),
  ]
  result = validate_report("equity_statement", rows, period_labels=["2026"])
  assert result.status == "failed"
  assert any("MinorityInterest does not roll forward" in f for f in result.failures)


def test_a_roll_forward_with_nothing_in_it_is_left_out(tenant):
  """A company with no outside owners shows no interest at all."""
  _post(tenant, date(2025, 3, 1), [("cash", 1000, 0), ("apic", 0, 1000)])
  facts = _facts(tenant, [FY26])
  ids = {f.element_qname: f.element_id for f in facts}
  hierarchy = [
    _HierarchyNode(
      element_id=ids.get(root, root),
      qname=root,
      name=root.split(":")[1],
      classification="",
      balance_type="credit",
      is_abstract=False,
      depth=0,
    )
    for root in (SE, NCI)
  ]
  rows = _build_rows(
    hierarchy,
    [_facts_to_balance_dict(facts, FY26.start, FY26.end)],
    {},
    pre_signed=True,
    keep=ROLLFORWARD_ROOTS,
  )
  rows = _with_opening_balance(rows, facts, [FY26], hierarchy)
  assert {r.element_qname for r in rows} == {SE}
