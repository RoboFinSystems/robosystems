"""The consolidation engine's preview on real Postgres: a parent, a member it
owns seventy percent of, a forty-percent associate on the equity method and
the consolidation entity. Every account is found by its concept and every
intercompany line by the entity its event names, so the targets follow from
the books; what the consolidation entity already holds for a kind and member
is subtracted, so posting the proposals leaves the month up to date."""

from __future__ import annotations

from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.extensions.consolidation import (
  ProposeConsolidationEntriesRequest,
  SetAllocationOverrideRequest,
)
from robosystems.models.api.extensions.entity import (
  CreateEntityRequest,
  UpdateEntityRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
)
from robosystems.models.extensions import Element, Entity
from robosystems.models.extensions.association import Association
from robosystems.models.extensions.element_trait import ElementTrait
from robosystems.models.extensions.roboledger import Entry, FiscalCalendar, LineItem
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.structure import Structure
from robosystems.models.extensions.taxonomy import Taxonomy
from robosystems.models.extensions.trait import Trait
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError
from robosystems.operations.roboledger.commands.consolidation import (
  AllocationOverrideError,
  propose_consolidation_entries,
  set_allocation_override,
)
from robosystems.operations.roboledger.commands.entity import (
  ConsolidatedHistoryError,
  EntityHierarchyError,
  create_entity,
  update_entity,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  ConsolidationAdjustmentError,
  create_journal_entry,
)
from robosystems.operations.roboledger.reads import consolidation as engine
from tests.ledger_entity import PARENT_ENTITY_ID, entity_account, seed_parent_entity_on

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd50"
MEMBER = "ent_test_member"
ASSOCIATE = "ent_test_associate"
ADJUSTMENTS = "ent_test_adjustments"

# Concept → (balance, period type, classification).
_CONCEPTS = {
  "rs-gaap:Cash": ("debit", "instant", "asset"),
  "rs-gaap:MembersEquity": ("credit", "instant", "equity"),
  "rs-gaap:Revenues": ("credit", "duration", "revenue"),
  "rs-gaap:UtilitiesCosts": ("debit", "duration", "expense"),
  "rs-gaap:DueFromAffiliateCurrent": ("debit", "instant", "asset"),
  "rs-gaap:DueToAffiliateCurrent": ("credit", "instant", "liability"),
  "rs-gaap:LongTermInvestmentsAndReceivablesNet": ("debit", "instant", "asset"),
  "rs-gaap:LongTermDebtAndCapitalLeaseObligations": (
    "credit",
    "instant",
    "liability",
  ),
  "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax": (
    "credit",
    "duration",
    "revenue",
  ),
  "rs-gaap:InvestmentIncomeInterest": ("credit", "duration", "revenue"),
  "rs-gaap:GeneralAndAdministrativeExpense": ("debit", "duration", "expense"),
  "rs-gaap:InterestExpense": ("debit", "duration", "expense"),
  "rs-gaap:OtherNonoperatingIncomeExpense": ("credit", "duration", "revenue"),
  engine.INVESTMENT_IN_SUBSIDIARIES: ("debit", "instant", "asset"),
  engine.EQUITY_METHOD_INVESTMENT: ("debit", "instant", "asset"),
  engine.EQUITY_METHOD_INCOME: ("credit", "duration", "revenue"),
  engine.NONCONTROLLING_INTEREST: ("credit", "instant", "equity"),
  engine.NCI_SHARE_OF_INCOME: ("debit", "duration", "expense"),
}

# Each entity's accounts: code → concept.
_CHARTS = {
  PARENT_ENTITY_ID: {
    "cash": "rs-gaap:Cash",
    "investment": engine.INVESTMENT_IN_SUBSIDIARIES,
    "associate": engine.EQUITY_METHOD_INVESTMENT,
    "due_from": "rs-gaap:DueFromAffiliateCurrent",
    "note": "rs-gaap:LongTermInvestmentsAndReceivablesNet",
    "fees": "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
    "interest": "rs-gaap:InvestmentIncomeInterest",
    "distributions": "rs-gaap:OtherNonoperatingIncomeExpense",
  },
  MEMBER: {
    "cash": "rs-gaap:Cash",
    "equity": "rs-gaap:MembersEquity",
    "sales": "rs-gaap:Revenues",
    "due_to": "rs-gaap:DueToAffiliateCurrent",
    "note": "rs-gaap:LongTermDebtAndCapitalLeaseObligations",
    "fees": "rs-gaap:GeneralAndAdministrativeExpense",
    "interest": "rs-gaap:InterestExpense",
    "utilities": "rs-gaap:UtilitiesCosts",
  },
  ASSOCIATE: {
    "cash": "rs-gaap:Cash",
    "equity": "rs-gaap:MembersEquity",
    "sales": "rs-gaap:Revenues",
  },
  ADJUSTMENTS: {
    "1150": "rs-gaap:DueFromAffiliateCurrent",
    "1600": engine.INVESTMENT_IN_SUBSIDIARIES,
    "1650": "rs-gaap:LongTermInvestmentsAndReceivablesNet",
    "1700": engine.EQUITY_METHOD_INVESTMENT,
    "2150": "rs-gaap:DueToAffiliateCurrent",
    "2600": "rs-gaap:LongTermDebtAndCapitalLeaseObligations",
    "3020": "rs-gaap:MembersEquity",
    "3100": "rs-gaap:MembersEquity",  # the close target, on the same concept
    "3200": engine.NONCONTROLLING_INTEREST,
    "4000": "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
    "4100": "rs-gaap:InvestmentIncomeInterest",
    "4500": engine.EQUITY_METHOD_INCOME,
    "5000": "rs-gaap:GeneralAndAdministrativeExpense",
    "5100": "rs-gaap:InterestExpense",
    "5900": engine.NCI_SHARE_OF_INCOME,
    "4200": "rs-gaap:OtherNonoperatingIncomeExpense",
  },
}

DEC = date(2025, 12, 31)
JAN_15 = date(2026, 1, 15)
JAN_31 = date(2026, 1, 31)


@pytest.fixture
def group(monkeypatch):
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
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  try:
    with db.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=tables,
      )
      seed_parent_entity_on(
        conn.execution_options(schema_translate_map={None: GRAPH}),
        name="Parent Co",
      )
    group = _seed()
    # One mapping serves every entity's chart here.
    monkeypatch.setattr(
      engine,
      "find_entity_mapping",
      lambda _s, _e, _f: SimpleNamespace(id=group["mapping"]),
    )
    yield group
  finally:
    with db.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    db.dispose()


def _seed() -> dict:
  group: dict = {"accounts": {}, "agents": {}}
  with extensions_session(GRAPH) as session:
    for entity_id, name, role, method, pct in (
      (MEMBER, "Member LLC", "operating", "full", 70),
      (ASSOCIATE, "Associate LLC", "operating", "equity", 40),
      (ADJUSTMENTS, "Consolidation Adjustments", "consolidation", "none", None),
    ):
      session.add(
        Entity(
          id=entity_id,
          name=name,
          is_parent=False,
          parent_entity_id=PARENT_ENTITY_ID,
          source="native",
          entity_role=role,
          consolidation_method=method,
          ownership_pct=pct,
          created_by="test",
        )
      )
    session.flush()

    traits = {}
    for identifier in ("asset", "liability", "equity", "revenue", "expense"):
      trait = Trait(category="elementsOfFinancialStatements", identifier=identifier)
      session.add(trait)
      session.flush()
      traits[identifier] = trait.id
    taxonomy = Taxonomy(name="mapping", taxonomy_type="mapping", created_by="test")
    session.add(taxonomy)
    session.flush()
    mapping = Structure(
      name="CoA mapping", block_type="coa_mapping", taxonomy_id=taxonomy.id
    )
    session.add(mapping)
    session.flush()
    group["mapping"] = mapping.id

    concepts = {}
    for qname, (balance, period, classification) in _CONCEPTS.items():
      concept = Element(
        qname=qname,
        name=qname.split(":")[1],
        balance_type=balance,
        period_type=period,
        element_type="concept",
        is_abstract=False,
        created_by="test",
      )
      session.add(concept)
      session.flush()
      session.add(
        ElementTrait(
          element_id=concept.id, trait_id=traits[classification], is_primary=True
        )
      )
      concepts[qname] = concept.id

    for entity_id, chart in _CHARTS.items():
      for code, qname in chart.items():
        account = entity_account(session, entity_id, code)
        session.add(
          Association(
            structure_id=mapping.id,
            from_element_id=account,
            to_element_id=concepts[qname],
            association_type="mapping",
          )
        )
        group["accounts"][(entity_id, code)] = account

    for entity_id, name in (
      (PARENT_ENTITY_ID, "Parent Co"),
      (MEMBER, "Member LLC"),
      (ASSOCIATE, "Associate LLC"),
    ):
      agent = Agent(
        agent_type="self", name=name, external_id=entity_id, created_by="test"
      )
      session.add(agent)
      session.flush()
      group["agents"][entity_id] = agent.id
    owner = Agent(agent_type="owner", name="Outside Partner", created_by="test")
    session.add(owner)
    session.flush()
    group["agents"]["owner"] = owner.id
  return group


def post(
  group: dict,
  entity_id: str,
  on: date,
  lines: list[tuple[str, int]],
  *,
  agent: str | None = None,
  kind: str | None = None,
  adjusts: str | None = None,
  entry_type: str = "standard",
) -> None:
  """A posted entry in ``entity_id``'s books; ``lines`` are (code, cents),
  debit-positive. ``agent`` is the entity (or ``"owner"``) the event names."""
  assert sum(amount for _, amount in lines) == 0
  with extensions_session(GRAPH) as session:
    event = Event(
      entity_id=entity_id,
      event_type="journal_entry_recorded",
      event_category="adjustment",
      occurred_at=datetime(on.year, on.month, on.day, tzinfo=UTC),
      status="fulfilled",
      source="manual",
      agent_id=group["agents"][agent] if agent else None,
      created_by="test",
    )
    session.add(event)
    session.flush()
    entry = Entry(
      entity_id=entity_id,
      type=entry_type,
      status="posted",
      posting_date=on,
      triggered_by_event_id=event.id,
      adjustment_kind=kind,
      adjusts_entity_id=adjusts,
      created_by="test",
    )
    session.add(entry)
    session.flush()
    for order, (code, amount) in enumerate(lines, 1):
      session.add(
        LineItem(
          entry_id=entry.id,
          element_id=group["accounts"][(entity_id, code)],
          debit_amount=max(amount, 0),
          credit_amount=max(-amount, 0),
          line_order=order,
        )
      )


def _form_the_group(group: dict) -> None:
  """December: the parent forms the member with an outside partner and lends
  it 5,000, and buys forty percent of the associate."""
  P, M, K = PARENT_ENTITY_ID, MEMBER, ASSOCIATE
  post(group, P, DEC, [("investment", 700_000), ("cash", -700_000)], agent=M)
  post(group, M, DEC, [("cash", 700_000), ("equity", -700_000)], agent=P)
  post(group, M, DEC, [("cash", 300_000), ("equity", -300_000)], agent="owner")
  post(group, P, DEC, [("note", 500_000), ("cash", -500_000)], agent=M)
  post(group, M, DEC, [("cash", 500_000), ("note", -500_000)], agent=P)
  post(group, P, DEC, [("associate", 400_000), ("cash", -400_000)], agent=K)
  post(group, K, DEC, [("cash", 1_000_000), ("equity", -1_000_000)])


def _january(group: dict) -> None:
  """The member sells 2,000 and pays the parent a 100 fee and 25 of
  interest; the parent pays a 50 utility bill for it and rebills it at cost;
  the member distributes 1,000; the associate earns 500."""
  P, M, K = PARENT_ENTITY_ID, MEMBER, ASSOCIATE
  post(group, M, JAN_15, [("cash", 200_000), ("sales", -200_000)])
  post(group, P, JAN_31, [("due_from", 10_000), ("fees", -10_000)], agent=M)
  post(group, M, JAN_31, [("fees", 10_000), ("due_to", -10_000)], agent=P)
  post(group, P, JAN_31, [("due_from", 2_500), ("interest", -2_500)], agent=M)
  post(group, M, JAN_31, [("interest", 2_500), ("due_to", -2_500)], agent=P)
  post(group, P, JAN_31, [("due_from", 5_000), ("cash", -5_000)], agent=M)
  post(group, M, JAN_31, [("utilities", 5_000), ("due_to", -5_000)], agent=P)
  post(group, M, JAN_31, [("equity", 100_000), ("cash", -100_000)])
  post(group, P, JAN_31, [("cash", 70_000), ("investment", -70_000)], agent=M)
  post(group, K, JAN_15, [("cash", 50_000), ("sales", -50_000)])


def preview(period: str = "2026-01"):
  with extensions_session(GRAPH) as session:
    return engine.preview_consolidation_entries(session, period=period)


def _proposal(
  response, kind: str, *, charges: bool | None = None, flows: bool | None = None
):
  """The one proposal of ``kind``; intercompany ones split by ``charges``
  (the month's income and expense) and ``flows`` (the lines offsetting a
  cash flow, posted apart from the rest)."""
  found = [
    p
    for p in response.proposals
    if p.adjustment_kind == kind
    and (charges is None or ("charges" in p.memo) == charges)
    and (flows is None or ("cash flows" in p.memo) == flows)
  ]
  assert len(found) == 1, [p.memo for p in response.proposals]
  return found[0]


def _lines(response, kind: str, **which: bool) -> dict[str, int]:
  """``{concept: debit-positive cents}`` of the one proposal of ``kind``."""
  return {
    line.concept: line.debit_amount - line.credit_amount
    for line in _proposal(response, kind, **which).lines
  }


def _flows(response, kind: str, **which: bool) -> dict[str, str | None]:
  return {
    line.concept: line.flow_qname for line in _proposal(response, kind, **which).lines
  }


def test_the_month_needs_every_kind_from_the_books(group):
  _form_the_group(group)
  _january(group)
  response = preview()

  assert response.residuals == []
  assert response.reconciling_items == []
  # Capital 10,000 less the 1,000 distribution; the parent's 70 % against its
  # investment, the outside 30 % as noncontrolling interest.
  assert _lines(response, "elimination_investment") == {
    "rs-gaap:MembersEquity": 900_000,
    engine.INVESTMENT_IN_SUBSIDIARIES: -630_000,
    engine.NONCONTROLLING_INTEREST: -270_000,
  }
  # The 175 owed (fee, interest and the rebill) and the note, on both sides:
  # the note offsets the cash it moved, so it posts apart.
  assert _lines(response, "elimination_intercompany", charges=False, flows=False) == {
    "rs-gaap:DueToAffiliateCurrent": 17_500,
    "rs-gaap:DueFromAffiliateCurrent": -17_500,
  }
  assert _lines(response, "elimination_intercompany", flows=True) == {
    "rs-gaap:LongTermDebtAndCapitalLeaseObligations": 500_000,
    "rs-gaap:LongTermInvestmentsAndReceivablesNet": -500_000,
  }
  # The fee and the interest; the rebilled utilities stay, being a cost the
  # group really bore.
  assert _lines(response, "elimination_intercompany", charges=True) == {
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax": 10_000,
    "rs-gaap:InvestmentIncomeInterest": 2_500,
    "rs-gaap:GeneralAndAdministrativeExpense": -10_000,
    "rs-gaap:InterestExpense": -2_500,
  }
  # The member's month: 2,000 less 100, 25 and 50 is 1,825; 30 % is 547.50.
  assert _lines(response, "nci_allocation") == {
    engine.NCI_SHARE_OF_INCOME: 54_750,
    engine.NONCONTROLLING_INTEREST: -54_750,
  }
  assert _lines(response, "equity_method_pickup") == {
    engine.EQUITY_METHOD_INVESTMENT: 20_000,
    engine.EQUITY_METHOD_INCOME: -20_000,
  }
  # The eliminated capital posts to the eliminated-equity row, never to the
  # close target that maps to the same concept.
  investment = next(
    p for p in response.proposals if p.adjustment_kind == "elimination_investment"
  )
  equity_line = next(
    line for line in investment.lines if line.concept == "rs-gaap:MembersEquity"
  )
  assert equity_line.element_id == group["accounts"][(ADJUSTMENTS, "3020")]
  assert not response.up_to_date


def test_eliminations_carry_the_flow_they_offset(group):
  """Each eliminating line takes the cash flow row the member's own line
  produced, so the consolidated cash flow nets it; the outside owners' share
  is the group's real flow. The month's income and expense move no tagged
  cash."""
  _form_the_group(group)
  _january(group)
  response = preview()

  assert _flows(response, "elimination_investment") == {
    "rs-gaap:MembersEquity": "rs-gaap:ProceedsFromContributedCapital",
    engine.INVESTMENT_IN_SUBSIDIARIES: (
      "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates"
    ),
    engine.NONCONTROLLING_INTEREST: "rs-gaap:ProceedsFromMinorityShareholders",
  }
  assert _flows(response, "elimination_intercompany", flows=True) == {
    "rs-gaap:LongTermDebtAndCapitalLeaseObligations": (
      "rs-gaap:ProceedsFromRelatedPartyDebt"
    ),
    "rs-gaap:LongTermInvestmentsAndReceivablesNet": "rs-gaap:PaymentsToAcquireInvestments",
  }
  for kind, which in (
    ("elimination_intercompany", {"charges": False, "flows": False}),
    ("elimination_intercompany", {"charges": True}),
    ("nci_allocation", {}),
    ("equity_method_pickup", {}),
  ):
    assert set(_flows(response, kind, **which).values()) == {None}


def test_a_tag_stays_on_the_line_it_was_written_on(group):
  """The consolidation entity moves no cash, so the both-lines rule does not
  run there: a tag on the investment stays off the untagged capital line."""
  qname = "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates"
  with extensions_session(GRAPH) as session:
    flow = Element(
      qname=qname,
      name="PaymentsToAcquireInterestInSubsidiariesAndAffiliates",
      balance_type="credit",
      period_type="duration",
      element_type="concept",
      is_abstract=False,
      created_by="test",
    )
    activity = Trait(category="activityType", identifier="investing")
    session.add_all([flow, activity])
    session.flush()
    session.add(ElementTrait(element_id=flow.id, trait_id=activity.id))
    session.flush()
    entry = create_journal_entry(
      session,
      CreateJournalEntryRequest(
        posting_date=JAN_31,
        memo="Eliminate the investment",
        adjustment_kind="elimination_investment",
        adjusts_entity_id=MEMBER,
        line_items=[
          JournalEntryLineItemInput(
            element_id=group["accounts"][(ADJUSTMENTS, "3020")],
            debit_amount=1_000,
          ),
          JournalEntryLineItemInput(
            element_id=group["accounts"][(ADJUSTMENTS, "1600")],
            credit_amount=1_000,
            flow_qname=qname,
          ),
        ],
      ),
      "test",
      entity_id=ADJUSTMENTS,
    )
  assert [line.flow_qname for line in entry.line_items] == [None, qname]


def _post_proposals(group: dict, response) -> None:
  for proposal in response.proposals:
    with extensions_session(GRAPH) as session:
      event = Event(
        entity_id=ADJUSTMENTS,
        event_type="journal_entry_recorded",
        event_category="adjustment",
        occurred_at=datetime(2026, 1, 31, tzinfo=UTC),
        status="fulfilled",
        source="manual",
        created_by="test",
      )
      session.add(event)
      session.flush()
      entry = Entry(
        entity_id=ADJUSTMENTS,
        status="posted",
        posting_date=proposal.posting_date,
        triggered_by_event_id=event.id,
        adjustment_kind=proposal.adjustment_kind,
        adjusts_entity_id=proposal.adjusts_entity_id,
        created_by="test",
      )
      session.add(entry)
      session.flush()
      for order, line in enumerate(proposal.lines, 1):
        session.add(
          LineItem(
            entry_id=entry.id,
            element_id=line.element_id,
            debit_amount=line.debit_amount,
            credit_amount=line.credit_amount,
            line_order=order,
          )
        )


def test_posting_the_proposals_leaves_the_month_up_to_date(group):
  _form_the_group(group)
  _january(group)
  _post_proposals(group, preview())

  again = preview()
  assert again.proposals == []
  assert again.up_to_date


def test_held_balances_carry_into_the_next_month(group):
  """A cumulative balance held at January's end is not proposed again in
  February; only what moved is."""
  _form_the_group(group)
  _january(group)
  _post_proposals(group, preview())

  february = preview("2026-02")
  kinds = {p.adjustment_kind for p in february.proposals}
  # Nothing moved the capital, the balances or the note; February has no
  # income, so no share of it either.
  assert kinds == set()
  assert february.up_to_date


def test_a_hand_entry_of_the_same_kind_counts(group):
  """A person's entry carrying the kind it adjusts is held like the
  engine's own (D8): only the rest is proposed."""
  _form_the_group(group)
  _january(group)
  post(
    group,
    ADJUSTMENTS,
    JAN_31,
    [("5900", 50_000), ("3200", -50_000)],
    kind="nci_allocation",
    adjusts=MEMBER,
  )
  assert _lines(preview(), "nci_allocation") == {
    engine.NCI_SHARE_OF_INCOME: 4_750,
    engine.NONCONTROLLING_INTEREST: -4_750,
  }


def test_a_manual_entry_is_never_reversed(group):
  """An entry of the ``manual`` kind is a shape outside the engine's kinds:
  it is neither counted toward a target nor proposed back out."""
  _form_the_group(group)
  _january(group)
  _post_proposals(group, preview())
  post(
    group,
    ADJUSTMENTS,
    JAN_31,
    [("1150", 1_000), ("2150", -1_000)],
    kind="manual",
    adjusts=MEMBER,
  )
  assert preview().up_to_date


def test_an_unexplained_investment_difference_is_withheld(group):
  """The parent carries 100 more than its share of the member's capital: the
  investment elimination is withheld with the residual, never plugged, and
  everything else is still proposed."""
  _form_the_group(group)
  _january(group)
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_31,
    [("investment", 10_000), ("cash", -10_000)],
    agent=MEMBER,
  )
  response = preview()

  assert [(r.entity_id, r.amount) for r in response.residuals] == [(MEMBER, 10_000)]
  assert "elimination_investment" not in {p.adjustment_kind for p in response.proposals}
  assert "nci_allocation" in {p.adjustment_kind for p in response.proposals}
  assert not response.up_to_date


def test_a_recharge_at_cost_is_a_note_not_a_disagreement(group):
  """When the rebilled cost sits on the same expense concept as the fee, the
  fee is eliminated against the parent's income and the rest stays, noted."""
  _form_the_group(group)
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_31,
    [("due_from", 10_000), ("fees", -10_000)],
    agent=MEMBER,
  )
  post(
    group,
    MEMBER,
    JAN_31,
    [("fees", 15_000), ("due_to", -15_000)],
    agent=PARENT_ENTITY_ID,
  )
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_31,
    [("due_from", 5_000), ("cash", -5_000)],
    agent=MEMBER,
  )
  response = preview()

  assert _lines(response, "elimination_intercompany", charges=True) == {
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax": 10_000,
    "rs-gaap:GeneralAndAdministrativeExpense": -10_000,
  }
  assert response.reconciling_items == []
  assert any("recharged at cost" in note for note in response.notes)


def test_balances_the_two_sides_disagree_on_are_reconciling_items(group):
  """The parent says the member owes 175; the member's books say 150. The
  lower amount is eliminated and the difference left visible."""
  _form_the_group(group)
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_31,
    [("due_from", 17_500), ("cash", -17_500)],
    agent=MEMBER,
  )
  post(
    group,
    MEMBER,
    JAN_31,
    [("cash", 15_000), ("due_to", -15_000)],
    agent=PARENT_ENTITY_ID,
  )
  response = preview()

  balances = _lines(response, "elimination_intercompany", charges=False, flows=False)
  assert balances["rs-gaap:DueFromAffiliateCurrent"] == -15_000
  assert balances["rs-gaap:DueToAffiliateCurrent"] == 15_000
  assert [(i.entity_id, i.concept, i.amount) for i in response.reconciling_items] == [
    (PARENT_ENTITY_ID, "rs-gaap:DueFromAffiliateCurrent", 2_500)
  ]


def test_a_member_outside_its_interval_is_not_consolidated(group):
  _form_the_group(group)
  _january(group)
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entities SET joined_on = '2026-02-01' WHERE id = :id"),
      {"id": MEMBER},
    )
  adjusted = {p.adjusts_entity_id for p in preview().proposals}
  assert MEMBER not in adjusted
  assert ASSOCIATE in adjusted


def test_a_member_whose_books_are_not_read_leaves_the_month_open(group, monkeypatch):
  """No chart mapping: the member's capital is not read as zero against the
  parent's investment, and the month is not up to date."""
  _form_the_group(group)
  _january(group)
  monkeypatch.setattr(
    engine,
    "find_entity_mapping",
    lambda _s, entity_id, _f: (
      None if entity_id == MEMBER else SimpleNamespace(id=group["mapping"])
    ),
  )
  response = preview()

  assert not response.residuals
  assert ("elimination_investment", MEMBER) not in {
    (p.adjustment_kind, p.adjusts_entity_id) for p in response.proposals
  }
  assert any("has no chart mapping" in n for n in response.notes)
  assert not response.up_to_date


def _close_the_consolidation_entity_through(period: str) -> None:
  with extensions_session(GRAPH) as session:
    session.add(
      FiscalCalendar(
        graph_id=GRAPH,
        entity_id=ADJUSTMENTS,
        closed_through_period=period,
        close_target_period=period,
        last_close_at=datetime.now(UTC),
      )
    )


def test_a_closed_month_is_as_posted(group):
  _form_the_group(group)
  _january(group)
  _close_the_consolidation_entity_through("2026-01")
  response = preview()

  assert response.closed
  assert response.up_to_date
  assert response.proposals == []
  with extensions_session(GRAPH) as session:
    drafted = propose_consolidation_entries(
      session, ProposeConsolidationEntriesRequest(period="2026-01"), "test"
    )
  assert drafted.closed
  assert drafted.entries == []


def _draft_memos() -> list[str]:
  with extensions_session(GRAPH) as session:
    return [
      e.memo
      for e in session.query(Entry).filter(
        Entry.entity_id == ADJUSTMENTS, Entry.status == "draft"
      )
    ]


def _propose():
  with extensions_session(GRAPH) as session:
    return propose_consolidation_entries(
      session, ProposeConsolidationEntriesRequest(period="2026-01"), "test"
    )


def test_proposing_drafts_the_month_and_replaces_its_own_drafts(group, monkeypatch):
  """The proposals become drafts in the consolidation entity; proposing again
  replaces them, never a person's; once close posts them the month is up to
  date."""
  # The test library has no flow concepts to resolve the tags against.
  monkeypatch.setattr(engine, "_FLOWS", {})
  _form_the_group(group)
  _january(group)
  with extensions_session(GRAPH) as session:
    create_journal_entry(
      session,
      CreateJournalEntryRequest(
        posting_date=JAN_31,
        memo="A person's draft",
        adjustment_kind="manual",
        line_items=[
          JournalEntryLineItemInput(
            element_id=group["accounts"][(ADJUSTMENTS, "5000")], debit_amount=100
          ),
          JournalEntryLineItemInput(
            element_id=group["accounts"][(ADJUSTMENTS, "2150")], credit_amount=100
          ),
        ],
      ),
      "test",
      entity_id=ADJUSTMENTS,
    )
  expected = len(preview().proposals)

  first = _propose()
  assert len(first.entries) == expected
  assert first.replaced == 0
  assert {e.status for e in first.entries} == {"draft"}
  assert set(preview().drafted_entry_ids) == {e.id for e in first.entries}
  # Drafts are not held until close posts them: the proposals stand, and the
  # month is up to date because its drafts hold them.
  assert len(preview().proposals) == expected
  assert preview().up_to_date

  second = _propose()
  assert second.replaced == expected
  assert len(second.entries) == expected
  memos = _draft_memos()
  assert len(memos) == expected + 1
  assert "A person's draft" in memos

  with extensions_session(GRAPH) as session:
    session.execute(
      text(
        "UPDATE entries SET status = 'posted' WHERE entity_id = :e "
        "AND adjustment_kind <> 'manual'"
      ),
      {"e": ADJUSTMENTS},
    )
  assert preview().up_to_date


def test_drafts_short_of_the_month_leave_it_out_of_date(group, monkeypatch):
  """A month whose drafts no longer hold what it needs, here one deleted, is
  not up to date, though drafts remain."""
  monkeypatch.setattr(engine, "_FLOWS", {})
  _form_the_group(group)
  _january(group)
  drafted = _propose()
  assert len(drafted.entries) > 1

  with extensions_session(GRAPH) as session:
    session.execute(
      text("DELETE FROM line_items WHERE entry_id = :id"),
      {"id": drafted.entries[0].id},
    )
    session.execute(
      text("DELETE FROM entries WHERE id = :id"), {"id": drafted.entries[0].id}
    )
    session.commit()

  response = preview()
  assert response.drafted_entry_ids
  assert not response.up_to_date


def test_drafts_left_over_when_the_month_needs_nothing_are_out_of_date(
  group, monkeypatch
):
  """Drafts written, then the same entries posted by hand: the month needs
  nothing more, and the drafts left over would post it twice at close."""
  monkeypatch.setattr(engine, "_FLOWS", {})
  _form_the_group(group)
  _january(group)
  needed = preview()
  _propose()
  _post_proposals(group, needed)

  response = preview()
  assert response.proposals == []
  assert response.drafted_entry_ids
  assert not response.up_to_date


def test_drafts_of_an_earlier_open_month_are_held(group, monkeypatch):
  """January's drafts wait for its close; February's proposal counts them,
  so closing both months eliminates once."""
  monkeypatch.setattr(engine, "_FLOWS", {})
  _form_the_group(group)
  _january(group)
  _propose()

  february = preview("2026-02")
  assert february.proposals == []
  assert february.up_to_date


def test_a_new_member_gets_its_own_counterparty(group):
  """create-entity adds the ``self`` agent an intercompany event names, for
  the member and, once, for the group parent."""
  with extensions_session(GRAPH) as session:
    created = create_entity(
      session, CreateEntityRequest(name="New Member LLC", ownership_pct=60), "test"
    )
  with extensions_session(GRAPH) as session:
    selves = session.execute(
      text("SELECT external_id FROM agents WHERE agent_type = 'self'")
    ).scalars()
    counts: dict[str, int] = {}
    for external_id in selves:
      counts[external_id] = counts.get(external_id, 0) + 1
  assert counts[created.id] == 1
  assert counts[PARENT_ENTITY_ID] == 1


def test_a_method_change_is_refused_once_the_group_has_closed(group):
  """Whether a member is summed is not an entry, so a closed month cannot
  hold it; a share that keeps the method changes freely."""
  _close_the_consolidation_entity_through("2026-01")
  with extensions_session(GRAPH) as session:
    updated = update_entity(
      session, UpdateEntityRequest(entity_id=MEMBER, ownership_pct=85), "test"
    )
  assert updated.consolidation_method == "full"
  with extensions_session(GRAPH) as session, pytest.raises(ConsolidatedHistoryError):
    update_entity(
      session, UpdateEntityRequest(entity_id=ASSOCIATE, ownership_pct=60), "test"
    )
  with extensions_session(GRAPH) as session, pytest.raises(ConsolidatedHistoryError):
    update_entity(
      session,
      UpdateEntityRequest(entity_id=MEMBER, entity_role="consolidation"),
      "test",
    )


def _january_with_distributions_as_income(group: dict) -> None:
  """January as before, except that the parent takes its 700 of the
  member's distribution, and 200 from the associate, as income."""
  P, M, K = PARENT_ENTITY_ID, MEMBER, ASSOCIATE
  post(group, M, JAN_15, [("cash", 200_000), ("sales", -200_000)])
  post(group, M, JAN_31, [("equity", 100_000), ("cash", -100_000)])
  post(group, P, JAN_31, [("cash", 70_000), ("distributions", -70_000)], agent=M)
  post(group, K, JAN_15, [("cash", 50_000), ("sales", -50_000)])
  post(group, P, JAN_31, [("cash", 20_000), ("distributions", -20_000)], agent=K)


def test_distributions_taken_as_income_explain_the_residual(group):
  """The investment the parent still carries is its share of the capital
  plus the distributions it took as income: that income is reversed against
  the investment, nothing is withheld, and the one-sided income is no
  disagreement."""
  _form_the_group(group)
  _january_with_distributions_as_income(group)
  response = preview()

  assert response.residuals == []
  assert response.reconciling_items == []
  assert _lines(response, "elimination_investment") == {
    "rs-gaap:MembersEquity": 900_000,
    engine.INVESTMENT_IN_SUBSIDIARIES: -700_000,
    engine.NONCONTROLLING_INTEREST: -270_000,
    "rs-gaap:OtherNonoperatingIncomeExpense": 70_000,
  }
  # On the equity method the associate's distribution reduces the investment.
  assert _lines(response, "equity_method_distribution") == {
    "rs-gaap:OtherNonoperatingIncomeExpense": 20_000,
    engine.EQUITY_METHOD_INVESTMENT: -20_000,
  }

  _post_proposals(group, response)
  assert preview().up_to_date
  assert preview("2026-02").up_to_date


def test_income_that_explains_only_part_of_the_residual_withholds_it(group):
  _form_the_group(group)
  _january_with_distributions_as_income(group)
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_31,
    [("investment", 10_000), ("cash", -10_000)],
    agent=MEMBER,
  )
  response = preview()

  assert [(r.entity_id, r.amount) for r in response.residuals] == [(MEMBER, 80_000)]
  assert "explains only part" in response.residuals[0].explanation


def test_distribution_income_the_residual_does_not_need_is_a_disagreement(group):
  """The parent took the distribution against its investment and also as
  income: the investment needs no explaining, so the income is not
  reversed with it, and it comes back as one-sided intercompany income
  rather than dropping out."""
  _form_the_group(group)
  _january(group)
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_31,
    [("cash", 5_000), ("distributions", -5_000)],
    agent=MEMBER,
  )
  response = preview()

  assert response.residuals == []
  assert "rs-gaap:OtherNonoperatingIncomeExpense" not in _lines(
    response, "elimination_investment"
  )
  assert [(i.entity_id, i.concept, i.amount) for i in response.reconciling_items] == [
    (PARENT_ENTITY_ID, "rs-gaap:OtherNonoperatingIncomeExpense", -5_000)
  ]


def _override(**fields):
  with extensions_session(GRAPH) as session:
    return set_allocation_override(
      session,
      SetAllocationOverrideRequest(period="2026-01", **fields),
      "test",
    )


def test_an_allocation_override_replaces_the_pro_rata_share(group):
  """The member's month is 1,825; pro rata the outside 30 % is 547.50. An
  override sets the share as an amount or a percentage, and clearing it
  goes back to pro rata."""
  _form_the_group(group)
  _january(group)

  _override(entity_id=MEMBER, amount=100_000, basis="Preferred return first")
  proposal = _proposal(preview(), "nci_allocation")
  assert {line.concept: line.debit_amount for line in proposal.lines}[
    engine.NCI_SHARE_OF_INCOME
  ] == 100_000
  assert "Preferred return first" in proposal.basis

  _override(entity_id=MEMBER, percentage=50, basis="Waterfall tier two")
  assert _lines(preview(), "nci_allocation")[engine.NCI_SHARE_OF_INCOME] == 91_250

  cleared = _override(entity_id=MEMBER, clear=True)
  assert cleared.cleared
  assert _lines(preview(), "nci_allocation")[engine.NCI_SHARE_OF_INCOME] == 54_750


@pytest.mark.parametrize(
  "fields",
  [
    {"entity_id": ASSOCIATE, "amount": 1, "basis": "Not in full"},
    {"entity_id": MEMBER, "amount": 1},
    {"entity_id": MEMBER, "amount": 1, "percentage": 10, "basis": "Both"},
    {"entity_id": MEMBER, "amount": 1, "clear": True},
  ],
)
def test_an_override_needs_a_member_in_full_one_share_and_a_basis(group, fields):
  with pytest.raises(AllocationOverrideError):
    _override(**fields)


def test_an_override_is_refused_in_a_closed_month(group):
  _close_the_consolidation_entity_through("2026-01")
  with pytest.raises(ClosedPeriodError):
    _override(entity_id=MEMBER, amount=1, basis="Too late")


def test_no_consolidation_entity_is_refused(group):
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entities SET entity_role = 'operating' WHERE id = :id"),
      {"id": ADJUSTMENTS},
    )
  with pytest.raises(engine.NoConsolidationEntityError):
    preview()


def _entry(kind: str | None, adjusts: str | None) -> CreateJournalEntryRequest:
  return CreateJournalEntryRequest(
    posting_date=JAN_31,
    memo="consolidating",
    status="posted",
    line_items=[
      JournalEntryLineItemInput(element_id="x", debit_amount=100, credit_amount=0),
      JournalEntryLineItemInput(element_id="y", debit_amount=0, credit_amount=100),
    ],
    adjustment_kind=kind,
    adjusts_entity_id=adjusts,
  )


@pytest.mark.parametrize(
  ("entity_id", "kind", "adjusts", "message"),
  [
    (PARENT_ENTITY_ID, "nci_allocation", MEMBER, "only for the group's consolidation"),
    (ADJUSTMENTS, "nci_allocation", None, "names the entity it adjusts"),
    (ADJUSTMENTS, "nci_allocation", ADJUSTMENTS, "not one of the group's own"),
    (ADJUSTMENTS, None, MEMBER, "without an adjustment_kind"),
  ],
)
def test_an_adjustment_kind_belongs_on_the_consolidation_entity(
  group, entity_id, kind, adjusts, message
):
  with extensions_session(GRAPH) as session:
    with pytest.raises(ConsolidationAdjustmentError, match=message):
      create_journal_entry(session, _entry(kind, adjusts), "test", entity_id=entity_id)


def _status(period: str = "2026-01"):
  with extensions_session(GRAPH) as session:
    return engine.group_close_status(session, GRAPH, period)


def _close_through(entity_id: str, period: str) -> None:
  with extensions_session(GRAPH) as session:
    session.add(
      FiscalCalendar(
        graph_id=GRAPH,
        entity_id=entity_id,
        closed_through_period=period,
        close_target_period=period,
      )
    )


def test_the_group_month_waits_on_its_entities_and_entries(group, monkeypatch):
  """The group close (§4.5) holds until every entity the month consolidates,
  the equity-method associate included, has closed it and the consolidation
  entity holds or has drafted the month's entries."""
  monkeypatch.setattr(engine, "_FLOWS", {})
  _form_the_group(group)
  _january(group)

  waiting = _status()
  with extensions_session(GRAPH) as session:
    names = dict(session.execute(text("SELECT id, name FROM entities")).all())
  assert set(waiting.open_entities) == {
    names[PARENT_ENTITY_ID],
    names[MEMBER],
    names[ASSOCIATE],
  }
  assert waiting.pending_adjustments
  assert waiting.unexplained == []

  for entity_id in (PARENT_ENTITY_ID, MEMBER, ASSOCIATE):
    _close_through(entity_id, "2026-01")
  _propose()
  ready = _status()
  assert ready.open_entities == []
  assert ready.pending_adjustments == []


def test_a_member_month_the_group_closed_reopens_after_the_group(group):
  """The group's stamp is made from each member's month, so a member reopens
  it only once the consolidation entity has."""
  from robosystems.operations.roboledger.commands.fiscal_calendar import (
    _group_closed,
  )

  _close_the_consolidation_entity_through("2026-01")
  with extensions_session(GRAPH) as session:
    assert _group_closed(session, MEMBER, "2026-01") is not None
    assert _group_closed(session, MEMBER, "2026-02") is None
    assert _group_closed(session, ADJUSTMENTS, "2026-01") is None


def test_the_consolidation_entity_close_is_the_group_close(group, monkeypatch):
  """The calendar gate on the consolidation entity names the group's
  blockers; an operating entity's gate never runs them."""
  from robosystems.operations.roboledger.fiscal_calendar.service import (
    CloseableGateResult,
    FiscalCalendarService,
  )

  monkeypatch.setattr(engine, "_FLOWS", {})
  _form_the_group(group)
  _january(group)
  _close_the_consolidation_entity_through("2025-12")
  _close_through(MEMBER, "2025-12")

  with extensions_session(GRAPH) as session:
    service = FiscalCalendarService()
    held = service.closeable_gate(
      session, GRAPH, "2026-01", today=date(2026, 3, 1), entity_id=ADJUSTMENTS
    )
    member = service.closeable_gate(
      session, GRAPH, "2026-01", today=date(2026, 3, 1), entity_id=MEMBER
    )

  assert CloseableGateResult.ENTITY_OPEN in held.blockers
  assert CloseableGateResult.ADJUSTMENTS_UNPOSTED in held.blockers
  assert held.group_open_entities
  assert not any(b in member.blockers for b in _group_codes())
  assert member.group_open_entities == []


def _group_codes() -> tuple[str, ...]:
  from robosystems.operations.roboledger.fiscal_calendar.service import (
    CloseableGateResult,
  )

  return (
    CloseableGateResult.ENTITY_OPEN,
    CloseableGateResult.ADJUSTMENTS_UNPOSTED,
    CloseableGateResult.RESIDUAL_UNEXPLAINED,
    CloseableGateResult.CONSOLIDATED_DOES_NOT_FOOT,
  )


def test_a_residual_does_not_excuse_the_other_entries(group, monkeypatch):
  """A residual on the member and nothing drafted: both hold the close, so
  overriding the residual alone would still wait for the entries."""
  monkeypatch.setattr(engine, "_FLOWS", {})
  _form_the_group(group)
  _january(group)
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_31,
    [("investment", 10_000), ("cash", -10_000)],
    agent=MEMBER,
  )

  status = _status()
  assert status.unexplained
  assert status.pending_adjustments


def test_an_entry_the_engine_left_out_holds_the_close_beside_a_residual(
  group, monkeypatch
):
  """Overriding the member's residual must not also waive the associate's
  entry, which the engine could not compute from unread books."""
  monkeypatch.setattr(engine, "_FLOWS", {})
  _form_the_group(group)
  _january(group)
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_31,
    [("investment", 10_000), ("cash", -10_000)],
    agent=MEMBER,
  )
  monkeypatch.setattr(
    engine,
    "find_entity_mapping",
    lambda _s, entity_id, _f: (
      None if entity_id == ASSOCIATE else SimpleNamespace(id=group["mapping"])
    ),
  )
  for entity_id in (PARENT_ENTITY_ID, MEMBER, ASSOCIATE):
    _close_through(entity_id, "2026-01")
  _propose()

  assert preview().incomplete
  status = _status()
  assert status.unexplained
  assert any("has no chart mapping" in p for p in status.pending_adjustments)


def test_a_graph_has_one_consolidation_entity(group):
  with (
    extensions_session(GRAPH) as session,
    pytest.raises(
      EntityHierarchyError, match="already this graph's consolidation entity"
    ),
  ):
    create_entity(
      session,
      CreateEntityRequest(name="Second Adjustments", entity_role="consolidation"),
      "test",
    )
  with (
    extensions_session(GRAPH) as session,
    pytest.raises(
      EntityHierarchyError, match="already this graph's consolidation entity"
    ),
  ):
    update_entity(
      session,
      UpdateEntityRequest(entity_id=ASSOCIATE, entity_role="consolidation"),
      "test",
    )


def test_the_consolidation_entity_keeps_its_role_once_used(group):
  """Demoted, its eliminations would sum as an operating company's books and
  the guards on the group's closed months would fall with it."""
  _close_the_consolidation_entity_through("2026-01")
  with extensions_session(GRAPH) as session, pytest.raises(ConsolidatedHistoryError):
    update_entity(
      session,
      UpdateEntityRequest(entity_id=ADJUSTMENTS, entity_role="operating"),
      "test",
    )


def test_membership_dates_cannot_move_within_closed_months(group):
  _close_the_consolidation_entity_through("2026-01")
  for change in ({"joined_on": date(2026, 6, 1)}, {"left_on": date(2026, 1, 15)}):
    with extensions_session(GRAPH) as session, pytest.raises(ConsolidatedHistoryError):
      update_entity(session, UpdateEntityRequest(entity_id=MEMBER, **change), "test")
  with extensions_session(GRAPH) as session:
    left = update_entity(
      session, UpdateEntityRequest(entity_id=MEMBER, left_on=date(2026, 6, 30)), "test"
    )
  assert left.left_on == date(2026, 6, 30)


def test_a_new_member_joins_after_the_months_the_group_closed(group):
  _close_the_consolidation_entity_through("2026-01")
  with extensions_session(GRAPH) as session, pytest.raises(ConsolidatedHistoryError):
    create_entity(
      session, CreateEntityRequest(name="Late Member", ownership_pct=100), "test"
    )
  with extensions_session(GRAPH) as session:
    created = create_entity(
      session,
      CreateEntityRequest(
        name="Late Member", ownership_pct=100, joined_on=date(2026, 2, 1)
      ),
      "test",
    )
  assert created.consolidation_method == "full"
