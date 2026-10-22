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
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.adapters.bank_feed.accounts import BANK_FEED_KEY
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.extensions.consolidation import (
  AcquisitionAllocation,
  ProposeConsolidationEntriesRequest,
  RecordAcquisitionRequest,
  RecordDeconsolidationRequest,
  RecordOwnershipChangeRequest,
  SetAllocationOverrideRequest,
)
from robosystems.models.api.extensions.entity import (
  CreateEntityRequest,
  UpdateEntityRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  JournalEntryLineItemInput,
  ReverseJournalEntryRequest,
)
from robosystems.models.extensions import Element, Entity
from robosystems.models.extensions.association import Association
from robosystems.models.extensions.element_trait import ElementTrait
from robosystems.models.extensions.roboledger import (
  Entry,
  FiscalCalendar,
  FiscalPeriod,
  LineItem,
)
from robosystems.models.extensions.roboledger.agent import Agent
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.structure import Structure
from robosystems.models.extensions.taxonomy import Taxonomy
from robosystems.models.extensions.trait import Trait
from robosystems.operations.roboledger.commands import consolidation as commands
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError
from robosystems.operations.roboledger.commands.consolidation import (
  BARGAIN_PURCHASE_GAIN,
  DECONSOLIDATION_GAIN,
  AcquisitionError,
  AllocationOverrideError,
  DeconsolidationError,
  OwnershipChangeError,
  propose_consolidation_entries,
  record_acquisition,
  record_deconsolidation,
  record_ownership_change,
  set_allocation_override,
)
from robosystems.operations.roboledger.commands.entity import (
  ConsolidatedHistoryError,
  EntityExitError,
  EntityHierarchyError,
  EntityMembershipError,
  create_entity,
  update_entity,
)
from robosystems.operations.roboledger.commands.journal_entries import (
  ConsolidationAdjustmentError,
  create_journal_entry,
  reverse_journal_entry,
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
  engine.GOODWILL: ("debit", "instant", "asset"),
  BARGAIN_PURCHASE_GAIN: ("credit", "duration", "revenue"),
  "rs-gaap:AdditionalPaidInCapital": ("credit", "instant", "equity"),
  "rs-gaap:RetainedEarningsAccumulatedDeficit": ("credit", "instant", "equity"),
  DECONSOLIDATION_GAIN: ("credit", "duration", "revenue"),
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
    "gain": DECONSOLIDATION_GAIN,
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
    "1800": engine.GOODWILL,
    "4300": BARGAIN_PURCHASE_GAIN,
    "3050": "rs-gaap:AdditionalPaidInCapital",
    "3900": "rs-gaap:RetainedEarningsAccumulatedDeficit",
    "4400": DECONSOLIDATION_GAIN,
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
    for module in (engine, commands):
      monkeypatch.setattr(
        module,
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


def _proposals(
  response, kind: str, *, charges: bool | None = None, flows: bool | None = None
):
  """The proposals of ``kind``; intercompany ones split by ``charges`` (the
  month's income and expense), and any by ``flows`` (the lines offsetting
  the month's cash flows, posted apart from the rest)."""
  return [
    p
    for p in response.proposals
    if p.adjustment_kind == kind
    and (charges is None or ("charges" in p.memo) == charges)
    and (flows is None or ("cash flows" in p.memo) == flows)
  ]


def _proposal(response, kind: str, **which: bool):
  """The one proposal of ``kind``."""
  found = _proposals(response, kind, **which)
  assert len(found) == 1, [p.memo for p in response.proposals]
  return found[0]


def _lines(response, kind: str, **which: bool) -> dict[str, int]:
  """``{concept: debit-positive cents}`` the proposals of ``kind`` hold."""
  net: dict[str, int] = {}
  for proposal in _proposals(response, kind, **which):
    for line in proposal.lines:
      net[line.concept] = (
        net.get(line.concept, 0) + line.debit_amount - line.credit_amount
      )
  assert net, [p.memo for p in response.proposals]
  return {concept: amount for concept, amount in net.items() if amount}


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
  # The 175 owed (fee, interest and the rebill) and the note, on both sides.
  assert _lines(response, "elimination_intercompany", charges=False) == {
    "rs-gaap:DueToAffiliateCurrent": 17_500,
    "rs-gaap:DueFromAffiliateCurrent": -17_500,
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
  response = preview("2025-12")

  assert _flows(response, "elimination_investment") == {
    "rs-gaap:MembersEquity": "rs-gaap:ProceedsFromContributedCapital",
    engine.INVESTMENT_IN_SUBSIDIARIES: (
      "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates"
    ),
    engine.NONCONTROLLING_INTEREST: "rs-gaap:ProceedsFromMinorityShareholders",
  }
  assert _flows(response, "elimination_intercompany", charges=False) == {
    "rs-gaap:LongTermDebtAndCapitalLeaseObligations": (
      "rs-gaap:ProceedsFromRelatedPartyDebt"
    ),
    "rs-gaap:LongTermInvestmentsAndReceivablesNet": "rs-gaap:PaymentsToAcquireInvestments",
  }

  _january(group)
  response = preview()
  for kind, which in (
    ("elimination_intercompany", {"charges": True}),
    ("nci_allocation", {}),
    ("equity_method_pickup", {}),
  ):
    assert set(_flows(response, kind, **which).values()) == {None}


def test_a_catch_up_from_earlier_months_moves_no_cash_in_this_one(group):
  """Nothing held when January is first proposed: December's funding and
  loan are caught up untagged, and only January's distribution offsets
  January's cash flows."""
  _form_the_group(group)
  _january(group)
  response = preview()

  assert _lines(response, "elimination_investment", flows=True) == {
    "rs-gaap:MembersEquity": -100_000,
    engine.INVESTMENT_IN_SUBSIDIARIES: 70_000,
    engine.NONCONTROLLING_INTEREST: 30_000,
  }
  assert _flows(response, "elimination_investment", flows=True) == {
    "rs-gaap:MembersEquity": "rs-gaap:PaymentsOfCapitalDistribution",
    engine.INVESTMENT_IN_SUBSIDIARIES: (
      "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates"
    ),
    engine.NONCONTROLLING_INTEREST: "rs-gaap:PaymentsToMinorityShareholders",
  }
  assert set(_flows(response, "elimination_investment", flows=False).values()) == {None}
  assert set(_flows(response, "elimination_intercompany", charges=False).values()) == {
    None
  }


def test_an_entry_that_cannot_offset_every_flow_offsets_none(group, monkeypatch):
  """With no row for the member's capital on the consolidation entity's
  statement, tagging the investment and outside owners alone would add a
  flow nothing offsets."""
  monkeypatch.setattr(
    engine,
    "taggable_flow_qnames",
    lambda _s, _style: {
      "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates",
      "rs-gaap:ProceedsFromMinorityShareholders",
    },
  )
  _form_the_group(group)
  response = preview("2025-12")

  assert set(_flows(response, "elimination_investment").values()) == {None}
  assert any("ProceedsFromContributedCapital" in n for n in response.notes)


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


def test_a_member_that_left_holds_the_month_until_its_deconsolidation(group):
  """January is consolidated and posted; the member and the associate leave
  at its end. February reverses the capital elimination, but the outside
  owners' share of January's result and the associate's pickup are still
  held, so the month stays incomplete until a person deconsolidates them."""
  _form_the_group(group)
  _january(group)
  _post_proposals(group, preview())
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entities SET left_on = '2026-01-31' WHERE id IN (:m, :k)"),
      {"m": MEMBER, "k": ASSOCIATE},
    )
  february = preview("2026-02")

  assert (
    _lines(february, "elimination_investment")[engine.NONCONTROLLING_INTEREST]
    == 270_000
  )
  assert february.incomplete and not february.up_to_date
  held = [n for n in february.notes if "no longer in the group" in n]
  assert any("Member LLC" in n and "MinorityInterest 547.50 credit" in n for n in held)
  assert any(
    "Associate LLC" in n and "EquityMethodInvestments 200.00 debit" in n for n in held
  )

  _post_proposals(group, february)
  feb_28 = date(2026, 2, 28)
  post(
    group,
    ADJUSTMENTS,
    feb_28,
    [("3200", 54_750), ("3100", -54_750)],
    kind="deconsolidation",
    adjusts=MEMBER,
  )
  post(
    group,
    ADJUSTMENTS,
    feb_28,
    [("3100", 20_000), ("1700", -20_000)],
    kind="deconsolidation",
    adjusts=ASSOCIATE,
  )
  settled = preview("2026-02")
  assert not settled.incomplete
  assert settled.up_to_date


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
  """The calendar closed through ``period`` and the month closed by a close,
  not seeded closed."""
  start, end = engine.month_bounds(period)
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
    session.add(
      FiscalPeriod(
        graph_id=GRAPH,
        entity_id=ADJUSTMENTS,
        name=period,
        start_date=start,
        end_date=end,
        period_type="monthly",
        status="closed",
        closed_by="test",
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
    # Leaving at June's end takes the member's books closed through June.
    session.add(
      FiscalPeriod(
        graph_id=GRAPH,
        entity_id=MEMBER,
        name="2026-06",
        start_date=date(2026, 6, 1),
        end_date=date(2026, 6, 30),
        period_type="monthly",
        status="closed",
      )
    )
  with extensions_session(GRAPH) as session:
    left = update_entity(
      session, UpdateEntityRequest(entity_id=MEMBER, left_on=date(2026, 6, 30)), "test"
    )
  assert left.left_on == date(2026, 6, 30)


def test_the_consolidation_entity_takes_no_membership_dates(group):
  """Dated, it would drop out of the consolidated reads of a month it had
  closed and stamped, while its eliminations still stood."""
  for change in ({"left_on": date(2025, 12, 31)}, {"joined_on": date(2026, 2, 1)}):
    with extensions_session(GRAPH) as session, pytest.raises(EntityMembershipError):
      update_entity(
        session, UpdateEntityRequest(entity_id=ADJUSTMENTS, **change), "test"
      )
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entities SET entity_role = 'operating' WHERE id = :id"),
      {"id": ADJUSTMENTS},
    )
    session.commit()
  with extensions_session(GRAPH) as session, pytest.raises(EntityMembershipError):
    create_entity(
      session,
      CreateEntityRequest(
        name="Dated Adjustments",
        entity_role="consolidation",
        joined_on=date(2026, 2, 1),
      ),
      "test",
    )


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


def test_a_reversed_consolidating_entry_is_no_longer_held(group):
  """A hand entry of a kind, then its reversal: the consolidation entity
  holds nothing of it, and the month needs the whole share again."""
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
  assert _lines(preview(), "nci_allocation")[engine.NCI_SHARE_OF_INCOME] == 4_750
  with extensions_session(GRAPH) as session:
    entry_id = session.execute(
      text(
        "SELECT id FROM entries WHERE entity_id = :e AND adjustment_kind IS NOT NULL"
      ),
      {"e": ADJUSTMENTS},
    ).scalar_one()
    reverse_journal_entry(
      session,
      ReverseJournalEntryRequest(entry_id=str(entry_id), posting_date=JAN_31),
      "test",
    )
    session.commit()

  assert _lines(preview(), "nci_allocation")[engine.NCI_SHARE_OF_INCOME] == 54_750


def _reverse(group: dict, entity_id: str, code: str, on: date) -> None:
  """Reverse the posted entry of ``entity_id`` that touches ``code``."""
  with extensions_session(GRAPH) as session:
    entry_id = session.execute(
      text(
        "SELECT e.id FROM entries e JOIN line_items li ON li.entry_id = e.id "
        "WHERE e.entity_id = :e AND li.element_id = :a AND e.status = 'posted'"
      ),
      {"e": entity_id, "a": group["accounts"][(entity_id, code)]},
    ).scalar_one()
    reverse_journal_entry(
      session,
      ReverseJournalEntryRequest(entry_id=str(entry_id), posting_date=on),
      "test",
    )
    session.commit()


def _fee_on_account(group: dict) -> None:
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_15,
    [("due_from", 10_000), ("fees", -10_000)],
    agent=MEMBER,
  )
  post(
    group,
    MEMBER,
    JAN_15,
    [("fees", 10_000), ("due_to", -10_000)],
    agent=PARENT_ENTITY_ID,
  )


def test_an_intercompany_entry_reversed_on_both_sides_is_not_eliminated(group):
  """A fee booked in error and reversed by both companies nets to nothing:
  there is nothing left to eliminate."""
  _form_the_group(group)
  _fee_on_account(group)
  _reverse(group, PARENT_ENTITY_ID, "due_from", JAN_31)
  _reverse(group, MEMBER, "due_to", JAN_31)
  response = preview()

  assert _proposals(response, "elimination_intercompany", charges=True) == []
  balances = _lines(response, "elimination_intercompany", charges=False)
  assert "rs-gaap:DueFromAffiliateCurrent" not in balances
  assert "rs-gaap:DueToAffiliateCurrent" not in balances
  assert response.reconciling_items == []


def test_an_intercompany_entry_reversed_on_one_side_is_a_disagreement(group):
  """The parent reverses its receivable, the member still owes: the member's
  payable has no counterpart, and nothing is eliminated against it."""
  _form_the_group(group)
  _fee_on_account(group)
  _reverse(group, PARENT_ENTITY_ID, "due_from", JAN_31)
  response = preview()

  assert {(i.entity_id, i.concept, i.amount) for i in response.reconciling_items} >= {
    (MEMBER, "rs-gaap:DueToAffiliateCurrent", -10_000)
  }
  balances = _lines(response, "elimination_intercompany", charges=False)
  assert "rs-gaap:DueFromAffiliateCurrent" not in balances


def test_an_intercompany_entry_reversed_a_month_later_releases_its_elimination(
  group,
):
  """January's fee is eliminated and posted; both sides reverse it in
  February, so February takes the elimination back out."""
  _form_the_group(group)
  _fee_on_account(group)
  _post_proposals(group, preview())
  feb = date(2026, 2, 10)
  _reverse(group, PARENT_ENTITY_ID, "due_from", feb)
  _reverse(group, MEMBER, "due_to", feb)
  february = preview("2026-02")

  assert _lines(february, "elimination_intercompany", charges=False) == {
    "rs-gaap:DueFromAffiliateCurrent": 10_000,
    "rs-gaap:DueToAffiliateCurrent": -10_000,
  }
  assert _lines(february, "elimination_intercompany", charges=True) == {
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax": -10_000,
    "rs-gaap:GeneralAndAdministrativeExpense": 10_000,
  }
  assert february.reconciling_items == []


def test_an_intercompany_balance_the_consolidation_entity_cannot_post_holds_the_month(
  group,
):
  """A fee billed on account to receivables and payables, which the
  consolidation chart does not carry: the balance is named, never dropped."""
  _form_the_group(group)
  with extensions_session(GRAPH) as session:
    for qname, balance, trait, entity_id, code in (
      ("rs-gaap:ReceivablesNetCurrent", "debit", "asset", PARENT_ENTITY_ID, "ar"),
      ("rs-gaap:AccountsPayableCurrent", "credit", "liability", MEMBER, "ap"),
    ):
      concept = Element(
        qname=qname,
        name=qname.split(":")[1],
        balance_type=balance,
        period_type="instant",
        element_type="concept",
        is_abstract=False,
        created_by="test",
      )
      session.add(concept)
      session.flush()
      session.add(
        ElementTrait(
          element_id=concept.id,
          trait_id=session.execute(
            select(Trait.id).where(Trait.identifier == trait)
          ).scalar_one(),
          is_primary=True,
        )
      )
      account = entity_account(session, entity_id, code)
      session.add(
        Association(
          structure_id=group["mapping"],
          from_element_id=account,
          to_element_id=concept.id,
          association_type="mapping",
        )
      )
      group["accounts"][(entity_id, code)] = account
    session.commit()
  post(
    group, PARENT_ENTITY_ID, JAN_31, [("ar", 10_000), ("fees", -10_000)], agent=MEMBER
  )
  post(
    group, MEMBER, JAN_31, [("fees", 10_000), ("ap", -10_000)], agent=PARENT_ENTITY_ID
  )
  response = preview()

  assert response.incomplete
  assert not response.up_to_date
  assert any("ReceivablesNetCurrent" in n for n in response.notes)
  assert any("AccountsPayableCurrent" in n for n in response.notes)


def _map_account(group: dict, entity_id: str, code: str, qname: str, balance: str):
  """An account of ``entity_id`` mapped to ``qname``, a liability concept
  the fixture's chart does not carry."""
  with extensions_session(GRAPH) as session:
    concept = Element(
      qname=qname,
      name=qname.split(":")[1],
      balance_type=balance,
      period_type="instant",
      element_type="concept",
      is_abstract=False,
      created_by="test",
    )
    session.add(concept)
    session.flush()
    session.add(
      ElementTrait(
        element_id=concept.id,
        trait_id=session.execute(
          select(Trait.id).where(Trait.identifier == "liability")
        ).scalar_one(),
        is_primary=True,
      )
    )
    account = entity_account(session, entity_id, code)
    session.add(
      Association(
        structure_id=group["mapping"],
        from_element_id=account,
        to_element_id=concept.id,
        association_type="mapping",
      )
    )
    group["accounts"][(entity_id, code)] = account
    session.commit()


def test_a_third_partys_balance_on_a_rebill_is_not_owed_within_the_group(group):
  """The parent takes the member's utility bill on its own payables and
  rebills it: the payable is the vendor's, so nothing is left out."""
  _form_the_group(group)
  _map_account(
    group, PARENT_ENTITY_ID, "ap", "rs-gaap:AccountsPayableCurrent", "credit"
  )
  post(
    group, PARENT_ENTITY_ID, JAN_31, [("due_from", 5_000), ("ap", -5_000)], agent=MEMBER
  )
  post(
    group,
    MEMBER,
    JAN_31,
    [("utilities", 5_000), ("due_to", -5_000)],
    agent=PARENT_ENTITY_ID,
  )
  response = preview()

  assert not response.incomplete
  assert not any("AccountsPayableCurrent" in n for n in response.notes)


def test_a_distribution_taken_as_income_offsets_its_flows(group):
  """The income reversal rides with the tagged capital lines, as the
  operating side of the distribution they offset."""
  _form_the_group(group)
  _post_proposals(group, preview("2025-12"))
  _january_with_distributions_as_income(group)
  flows = _flows(preview(), "elimination_investment")

  assert flows["rs-gaap:MembersEquity"] == "rs-gaap:PaymentsOfCapitalDistribution"
  assert [q for q, f in flows.items() if f is None] == [
    "rs-gaap:OtherNonoperatingIncomeExpense"
  ]


def test_a_month_opening_on_a_withheld_residual_tags_nothing(group):
  """With no opening target to measure from, the whole balance would read
  as the month's movement."""
  _form_the_group(group)
  _post_proposals(group, preview("2025-12"))
  _january(group)
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_31,
    [("investment", 7_000), ("cash", -7_000)],
    agent=MEMBER,
  )
  january = preview()
  assert [r.entity_id for r in january.residuals] == [MEMBER]
  _post_proposals(group, january)
  post(group, MEMBER, date(2026, 2, 2), [("cash", 7_000), ("equity", -7_000)])
  post(
    group,
    MEMBER,
    date(2026, 2, 2),
    [("cash", 3_000), ("equity", -3_000)],
    agent="owner",
  )
  february = preview("2026-02")

  assert february.residuals == []
  assert not _proposals(february, "elimination_investment", flows=True)


def test_a_reopened_group_month_no_longer_holds_membership(group):
  """Reopened, the month is not closed, and the month before it never was."""
  _close_the_consolidation_entity_through("2026-01")
  with extensions_session(GRAPH) as session:
    session.execute(
      text(
        "UPDATE fiscal_periods SET status = 'closing', closed_by = NULL "
        "WHERE entity_id = :e AND name = '2026-01'"
      ),
      {"e": ADJUSTMENTS},
    )
    session.execute(
      text(
        "UPDATE fiscal_calendar SET closed_through_period = '2025-12' "
        "WHERE entity_id = :e"
      ),
      {"e": ADJUSTMENTS},
    )
  with extensions_session(GRAPH) as session:
    updated = update_entity(
      session,
      UpdateEntityRequest(entity_id=MEMBER, joined_on=date(2026, 1, 14)),
      "test",
    )
  assert updated.joined_on == date(2026, 1, 14)


# ── A member bought from its owners (group-lifecycle Phase 1) ───────────


JAN_1 = date(2026, 1, 1)


def _buy_the_member(group: dict, price: int = 900_000) -> None:
  """The member, formed by others, joins on 1 January with 10,000 of equity
  in its opening balance; the parent pays the outside owner who sold it 70 %.
  The purchase names the seller, not the member."""
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entities SET joined_on = :d WHERE id = :id"),
      {"d": JAN_1, "id": MEMBER},
    )
  post(
    group,
    MEMBER,
    DEC,
    [("cash", 1_000_000), ("equity", -1_000_000)],
    entry_type="opening",
  )
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_1,
    [("investment", price), ("cash", -price)],
    agent="owner",
  )


def _acquire(**fields):
  with extensions_session(GRAPH) as session:
    return record_acquisition(
      session,
      RecordAcquisitionRequest(entity_id=MEMBER, basis="Purchase agreement", **fields),
      "test",
    )


def _acquisition_lines() -> dict[str, int]:
  with extensions_session(GRAPH) as session:
    rows = session.execute(
      text(
        """
        SELECT el.code, SUM(li.debit_amount - li.credit_amount) AS amount
        FROM line_items li JOIN entries e ON e.id = li.entry_id
        JOIN elements el ON el.id = li.element_id
        WHERE e.adjustment_kind = 'acquisition' AND e.status = 'draft'
        GROUP BY el.code
        """
      )
    ).all()
  return {r.code: int(r.amount) for r in rows if r.amount}


def test_an_unrecorded_acquisition_is_a_pre_acquisition_residual(group):
  _buy_the_member(group)
  response = preview()

  (residual,) = response.residuals
  assert residual.entity_id == MEMBER
  assert residual.cause == "pre_acquisition"
  # Nothing names the member, against 70 % of its 10,000.
  assert residual.amount == -700_000
  assert "record-acquisition" in residual.explanation
  assert "9,000.00 naming no member" in residual.explanation
  assert not _proposals(response, "elimination_investment")


def test_a_recorded_acquisition_explains_the_residual_with_goodwill(group):
  _buy_the_member(group)
  recorded = _acquire(consideration=900_000)

  assert (recorded.equity_at_acquisition, recorded.parent_share) == (1_000_000, 700_000)
  assert (recorded.excess, recorded.goodwill, recorded.noncontrolling_interest) == (
    200_000,
    200_000,
    0,
  )
  assert recorded.entry.status == "draft"
  assert recorded.entry.posting_date == JAN_1
  assert _acquisition_lines() == {"1800": 200_000, "1600": -200_000}

  response = preview()
  assert response.residuals == []
  # The engine eliminates the parent's share of the equity at acquisition; the
  # acquisition entry clears the rest of the 9,000 the parent paid.
  assert _lines(response, "elimination_investment") == {
    "rs-gaap:MembersEquity": 1_000_000,
    engine.INVESTMENT_IN_SUBSIDIARIES: -700_000,
    engine.NONCONTROLLING_INTEREST: -300_000,
  }
  # Never reversed: the acquisition is a person's entry, not a computed kind.
  assert not _proposals(response, "acquisition")


def test_fair_value_of_the_outside_owners_adds_their_goodwill(group):
  _buy_the_member(group)
  recorded = _acquire(consideration=900_000, nci_fair_value=400_000)

  # 4,000 for an interest whose book share is 3,000.
  assert (recorded.goodwill, recorded.noncontrolling_interest) == (300_000, 100_000)
  assert _acquisition_lines() == {"1800": 300_000, "1600": -200_000, "3200": -100_000}
  assert preview().residuals == []


def test_a_step_up_takes_its_part_of_the_excess(group):
  _buy_the_member(group)
  recorded = _acquire(
    consideration=900_000,
    allocations=[
      AcquisitionAllocation(
        concept="rs-gaap:LongTermInvestmentsAndReceivablesNet", amount=50_000
      )
    ],
  )

  # The outside owners carry 30 % of the step-up; goodwill is the parent's alone.
  assert (recorded.goodwill, recorded.noncontrolling_interest) == (165_000, 15_000)
  assert _acquisition_lines() == {
    "1800": 165_000,
    "1650": 50_000,
    "1600": -200_000,
    "3200": -15_000,
  }
  assert preview().residuals == []


def test_a_bargain_purchase_waits_for_its_review(group):
  _buy_the_member(group, price=600_000)
  with pytest.raises(AcquisitionError, match=r"bargain purchase of 1,000\.00"):
    _acquire(consideration=600_000)

  recorded = _acquire(consideration=600_000, confirm_bargain_purchase=True)
  assert recorded.goodwill == -100_000
  assert _acquisition_lines() == {"1600": 100_000, "4300": -100_000}
  assert preview().residuals == []


def test_recording_again_replaces_the_draft_but_never_a_posted_one(group):
  _buy_the_member(group)
  _acquire(consideration=800_000)
  replaced = _acquire(consideration=900_000)
  assert replaced.replaced
  assert _acquisition_lines() == {"1800": 200_000, "1600": -200_000}

  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entries SET status = 'posted' WHERE adjustment_kind = 'acquisition'")
    )
  with pytest.raises(AcquisitionError, match="Reverse that entry first"):
    _acquire(consideration=900_000)


def test_consideration_the_parents_books_do_not_hold_leaves_the_month_open(group):
  _buy_the_member(group)
  _acquire(consideration=1_200_000)

  response = preview()
  assert response.incomplete
  assert any("naming no member" in note for note in response.notes)
  assert not response.up_to_date


def test_an_acquisition_needs_a_member_that_joined_with_an_opening(group):
  with pytest.raises(AcquisitionError, match="no joined_on"):
    _acquire(consideration=900_000)
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entities SET joined_on = :d WHERE id = :id"),
      {"d": JAN_1, "id": MEMBER},
    )
  with pytest.raises(AcquisitionError, match="no opening balance"):
    _acquire(consideration=900_000)


def test_a_member_formed_by_the_parent_keeps_the_plain_residual(group):
  """No join date and no opening: a difference is unexplained, as before."""
  _form_the_group(group)
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_31,
    [("investment", 10_000), ("cash", -10_000)],
    agent=MEMBER,
  )
  (residual,) = preview().residuals
  assert residual.cause == "unexplained"


def test_the_allocation_share_reads_pro_rata_and_the_override(group):
  _form_the_group(group)
  _january(group)
  with extensions_session(GRAPH) as session:
    share = engine.allocation_share(session, entity_id=MEMBER, period="2026-01")
  # The member's 1,825 for January; the outside owners' 30 % is 547.50.
  assert (share.month_result, share.outside_pct, share.pro_rata_amount) == (
    182_500,
    30.0,
    54_750,
  )
  assert share.override is None
  assert share.effective_amount == 54_750
  assert not share.closed

  _override(entity_id=MEMBER, amount=100_000, basis="Preferred return")
  with extensions_session(GRAPH) as session:
    share = engine.allocation_share(session, entity_id=MEMBER, period="2026-01")
    listed = engine.allocation_overrides(session, entity_id=MEMBER)
  assert share.pro_rata_amount == 54_750
  assert share.effective_amount == 100_000
  assert share.override is not None
  assert (share.override.amount, share.override.basis) == (100_000, "Preferred return")
  assert [o.period for o in listed.overrides] == ["2026-01"]


def test_a_step_up_names_an_asset_or_liability_never_what_is_computed(group):
  _buy_the_member(group)
  for concept in (
    engine.GOODWILL,
    engine.INVESTMENT_IN_SUBSIDIARIES,
    engine.NONCONTROLLING_INTEREST,
    BARGAIN_PURCHASE_GAIN,
  ):
    with pytest.raises(AcquisitionError, match="computed from the consideration"):
      _acquire(
        consideration=900_000,
        allocations=[AcquisitionAllocation(concept=concept, amount=50_000)],
      )
  with pytest.raises(AcquisitionError, match="is zero"):
    _acquire(
      consideration=900_000,
      allocations=[
        AcquisitionAllocation(
          concept="rs-gaap:LongTermInvestmentsAndReceivablesNet", amount=0
        )
      ],
    )


def test_an_acquisition_needs_a_basis_that_says_something():
  with pytest.raises(ValueError, match="Give the basis"):
    RecordAcquisitionRequest(entity_id=MEMBER, consideration=1, basis="   ")


def test_a_bargain_review_is_recorded_only_for_a_bargain(group):
  _buy_the_member(group)
  _acquire(consideration=900_000, confirm_bargain_purchase=True)
  with extensions_session(GRAPH) as session:
    meta = session.execute(
      select(Entry.metadata_).where(Entry.adjustment_kind == "acquisition")
    ).scalar_one()
  assert meta["acquisition"]["bargain_purchase_confirmed"] is False


def test_a_member_acquired_mid_month_joins_that_month(group):
  """Membership is by month: joined on the 15th, the member is consolidated
  for January and the acquisition is dated the day it joined."""
  _buy_the_member(group)
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entities SET joined_on = :d WHERE id = :id"),
      {"d": JAN_15, "id": MEMBER},
    )
  recorded = _acquire(consideration=900_000)
  assert recorded.entry.posting_date == JAN_15
  assert preview().residuals == []


def test_the_allocation_share_is_for_a_member_in_full_in_its_months(group):
  _form_the_group(group)
  with extensions_session(GRAPH) as session:
    assert (
      engine.allocation_share(session, entity_id=ASSOCIATE, period="2026-01") is None
    )
    session.execute(
      text("UPDATE entities SET joined_on = :d WHERE id = :id"),
      {"d": date(2026, 2, 1), "id": MEMBER},
    )
  with extensions_session(GRAPH) as session:
    assert engine.allocation_share(session, entity_id=MEMBER, period="2026-01") is None
    assert (
      engine.allocation_share(session, entity_id=MEMBER, period="2026-02") is not None
    )


# ── The parent buys more of a member (group-lifecycle Phase 2) ──────────


FEB_1 = date(2026, 2, 1)


def _flow_rows() -> None:
  """The cash flow rows an ownership change tags, as flow concepts."""
  with extensions_session(GRAPH) as session:
    traits: dict[str, Trait] = {}
    for qname, activity in (
      ("rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates", "investing"),
      ("rs-gaap:PaymentsToMinorityShareholders", "financing"),
      (
        "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates",
        "investing",
      ),
      ("rs-gaap:ProceedsFromMinorityShareholders", "financing"),
    ):
      flow = Element(
        qname=qname,
        name=qname.split(":")[1],
        balance_type="credit",
        period_type="duration",
        element_type="concept",
        is_abstract=False,
        created_by="test",
      )
      trait = traits.get(activity)
      if trait is None:
        trait = Trait(category="activityType", identifier=activity)
        traits[activity] = trait
      session.add_all([flow, trait])
      session.flush()
      session.add(ElementTrait(element_id=flow.id, trait_id=trait.id))


def _january_posted(group: dict) -> None:
  """The group formed in December, January's books, and January's
  consolidating entries posted at 70 %."""
  _flow_rows()
  _form_the_group(group)
  _january(group)
  _post_proposals(group, preview("2026-01"))


def _buy_ten_percent(group: dict, price: int = 150_000) -> None:
  """On 1 February the parent buys 10 % of the member from the outside owner."""
  post(
    group,
    PARENT_ENTITY_ID,
    FEB_1,
    [("investment", price), ("cash", -price)],
    agent="owner",
  )


def _change(**fields):
  values = {
    "entity_id": MEMBER,
    "period": "2026-02",
    "ownership_pct": 80,
    "consideration": 150_000,
    "basis": "Share purchase agreement",
  } | fields
  with extensions_session(GRAPH) as session:
    return record_ownership_change(
      session, RecordOwnershipChangeRequest(**values), "test"
    )


def _change_lines() -> dict[str, tuple[int, str | None]]:
  with extensions_session(GRAPH) as session:
    rows = session.execute(
      text(
        """
        SELECT el.code, li.debit_amount - li.credit_amount AS amount,
               fe.qname AS flow
        FROM line_items li JOIN entries e ON e.id = li.entry_id
        JOIN elements el ON el.id = li.element_id
        LEFT JOIN elements fe ON fe.id = li.flow_element_id
        WHERE e.adjustment_kind = 'ownership_change' AND e.status = 'draft'
        """
      )
    ).all()
  return {r.code: (int(r.amount), r.flow) for r in rows}


def test_buying_more_is_an_equity_transaction(group):
  """Capital 9,000 and January's 1,825 of results: 10 % is 900 of book and
  182.50 of earnings. The parent paid 1,500, so 600 goes against the
  investment and 417.50 is taken from its equity; no gain or loss."""
  _january_posted(group)
  _buy_ten_percent(group)
  recorded = _change()

  assert (recorded.from_pct, recorded.to_pct) == (70.0, 80.0)
  assert (recorded.book_value, recorded.earnings_transferred) == (90_000, 18_250)
  assert recorded.equity_adjustment == 41_750
  assert recorded.entry.posting_date == FEB_1
  lines = _change_lines()
  assert {code: amount for code, (amount, _) in lines.items()} == {
    "1600": -60_000,
    "3200": 18_250,
    "3050": 41_750,
  }
  # The price is the group's payment to its outside owners: the investing
  # outflow offset here, the rest financing.
  assert lines["1600"][1] == (
    "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates"
  )
  assert (
    lines["3200"][1] == lines["3050"][1] == "rs-gaap:PaymentsToMinorityShareholders"
  )
  with extensions_session(GRAPH) as session:
    assert float(session.get(Entity, MEMBER).ownership_pct) == 80.0


def test_a_price_below_the_earnings_bought_still_rides_the_payment_row(group):
  """Paying 1,000 for 10 % whose book value is 900 and whose share of
  January's result is 182.50: the premium (100) is less than the earnings
  transferred, so the parent's equity is credited the difference — and
  that credit is part of the price paid to the outside owners, on the
  payment row with the rest, never proceeds from them."""
  _january_posted(group)
  _buy_ten_percent(group, price=100_000)
  recorded = _change(consideration=100_000)

  assert recorded.equity_adjustment == -8_250
  lines = _change_lines()
  assert {code: amount for code, (amount, _) in lines.items()} == {
    "1600": -10_000,
    "3200": 18_250,
    "3050": -8_250,
  }
  assert lines["1600"][1] == (
    "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates"
  )
  assert (
    lines["3200"][1] == lines["3050"][1] == "rs-gaap:PaymentsToMinorityShareholders"
  )


def _sell_ten_percent(group: dict, price: int, released: int) -> None:
  """On 1 February the parent sells 10 % of the member to the outside owner:
  the proceeds against the carrying amount of the shares sold, the rest its
  own gain or loss."""
  post(
    group,
    PARENT_ENTITY_ID,
    FEB_1,
    [("cash", price), ("investment", -released), ("gain", released - price)],
    agent="owner",
  )


def test_selling_within_the_method_reverses_the_parents_gain_and_credits_the_outside_owners(
  group,
):
  """The parent carries 6,300 for its 70 % (7,000 put in, 700 of
  distributions back); selling a seventh of it for 1,500 releases 900 of
  carrying amount and books a 600 gain on its own books. In the group the
  book value sold (900) moves to the outside owners through the elimination,
  their share of January's result rises by 182.50, the gain is reversed, and
  the 417.50 difference is the parent's paid-in capital — ASC 810, no gain.
  The parent's cost on a formed member is its book value, so nothing is
  released against the investment."""
  _january_posted(group)
  recorded = _change(ownership_pct=60, consideration=150_000)

  assert (recorded.from_pct, recorded.to_pct) == (70.0, 60.0)
  assert (recorded.book_value, recorded.earnings_transferred) == (-90_000, -18_250)
  assert (recorded.carrying_released, recorded.gain_reversed) == (90_000, 60_000)
  assert recorded.equity_adjustment == -41_750
  lines = _change_lines()
  assert {code: amount for code, (amount, _) in lines.items()} == {
    "4400": 60_000,
    "3200": -18_250,
    "3050": -41_750,
  }
  # The proceeds leave the divestiture row and arrive as a financing inflow
  # from the outside owners.
  assert lines["4400"][1] == (
    "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates"
  )
  assert (
    lines["3200"][1] == lines["3050"][1] == "rs-gaap:ProceedsFromMinorityShareholders"
  )
  with extensions_session(GRAPH) as session:
    assert float(session.get(Entity, MEMBER).ownership_pct) == 60.0
    recorded_consideration = session.execute(
      text(
        "SELECT (metadata->'ownership_change'->>'consideration')::bigint "
        "FROM entries WHERE adjustment_kind = 'ownership_change'"
      )
    ).scalar_one()
  assert recorded_consideration == -90_000

  _sell_ten_percent(group, 150_000, 90_000)
  response = preview("2026-02")
  assert response.residuals == [] and not response.incomplete
  assert _lines(response, "elimination_investment") == {
    engine.INVESTMENT_IN_SUBSIDIARIES: 90_000,
    engine.NONCONTROLLING_INTEREST: -90_000,
  }


def test_selling_after_buying_moves_only_the_premium_not_the_earnings_moved_before(
  group,
):
  """February: the parent buys 10 % for 1,500, moving 182.50 of the outside
  owners' share of January's result to itself. March: it sells that 10 %
  back for 1,200. The outside owners get their 182.50 of results back —
  measured from the results, not read off February's line as if it were a
  premium — and nothing else moves on their interest, since no acquisition
  put a fair-value premium there. The parent's cost on the shares sold,
  975 (a tenth of 7,800 pro rata), is 75 above their 900 of book value,
  released against the investment; the 225 gain on its books is reversed;
  117.50 is the parent's paid-in capital."""
  _january_posted(group)
  _buy_ten_percent(group)
  _change()
  recorded = _change(period="2026-03", ownership_pct=70, consideration=120_000)

  assert (recorded.earnings_transferred, recorded.premium_transferred) == (-18_250, 0)
  assert (recorded.carrying_released, recorded.gain_reversed) == (97_500, 22_500)
  lines = {code: amount for code, (amount, _) in _change_lines().items()}
  # Both drafts are in the books; March's lines are the ones with the gain.
  assert lines["4400"] == 22_500


def test_a_sale_at_a_loss_reverses_the_loss_and_still_matches_the_parents_books(group):
  _january_posted(group)
  recorded = _change(ownership_pct=60, consideration=50_000)

  assert (recorded.carrying_released, recorded.gain_reversed) == (90_000, -40_000)
  assert recorded.equity_adjustment == 58_250
  lines = {code: amount for code, (amount, _) in _change_lines().items()}
  assert lines == {"4400": -40_000, "3200": -18_250, "3050": 58_250}
  _sell_ten_percent(group, 50_000, 90_000)
  response = preview("2026-02")
  assert response.residuals == [] and not response.incomplete


def test_a_second_sale_releases_the_carrying_amount_left_pro_rata(group):
  """After February's sale the parent carries 5,400 for its 60 %; selling a
  sixth of that in March releases 900 again, the pool of premium stays
  empty (both changes recorded none), and the member sits at exactly half —
  still in full."""
  _january_posted(group)
  _change(ownership_pct=60, consideration=150_000)
  _sell_ten_percent(group, 150_000, 90_000)
  recorded = _change(period="2026-03", ownership_pct=50, consideration=100_000)

  assert (recorded.carrying_released, recorded.gain_reversed) == (90_000, 10_000)
  assert (recorded.earnings_transferred, recorded.premium_transferred) == (-18_250, 0)
  assert recorded.equity_adjustment == 8_250
  with extensions_session(GRAPH) as session:
    assert float(session.get(Entity, MEMBER).ownership_pct) == 50.0
    recorded_sum = session.execute(
      text(
        "SELECT SUM((metadata->'ownership_change'->>'consideration')::bigint) "
        "FROM entries WHERE adjustment_kind = 'ownership_change'"
      )
    ).scalar_one()
  assert recorded_sum == -180_000
  post(
    group,
    PARENT_ENTITY_ID,
    date(2026, 3, 1),
    [("cash", 100_000), ("investment", -90_000), ("gain", -10_000)],
    agent="owner",
  )
  response = preview("2026-03")
  assert response.residuals == [] and not response.incomplete


def test_a_sale_needs_an_account_for_the_gain_it_reverses(group):
  _january_posted(group)
  with extensions_session(GRAPH) as session:
    session.execute(
      text(
        """
        DELETE FROM associations
        WHERE from_element_id IN (
          SELECT el.id FROM elements el
          JOIN entity_taxonomies et ON et.taxonomy_id = el.taxonomy_id
          WHERE et.entity_id = :c AND el.code = '4400'
        )
        """
      ),
      {"c": ADJUSTMENTS},
    )
    session.commit()
  with pytest.raises(OwnershipChangeError, match="GainLossOnDispositionOfAssets"):
    _change(ownership_pct=60, consideration=150_000)


def test_a_sale_keeps_control_or_is_a_change_of_method(group):
  _january_posted(group)
  with pytest.raises(OwnershipChangeError, match="change of method"):
    _change(ownership_pct=40, consideration=400_000)
  with pytest.raises(OwnershipChangeError, match="no change to record"):
    _change(ownership_pct=70, consideration=0)


def test_the_month_eliminates_at_the_new_share_and_moves_the_book_value(group):
  _january_posted(group)
  _buy_ten_percent(group)
  _change()
  response = preview("2026-02")

  assert response.residuals == []
  # 10 % of the 9,000 of capital moves from the outside owners to the parent,
  # tagged as the month's own movement.
  assert _lines(response, "elimination_investment") == {
    engine.INVESTMENT_IN_SUBSIDIARIES: -90_000,
    engine.NONCONTROLLING_INTEREST: 90_000,
  }
  assert set(_flows(response, "elimination_investment").values()) == {
    "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates",
    "rs-gaap:PaymentsToMinorityShareholders",
  }
  # January keeps its share.
  assert preview("2026-01").up_to_date


def test_the_share_holds_from_its_month(group):
  _january_posted(group)
  _buy_ten_percent(group)
  _change()
  with extensions_session(GRAPH) as session:
    january = engine.allocation_share(session, entity_id=MEMBER, period="2026-01")
    february = engine.allocation_share(session, entity_id=MEMBER, period="2026-02")
  assert january is not None and february is not None
  assert (january.outside_pct, february.outside_pct) == (30.0, 20.0)


def test_without_the_change_recorded_the_price_is_unexplained(group):
  _january_posted(group)
  _buy_ten_percent(group)
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entities SET ownership_pct = 80 WHERE id = :id"), {"id": MEMBER}
    )
  (residual,) = preview("2026-02").residuals
  assert residual.entity_id == MEMBER


def test_a_change_stays_within_the_method_and_buys_more(group):
  _january_posted(group)
  with pytest.raises(OwnershipChangeError, match="no longer controls"):
    _change(ownership_pct=40)
  with pytest.raises(OwnershipChangeError, match="no change to record"):
    _change(ownership_pct=70)
  with pytest.raises(OwnershipChangeError, match="not a member consolidated in full"):
    _change(entity_id=ASSOCIATE)


def test_changes_are_recorded_in_order_and_a_draft_is_replaced(group):
  _january_posted(group)
  _buy_ten_percent(group)
  _change(period="2026-03", ownership_pct=80)
  with pytest.raises(OwnershipChangeError, match="in the order they happened"):
    _change(period="2026-02", ownership_pct=75)
  replaced = _change(period="2026-03", ownership_pct=85, consideration=200_000)
  assert replaced.replaced
  assert replaced.from_pct == 70.0

  with extensions_session(GRAPH) as session:
    session.execute(
      text(
        "UPDATE entries SET status = 'posted' WHERE adjustment_kind = 'ownership_change'"
      )
    )
  with pytest.raises(OwnershipChangeError, match="Reverse that entry first"):
    _change(period="2026-03", ownership_pct=90)


# ── Pre-release review fixes ─────────────────────────────────────────────


def _acquire_sixty_percent_of_an_odd_capital(group: dict) -> None:
  """The member joins on 1 January with 10,000.03 of equity; the parent buys
  60 % for 9,000, the outside owners' 40 % valued at 5,500."""
  _flow_rows()
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entities SET joined_on = :d, ownership_pct = 60 WHERE id = :id"),
      {"d": JAN_1, "id": MEMBER},
    )
  post(
    group,
    MEMBER,
    DEC,
    [("cash", 1_000_003), ("equity", -1_000_003)],
    entry_type="opening",
  )
  post(
    group,
    PARENT_ENTITY_ID,
    JAN_1,
    [("investment", 900_000), ("cash", -900_000)],
    agent="owner",
  )
  _acquire(consideration=900_000, nci_fair_value=550_000)


def test_buying_more_moves_the_acquisition_premium_and_rounds_as_the_elimination(group):
  """The outside owners' 5,500 is 4,000.01 of book value and 1,499.99 above
  it. Buying half of their 40 % moves half of that premium to the parent with
  the shares (ASC 810), and the book value bought is the elimination's own
  difference of rounded shares, so the month does not withhold a cent."""
  _acquire_sixty_percent_of_an_odd_capital(group)
  post(
    group,
    PARENT_ENTITY_ID,
    FEB_1,
    [("investment", 300_000), ("cash", -300_000)],
    agent="owner",
  )
  recorded = _change(ownership_pct=80, consideration=300_000)

  assert recorded.book_value == 200_000  # 800,002 less 600,002
  assert recorded.premium_transferred == 75_000
  lines = {code: amount for code, (amount, _) in _change_lines().items()}
  assert lines == {"1600": -100_000, "3200": 75_000, "3050": 25_000}
  assert preview("2026-02").residuals == []


def test_undoing_a_change_puts_the_share_back(group):
  _january_posted(group)
  _buy_ten_percent(group)
  _change()
  with extensions_session(GRAPH) as session:
    entry_id = session.execute(
      text("SELECT id FROM entries WHERE adjustment_kind = 'ownership_change'")
    ).scalar_one()
    session.execute(
      text("UPDATE entries SET status = 'posted' WHERE id = :id"), {"id": entry_id}
    )
  with extensions_session(GRAPH) as session:
    reverse_journal_entry(
      session,
      ReverseJournalEntryRequest(entry_id=str(entry_id), posting_date=FEB_1),
      "test",
    )
    session.commit()
  with extensions_session(GRAPH) as session:
    assert float(session.get(Entity, MEMBER).ownership_pct) == 70.0
    february = engine.allocation_share(session, entity_id=MEMBER, period="2026-02")
  assert february is not None and february.outside_pct == 30.0
  # And it can be recorded again, from 70 %.
  assert _change().from_pct == 70.0


def test_an_acquisition_is_measured_at_the_share_bought(group):
  """Recorded again after a later change, the acquisition still measures the
  equity at the 60 % the parent bought, not the share it holds now."""
  _acquire_sixty_percent_of_an_odd_capital(group)
  post(
    group,
    PARENT_ENTITY_ID,
    FEB_1,
    [("investment", 300_000), ("cash", -300_000)],
    agent="owner",
  )
  _change(ownership_pct=80, consideration=300_000)
  again = _acquire(consideration=900_000, nci_fair_value=550_000)
  assert again.parent_share == 600_002


def test_a_reversed_acquisition_leaves_no_premium_to_move(group):
  """A reversed acquisition's reversal carries its kind with its lines
  flipped; neither is the outside owners' interest any more."""
  _acquire_sixty_percent_of_an_odd_capital(group)
  with extensions_session(GRAPH) as session:
    entry_id = session.execute(
      text("SELECT id FROM entries WHERE adjustment_kind = 'acquisition'")
    ).scalar_one()
    session.execute(
      text("UPDATE entries SET status = 'posted' WHERE id = :id"), {"id": entry_id}
    )
  with extensions_session(GRAPH) as session:
    reverse_journal_entry(
      session,
      ReverseJournalEntryRequest(entry_id=str(entry_id), posting_date=JAN_1),
      "test",
    )
    session.commit()
  post(
    group,
    PARENT_ENTITY_ID,
    FEB_1,
    [("investment", 300_000), ("cash", -300_000)],
    agent="owner",
  )
  assert _change(ownership_pct=80, consideration=300_000).premium_transferred == 0


def test_an_acquisition_ignores_a_change_in_the_month_it_joined(group):
  _acquire_sixty_percent_of_an_odd_capital(group)
  _change(period="2026-01", ownership_pct=80, consideration=300_000)
  again = _acquire(consideration=900_000, nci_fair_value=550_000)
  assert again.parent_share == 600_002


# ── A member leaves the group (group-lifecycle Phase 3) ────────────────


def _leave(entity_id: str, on: date) -> None:
  """The entity row as update-entity leaves it, past the exit guards."""
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE entities SET left_on = :d WHERE id = :id"),
      {"d": on, "id": entity_id},
    )


def _deconsolidate(**fields):
  values = {
    "entity_id": MEMBER,
    "consideration": 800_000,
    "basis": "Sale agreement",
  } | fields
  with extensions_session(GRAPH) as session:
    return record_deconsolidation(
      session, RecordDeconsolidationRequest(**values), "test"
    )


def _deconsolidation_lines(member: str = MEMBER) -> dict[str, int]:
  with extensions_session(GRAPH) as session:
    rows = session.execute(
      text(
        """
        SELECT el.code, SUM(li.debit_amount - li.credit_amount) AS amount
        FROM line_items li JOIN entries e ON e.id = li.entry_id
        JOIN elements el ON el.id = li.element_id
        WHERE e.adjustment_kind = 'deconsolidation' AND e.status = 'draft'
          AND e.adjusts_entity_id = :m
        GROUP BY el.code
        """
      ),
      {"m": member},
    ).all()
  return {r.code: int(r.amount) for r in rows if r.amount}


def test_a_member_that_leaves_has_its_results_put_back_and_the_gain_adjusted(group):
  """January posted at 70 %; the member leaves at its end. The outside
  owners' 547.50 of January's result, held as noncontrolling interest, is
  taken off; the member's 1,825 of results leave with its books and are
  put back into the group's equity; the 1,277.50 difference — the parent's
  70 % of those results, already the group's income — is the loss on its
  leaving against the gain the parent's own books take. The associate's
  pickup goes the same way, with nothing to put back, and February then
  needs nothing a person must compute."""
  _form_the_group(group)
  _january(group)
  _post_proposals(group, preview())
  _leave(MEMBER, JAN_31)
  recorded = _deconsolidate()

  assert (recorded.period, recorded.entry.posting_date) == ("2026-02", FEB_1)
  assert [(d.concept, d.amount) for d in recorded.derecognized] == [
    (engine.NONCONTROLLING_INTEREST, -54_750)
  ]
  assert (recorded.earnings_restored, recorded.gain_loss) == (182_500, -127_750)
  assert _deconsolidation_lines() == {
    "3200": 54_750,
    "3900": -182_500,
    "4400": 127_750,
  }
  february = preview("2026-02")
  assert not any("no longer in the group" in n for n in february.notes)
  assert not february.incomplete

  _leave(ASSOCIATE, JAN_31)
  assert preview("2026-02").incomplete  # the associate's pickup is held now
  associate = _deconsolidate(entity_id=ASSOCIATE, consideration=300_000)
  assert [(d.concept, d.amount) for d in associate.derecognized] == [
    (engine.EQUITY_METHOD_INVESTMENT, 20_000)
  ]
  assert (associate.earnings_restored, associate.gain_loss) == (0, -20_000)
  assert _deconsolidation_lines(ASSOCIATE) == {"1700": -20_000, "4400": 20_000}
  february = preview("2026-02")
  assert not february.incomplete
  _post_proposals(group, february)
  assert preview("2026-02").up_to_date


def test_an_acquired_members_goodwill_and_premium_leave_with_it(group):
  """Bought at a premium and sold before earning anything: the goodwill,
  the price above book against the investment and the outside owners'
  interest at fair value are taken off together, and they net to nothing
  — the carrying amount is what the parent paid, so the group's gain is
  the parent's own."""
  _acquire_sixty_percent_of_an_odd_capital(group)
  _post_proposals(group, preview())
  _leave(MEMBER, JAN_31)
  recorded = _deconsolidate(consideration=950_000)

  assert {d.concept: d.amount for d in recorded.derecognized} == {
    engine.GOODWILL: 449_997,
    engine.INVESTMENT_IN_SUBSIDIARIES: -299_998,
    engine.NONCONTROLLING_INTEREST: -149_999,
  }
  assert (recorded.earnings_restored, recorded.gain_loss) == (0, 0)
  assert _deconsolidation_lines() == {
    "1800": -449_997,
    "1600": 299_998,
    "3200": 149_999,
  }
  assert not preview("2026-02").incomplete


def test_a_deconsolidation_needs_a_member_that_left_and_something_to_record(group):
  _form_the_group(group)
  _january(group)
  with pytest.raises(DeconsolidationError, match="still a member"):
    _deconsolidate()
  with pytest.raises(DeconsolidationError, match="not a member"):
    _deconsolidate(entity_id=PARENT_ENTITY_ID)
  _leave(ASSOCIATE, DEC)
  with pytest.raises(DeconsolidationError, match="nothing to record"):
    _deconsolidate(entity_id=ASSOCIATE)
  with pytest.raises(ValueError, match="basis"):
    RecordDeconsolidationRequest(entity_id=MEMBER, consideration=0, basis="  ")


def test_recording_a_deconsolidation_again_replaces_the_draft_never_a_posted_one(
  group,
):
  _form_the_group(group)
  _january(group)
  _post_proposals(group, preview())
  _leave(MEMBER, JAN_31)
  assert not _deconsolidate().replaced
  assert _deconsolidate(consideration=850_000).replaced
  with extensions_session(GRAPH) as session:
    session.execute(
      text(
        "UPDATE entries SET status = 'posted' WHERE adjustment_kind = 'deconsolidation'"
      )
    )
  with pytest.raises(DeconsolidationError, match="Reverse"):
    _deconsolidate()


def test_a_member_leaves_only_closed_unfed_and_agreed(group):
  """left_on is refused while the member's books are open through the
  month, while a bank feed is linked to its chart, and while an
  intercompany balance disagrees with the group's. An agreed balance is
  not in the way: it stays as a related-party balance."""
  _form_the_group(group)
  # The parent says the member owes 175; the member's books say 150.
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
  with extensions_session(GRAPH) as session:
    session.add(
      FiscalPeriod(
        graph_id=GRAPH,
        entity_id=MEMBER,
        name="2026-01",
        start_date=JAN_1,
        end_date=JAN_31,
        period_type="monthly",
        status="open",
      )
    )

  def leave():
    with extensions_session(GRAPH) as session:
      return update_entity(
        session, UpdateEntityRequest(entity_id=MEMBER, left_on=JAN_31), "test"
      )

  with pytest.raises(EntityExitError, match="open for 2026-01"):
    leave()
  # Dropping the share to cost in the same call does not get past the guard.
  with (
    extensions_session(GRAPH) as session,
    pytest.raises(EntityExitError, match="open for 2026-01"),
  ):
    update_entity(
      session,
      UpdateEntityRequest(entity_id=MEMBER, left_on=JAN_31, ownership_pct=10),
      "test",
    )
  with extensions_session(GRAPH) as session:
    session.execute(
      text("UPDATE fiscal_periods SET status = 'closed' WHERE entity_id = :id"),
      {"id": MEMBER},
    )
  # Closed books mean closed through the month, not merely no open row.
  with (
    extensions_session(GRAPH) as session,
    pytest.raises(EntityExitError, match="closed through 2026-01"),
  ):
    update_entity(session, UpdateEntityRequest(entity_id=MEMBER, left_on=FEB_1), "test")
  with extensions_session(GRAPH) as session:
    cash = session.get(Element, group["accounts"][(MEMBER, "cash")])
    cash.metadata_ = {**(cash.metadata_ or {}), BANK_FEED_KEY: {"connection_id": "c"}}
  with pytest.raises(EntityExitError, match="bank feed"):
    leave()
  with extensions_session(GRAPH) as session:
    cash = session.get(Element, group["accounts"][(MEMBER, "cash")])
    cash.metadata_ = {
      k: v for k, v in (cash.metadata_ or {}).items() if k != BANK_FEED_KEY
    }
  with pytest.raises(EntityExitError, match="disagree"):
    leave()
  post(
    group,
    MEMBER,
    JAN_31,
    [("cash", 2_500), ("due_to", -2_500)],
    agent=PARENT_ENTITY_ID,
  )
  assert leave().left_on == JAN_31
