"""The consolidating entries a group's month needs (spec ``consolidation`` C2).

The engine reads the books and never an account code. An account is known by
the rs-gaap concept it maps to, and the consolidation entity's account for a
concept is its own account mapped there (D11). An intercompany line is one
whose event names another of the group's entities as its counterparty,
through that entity's ``self`` agent (D10). Each adjustment is brought to the
balance it should hold for the month, and only the difference from what the
consolidation entity already holds for that kind and member is proposed
(D8), so a person's entry of the same kind is respected by construction.

What the month needs, per member consolidated in full:

* the parent's investment eliminated against the member's capital (its
  equity lines other than closing entries: contributions net of
  distributions), the outside owners' share of that capital carried as
  noncontrolling interest — cumulative;
* the intercompany balances between the group's entities — cumulative — and
  the month's intercompany income against the matching expense;
* the outside owners' share of the month's income;

and, per member on the equity method, the parent's share of its month's
income. A difference between the parent's investment and its share of the
member's capital is a residual: withheld, never plugged.
"""

from __future__ import annotations

import calendar
from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import date, timedelta

from sqlalchemy import bindparam, select, text
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.api.extensions.consolidation import (
  AllocationOverrideListResponse,
  AllocationOverrideRecord,
  AllocationShareResponse,
  ConsolidationProposal,
  ConsolidationProposalLine,
  ConsolidationReconcilingItem,
  ConsolidationResidual,
  PreviewConsolidationEntriesResponse,
)
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.operations.roboledger.commands._guards import closed_periods
from robosystems.operations.roboledger.commands.opening_balances import OPENING
from robosystems.operations.roboledger.entity_scope import (
  GroupMember,
  find_consolidation_entity,
  is_group_parent,
  resolve_entity,
  resolve_entity_id,
)
from robosystems.operations.roboledger.entry_status import (
  SUPERSEDED_ENTRY_STATUS,
  landed_entry_bindparam,
)
from robosystems.operations.roboledger.fiscal_calendar.periods import next_period
from robosystems.operations.roboledger.reports.fact_grid import (
  _CASH_ANCHOR_QNAMES,
  _infer_classification,
)
from robosystems.operations.roboledger.reports.network_picker import (
  load_entity_reporting_style,
  taggable_flow_qnames,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)

INVESTMENT_IN_SUBSIDIARIES = (
  "rs-gaap:InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures"
)
EQUITY_METHOD_INVESTMENT = "rs-gaap:EquityMethodInvestments"
EQUITY_METHOD_INCOME = "rs-gaap:IncomeLossFromEquityMethodInvestments"
NONCONTROLLING_INTEREST = "rs-gaap:MinorityInterest"
NCI_SHARE_OF_INCOME = "rs-gaap:NetIncomeLossAttributableToNoncontrollingInterest"
GOODWILL = "rs-gaap:Goodwill"

# Income a parent may take a member's distribution to. Its line names the
# member as counterparty; in the group the distribution is the member's own
# capital returned, never income.
DISTRIBUTION_INCOME = frozenset(
  {
    "rs-gaap:OtherNonoperatingIncomeExpense",
    "rs-gaap:OtherNonoperatingIncome",
    "rs-gaap:InvestmentIncomeDividend",
    "rs-gaap:DividendIncomeOperating",
  }
)

# Where a member's allocation overrides live on its entity row: period →
# {"amount": cents} or {"percentage": 0 to 100}, with the basis and who set it.
ALLOCATION_OVERRIDES = "nci_allocation_overrides"

# Concepts the consolidation entity posts for its own reasons, never as an
# intercompany elimination.
_NOT_INTERCOMPANY = frozenset(
  {
    INVESTMENT_IN_SUBSIDIARIES,
    EQUITY_METHOD_INVESTMENT,
    EQUITY_METHOD_INCOME,
    NONCONTROLLING_INTEREST,
    NCI_SHARE_OF_INCOME,
    GOODWILL,
  }
)

EQUITY_CLASSIFICATIONS = frozenset(
  {"equity", "contraEquity", "distributionToOwners", "investmentByOwners"}
)
_BALANCE = frozenset({"asset", "contraAsset", "liability", "contraLiability"})
_INCOME = frozenset({"revenue", "gain"})
_EXPENSE = frozenset({"expense", "loss", "expenseReversal"})
_RESULT = _INCOME | _EXPENSE

# Kinds measured within the month; the rest are balances measured through
# its end. An intercompany elimination is either, by its account.
_PERIOD_KINDS = frozenset(
  {
    "nci_allocation",
    "nci_distribution",
    "equity_method_pickup",
    "equity_method_distribution",
  }
)


# The kinds the engine computes a target for. Held balances of any other kind
# (a manual entry, an acquisition) are a person's, never reversed (hatch
# rule 3).
COMPUTED_KINDS = frozenset(
  {
    "elimination_investment",
    "elimination_intercompany",
    "nci_allocation",
    "equity_method_pickup",
    "equity_method_distribution",
  }
)


# The cash flow row a cumulative elimination's line takes, by concept and kind,
# for a debit and for a credit: the row the line it eliminates produced, so the
# consolidation entity's tagged line offsets it (a credit is an inflow). The
# outside owners' share of capital is the group's real flow, on the
# noncontrolling-interest rows. A concept not here moves no tagged cash.
_FLOWS: dict[tuple[str, str], tuple[str, str]] = {
  (INVESTMENT_IN_SUBSIDIARIES, "elimination_investment"): (
    "rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates",
    "rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates",
  ),
  (NONCONTROLLING_INTEREST, "elimination_investment"): (
    "rs-gaap:PaymentsToMinorityShareholders",
    "rs-gaap:ProceedsFromMinorityShareholders",
  ),
  ("rs-gaap:MembersEquity", "elimination_investment"): (
    "rs-gaap:ProceedsFromContributedCapital",
    "rs-gaap:PaymentsOfCapitalDistribution",
  ),
  ("rs-gaap:PartnersCapital", "elimination_investment"): (
    "rs-gaap:ProceedsFromPartnershipContribution",
    "rs-gaap:PaymentsOfCapitalDistribution",
  ),
  ("rs-gaap:AdditionalPaidInCapital", "elimination_investment"): (
    "rs-gaap:ProceedsFromIssuanceOfCommonStock",
    "rs-gaap:PaymentsOfDividends",
  ),
  ("rs-gaap:CommonStockValue", "elimination_investment"): (
    "rs-gaap:ProceedsFromIssuanceOfCommonStock",
    "rs-gaap:PaymentsForRepurchaseOfCommonStock",
  ),
  # A corporation's dividends are its retained earnings' only cash movement.
  ("rs-gaap:RetainedEarningsAccumulatedDeficit", "elimination_investment"): (
    "rs-gaap:PaymentsOfDividends",
    "rs-gaap:PaymentsOfDividends",
  ),
  ("rs-gaap:LongTermDebtAndCapitalLeaseObligations", "elimination_intercompany"): (
    "rs-gaap:ProceedsFromRelatedPartyDebt",
    "rs-gaap:RepaymentsOfRelatedPartyDebt",
  ),
  ("rs-gaap:LongTermInvestmentsAndReceivablesNet", "elimination_intercompany"): (
    "rs-gaap:ProceedsFromSaleMaturityAndCollectionsOfInvestments",
    "rs-gaap:PaymentsToAcquireInvestments",
  ),
}

# The engine's drafts: computed, and written by propose-consolidation-entries.
ENGINE_SOURCE = "system"
ENGINE_PROVENANCE = "system_computed"


def engine_drafts(
  session: Session, consolidation_id: str, start: date, end: date
) -> list[Entry]:
  """The month's drafts the engine wrote in the consolidation entity."""
  return list(
    session.execute(
      select(Entry).where(
        Entry.entity_id == consolidation_id,
        Entry.status == "draft",
        Entry.provenance == ENGINE_PROVENANCE,
        Entry.adjustment_kind.in_(COMPUTED_KINDS),
        Entry.posting_date >= start,
        Entry.posting_date <= end,
      )
    ).scalars()
  )


_DraftKey = tuple[str, str, str, str | None]  # kind, member, account, flow


def _drafted(session: Session, drafts: list[Entry]) -> dict[_DraftKey, int]:
  """What the engine's drafts hold, net per kind, member, account and flow."""
  if not drafts:
    return {}
  rows = session.execute(
    text(
      """
      SELECT e.adjustment_kind, e.adjusts_entity_id, li.element_id,
             fe.qname AS flow_qname,
             SUM(li.debit_amount - li.credit_amount) AS amount
      FROM line_items li
      JOIN entries e ON e.id = li.entry_id
      LEFT JOIN elements fe ON fe.id = li.flow_element_id
      WHERE e.id IN :ids
      GROUP BY e.adjustment_kind, e.adjusts_entity_id, li.element_id, fe.qname
      """
    ).bindparams(bindparam("ids", expanding=True)),
    {"ids": [str(e.id) for e in drafts]},
  ).all()
  return {
    (r.adjustment_kind, r.adjusts_entity_id, r.element_id, r.flow_qname): int(r.amount)
    for r in rows
    if r.amount
  }


def _proposed(proposals: list[ConsolidationProposal]) -> dict[_DraftKey, int]:
  """What drafting the proposals would hold, keyed as ``_drafted``."""
  held: dict[_DraftKey, int] = defaultdict(int)
  for p in proposals:
    if p.opening:
      continue
    for line in p.lines:
      key = (p.adjustment_kind, p.adjusts_entity_id, line.element_id, line.flow_qname)
      held[key] += line.debit_amount - line.credit_amount
  return {k: v for k, v in held.items() if v}


def elimination_flow(qname: str, kind: str, amount: int) -> str | None:
  flows = _FLOWS.get((qname, kind))
  if flows is None:
    return None
  return flows[0] if amount > 0 else flows[1]


def _tagged_part(
  lines: dict[str, int],
  kind: str,
  periodic: bool,
  *,
  carried: dict[str, int],
  taggable: set[str],
  untaggable: set[str],
) -> tuple[dict[str, int], dict[str, str]]:
  """The part of an entry that offsets the month's own cash flows, with each
  line's row; the rest posts untagged.

  Only the month's movement is tagged: what an entry catches up from earlier
  months (``carried``) moved no cash in this one. The tagged lines must net
  to nothing, as the cash flows they offset do; when one cannot be tagged
  (the consolidation entity's statement has no row for it) or they would
  not net, the entry offsets nothing, since half an offset is a flow. A
  distribution the parent took as income is reversed beside them untagged:
  it is the operating side of the flow they offset."""
  if periodic:
    return {}, {}
  movement = {q: a - carried.get(q, 0) for q, a in lines.items()}
  flows = {q: elimination_flow(q, kind, a) for q, a in movement.items() if a}
  missing = {f for f in flows.values() if f and taggable and f not in taggable}
  if missing:
    untaggable |= missing
    return {}, {}
  tagged = {q: movement[q] for q, f in flows.items() if f}
  if tagged:
    tagged |= {q: a for q, a in movement.items() if a and q in DISTRIBUTION_INCOME}
  if sum(tagged.values()):
    return {}, {}
  return tagged, {q: f for q, f in flows.items() if f}


class NoConsolidationEntityError(ValueError):
  """The group has no consolidation entity to post its adjustments in."""


@dataclass(frozen=True)
class _Concept:
  qname: str
  period_type: str | None
  classification: str | None


@dataclass
class _Entity:
  id: str
  name: str
  role: str
  method: str
  share: float  # the parent's, as a fraction
  member: GroupMember
  overrides: dict = field(default_factory=dict)  # period → allocation override


@dataclass(frozen=True)
class _Row:
  entity_id: str
  element_id: str
  account_name: str
  concept: _Concept
  counterparty: str | None  # the group entity the event names
  closing: bool
  kind: str | None
  adjusts: str | None
  in_period: bool
  amount: int  # cents, debit-positive
  opening: bool = False  # the engine's opening of the consolidation entity


@dataclass
class _Plan:
  """Targets and held balances, keyed (concept, kind, member, periodic)."""

  targets: dict[tuple[str, str, str, bool], int] = field(
    default_factory=lambda: defaultdict(int)
  )
  basis: dict[tuple[str, str, bool], list[str]] = field(
    default_factory=lambda: defaultdict(list)
  )
  residuals: list[ConsolidationResidual] = field(default_factory=list)
  reconciling: list[ConsolidationReconcilingItem] = field(default_factory=list)
  withheld: set[tuple[str, str]] = field(default_factory=set)
  # Members whose distributions the parent took as income, reversed with
  # their investment elimination.
  explained: set[str] = field(default_factory=set)
  notes: list[str] = field(default_factory=list)
  # Something the month needs was left out, so it cannot be up to date.
  incomplete: bool = False


def month_bounds(period: str) -> tuple[date, date]:
  year, month = (int(part) for part in period.split("-"))
  return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def _is_periodic(kind: str, concept: _Concept) -> bool:
  if kind in _PERIOD_KINDS:
    return True
  if kind == "elimination_intercompany":
    return concept.period_type == "duration"
  return False


def share_of(cents: int, share: float) -> int:
  return round(cents * share)


def _money(cents: int) -> str:
  return f"{cents / 100:,.2f}"


# ── Loading ─────────────────────────────────────────────────────────────


def _group(
  session: Session, parent_id: str, period: str | None = None
) -> list[_Entity]:
  """The group's entities, the parent first; each member's share is the
  parent's for ``period`` when given, from its recorded ownership changes."""
  rows = session.execute(
    text(
      "SELECT id, name, entity_role, consolidation_method, ownership_pct, "
      "       joined_on, left_on, metadata FROM entities "
      "WHERE source <> 'linked' "
      "ORDER BY (id <> :parent), created_at ASC, id ASC"
    ),
    {"parent": parent_id},
  ).all()
  changes = ownership_changes(session) if period is not None else {}
  entities: list[_Entity] = []
  for row in rows:
    pct = float(row.ownership_pct) if row.ownership_pct is not None else None
    if period is not None:
      pct = pct_as_of(pct, changes.get(str(row.id), []), period)
    entities.append(
      _Entity(
        id=str(row.id),
        name=str(row.name),
        role=str(row.entity_role),
        method="full" if row.id == parent_id else str(row.consolidation_method),
        share=pct / 100.0 if pct is not None else 1.0,
        member=GroupMember(
          str(row.id),
          str(row.entity_role),
          None if row.id == parent_id else row.joined_on,
          None if row.id == parent_id else row.left_on,
        ),
        overrides=dict((row.metadata or {}).get(ALLOCATION_OVERRIDES) or {}),
      )
    )
  return entities


def primary_concepts(session: Session, mapping_ids: list[str]) -> dict[str, _Concept]:
  """Each mapped account's primary concept: the classified target first, then
  by qname — the rule the statements' close uses, so a posting mapped to two
  concepts is read once."""
  if not mapping_ids:
    return {}
  rows = session.execute(
    text(
      """
      SELECT a.from_element_id, t.qname, t.period_type, t.balance_type,
             tcls.identifier AS classification
      FROM associations a
      JOIN elements t ON t.id = a.to_element_id
      LEFT JOIN (
        SELECT et.element_id, tr.identifier
        FROM element_traits et
        JOIN traits tr ON tr.id = et.trait_id
        WHERE et.is_primary = TRUE
          AND tr.category = 'elementsOfFinancialStatements'
      ) tcls ON tcls.element_id = t.id
      WHERE a.association_type = 'mapping'
        AND a.structure_id IN :mappings
        AND t.element_type = 'concept'
        AND t.is_abstract = false
      """
    ).bindparams(bindparam("mappings", expanding=True)),
    {"mappings": mapping_ids},
  ).all()
  ranked: dict[str, list[_Concept]] = defaultdict(list)
  for row in rows:
    ranked[str(row.from_element_id)].append(
      _Concept(
        qname=str(row.qname),
        period_type=row.period_type,
        classification=row.classification
        or _infer_classification(row.qname, row.balance_type),
      )
    )
  return {
    element_id: sorted(concepts, key=lambda c: (c.classification is None, c.qname))[0]
    for element_id, concepts in ranked.items()
  }


def _rows(
  session: Session,
  entity_ids: list[str],
  concepts: dict[str, _Concept],
  start: date,
  end: date,
  *,
  drafts_of: str,
) -> list[_Row]:
  """Every landed line of the group's entities through ``end``, summed by
  account, counterparty, kind and whether it falls in the month. A reversal
  names the counterparty its original did, so the two net. The
  consolidation entity's (``drafts_of``) drafts count as well, since its
  close will post them, but not the engine's own drafts of this month,
  which the proposal replaces: so two open months never eliminate twice."""
  result = session.execute(
    text(
      """
      SELECT e.entity_id, li.element_id, src.name AS account_name,
             COALESCE(oag.external_id, ag.external_id) AS counterparty,
             (e.type = 'closing') AS closing,
             CASE WHEN e.type = 'opening' AND e.provenance = :engine
                  THEN li.metadata->>'adjustment_kind'
                  ELSE e.adjustment_kind END AS adjustment_kind,
             CASE WHEN e.type = 'opening' AND e.provenance = :engine
                  THEN li.metadata->>'adjusts_entity_id'
                  ELSE e.adjusts_entity_id END AS adjusts_entity_id,
             (e.type = 'opening' AND e.provenance = :engine) AS opening,
             (e.posting_date >= :start) AS in_period,
             SUM(li.debit_amount - li.credit_amount) AS amount
      FROM line_items li
      JOIN entries e ON e.id = li.entry_id
      JOIN elements src ON src.id = li.element_id
      LEFT JOIN events ev ON ev.id = e.triggered_by_event_id
      LEFT JOIN agents ag ON ag.id = ev.agent_id AND ag.agent_type = 'self'
      LEFT JOIN entries orig ON orig.id = e.reversal_of
      LEFT JOIN events oev ON oev.id = orig.triggered_by_event_id
      LEFT JOIN agents oag ON oag.id = oev.agent_id AND oag.agent_type = 'self'
      WHERE (
          e.status IN :landed_entry_statuses
          OR (
            e.status = 'draft'
            AND e.entity_id = :drafts_of
            AND NOT (e.provenance = :engine AND e.posting_date >= :start)
          )
        )
        AND e.posting_date <= :end
        AND e.entity_id IN :entities
      GROUP BY e.entity_id, li.element_id, src.name,
               COALESCE(oag.external_id, ag.external_id),
               (e.type = 'closing'),
               (e.type = 'opening' AND e.provenance = :engine),
               e.adjustment_kind, e.adjusts_entity_id,
               li.metadata->>'adjustment_kind', li.metadata->>'adjusts_entity_id',
               (e.posting_date >= :start)
      """
    ).bindparams(landed_entry_bindparam(), bindparam("entities", expanding=True)),
    {
      "entities": entity_ids,
      "start": start,
      "end": end,
      "drafts_of": drafts_of,
      "engine": ENGINE_PROVENANCE,
    },
  ).all()
  members = set(entity_ids)
  rows: list[_Row] = []
  for row in result:
    concept = concepts.get(str(row.element_id))
    if concept is None or not row.amount:
      continue
    counterparty = str(row.counterparty) if row.counterparty else None
    rows.append(
      _Row(
        entity_id=str(row.entity_id),
        element_id=str(row.element_id),
        account_name=str(row.account_name),
        concept=concept,
        counterparty=counterparty if counterparty in members else None,
        closing=bool(row.closing),
        kind=row.adjustment_kind,
        adjusts=row.adjusts_entity_id,
        in_period=bool(row.in_period),
        amount=int(row.amount),
        opening=bool(row.opening),
      )
    )
  return rows


def posting_accounts(
  session: Session, consolidation_id: str, concepts: dict[str, _Concept]
) -> dict[str, tuple[str, str]]:
  """The consolidation entity's account for each concept it can post (D11):
  of its accounts mapped to the concept, one it already posts to and that no
  closing entry reaches (a retained-earnings close target can map to the
  same equity concept as an eliminated capital row), the code breaking a
  tie."""
  rows = session.execute(
    text(
      """
      SELECT el.id, el.name, el.code,
             COUNT(li.id) FILTER (WHERE e.type = 'closing') AS closings,
             COUNT(li.id) FILTER (WHERE e.type <> 'closing') AS postings
      FROM elements el
      JOIN entity_taxonomies et ON et.taxonomy_id = el.taxonomy_id
      LEFT JOIN line_items li ON li.element_id = el.id
      LEFT JOIN entries e ON e.id = li.entry_id AND e.entity_id = :c
      WHERE et.entity_id = :c AND et.basis = 'chart_of_accounts'
      GROUP BY el.id, el.name, el.code
      """
    ),
    {"c": consolidation_id},
  ).all()
  ranked: dict[str, list[tuple[tuple, str, str]]] = defaultdict(list)
  for row in rows:
    concept = concepts.get(str(row.id))
    if concept is None:
      continue
    rank = (row.closings > 0, row.postings == 0, row.code or "", str(row.id))
    ranked[concept.qname].append((rank, str(row.id), str(row.name)))
  return {
    qname: (min(options)[1], min(options)[2]) for qname, options in ranked.items()
  }


# ── Targets ─────────────────────────────────────────────────────────────


def capital_and_earnings(
  session: Session, member_id: str, through: date
) -> tuple[int, int]:
  """A member's capital (its equity lines other than closing entries, as the
  investment elimination reads it) and its results to date (the result lines
  the closes carried to retained earnings), credit-positive, through
  ``through``. An acquired member's earnings before it joined are capital,
  being in the opening balance."""
  mapping = find_entity_mapping(session, member_id, BOOK_FRAMEWORK)
  concepts = primary_concepts(session, [str(mapping.id)]) if mapping else {}
  rows = _rows(session, [member_id], concepts, through, through, drafts_of="")
  capital = -sum(
    r.amount
    for r in rows
    if not r.closing
    and r.concept.classification in EQUITY_CLASSIFICATIONS
    and r.concept.qname != NONCONTROLLING_INTEREST
  )
  earnings = -sum(
    r.amount for r in rows if not r.closing and r.concept.classification in _RESULT
  )
  return capital, earnings


def _net_income(rows: list[_Row], entity_id: str) -> int:
  """The month's result, credit-positive, as the close computes it."""
  return -sum(
    r.amount
    for r in rows
    if r.entity_id == entity_id
    and r.in_period
    and not r.closing
    and r.concept.classification in _RESULT
  )


ACQUISITION = "acquisition"


OWNERSHIP_CHANGE = "ownership_change"
# The kinds a person records the parent's price for a member's shares on.
PRICED_KINDS = (ACQUISITION, OWNERSHIP_CHANGE)
DECONSOLIDATION = "deconsolidation"


def acquisitions(
  session: Session, consolidation_id: str, through: date
) -> dict[str, int]:
  """What the parent paid for each member's shares outside the member's own
  books, through ``through``, posted or waiting in a draft the consolidation
  entity's close posts: an acquisition's consideration, and the price of each
  later change in its share, negative for shares it sold."""
  rows = session.execute(
    text(
      """
      SELECT adjusts_entity_id AS member,
             SUM(COALESCE(
               (metadata->'acquisition'->>'consideration')::bigint,
               (metadata->'ownership_change'->>'consideration')::bigint
             )) AS paid
      FROM entries
      WHERE entity_id = :c AND adjustment_kind IN :kinds
        AND status IN ('posted', 'draft') AND posting_date <= :through
        AND (metadata ? 'acquisition' OR metadata ? 'ownership_change')
      GROUP BY adjusts_entity_id
      """
    ).bindparams(bindparam("kinds", expanding=True)),
    {"c": consolidation_id, "kinds": list(PRICED_KINDS), "through": through},
  ).all()
  return {str(r.member): int(r.paid) for r in rows if r.paid}


@dataclass(frozen=True)
class OwnershipChange:
  period: str  # YYYY-MM, the first month the new share holds
  from_pct: float
  to_pct: float


def ownership_changes(session: Session) -> dict[str, list[OwnershipChange]]:
  """Each member's recorded changes in the parent's share, oldest first."""
  rows = session.execute(
    text(
      """
      SELECT adjusts_entity_id AS member, metadata->'ownership_change' AS change
      FROM entries
      WHERE adjustment_kind = :kind AND status IN ('posted', 'draft')
        AND metadata ? 'ownership_change'
      ORDER BY posting_date, created_at
      """
    ),
    {"kind": OWNERSHIP_CHANGE},
  ).all()
  changes: dict[str, list[OwnershipChange]] = defaultdict(list)
  for row in rows:
    change = row.change or {}
    changes[str(row.member)].append(
      OwnershipChange(
        period=str(change["period"]),
        from_pct=float(change["from_pct"]),
        to_pct=float(change["to_pct"]),
      )
    )
  return changes


def pct_as_of(
  current: float | None, changes: list[OwnershipChange], period: str
) -> float | None:
  """The parent's share of a member for ``period``: the share the latest
  change at or before it set, or the one the first later change started
  from; the entity row's own with no change recorded."""
  if not changes:
    return current
  held = [c for c in changes if c.period <= period]
  return held[-1].to_pct if held else changes[0].from_pct


def _opened(session: Session, entity_ids: list[str]) -> set[str]:
  """The entities whose books start from a recorded opening balance."""
  if not entity_ids:
    return set()
  rows = session.execute(
    text(
      "SELECT DISTINCT entity_id FROM entries "
      "WHERE type = :opening AND status <> :superseded AND entity_id IN :ids"
    ).bindparams(bindparam("ids", expanding=True)),
    {"opening": OPENING, "superseded": SUPERSEDED_ENTRY_STATUS, "ids": entity_ids},
  ).all()
  return {str(r.entity_id) for r in rows}


def _unattributed_investment(rows: list[_Row], parent: _Entity) -> int:
  """The parent's investment in subsidiaries naming no member: what it paid
  sellers for the members it bought."""
  return sum(
    r.amount
    for r in rows
    if r.entity_id == parent.id
    and r.counterparty is None
    and r.concept.qname == INVESTMENT_IN_SUBSIDIARIES
  )


def _plan_capital(
  plan: _Plan,
  rows: list[_Row],
  parent: _Entity,
  member: _Entity,
  *,
  consolidation_id: str,
  acquired: dict[str, int],
  opened: set[str],
) -> None:
  """The parent's investment against the member's capital, and the outside
  owners' share of that capital as noncontrolling interest. For a member
  bought from its owners, the investment is the recorded consideration, and
  the acquisition entry carries the excess over the parent's share of the
  equity it bought."""
  key = ("elimination_investment", member.id, False)
  capital_by_concept: dict[str, int] = defaultdict(int)
  for r in rows:
    if (
      r.entity_id == member.id
      and not r.closing
      and r.concept.classification in EQUITY_CLASSIFICATIONS
      and r.concept.qname != NONCONTROLLING_INTEREST
    ):
      capital_by_concept[r.concept.qname] += r.amount
  capital = -sum(capital_by_concept.values())
  parent_part = share_of(capital, member.share)
  outside_part = capital - parent_part

  invested_lines = [
    r
    for r in rows
    if r.entity_id == parent.id
    and r.counterparty == member.id
    and r.concept.qname == INVESTMENT_IN_SUBSIDIARIES
  ]
  consideration = acquired.get(member.id, 0)
  # The acquisition entry's investment line credits the excess paid over the
  # parent's share of the equity at acquisition.
  excess_held = sum(
    r.amount
    for r in rows
    if r.entity_id == consolidation_id
    and r.kind in PRICED_KINDS
    and r.adjusts == member.id
    and r.concept.qname == INVESTMENT_IN_SUBSIDIARIES
  )
  invested = sum(r.amount for r in invested_lines) + consideration
  residual = invested + excess_held - parent_part
  distributed_lines = _distribution_income(rows, parent, member)
  distributed_by_concept: dict[str, int] = defaultdict(int)
  for r in distributed_lines:
    distributed_by_concept[r.concept.qname] -= r.amount
  distributed = sum(distributed_by_concept.values())
  # Each of the parent's investment entries rounds its own share; a sum of
  # rounded parts may differ from the rounded whole by half a cent each.
  explained = bool(distributed) and abs(residual - distributed) * 2 <= len(
    invested_lines
  ) + len(distributed_lines)
  if explained:
    plan.explained.add(member.id)
    for qname, amount in distributed_by_concept.items():
      if amount:
        plan.targets[(qname, *key)] += amount
    plan.basis[key].append(
      f"{parent.name} took {_money(distributed)} of {member.name}'s "
      "distributions as income; in the group they are the member's capital "
      "returned, so that income is reversed against its investment."
    )
  elif abs(residual) * 2 > len(invested_lines):
    if (
      not consideration and member.member.joined_on is not None and member.id in opened
    ):
      unattributed = _unattributed_investment(rows, parent)
      plan.residuals.append(
        ConsolidationResidual(
          entity_id=member.id,
          entity_name=member.name,
          amount=residual,
          cause="pre_acquisition",
          explanation=(
            f"{member.name} joined the group on {member.member.joined_on} "
            f"with its books opening on {_money(capital)} of capital, of "
            f"which {parent.name}'s {member.share:.0%} is "
            f"{_money(parent_part)}; its acquisition is not recorded. Record "
            "it with record-acquisition: the consideration paid to its "
            "sellers, the excess going to goodwill or fair-value step-ups."
            + (
              f" {parent.name}'s investment account holds "
              f"{_money(unattributed)} naming no member."
              if unattributed
              else ""
            )
          ),
        )
      )
      plan.withheld.add(("elimination_investment", member.id))
      return
    plan.residuals.append(
      ConsolidationResidual(
        entity_id=member.id,
        entity_name=member.name,
        amount=residual,
        explanation=(
          f"{parent.name} carries {_money(invested)} as its investment in "
          f"{member.name}; its {member.share:.0%} of the member's capital "
          f"({_money(capital)}) is {_money(parent_part)}."
          + (
            f" Of the investment, {_money(consideration)} is the consideration "
            f"its recorded acquisition paid the sellers, and the acquisition "
            f"entry allocates {_money(-excess_held)} of it beyond book: record "
            "the acquisition again if the member's opening balance changed."
            if consideration
            else ""
          )
          + (
            f" {_money(distributed)} of its income names {member.name}, which "
            "explains only part of the difference."
            if distributed
            else ""
          )
        ),
      )
    )
    plan.withheld.add(("elimination_investment", member.id))
    return

  for qname, amount in capital_by_concept.items():
    if amount:
      plan.targets[(qname, *key)] -= amount
  plan.targets[(INVESTMENT_IN_SUBSIDIARIES, *key)] -= parent_part + (
    distributed if explained else 0
  )
  if outside_part:
    plan.targets[(NONCONTROLLING_INTEREST, *key)] -= outside_part
  plan.basis[key].append(
    f"{member.name}'s capital net of distributions {_money(capital)}: "
    f"{parent.name}'s {member.share:.0%} {_money(parent_part)} eliminated "
    f"against its investment"
    + (
      f", the outside owners' {_money(outside_part)} as noncontrolling interest."
      if outside_part
      else "."
    )
  )


def _check_consideration(
  plan: _Plan,
  rows: list[_Row],
  parent: _Entity,
  members: list[_Entity],
  acquired: dict[str, int],
) -> None:
  """Hold the month when the recorded acquisitions and changes in the
  parent's share, net of shares sold, paid the sellers more than the
  parent's books hold as investment naming no member: the eliminations
  would credit an investment the parent never carried."""
  paid = sum(acquired.get(m.id, 0) for m in members)
  if not paid:
    return
  carried = _unattributed_investment(rows, parent)
  if paid > carried:
    plan.notes.append(
      f"The recorded acquisitions and changes in the parent's share paid the "
      f"sellers {_money(paid)} net of shares sold, but {parent.name}'s "
      f"investment account holds {_money(carried)} naming no member; post the "
      "purchase or sale in the parent's books (a sale credits the investment "
      "by the carrying amount of the shares sold, its gain apart), or correct "
      "the consideration with record-acquisition or record-ownership-change."
    )
    plan.incomplete = True


def _match(
  items: list[tuple[str, str, int]],
) -> tuple[list[tuple[str, str, int]], list[tuple[str, str, int, int]]]:
  """Pair debit items with credit items of equal size; when one of each is
  left over, eliminate the lower amount. Returns the eliminations (entity,
  concept, amount to reverse) and what is left (entity, concept, left,
  original)."""
  debits = sorted((i for i in items if i[2] > 0), key=lambda i: -i[2])
  credits = sorted((i for i in items if i[2] < 0), key=lambda i: i[2])
  eliminated: list[tuple[str, str, int]] = []
  for d in list(debits):
    for c in credits:
      if c[2] == -d[2]:
        eliminated += [d, c]
        debits.remove(d)
        credits.remove(c)
        break
  left: list[tuple[str, str, int, int]] = []
  if len(debits) == 1 and len(credits) == 1:
    d, c = debits[0], credits[0]
    lower = min(d[2], -c[2])
    eliminated += [(d[0], d[1], lower), (c[0], c[1], -lower)]
    if d[2] > lower:
      left.append((d[0], d[1], d[2] - lower, d[2]))
    if -c[2] > lower:
      left.append((c[0], c[1], c[2] + lower, c[2]))
  else:
    left += [(e, q, a, a) for e, q, a in debits + credits]
  return eliminated, left


def _plan_intercompany(
  plan: _Plan,
  rows: list[_Row],
  consolidated: list[_Entity],
  postable: set[str],
) -> None:
  """The balances the group's entities owe each other, and the month's
  income one earns from another against the other's expense. A balance on
  a concept the consolidation entity has no account for cannot be
  eliminated, so it is named and the month left incomplete."""
  ids = [e.id for e in consolidated]
  parent_id = ids[0]
  names = {e.id: e.name for e in consolidated}
  for i, a in enumerate(ids):
    for b in ids[i + 1 :]:
      member = b if a == parent_id else a
      unpostable: dict[tuple[str, str], int] = defaultdict(int)
      for periodic in (False, True):
        sums: dict[tuple[str, str], int] = defaultdict(int)
        for r in rows:
          if {r.entity_id, r.counterparty} != {a, b} or r.closing or r.kind:
            continue
          if (
            r.entity_id == parent_id
            and r.counterparty in plan.explained
            and r.concept.qname in DISTRIBUTION_INCOME
          ):
            continue
          q = r.concept
          if q.qname in _NOT_INTERCOMPANY or q.qname in _CASH_ANCHOR_QNAMES:
            continue
          if periodic:
            if not r.in_period or q.classification not in _RESULT:
              continue
          elif q.classification not in _BALANCE:
            continue
          if q.qname not in postable:
            if not periodic:
              unpostable[(r.entity_id, q.qname)] += r.amount
            continue
          sums[(r.entity_id, q.qname)] += r.amount
        items = [(e, q, amt) for (e, q), amt in sums.items() if amt]
        if not items:
          continue
        eliminated, left = _match(items)
        key = ("elimination_intercompany", member, periodic)
        for _entity, qname, amount in eliminated:
          plan.targets[(qname, *key)] -= amount
        if eliminated:
          what = "charges for the month" if periodic else "balances"
          plan.basis[key].append(
            f"Intercompany {what} between {names[a]} and {names[b]}: "
            + ", ".join(
              f"{names[e]} {q.split(':')[-1]} {_money(abs(amt))}"
              for e, q, amt in eliminated
            )
            + "."
          )
        for entity_id, qname, amount, original in left:
          other = b if entity_id == a else a
          if periodic and amount > 0:
            # An expense with no income against it is a cost one entity paid
            # and the other recharged at cost: it stays in the group's
            # result, and nothing disagrees.
            plan.notes.append(
              f"{names[entity_id]}'s {qname.split(':')[-1]} with {names[other]} "
              f"includes {_money(amount)} with no income on {names[other]}'s "
              "books; it stays in the consolidated statements as a cost "
              "recharged at cost."
            )
            continue
          plan.reconciling.append(
            ConsolidationReconcilingItem(
              entity_ids=[a, b],
              concept=qname,
              entity_id=entity_id,
              amount=amount,
              description=(
                f"{names[entity_id]}'s {qname.split(':')[-1]} with "
                f"{names[other]} ({_money(abs(original))}) has "
                f"{_money(abs(amount))} with no counterpart on "
                f"{names[other]}'s books."
              ),
            )
          )
      # A balance only one side holds is a third party's carried on an
      # intercompany entry (the vendor's payable on a rebilled bill); only
      # one both sides hold is owed between them.
      holders = {entity_id for (entity_id, _q), amount in unpostable.items() if amount}
      if holders != {a, b}:
        continue
      for (entity_id, qname), amount in sorted(unpostable.items()):
        if not amount:
          continue
        other = b if entity_id == a else a
        plan.notes.append(
          f"{names[entity_id]}'s {qname.split(':')[-1]} with {names[other]} "
          f"({_money(abs(amount))}) is owed within the group, and the "
          f"consolidation entity has no account mapped to {qname} to "
          "eliminate it; the balance is left out."
        )
        plan.incomplete = True


def _distribution_income(
  rows: list[_Row], parent: _Entity, member: _Entity
) -> list[_Row]:
  """The parent's income lines that name ``member``: its distributions,
  taken as income. Through the month end, not within it: the capital
  check that they explain is cumulative."""
  return [
    r
    for r in rows
    if r.entity_id == parent.id
    and r.counterparty == member.id
    and not r.closing
    and not r.kind
    and r.concept.qname in DISTRIBUTION_INCOME
  ]


def _plan_income_shares(
  plan: _Plan,
  rows: list[_Row],
  parent: _Entity,
  members: list[_Entity],
  associates: list[_Entity],
  period: str,
) -> None:
  """The outside owners' share of each member's month, and the parent's
  share of each equity-method member's."""
  for member in members:
    outside = 1.0 - member.share
    override = member.overrides.get(period)
    key = ("nci_allocation", member.id, True)
    if override:
      income = _net_income(rows, member.id)
      share = _override_share(override, income)
      how = (
        _money(share)
        if override.get("amount") is not None
        else f"{float(override['percentage']):g}% of {_money(income)}, {_money(share)}"
      )
      plan.basis[key].append(
        f"The outside owners' share of {member.name}'s result for the month, "
        f"by an allocation override: {how} (pro rata would be "
        f"{_money(share_of(income, max(outside, 0.0)))}). "
        f"{override.get('basis') or ''}".rstrip()
      )
    elif outside <= 0:
      continue
    else:
      share = share_of(_net_income(rows, member.id), outside)
      plan.basis[key].append(
        f"The outside owners' {outside:.0%} of {member.name}'s result for the "
        f"month: {_money(share)}."
      )
    if not share:
      continue
    plan.targets[(NCI_SHARE_OF_INCOME, *key)] += share
    plan.targets[(NONCONTROLLING_INTEREST, *key)] -= share
  for associate in associates:
    pickup = share_of(_net_income(rows, associate.id), associate.share)
    if not pickup:
      continue
    key = ("equity_method_pickup", associate.id, True)
    plan.targets[(EQUITY_METHOD_INVESTMENT, *key)] += pickup
    plan.targets[(EQUITY_METHOD_INCOME, *key)] -= pickup
    plan.basis[key].append(
      f"The parent's {associate.share:.0%} of {associate.name}'s result for the "
      f"month: {_money(pickup)}."
    )
  for associate in associates:
    # The equity method carries a distribution against the investment; a
    # parent at cost that took it as income has it reclassed there.
    key = ("equity_method_distribution", associate.id, True)
    received = 0
    for r in _distribution_income(rows, parent, associate):
      if r.in_period and r.amount:
        plan.targets[(r.concept.qname, *key)] -= r.amount
        received -= r.amount
    if received:
      plan.targets[(EQUITY_METHOD_INVESTMENT, *key)] -= received
      plan.basis[key].append(
        f"{parent.name} took {_money(received)} of {associate.name}'s "
        "distributions as income; on the equity method they reduce the "
        "investment."
      )


def _override_share(override: dict, income: int) -> int:
  """The outside owners' share an allocation override gives of ``income``."""
  if override.get("amount") is not None:
    return int(override["amount"])
  return share_of(income, float(override["percentage"]) / 100.0)


def _override_record(period: str, override: dict) -> AllocationOverrideRecord:
  return AllocationOverrideRecord(
    period=period,
    amount=override.get("amount"),
    percentage=override.get("percentage"),
    basis=override.get("basis"),
    set_by=override.get("set_by"),
  )


def allocation_share(
  session: Session, *, entity_id: str, period: str
) -> AllocationShareResponse | None:
  """The outside owners' share of a member's result for ``period``: the
  pro-rata figure, the override if one is set, and the share the
  consolidating entries use, rounded as the engine rounds them. None for an
  entity that is not a member consolidated in full that month, whose
  outside owners have no share for the engine to allocate. Raises
  ``EntityNotInGraphError`` for an entity that is not this graph's."""
  entity = resolve_entity(session, entity_id)
  start, end = month_bounds(period)
  member = GroupMember(
    str(entity.id), str(entity.entity_role), entity.joined_on, entity.left_on
  )
  if (
    is_group_parent(session, str(entity.id))
    or entity.entity_role != "operating"
    or entity.consolidation_method != "full"
    or not member.is_member_for(start, end)
  ):
    return None
  mapping = find_entity_mapping(session, str(entity.id), BOOK_FRAMEWORK)
  concepts = primary_concepts(session, [str(mapping.id)]) if mapping else {}
  rows = _rows(session, [str(entity.id)], concepts, start, end, drafts_of="")
  income = _net_income(rows, str(entity.id))
  # As _group and _plan_income_shares compute it, at the month's share.
  pct = pct_as_of(
    float(entity.ownership_pct) if entity.ownership_pct is not None else None,
    ownership_changes(session).get(str(entity.id), []),
    period,
  )
  share = pct / 100.0 if pct is not None else 1.0
  outside = max(1.0 - share, 0.0)
  pro_rata = share_of(income, outside)
  override = dict((entity.metadata_ or {}).get(ALLOCATION_OVERRIDES) or {}).get(period)
  consolidation = find_consolidation_entity(session)
  return AllocationShareResponse(
    entity_id=str(entity.id),
    entity_name=str(entity.name),
    period=period,
    consolidation_method=str(entity.consolidation_method),
    ownership_pct=pct,
    outside_pct=round(outside * 100.0, 6),
    month_result=income,
    pro_rata_amount=pro_rata,
    override=_override_record(period, override) if override else None,
    effective_amount=_override_share(override, income) if override else pro_rata,
    closed=bool(
      consolidation is not None
      and closed_periods(session, [end], entity_id=str(consolidation.id))
    ),
  )


def allocation_overrides(
  session: Session, *, entity_id: str
) -> AllocationOverrideListResponse:
  """Every month a member's allocation override is set for, oldest first."""
  entity = resolve_entity(session, entity_id)
  overrides = dict((entity.metadata_ or {}).get(ALLOCATION_OVERRIDES) or {})
  return AllocationOverrideListResponse(
    entity_id=str(entity.id),
    overrides=[
      _override_record(period, overrides[period]) for period in sorted(overrides)
    ],
  )


# ── The preview ─────────────────────────────────────────────────────────


_MEMOS = {
  "elimination_investment": "Eliminate the investment in {name}",
  "elimination_intercompany": "Eliminate intercompany balances — {name}",
  "nci_allocation": "Share of {name}'s result attributable to outside owners",
  "equity_method_pickup": "Share of {name}'s result — equity method",
  "equity_method_distribution": "Distributions from {name} — equity method",
}


def preview_consolidation_entries(
  session: Session, *, period: str
) -> PreviewConsolidationEntriesResponse:
  """The consolidating entries ``period`` (YYYY-MM) needs: for each kind and
  member, the month-end target less what the consolidation entity holds.

  Raises ``NoConsolidationEntityError`` when the group has none."""
  start, end = month_bounds(period)
  parent_id = resolve_entity_id(session, None)
  entities = _group(session, parent_id, period)
  by_id = {e.id: e for e in entities}
  # What the month opened needing is measured at the share the month before
  # held, so a change in it moves the eliminations within the month.
  opening_share = {
    e.id: e.share
    for e in _group(session, parent_id, f"{start - timedelta(days=1):%Y-%m}")
  }
  consolidation = next((e for e in entities if e.role == "consolidation"), None)
  if consolidation is None:
    raise NoConsolidationEntityError(
      "This group has no consolidation entity; create one with "
      "entity_role 'consolidation' for its consolidating entries."
    )
  if closed_periods(session, [end], entity_id=consolidation.id):
    return PreviewConsolidationEntriesResponse(
      period=period,
      period_start=start,
      period_end=end,
      consolidation_entity_id=consolidation.id,
      notes=[
        f"{consolidation.name} has closed {period}; its consolidating entries "
        "are as posted."
      ],
      up_to_date=True,
      closed=True,
    )
  parent = by_id[parent_id]
  operating = [e for e in entities if e.role != "consolidation"]
  in_month = [
    e for e in operating if e.id == parent_id or e.member.is_member_for(start, end)
  ]
  members = [e for e in in_month if e.id != parent_id and e.method == "full"]
  associates = [e for e in in_month if e.method == "equity"]
  consolidated = [parent, *members]

  plan = _Plan()
  mapping_ids: list[str] = []
  unmapped: set[str] = set()
  for entity in [*consolidated, *associates, consolidation]:
    mapping = find_entity_mapping(session, entity.id, BOOK_FRAMEWORK)
    if mapping is None:
      plan.notes.append(f"{entity.name} has no chart mapping; its books are not read.")
      plan.incomplete = True
      unmapped.add(entity.id)
    else:
      mapping_ids.append(str(mapping.id))
  concepts = primary_concepts(session, mapping_ids)
  rows = _rows(
    session,
    [e.id for e in [*consolidated, *associates, consolidation]],
    concepts,
    start,
    end,
    drafts_of=consolidation.id,
  )

  accounts = posting_accounts(session, consolidation.id, concepts)
  # The rows the consolidation entity's own statement presents; none is a
  # Style read by trait alone.
  taggable = taggable_flow_qnames(
    session, load_entity_reporting_style(session, consolidation.id)
  )
  untaggable: set[str] = set()

  acquired = acquisitions(session, consolidation.id, end)
  acquired_before = acquisitions(session, consolidation.id, start - timedelta(days=1))
  opened = _opened(session, [m.id for m in members])
  # Unread books would read as zero capital: a residual that misnames the cause.
  if parent.id not in unmapped:
    for member in members:
      if member.id not in unmapped:
        _plan_capital(
          plan,
          rows,
          parent,
          member,
          consolidation_id=consolidation.id,
          acquired=acquired,
          opened=opened,
        )
    _check_consideration(plan, rows, parent, members, acquired)
  _plan_intercompany(plan, rows, consolidated, set(accounts))
  _plan_income_shares(plan, rows, parent, members, associates, period)

  # What the consolidation entity already holds, per kind and member, and of
  # the balances what it held at the month's start.
  # On the consolidation entity's first month its opening is recomputed, so
  # the one recorded earlier is not held.
  eve = first_month_eve(session, consolidation.id, start)
  held: dict[tuple[str, str, str, bool], int] = defaultdict(int)
  held_before: dict[tuple[str, str, str, bool], int] = defaultdict(int)
  for r in rows:
    if (
      r.entity_id != consolidation.id
      or r.kind not in COMPUTED_KINDS
      or r.adjusts not in by_id
      or (eve is not None and r.opening)
    ):
      continue
    periodic = _is_periodic(r.kind, r.concept)
    if periodic and not r.in_period:
      continue
    held[(r.concept.qname, r.kind, r.adjusts, periodic)] += r.amount
    if not periodic and not r.in_period:
      held_before[(r.concept.qname, r.kind, r.adjusts, periodic)] += r.amount

  # The balances the month opened needing: what a delta holds beyond the
  # month's own movement is earlier months', caught up, and moved no cash
  # in this one.
  before = _Plan()
  earlier = [r for r in rows if not r.in_period]
  if parent.id not in unmapped:
    for member in members:
      if member.id not in unmapped:
        _plan_capital(
          before,
          earlier,
          parent,
          replace(member, share=opening_share.get(member.id, member.share)),
          consolidation_id=consolidation.id,
          acquired=acquired_before,
          opened=opened,
        )
  _plan_intercompany(before, earlier, consolidated, set(accounts))

  opening = (
    _opening(before, held_before, rows, set(accounts)) if eve is not None else {}
  )
  if eve is not None:
    _note_what_cannot_open(plan, before, held_before, opening, by_id, period)
  for (kind, member_id), lines in opening.items():
    for qname, amount in lines.items():
      held[(qname, kind, member_id, False)] += amount
      held_before[(qname, kind, member_id, False)] += amount

  deltas: dict[tuple[str, str, bool], dict[str, int]] = defaultdict(dict)
  for key in set(plan.targets) | set(held):
    qname, kind, member_id, periodic = key
    if (kind, member_id) in plan.withheld:
      continue
    delta = plan.targets.get(key, 0) - held.get(key, 0)
    if delta:
      deltas[(kind, member_id, periodic)][qname] = delta

  proposals: list[ConsolidationProposal] = [
    ConsolidationProposal(
      adjustment_kind=kind,
      adjusts_entity_id=member_id,
      adjusts_entity_name=by_id[member_id].name,
      posting_date=eve,
      memo=f"Opening balance — {_MEMOS[kind].format(name=by_id[member_id].name)}",
      basis=(
        f"What {by_id[member_id].name}'s history before {period} already "
        "needed when the group's first month opened; the consolidated "
        "statements open on it."
      ),
      lines=[
        ConsolidationProposalLine(
          element_id=accounts[qname][0],
          account_name=accounts[qname][1],
          concept=qname,
          debit_amount=max(amount, 0),
          credit_amount=max(-amount, 0),
        )
        for qname, amount in sorted(lines.items(), key=lambda kv: -kv[1])
      ],
      opening=True,
    )
    for (kind, member_id), lines in sorted(opening.items(), key=lambda kv: kv[0][::-1])
    if eve is not None
  ]
  for (kind, member_id, periodic), lines in sorted(
    deltas.items(), key=lambda item: (item[0][1], item[0][0], item[0][2])
  ):
    missing = sorted(q for q in lines if q not in accounts)
    if missing:
      plan.notes.append(
        f"{consolidation.name} has no account mapped to "
        + ", ".join(missing)
        + f"; the {kind} entry for {by_id[member_id].name} is left out."
      )
      plan.incomplete = True
      continue
    if sum(lines.values()):
      plan.notes.append(
        f"The {kind} entry for {by_id[member_id].name} does not balance against "
        "what is held; a hand entry of that kind may mix members."
      )
      plan.incomplete = True
      continue
    name = by_id[member_id].name
    memo = _MEMOS.get(kind, kind).format(name=name)
    if kind == "elimination_intercompany" and periodic:
      memo = f"Eliminate intercompany charges — {name}"
    basis = plan.basis.get((kind, member_id, periodic)) or [
      f"Reverses {kind} held for {name} that the books no longer support."
    ]
    # A month that opened on a withheld residual has no opening target to
    # measure its movement from, so none of it is tagged.
    carried = (
      lines
      if (kind, member_id) in before.withheld
      else {
        q: before.targets.get((q, kind, member_id, False), 0)
        - held_before.get((q, kind, member_id, False), 0)
        for q in lines
      }
    )
    tagged, flows = _tagged_part(
      lines,
      kind,
      periodic,
      carried=carried,
      taggable=taggable,
      untaggable=untaggable,
    )
    untagged = {q: a - tagged.get(q, 0) for q, a in lines.items()}
    untagged = {q: a for q, a in untagged.items() if a}
    parts = [
      (part, suffix, part_flows)
      for part, suffix, part_flows in (
        (tagged, " — cash flows" if untagged else "", flows),
        (untagged, "", {}),
      )
      if part
    ]
    for part, suffix, part_flows in parts:
      proposals.append(
        ConsolidationProposal(
          adjustment_kind=kind,
          adjusts_entity_id=member_id,
          adjusts_entity_name=name,
          posting_date=end,
          memo=f"{memo}{suffix} ({period})",
          basis=" ".join(basis),
          lines=[
            ConsolidationProposalLine(
              element_id=accounts[qname][0],
              account_name=accounts[qname][1],
              concept=qname,
              debit_amount=max(amount, 0),
              credit_amount=max(-amount, 0),
              flow_qname=part_flows.get(qname),
            )
            for qname, amount in sorted(part.items(), key=lambda kv: -kv[1])
          ],
        )
      )

  _hold_what_a_former_member_leaves(
    plan, rows, proposals, [e for e in operating if e not in in_month], consolidation
  )
  if untaggable:
    plan.notes.append(
      f"{consolidation.name}'s cash flow statement has no "
      + ", ".join(sorted(untaggable))
      + " row; the entries that would offset it are left untagged."
    )
  # Drafting again replaces the month's own drafts, so they are not counted as
  # held; the month is up to date when they hold exactly what is proposed.
  drafts = engine_drafts(session, consolidation.id, start, end)
  return PreviewConsolidationEntriesResponse(
    period=period,
    period_start=start,
    period_end=end,
    consolidation_entity_id=consolidation.id,
    proposals=proposals,
    residuals=plan.residuals,
    reconciling_items=plan.reconciling,
    notes=plan.notes,
    up_to_date=(
      not plan.residuals
      and not plan.incomplete
      and _drafted(session, drafts) == _proposed(proposals)
      and (eve is None or opening_is_recorded(session, consolidation.id, proposals))
    ),
    incomplete=plan.incomplete,
    drafted_entry_ids=[str(e.id) for e in drafts],
  )


def first_month_eve(
  session: Session, consolidation_id: str, start: date
) -> date | None:
  """The eve of ``start`` when it is the consolidation entity's first month,
  whose opening the engine writes: the first open month of a calendar never
  closed, or the month an engine opening already opens (reopened back to).
  None when a person recorded the opening."""
  hand = session.execute(
    select(Entry.id).where(
      Entry.entity_id == consolidation_id,
      Entry.type == OPENING,
      Entry.provenance.is_distinct_from(ENGINE_PROVENANCE),
      Entry.status != SUPERSEDED_ENTRY_STATUS,
    )
  ).first()
  if hand:
    return None
  eve = start - timedelta(days=1)
  recorded = engine_opening(session, consolidation_id)
  if recorded is not None:
    return eve if recorded.posting_date == eve else None
  calendar = session.execute(
    text(
      "SELECT closed_through_period, last_close_at FROM fiscal_calendar "
      "WHERE entity_id = :e"
    ),
    {"e": consolidation_id},
  ).first()
  if calendar is None or calendar.last_close_at is not None:
    return None
  if calendar.closed_through_period:
    first = next_period(calendar.closed_through_period)
  else:
    first = session.execute(
      text("SELECT min(name) FROM fiscal_periods WHERE entity_id = :e"),
      {"e": consolidation_id},
    ).scalar()
  return eve if first == f"{start:%Y-%m}" else None


def engine_opening(session: Session, consolidation_id: str) -> Entry | None:
  return session.execute(
    select(Entry).where(
      Entry.entity_id == consolidation_id,
      Entry.type == OPENING,
      Entry.provenance == ENGINE_PROVENANCE,
      Entry.status != SUPERSEDED_ENTRY_STATUS,
    )
  ).scalar_one_or_none()


def opening_is_recorded(
  session: Session, consolidation_id: str, proposals: list[ConsolidationProposal]
) -> bool:
  """Whether the engine's recorded opening holds exactly the opening
  proposals, both empty included."""
  wanted: dict[_DraftKey, int] = defaultdict(int)
  for p in proposals:
    if p.opening:
      for line in p.lines:
        key = (p.adjustment_kind, p.adjusts_entity_id, line.element_id, None)
        wanted[key] += line.debit_amount - line.credit_amount
  entry = engine_opening(session, consolidation_id)
  recorded: dict[_DraftKey, int] = defaultdict(int)
  if entry is not None:
    for line in session.execute(
      select(LineItem).where(LineItem.entry_id == entry.id)
    ).scalars():
      meta = line.metadata_ or {}
      key = (
        meta.get("adjustment_kind"),
        meta.get("adjusts_entity_id"),
        str(line.element_id),
        None,
      )
      recorded[key] += int(line.debit_amount) - int(line.credit_amount)
  return {k: v for k, v in wanted.items() if v} == {
    k: v for k, v in recorded.items() if v
  }


def _opening(
  before: _Plan,
  held_before: dict[tuple[str, str, str, bool], int],
  rows: list[_Row],
  postable: set[str],
) -> dict[tuple[str, str], dict[str, int]]:
  """What the first month opened needing, per kind and member: the balances
  the members' earlier history already called for. A part the consolidation
  entity cannot post, that does not balance, or that reverses income (which
  an opening balance sheet cannot hold) is left to the month's entry."""
  classification = {r.concept.qname: r.concept.classification for r in rows}
  parts: dict[tuple[str, str], dict[str, int]] = defaultdict(dict)
  for (qname, kind, member_id, periodic), target in before.targets.items():
    if periodic or (kind, member_id) in before.withheld:
      continue
    amount = target - held_before.get((qname, kind, member_id, False), 0)
    if amount:
      parts[(kind, member_id)][qname] = amount
  return {
    key: lines
    for key, lines in parts.items()
    if not sum(lines.values())
    and all(q in postable and classification.get(q) not in _RESULT for q in lines)
  }


def _note_what_cannot_open(
  plan: _Plan,
  before: _Plan,
  held_before: dict[tuple[str, str, str, bool], int],
  opening: dict[tuple[str, str], dict[str, int]],
  by_id: dict[str, _Entity],
  period: str,
) -> None:
  """Name the earlier history the first month needs but cannot open on (a
  pair withheld at the eve, income taken before the books, a concept the
  consolidation entity cannot post): it is caught up within the month, where
  the consolidated equity statement may not roll forward over it."""
  needed = {
    (kind, member_id)
    for (qname, kind, member_id, periodic), target in before.targets.items()
    if not periodic and target != held_before.get((qname, kind, member_id, False), 0)
  } | {(kind, member_id) for kind, member_id in before.withheld}
  for kind, member_id in sorted(needed - set(opening)):
    if member_id in by_id:
      plan.notes.append(
        f"{by_id[member_id].name}'s {kind} from before {period} cannot be "
        "the consolidation entity's opening, so it is caught up within the "
        "month; the consolidated equity statement may not roll forward over it."
      )


def held_for_member(
  rows: list[_Row], consolidation_id: str, member_id: str, *, beyond_engine: bool
) -> dict[str, int]:
  """What the consolidation entity holds for ``member_id`` on its
  balance-sheet accounts and as the outside owners' interest, debit-positive
  by concept: every consolidating entry's, or with ``beyond_engine`` only
  what the engine does not reverse on its own once the member is out — the
  kinds it computes a cumulative target for (the investment and intercompany
  eliminations) go back the month after; an acquisition's goodwill and
  step-ups, the price above book of each change in the share, and the
  outside owners' share of the member's results wait for the
  deconsolidation."""
  held: dict[str, int] = defaultdict(int)
  for r in rows:
    if r.entity_id != consolidation_id or r.adjusts != member_id or r.closing:
      continue
    if beyond_engine and (
      not r.kind or (r.kind in COMPUTED_KINDS and not _is_periodic(r.kind, r.concept))
    ):
      continue
    if (
      r.concept.classification in _BALANCE or r.concept.qname == NONCONTROLLING_INTEREST
    ):
      held[r.concept.qname] += r.amount
  return dict(held)


def _hold_what_a_former_member_leaves(
  plan: _Plan,
  rows: list[_Row],
  proposals: list[ConsolidationProposal],
  outside: list[_Entity],
  consolidation: _Entity,
) -> None:
  """A member out of the month's group needs nothing from the engine, which
  reverses its cumulative eliminations; what the months' own entries built
  up (the outside owners' share of its results, an associate's pickups)
  stays on the balance sheet until a person posts the deconsolidation, so
  the month is left incomplete until no asset, liability or outside owners'
  share is held for it. The group's own equity is where that entry lands."""
  for entity in outside:
    left: dict[str, int] = defaultdict(
      int, held_for_member(rows, consolidation.id, entity.id, beyond_engine=False)
    )
    for p in proposals:
      if p.adjusts_entity_id == entity.id:
        for line in p.lines:
          if line.concept in left:
            left[line.concept] += line.debit_amount - line.credit_amount
    held = {q: a for q, a in left.items() if a}
    if not held:
      continue
    plan.notes.append(
      f"{entity.name} is no longer in the group, and {consolidation.name} "
      "still holds "
      + ", ".join(
        f"{q.split(':')[-1]} {_money(abs(a))} {'debit' if a > 0 else 'credit'}"
        for q, a in sorted(held.items())
      )
      + " for it beyond the eliminations proposed here; record-deconsolidation "
      "derecognizes them with the gain or loss on its leaving."
    )
    plan.incomplete = True


@dataclass
class GroupCloseStatus:
  """What holds the group's month open, each a list of named items: the
  entities not closed through it, the consolidating entries the
  consolidation entity's drafts and postings do not hold, the residuals and
  intercompany disagreements nobody has explained, and the consolidated
  statements' validation failures."""

  open_entities: list[str] = field(default_factory=list)
  pending_adjustments: list[str] = field(default_factory=list)
  unexplained: list[str] = field(default_factory=list)
  not_footing: list[str] = field(default_factory=list)


def group_close_status(
  session: Session, graph_id: str, period: str
) -> GroupCloseStatus:
  """The group close's checks for ``period`` (§4.5): every entity the month
  consolidates, the parent and its equity-method members included, closed
  through it; the month's consolidating entries posted or drafted; no
  residual or disagreement left; the consolidated balance sheet and equity
  statement passing their checks. Those read what has landed, without the
  month's drafts; the close checks them again once it has posted them."""
  start, end = month_bounds(period)
  parent_id = resolve_entity_id(session, None)
  status = GroupCloseStatus()
  members = [
    e
    for e in _group(session, parent_id)
    if e.role != "consolidation"
    and (
      e.id == parent_id
      or (e.method in ("full", "equity") and e.member.is_member_for(start, end))
    )
  ]
  closed_through: dict[str, str] = {
    str(row.entity_id): str(row.closed_through_period)
    for row in session.execute(
      text(
        "SELECT entity_id, closed_through_period FROM fiscal_calendar "
        "WHERE entity_id IN :ids AND closed_through_period IS NOT NULL"
      ).bindparams(bindparam("ids", expanding=True)),
      {"ids": [e.id for e in members]},
    )
  }
  status.open_entities = [
    e.name for e in members if closed_through.get(e.id, "") < period
  ]

  preview = preview_consolidation_entries(session, period=period)
  status.unexplained = [
    f"{r.entity_name}: {r.explanation}" for r in preview.residuals
  ] + [item.description for item in preview.reconciling_items]
  # Apart from the residuals: overriding them must not let the other
  # members' entries go undrafted.
  drafts = engine_drafts(session, preview.consolidation_entity_id, start, end)
  if _drafted(session, drafts) != _proposed(preview.proposals):
    status.pending_adjustments = [
      f"{p.adjustment_kind} for {p.adjusts_entity_name}"
      for p in preview.proposals
      if not p.opening
    ] or ["drafts that no longer match the books"]
  if first_month_eve(
    session, preview.consolidation_entity_id, start
  ) is not None and not opening_is_recorded(
    session, preview.consolidation_entity_id, preview.proposals
  ):
    status.pending_adjustments.append(
      "the consolidation entity's opening balances, which propose records"
    )
  # What the engine left out holds the close whatever the residuals: the
  # residual override must not waive an entry nobody computed.
  if preview.incomplete:
    status.pending_adjustments += preview.notes

  status.not_footing = consolidated_statement_failures(session, graph_id, period)
  return status


def consolidated_statement_failures(
  session: Session, graph_id: str, period: str
) -> list[str]:
  """The consolidated balance sheet's and equity statement's validation
  failures for ``period``, over what has landed."""
  from robosystems.operations.roboledger.reads.reports import (
    CoaMappingNotFoundError,
    get_live_financial_statement,
  )

  start, end = month_bounds(period)
  failures: list[str] = []
  for statement_type in ("balance_sheet", "equity_statement"):
    try:
      statement = get_live_financial_statement(
        session,
        graph_id=graph_id,
        statement_type=statement_type,
        period_start=start,
        period_end=end,
        entity_id=resolve_entity_id(session, None),
        scope="consolidated",
      )
    except CoaMappingNotFoundError as exc:
      failures.append(f"The consolidated {statement_type} cannot be read: {exc}")
      continue
    except Exception as exc:
      # A calendar read must not fail on it; the close still waits.
      logger.exception("group close: consolidated %s unreadable", statement_type)
      failures.append(
        f"The consolidated {statement_type} could not be read ({type(exc).__name__})"
      )
      continue
    if statement.validation is not None and statement.validation.status == "failed":
      # A check that could not run (a chart with no rollup for a class) is
      # not a disagreement; the close waits only on numbers that do not tie.
      failures.extend(
        f for f in statement.validation.failures if "validation inconclusive" not in f
      )
  return failures
