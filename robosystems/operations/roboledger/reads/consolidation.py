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
from dataclasses import dataclass, field
from datetime import date

from sqlalchemy import bindparam, select, text
from sqlalchemy.orm import Session

from robosystems.models.api.extensions.consolidation import (
  ConsolidationProposal,
  ConsolidationProposalLine,
  ConsolidationReconcilingItem,
  ConsolidationResidual,
  PreviewConsolidationEntriesResponse,
)
from robosystems.models.extensions.roboledger import Entry
from robosystems.operations.roboledger.commands._guards import closed_periods
from robosystems.operations.roboledger.entity_scope import (
  GroupMember,
  resolve_entity_id,
)
from robosystems.operations.roboledger.entry_status import landed_entry_bindparam
from robosystems.operations.roboledger.reports.fact_grid import (
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

_EQUITY = frozenset(
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
    for line in p.lines:
      key = (p.adjustment_kind, p.adjusts_entity_id, line.element_id, line.flow_qname)
      held[key] += line.debit_amount - line.credit_amount
  return {k: v for k, v in held.items() if v}


def _flow(qname: str, kind: str, amount: int) -> str | None:
  flows = _FLOWS.get((qname, kind))
  if flows is None:
    return None
  return flows[0] if amount > 0 else flows[1]


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


def _share(cents: int, share: float) -> int:
  return round(cents * share)


def _money(cents: int) -> str:
  return f"{cents / 100:,.2f}"


# ── Loading ─────────────────────────────────────────────────────────────


def _group(session: Session, parent_id: str) -> list[_Entity]:
  rows = session.execute(
    text(
      "SELECT id, name, entity_role, consolidation_method, ownership_pct, "
      "       joined_on, left_on, metadata FROM entities "
      "WHERE source <> 'linked' "
      "ORDER BY (id <> :parent), created_at ASC, id ASC"
    ),
    {"parent": parent_id},
  ).all()
  return [
    _Entity(
      id=str(row.id),
      name=str(row.name),
      role=str(row.entity_role),
      method="full" if row.id == parent_id else str(row.consolidation_method),
      share=(float(row.ownership_pct) / 100.0)
      if row.ownership_pct is not None
      else 1.0,
      member=GroupMember(
        str(row.id),
        str(row.entity_role),
        None if row.id == parent_id else row.joined_on,
        None if row.id == parent_id else row.left_on,
      ),
      overrides=dict((row.metadata or {}).get(ALLOCATION_OVERRIDES) or {}),
    )
    for row in rows
  ]


def _primary_concepts(session: Session, mapping_ids: list[str]) -> dict[str, _Concept]:
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
  account, counterparty, kind and whether it falls in the month. The
  consolidation entity's (``drafts_of``) drafts count as well, since its
  close will post them, but not the engine's own drafts of this month,
  which the proposal replaces: so two open months never eliminate twice."""
  result = session.execute(
    text(
      """
      SELECT e.entity_id, li.element_id, src.name AS account_name,
             ag.external_id AS counterparty,
             (e.type = 'closing') AS closing,
             e.adjustment_kind, e.adjusts_entity_id,
             (e.posting_date >= :start) AS in_period,
             SUM(li.debit_amount - li.credit_amount) AS amount
      FROM line_items li
      JOIN entries e ON e.id = li.entry_id
      JOIN elements src ON src.id = li.element_id
      LEFT JOIN events ev ON ev.id = e.triggered_by_event_id
      LEFT JOIN agents ag ON ag.id = ev.agent_id AND ag.agent_type = 'self'
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
      GROUP BY e.entity_id, li.element_id, src.name, ag.external_id,
               (e.type = 'closing'), e.adjustment_kind, e.adjusts_entity_id,
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
      )
    )
  return rows


def _posting_accounts(
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


def _plan_capital(
  plan: _Plan, rows: list[_Row], parent: _Entity, member: _Entity
) -> None:
  """The parent's investment against the member's capital, and the outside
  owners' share of that capital as noncontrolling interest."""
  key = ("elimination_investment", member.id, False)
  capital_by_concept: dict[str, int] = defaultdict(int)
  for r in rows:
    if (
      r.entity_id == member.id
      and not r.closing
      and r.concept.classification in _EQUITY
      and r.concept.qname != NONCONTROLLING_INTEREST
    ):
      capital_by_concept[r.concept.qname] += r.amount
  capital = -sum(capital_by_concept.values())
  parent_part = _share(capital, member.share)
  outside_part = capital - parent_part

  invested_lines = [
    r
    for r in rows
    if r.entity_id == parent.id
    and r.counterparty == member.id
    and r.concept.qname == INVESTMENT_IN_SUBSIDIARIES
  ]
  invested = sum(r.amount for r in invested_lines)
  residual = invested - parent_part
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
  income one earns from another against the other's expense."""
  ids = [e.id for e in consolidated]
  parent_id = ids[0]
  names = {e.id: e.name for e in consolidated}
  for i, a in enumerate(ids):
    for b in ids[i + 1 :]:
      member = b if a == parent_id else a
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
          if q.qname not in postable or q.qname in _NOT_INTERCOMPANY:
            continue
          if periodic:
            if not r.in_period or q.classification not in _RESULT:
              continue
          elif q.classification not in _BALANCE:
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
      if override.get("amount") is not None:
        share = int(override["amount"])
        how = _money(share)
      else:
        pct = float(override["percentage"])
        share = _share(income, pct / 100.0)
        how = f"{pct:g}% of {_money(income)}, {_money(share)}"
      plan.basis[key].append(
        f"The outside owners' share of {member.name}'s result for the month, "
        f"by an allocation override: {how} (pro rata would be "
        f"{_money(_share(income, max(outside, 0.0)))}). "
        f"{override.get('basis') or ''}".rstrip()
      )
    elif outside <= 0:
      continue
    else:
      share = _share(_net_income(rows, member.id), outside)
      plan.basis[key].append(
        f"The outside owners' {outside:.0%} of {member.name}'s result for the "
        f"month: {_money(share)}."
      )
    if not share:
      continue
    plan.targets[(NCI_SHARE_OF_INCOME, *key)] += share
    plan.targets[(NONCONTROLLING_INTEREST, *key)] -= share
  for associate in associates:
    pickup = _share(_net_income(rows, associate.id), associate.share)
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
  entities = _group(session, parent_id)
  by_id = {e.id: e for e in entities}
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
  concepts = _primary_concepts(session, mapping_ids)
  rows = _rows(
    session,
    [e.id for e in [*consolidated, *associates, consolidation]],
    concepts,
    start,
    end,
    drafts_of=consolidation.id,
  )

  accounts = _posting_accounts(session, consolidation.id, concepts)
  # The rows the consolidation entity's own statement presents; none is a
  # Style read by trait alone.
  taggable = taggable_flow_qnames(
    session, load_entity_reporting_style(session, consolidation.id)
  )
  untaggable: set[str] = set()

  # Unread books would read as zero capital: a residual that misnames the cause.
  if parent.id not in unmapped:
    for member in members:
      if member.id not in unmapped:
        _plan_capital(plan, rows, parent, member)
  _plan_intercompany(plan, rows, consolidated, set(accounts))
  _plan_income_shares(plan, rows, parent, members, associates, period)

  # What the consolidation entity already holds, per kind and member.
  held: dict[tuple[str, str, str, bool], int] = defaultdict(int)
  for r in rows:
    if (
      r.entity_id != consolidation.id
      or r.kind not in COMPUTED_KINDS
      or r.adjusts not in by_id
    ):
      continue
    periodic = _is_periodic(r.kind, r.concept)
    if periodic and not r.in_period:
      continue
    held[(r.concept.qname, r.kind, r.adjusts, periodic)] += r.amount

  deltas: dict[tuple[str, str, bool], dict[str, int]] = defaultdict(dict)
  for key in set(plan.targets) | set(held):
    qname, kind, member_id, periodic = key
    if (kind, member_id) in plan.withheld:
      continue
    delta = plan.targets.get(key, 0) - held.get(key, 0)
    if delta:
      deltas[(kind, member_id, periodic)][qname] = delta

  proposals: list[ConsolidationProposal] = []
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
    flows = {q: None if periodic else _flow(q, kind, a) for q, a in lines.items()}
    for q, flow in flows.items():
      if flow and taggable and flow not in taggable:
        untaggable.add(flow)
        flows[q] = None
    tagged = {q: a for q, a in lines.items() if flows[q]}
    untagged = {q: a for q, a in lines.items() if not flows[q]}
    # Tagged and untagged lines post apart when each part balances, so each
    # entry reads as one movement.
    if tagged and untagged and not sum(tagged.values()):
      parts = [(tagged, " — cash flows"), (untagged, "")]
    else:
      parts = [(lines, "")]
    for part, suffix in parts:
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
              flow_qname=flows[qname],
            )
            for qname, amount in sorted(part.items(), key=lambda kv: -kv[1])
          ],
        )
      )

  if untaggable:
    plan.notes.append(
      f"{consolidation.name}'s cash flow statement has no "
      + ", ".join(sorted(untaggable))
      + " row; the lines that would offset it are left untagged."
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
    ),
    drafted_entry_ids=[str(e.id) for e in drafts],
  )


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
  statement passing their checks."""
  from robosystems.operations.roboledger.reads.reports import (
    CoaMappingNotFoundError,
    get_live_financial_statement,
  )

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
  if not preview.residuals and not preview.up_to_date:
    drafts = engine_drafts(session, preview.consolidation_entity_id, start, end)
    if _drafted(session, drafts) != _proposed(preview.proposals):
      status.pending_adjustments = [
        f"{p.adjustment_kind} for {p.adjusts_entity_name}" for p in preview.proposals
      ] or ["drafts that no longer match the books"]
    else:
      # Held and drafted agree; what is missing is what the engine left out.
      status.pending_adjustments = list(preview.notes)

  for statement_type in ("balance_sheet", "equity_statement"):
    try:
      statement = get_live_financial_statement(
        session,
        graph_id=graph_id,
        statement_type=statement_type,
        period_start=start,
        period_end=end,
        entity_id=parent_id,
        scope="consolidated",
      )
    except CoaMappingNotFoundError as exc:
      status.not_footing.append(
        f"The consolidated {statement_type} cannot be read: {exc}"
      )
      continue
    if statement.validation is not None and statement.validation.status == "failed":
      # A check that could not run (a chart with no rollup for a class) is
      # not a disagreement; the close waits only on numbers that do not tie.
      status.not_footing.extend(
        f for f in statement.validation.failures if "validation inconclusive" not in f
      )
  return status
