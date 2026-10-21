"""Statement FactSet production, shared by close and publication.

Statement FactSets (``factset_type='report'``) have two producers:

- **Close (canonical).** Closing a period stamps the month's sets with
  ``report_id NULL``; reclose replaces them by period window and reopen
  retracts them. Readers bind these by ``(structure_id, factset_type,
  period)``.
- **Publication (snapshot).** ``create_report`` mints ``report_id``-owned
  sets frozen at generation time; report deletes touch only those.

Import-cycle rule: import nothing from ``operations/roboledger/commands/*``
except the leaf ``_guards`` module.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import text

from robosystems.logger import logger
from robosystems.models.api.fact_provenance import FactSource, PivotProvenance
from robosystems.models.extensions import Fact
from robosystems.operations.information_block.envelope import DISCLOSURE_BLOCK_TYPE
from robosystems.operations.information_block.rules.engine import (
  evaluate_rules_for_structure,
)
from robosystems.operations.roboledger.commands._guards import (
  rule_summary as _rule_summary,
)
from robosystems.operations.roboledger.entity_scope import (
  GroupMember,
  NoEntityError,
  group_members,
  resolve_entity_id,
)
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.reports.calc_dag import (
  load_rs_gaap_calculations,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec,
  ReportFact,
  ReportFacts,
  generate_report_facts,
)
from robosystems.operations.roboledger.reports.network_picker import (
  NoNetworkForStatementTypeError,
  get_render_network,
  load_close_target_concept,
  load_entity_reporting_style,
  network_structure_ids,
)
from robosystems.operations.taxonomy_block.coa_mappings import (
  BOOK_FRAMEWORK,
  find_entity_mapping,
)
from robosystems.utils.ulid import generate_prefixed_ulid

if TYPE_CHECKING:
  from datetime import date

  from sqlalchemy.orm import Session


class StatementStampError(Exception):
  """Raised when a reporting-configured tenant's close-time stamp fails.

  Unlike the soft-skip for a tenant without reporting set up, the close must
  roll back.
  """


@dataclass
class StatementStampResult:
  """Outcome of :func:`stamp_canonical_statement_sets`."""

  stamped: bool
  # Soft-skip reason when ``stamped`` is False:
  # 'no_coa_mapping' | 'no_entity' | 'no_statement_structures' | 'no_taxonomy'
  note: str | None = None
  # structure_id -> fact_set_id for every minted canonical set.
  fact_set_ids: dict[str, str] = field(default_factory=dict)
  # None when no statement rules exist or evaluation errored (non-fatal).
  rule_summary: dict[str, int] | None = None


def _evaluate_report_structures(
  session: Session,
  facts,
  element_to_structures: dict[str, list[str]],
  structure_to_factset: dict[str, str],
  period_start,
  period_end,
  created_by: str,
) -> dict[str, int] | None:
  """Evaluate rules for every structure that received facts.

  Returns the aggregated rule_summary, or None if no structure has rules.
  """
  structures_with_facts: set[str] = set()
  for f in facts.facts:
    for sid in element_to_structures.get(f.element_id, ()):
      structures_with_facts.add(sid)
  # Invariant across structures; loaded once.
  global_calculations = load_rs_gaap_calculations(session)
  all_results = []
  for structure_id in structures_with_facts:
    results = evaluate_rules_for_structure(
      session,
      structure_id,
      fact_set_id=structure_to_factset[structure_id],
      period_start=period_start,
      period_end=period_end,
      created_by=created_by,
      global_calculations=global_calculations,
    )
    all_results.extend(results)
  return _rule_summary(all_results)


_RENDER_TARGET_STATEMENT_TYPES: tuple[str, ...] = (
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
)


# The report's taxonomy plus every extension descending from it, so a report
# never pulls in another framework's notes.
_TAXONOMY_SCOPE_CTE = """
      WITH RECURSIVE scoped AS (
        SELECT id FROM taxonomies WHERE id = :taxonomy_id
        UNION ALL
        SELECT t.id FROM taxonomies t JOIN scoped sc
          ON t.parent_taxonomy_id = sc.id
      )
"""


def _pick_disclosure_structures(
  session: Session,
  fact_element_ids: set[str],
  taxonomy_id: str,
) -> list[str]:
  """Disclosure structures whose MEMBER concepts actually received facts.

  Reporting Styles compose statements only; a disclosure renders because the
  ledger produced values for its concepts. Only the member (``to``) end of a
  presentation arc counts: a note's total is usually a common leaf that
  almost always has a fact, which would pick notes with empty breakdowns.
  Scoped to the report's taxonomy closure.
  """
  if not fact_element_ids:
    return []
  rows = session.execute(
    text(
      _TAXONOMY_SCOPE_CTE
      + """
      SELECT DISTINCT a.structure_id AS structure_id
      FROM associations a
      JOIN structures s ON s.id = a.structure_id
      WHERE s.block_type = :disclosure_block_type
        AND s.is_active IS TRUE
        AND s.taxonomy_id IN (SELECT id FROM scoped)
        AND a.association_type = 'presentation'
        AND a.to_element_id = ANY(:element_ids)
      """
    ),
    {
      "disclosure_block_type": DISCLOSURE_BLOCK_TYPE,
      "element_ids": list(fact_element_ids),
      "taxonomy_id": taxonomy_id,
    },
  ).fetchall()
  return [row.structure_id for row in rows]


def _build_structure_mapping(
  session: Session,
  reporting_style_id: str,
  fact_element_ids: set[str] | None = None,
  taxonomy_id: str | None = None,
) -> tuple[dict[str, list[str]], dict[str, str]]:
  """Return (element_id→[structure_ids], structure_id→new fact_set_id).

  Covers one Network per statement type the Reporting Style composes, plus
  the disclosures picked from ``fact_element_ids`` when ``taxonomy_id`` is
  given. An element in several structures (``NetIncomeLoss`` in IS and CF)
  maps to all of them, so each block resolves its calc rollups locally.
  """
  element_to_structures: dict[str, list[str]] = {}
  structure_to_factset: dict[str, str] = {}
  picked_structure_ids: list[str] = []
  # A tenant extension's arcs read as its network's: its rows' facts land in
  # the statement's fact set, where the statement renders from.
  owner_of: dict[str, str] = {}

  for statement_type in _RENDER_TARGET_STATEMENT_TYPES:
    try:
      network = get_render_network(session, reporting_style_id, statement_type)
    except NoNetworkForStatementTypeError:
      # Optional for this Style; change-reporting-style validates required ones.
      continue
    picked_structure_ids.append(network.structure_id)
    for sid in network_structure_ids(session, network.structure_id):
      owner_of[sid] = network.structure_id

  if taxonomy_id is not None:
    for disclosure_id in _pick_disclosure_structures(
      session, fact_element_ids or set(), taxonomy_id
    ):
      if disclosure_id not in picked_structure_ids:
        picked_structure_ids.append(disclosure_id)

  if not picked_structure_ids:
    return {}, {}

  # Both arc endpoints: a top-of-tree subtotal (``rs-gaap:Assets``) appears
  # only as a parent, and the balance-identity rule needs its fact.
  rows = session.execute(
    text(
      """
      SELECT DISTINCT a.structure_id AS structure_id, a.to_element_id AS element_id
      FROM associations a
      WHERE a.structure_id = ANY(:struct_ids) AND a.to_element_id IS NOT NULL
      UNION
      SELECT DISTINCT a.structure_id AS structure_id, a.from_element_id AS element_id
      FROM associations a
      WHERE a.structure_id = ANY(:struct_ids) AND a.from_element_id IS NOT NULL
      """
    ),
    {
      "struct_ids": [
        *picked_structure_ids,
        *(set(owner_of) - set(picked_structure_ids)),
      ]
    },
  ).fetchall()

  for row in rows:
    owner = owner_of.get(row.structure_id, row.structure_id)
    owners = element_to_structures.setdefault(row.element_id, [])
    if owner not in owners:
      owners.append(owner)
  structure_to_factset = {
    sid: generate_prefixed_ulid("fs") for sid in picked_structure_ids
  }
  return element_to_structures, structure_to_factset


def _pre_create_report_fact_sets(
  session: Session,
  report_id: str | None,
  entity_id: str,
  created_by: str,
  periods,
  structure_to_factset: dict[str, str],
  mapping_id: str,
) -> None:
  """Insert one FactSet per picked structure, before facts are stamped.

  The period envelope is the span of ``periods``. ``report_id=None`` mints
  canonical close-time sets. Every structure gets a set even if no fact will
  land in it; consumers see a zero fact_count.
  """
  if not periods:
    return

  starts = [p.start for p in periods if getattr(p, "start", None) is not None]
  envelope_start = min(starts) if starts else None
  envelope_end = max(p.end for p in periods)

  # An instant envelope carries the end date alone, not ``/end``.
  period_key = (
    f"{envelope_start}/{envelope_end}" if envelope_start else str(envelope_end)
  )
  for structure_id, fact_set_id in structure_to_factset.items():
    create_fact_set(
      session,
      id=fact_set_id,
      structure_id=structure_id,
      period_start=envelope_start,
      period_end=envelope_end,
      factset_type="report",
      entity_id=entity_id,
      report_id=report_id,
      created_by=created_by,
      provenance=PivotProvenance(
        mapping_id=mapping_id, period=period_key, stamped_at=datetime.now(UTC)
      ),
    )
  # The fact INSERTs reference these rows; flush order is not FK-aware here.
  session.flush()


def to_cents_precision(value: float) -> float:
  """A dollar amount rounded to cents — the ledger's own precision."""
  return round(value, 2)


# XBRL @decimals for a value rounded to cents: what the stored value claims.
CENTS_DECIMALS = "2"


def _stamp_facts_into_sets(
  session: Session,
  facts,
  entity_id: str,
  element_to_structures: dict[str, list[str]],
  structure_to_factset: dict[str, str],
  *,
  mapping_id: str | None = None,
  frozen_at: datetime | None = None,
  freeze_through: date | None = None,
) -> None:
  """Persist generated report facts into their pre-created FactSets.

  One Fact row per owning structure; facts no picked structure reaches are
  skipped. Values are dollars rounded to cents: the pivot adds in float and
  ``facts.value`` is a double, so this strips float noise the ledger (integer
  cents) never had — and ``decimals`` says so, so the graph and every export
  state the precision the value has.

  Facts sharing an element and period persist as their sum, the way every
  reader of generated facts already combines them: the cash-flow plug can
  land on a leaf the ΔBS derivation filled, and two values in one context
  would be an inconsistent duplicate in each export.

  Each row carries its own descriptor when it is a pivot that foots to its
  accounts: ``PivotProvenance`` with ``sources``, keyed by the entity whose
  pivot summed each account. Any other construction — a subtotal, a
  derivation, a leaf a derivation added into — is left NULL, unknown, until
  it is stamped honestly. A row whose column has closed (``period_end`` on
  or before ``freeze_through``) is written frozen at ``frozen_at``.
  """
  totals: dict[tuple, float] = {}
  # The accounts summed into each key, or None once a value without any
  # joined it: a pivot is a pivot only while every contributor was one.
  sources: dict[tuple, dict[tuple[str, str], float] | None] = {}
  for fact in facts.facts:
    key = (fact.element_id, fact.period_start, fact.period_end, fact.period_type)
    totals[key] = totals.get(key, 0.0) + fact.value
    if sources.get(key, {}) is None:
      continue
    if fact.source_values is None:
      sources[key] = None
      continue
    cell = sources.get(key)
    if cell is None:
      cell = sources[key] = {}
    owners = fact.source_entities or {}
    for account_id, amount in fact.source_values.items():
      owner = (owners.get(account_id, entity_id), account_id)
      cell[owner] = cell.get(owner, 0.0) + amount

  stamped_at = datetime.now(UTC)
  for key, value in totals.items():
    element_id, period_start, period_end, period_type = key
    provenance = _pivot_descriptor(
      sources.get(key),
      value,
      mapping_id,
      period_start,
      period_end,
      period_type,
      stamped_at,
    )
    frozen = (
      frozen_at if freeze_through is not None and period_end <= freeze_through else None
    )
    for structure_id in element_to_structures.get(element_id, ()):
      fact_set_id = structure_to_factset.get(structure_id)
      if fact_set_id is None:
        continue
      rf = Fact(
        element_id=element_id,
        value=to_cents_precision(value),
        decimals=CENTS_DECIMALS,
        period_start=period_start,
        period_end=period_end,
        period_type=period_type,
        unit="USD",
        entity_id=entity_id,
        structure_id=structure_id,
        fact_set_id=fact_set_id,
        provenance=provenance,
        frozen_at=frozen,
      )
      session.add(rf)

  # autoflush=False: without this the first structure evaluated binds
  # against unflushed facts and skips every rule.
  session.flush()


def _pivot_descriptor(
  cell: dict[tuple[str, str], float] | None,
  value: float,
  mapping_id: str | None,
  period_start: date | None,
  period_end: date,
  period_type: str,
  stamped_at: datetime,
) -> dict | None:
  """A fact's own ``pivot`` descriptor, when its accounts foot to its value;
  None — unknown — for a subtotal, a derivation, or a leaf a derivation
  added into (the retained-earnings close, the cash-flow plug)."""
  if cell is None or mapping_id is None:
    return None
  if abs(sum(cell.values()) - value) > 0.005:
    return None
  listed = [
    FactSource(entity_id=owner, account_id=account_id, value=round(amount, 2))
    for (owner, account_id), amount in sorted(
      cell.items(), key=lambda item: -abs(item[1])
    )
    if amount != 0.0
  ]
  period = (
    str(period_end)
    if period_type == "instant" or period_start is None
    else f"{period_start}/{period_end}"
  )
  return PivotProvenance(
    mapping_id=mapping_id, period=period, stamped_at=stamped_at, sources=listed
  ).model_dump(mode="json")


def _persist_report_facts(
  session: Session,
  report_id: str,
  facts,
  entity_id: str,
  element_to_structures: dict[str, list[str]],
  structure_to_factset: dict[str, str],
  *,
  mapping_id: str | None = None,
  frozen_at: datetime | None = None,
  freeze_through: date | None = None,
) -> None:
  """Replace this report's facts; canonical (report_id NULL) sets are
  untouched. Columns closed through ``freeze_through`` are written frozen."""
  session.execute(
    text(
      "DELETE FROM facts WHERE fact_set_id IN "
      "(SELECT id FROM fact_sets WHERE report_id = :report_id)"
    ),
    {"report_id": report_id},
  )
  _stamp_facts_into_sets(
    session,
    facts,
    entity_id,
    element_to_structures,
    structure_to_factset,
    mapping_id=mapping_id,
    frozen_at=frozen_at,
    freeze_through=freeze_through,
  )


def _group_parent_of(session: Session, entity_id: str | None) -> str | None:
  """The group parent, when ``entity_id`` is the group's consolidation
  entity."""
  if entity_id is None:
    return None
  role = session.execute(
    text("SELECT entity_role FROM entities WHERE id = :id"), {"id": entity_id}
  ).scalar()
  return resolve_entity_id(session, None) if role == "consolidation" else None


class ConsolidatedPeriodSpansMembershipError(ValueError):
  """A consolidated column spans a member joining or leaving the group."""

  def __init__(self, member_id: str, period: PeriodSpec) -> None:
    super().__init__(
      f"Entity {member_id!r} joins or leaves the group during {period.label} "
      f"({period.start} to {period.end}); a consolidated column cannot state "
      "part of a member's time. Report the months on either side."
    )
    self.member_id = member_id
    self.period = period


def _month(day: date) -> tuple[int, int]:
  return (day.year, day.month)


def _member_covers(member: GroupMember, period: PeriodSpec) -> bool | None:
  """True when ``member`` is in the group for all of ``period``, False when
  for none of it, None when for part. By month, as ``is_member_for``."""
  if not member.is_member_for(period.start, period.end):
    return False
  if member.role == "consolidation":
    return True
  joined = member.joined_on is None or _month(member.joined_on) <= _month(period.start)
  stays = member.left_on is None or _month(member.left_on) >= _month(period.end)
  return True if joined and stays else None


def consolidated_report_facts(
  session: Session,
  *,
  taxonomy_id: str,
  mapping_id: str,
  periods: list[PeriodSpec],
  close_target: str,
  parent_id: str,
) -> ReportFacts:
  """The group's facts for each period: each entity the consolidated read
  sums (the parent, every member in full inside its interval, and the
  consolidation entity's adjustments), pivoted on its own chart under the
  parent's Style and added concept by concept, as ``live-financial-statement``
  adds them with ``scope: consolidated``. An entity with no mapping is left
  out, as there. Raises :class:`ConsolidatedPeriodSpansMembershipError` for a
  period a member is in the group for only part of."""
  summed: dict[tuple, ReportFact] = {}
  warnings: list[str] = []
  unmapped = 0
  for member in group_members(session, parent_id, consolidated=True):
    covered: list[PeriodSpec] = []
    for period in periods:
      covers = _member_covers(member, period)
      if covers is None:
        raise ConsolidatedPeriodSpansMembershipError(member.id, period)
      if covers:
        covered.append(period)
    if not covered:
      continue
    own = find_entity_mapping(session, member.id, BOOK_FRAMEWORK)
    if own is None:
      continue
    generated = generate_report_facts(
      session=session,
      taxonomy_id=taxonomy_id,
      mapping_id=str(own.id),
      periods=covered,
      close_target_qname=close_target,
      entity_id=member.id,
    )
    unmapped += generated.unmapped_count
    warnings.extend(generated.warnings)
    for f in generated.facts:
      key = (f.element_id, f.period_start, f.period_end, f.period_type, f.audit_only)
      # Each member posts only to its own chart, so every source account
      # names the member whose pivot summed it.
      owners = dict.fromkeys(f.source_values or (), member.id)
      held = summed.get(key)
      if held is None:
        summed[key] = replace(
          f,
          source_values=dict(f.source_values) if f.source_values else None,
          source_entities=owners or None,
        )
        continue
      held_close = held.value if held.close_value is None else held.close_value
      add_close = f.value if f.close_value is None else f.close_value
      if held.close_value is not None or f.close_value is not None:
        held.close_value = held_close + add_close
      held.value += f.value
      if f.source_values:
        held.source_values = {**(held.source_values or {}), **f.source_values}
        held.source_entities = {**(held.source_entities or {}), **owners}
  return ReportFacts(
    facts=list(summed.values()),
    periods=periods,
    unmapped_count=unmapped,
    taxonomy_id=taxonomy_id,
    mapping_id=mapping_id,
    warnings=warnings,
  )


def generate_entity_report_facts(
  session: Session,
  *,
  taxonomy_id: str,
  mapping_id: str,
  periods: list[PeriodSpec],
  entity_id: str,
) -> tuple[ReportFacts, str]:
  """One entity's report facts for ``periods`` and the Reporting Style they
  render under. The consolidation entity's are the group's consolidated
  statements, under the group parent's Style; any other entity's are its own
  books, under its own."""
  group_parent = _group_parent_of(session, entity_id)
  reporting_style_id = load_entity_reporting_style(session, group_parent or entity_id)
  close_target = load_close_target_concept(session, reporting_style_id)
  if group_parent is not None:
    facts = consolidated_report_facts(
      session,
      taxonomy_id=taxonomy_id,
      mapping_id=mapping_id,
      periods=periods,
      close_target=close_target,
      parent_id=group_parent,
    )
  else:
    facts = generate_report_facts(
      session=session,
      taxonomy_id=taxonomy_id,
      mapping_id=mapping_id,
      periods=periods,
      close_target_qname=close_target,
      entity_id=entity_id,
    )
  return facts, reporting_style_id


def _scope_entity_id(session: Session, entity_id: str | None) -> str | None:
  """The entity a stamp, retract or check acts on; None before any exists."""
  try:
    return resolve_entity_id(session, entity_id)
  except NoEntityError:
    return None


def _canonical_set_ids_in_window(
  session: Session, period_start: date, period_end: date, entity_id: str
) -> list[str]:
  """One entity's canonical statement set ids for exactly this period window.

  Window-scoped, not per-structure: a reclose after a reporting-style change
  must retire the old style's sets too. Entity-scoped: closing one entity
  never touches another's. Excludes publication snapshots, scenario months
  and the sets a reopen retracted.
  """
  return list(
    session.execute(
      text(
        "SELECT id FROM fact_sets "
        "WHERE period_start = :ps AND period_end = :pe "
        "  AND entity_id = :eid "
        "  AND factset_type = 'report' "
        "  AND report_id IS NULL AND scenario_id IS NULL "
        "  AND retracted_at IS NULL"
      ),
      {"ps": period_start, "pe": period_end, "eid": entity_id},
    )
    .scalars()
    .all()
  )


def has_canonical_statement_sets(
  session: Session,
  *,
  period_start: date,
  period_end: date,
  entity_id: str | None = None,
) -> bool:
  """Whether the entity's window already carries close-stamped canonical sets.

  ``entity_id`` defaults to the group parent.
  """
  scope = _scope_entity_id(session, entity_id)
  if scope is None:
    return False
  return bool(_canonical_set_ids_in_window(session, period_start, period_end, scope))


def retract_canonical_statement_sets(
  session: Session,
  *,
  period_start: date,
  period_end: date,
  entity_id: str | None = None,
) -> list[str]:
  """Retract one entity's canonical statement sets for the window (reopen /
  replace path). ``entity_id`` defaults to the group parent.

  The sets stay, with their facts and verification results, as the month as
  it was closed: marked ``retracted_at`` and read by nothing current. Returns
  the retracted fact_set ids.
  """
  scope = _scope_entity_id(session, entity_id)
  if scope is None:
    return []
  return _retract_window(session, period_start, period_end, scope)


def _retract_window(
  session: Session, period_start: date, period_end: date, entity_id: str
) -> list[str]:
  """Retract for an already-resolved entity."""
  set_ids = _canonical_set_ids_in_window(session, period_start, period_end, entity_id)
  if not set_ids:
    return []
  session.execute(
    text("UPDATE fact_sets SET retracted_at = :now WHERE id = ANY(:ids)"),
    {"ids": set_ids, "now": datetime.now(UTC)},
  )
  session.flush()
  return set_ids


def stamp_canonical_statement_sets(
  session: Session,
  *,
  graph_id: str,
  period_start: date,
  period_end: date,
  actor_id: str,
  entity_id: str | None = None,
) -> StatementStampResult:
  """Pivot the posted ledger and stamp one entity's canonical statement sets
  for the period. ``entity_id`` defaults to the group parent.

  Statements only; disclosures and text blocks are publication concerns.
  Idempotent: the window's existing canonical sets are retracted and
  replaced, and every fact is written frozen at the stamp.

  - **Soft-skip** (``stamped=False`` with a note): reporting isn't set up.
    The close proceeds.
  - **Hard fail** (:class:`StatementStampError`): reporting is configured
    but the stamp raised. The close must roll back rather than leave a hole
    in the statement series.

  Statement-rule verification afterwards is non-fatal; a failed check is a
  finding on the month (``rule_summary``), not a reason to refuse the close.
  """
  scope = _scope_entity_id(session, entity_id)
  # The consolidation entity's close is the group's, and its stamp is the
  # group's consolidated statements, under the parent's chart and Style.
  group_parent = _group_parent_of(session, scope)
  books = group_parent or scope
  mapping = find_entity_mapping(session, books, BOOK_FRAMEWORK)
  if mapping is None:
    return StatementStampResult(stamped=False, note="no_coa_mapping")
  if scope is None or books is None:
    return StatementStampResult(stamped=False, note="no_entity")
  entity_id = scope

  reporting_style_id = load_entity_reporting_style(session, books)
  element_to_structures, structure_to_factset = _build_structure_mapping(
    session, reporting_style_id, fact_element_ids=None, taxonomy_id=None
  )
  if not structure_to_factset:
    return StatementStampResult(stamped=False, note="no_statement_structures")

  taxonomy_row = session.execute(
    text(
      "SELECT id FROM taxonomies "
      "WHERE standard = 'rs-gaap' AND taxonomy_type = 'reporting_standard' "
      "ORDER BY version DESC LIMIT 1"
    )
  ).fetchone()
  if taxonomy_row is None:
    return StatementStampResult(stamped=False, note="no_taxonomy")

  period_label = period_end.strftime("%Y-%m")
  try:
    close_target = load_close_target_concept(session, reporting_style_id)
    period = PeriodSpec(start=period_start, end=period_end, label=period_label)
    if group_parent is not None:
      facts = consolidated_report_facts(
        session,
        taxonomy_id=taxonomy_row.id,
        mapping_id=mapping.id,
        periods=[period],
        close_target=close_target,
        parent_id=group_parent,
      )
    else:
      facts = generate_report_facts(
        session=session,
        taxonomy_id=taxonomy_row.id,
        mapping_id=mapping.id,
        periods=[period],
        close_target_qname=close_target,
        entity_id=entity_id,
      )
    _retract_window(session, period_start, period_end, entity_id)
    _pre_create_report_fact_sets(
      session,
      None,
      entity_id,
      actor_id,
      [PeriodSpec(start=period_start, end=period_end, label=period_label)],
      structure_to_factset,
      mapping.id,
    )
    _stamp_facts_into_sets(
      session,
      facts,
      entity_id,
      element_to_structures,
      structure_to_factset,
      mapping_id=str(mapping.id),
      frozen_at=datetime.now(UTC),
      freeze_through=period_end,
    )
  except Exception as exc:
    raise StatementStampError(
      f"Failed to stamp canonical statement sets for {period_label} "
      f"on graph {graph_id}: {exc}"
    ) from exc

  rule_summary: dict[str, int] | None = None
  try:
    # Savepoint: a DB failure here must not abort the close's transaction.
    with session.begin_nested():
      rule_summary = _evaluate_report_structures(
        session,
        facts,
        element_to_structures,
        structure_to_factset,
        period_start,
        period_end,
        actor_id,
      )
  except Exception as exc:
    logger.warning(
      f"Statement-rule evaluation failed after close stamp for "
      f"{period_label} on graph {graph_id}: {exc}"
    )

  return StatementStampResult(
    stamped=True,
    fact_set_ids=dict(structure_to_factset),
    rule_summary=rule_summary,
  )


__all__ = [
  "ConsolidatedPeriodSpansMembershipError",
  "NoEntityError",
  "StatementStampError",
  "StatementStampResult",
  "consolidated_report_facts",
  "generate_entity_report_facts",
  "retract_canonical_statement_sets",
  "stamp_canonical_statement_sets",
]
