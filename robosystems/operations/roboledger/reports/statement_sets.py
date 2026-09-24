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

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sqlalchemy import delete, text

from robosystems.logger import logger
from robosystems.models.api.fact_provenance import PivotProvenance
from robosystems.models.extensions import Fact, VerificationResult
from robosystems.models.extensions.roboledger import Structure
from robosystems.operations.information_block.envelope import DISCLOSURE_BLOCK_TYPE
from robosystems.operations.information_block.rules.engine import (
  evaluate_rules_for_structure,
)
from robosystems.operations.roboledger.commands._guards import (
  rule_summary as _rule_summary,
)
from robosystems.operations.roboledger.fact_set import create_fact_set
from robosystems.operations.roboledger.reports.calc_dag import (
  load_rs_gaap_calculations,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec,
  generate_report_facts,
)
from robosystems.operations.roboledger.reports.network_picker import (
  NoNetworkForStatementTypeError,
  get_render_network,
  load_close_target_concept,
  load_entity_reporting_style,
)
from robosystems.utils.ulid import generate_prefixed_ulid

if TYPE_CHECKING:
  from datetime import date

  from sqlalchemy.orm import Session


class NoEntityError(Exception):
  """Raised when the report path can't find an Entity to tag facts to."""


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


def _get_entity_id(session: Session, graph_id: str) -> str:
  """The earliest-created entity: the primary entity of a single-entity graph."""
  result = session.execute(
    text("SELECT id FROM entities ORDER BY created_at ASC LIMIT 1")
  )
  row = result.fetchone()
  if row is None:
    raise NoEntityError("No entity found. Import data before creating reports.")
  return row.id


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

  for statement_type in _RENDER_TARGET_STATEMENT_TYPES:
    try:
      network = get_render_network(session, reporting_style_id, statement_type)
    except NoNetworkForStatementTypeError:
      # Optional for this Style; change-reporting-style validates required ones.
      continue
    picked_structure_ids.append(network.structure_id)

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
    {"struct_ids": picked_structure_ids},
  ).fetchall()

  for row in rows:
    element_to_structures.setdefault(row.element_id, []).append(row.structure_id)
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
      provenance=PivotProvenance(mapping_id=mapping_id, period=period_key),
    )
  # The fact INSERTs reference these rows; flush order is not FK-aware here.
  session.flush()


def to_cents_precision(value: float) -> float:
  """A dollar amount rounded to cents — the ledger's own precision."""
  return round(value, 2)


def _stamp_facts_into_sets(
  session: Session,
  facts,
  entity_id: str,
  element_to_structures: dict[str, list[str]],
  structure_to_factset: dict[str, str],
) -> None:
  """Persist generated report facts into their pre-created FactSets.

  One Fact row per owning structure; facts no picked structure reaches are
  skipped. Values are dollars rounded to cents: the pivot adds in float and
  ``facts.value`` is a double, so this strips float noise the ledger (integer
  cents) never had.
  """
  for fact in facts.facts:
    for structure_id in element_to_structures.get(fact.element_id, ()):
      fact_set_id = structure_to_factset.get(structure_id)
      if fact_set_id is None:
        continue
      rf = Fact(
        element_id=fact.element_id,
        value=to_cents_precision(fact.value),
        period_start=fact.period_start,
        period_end=fact.period_end,
        period_type=fact.period_type,
        unit="USD",
        entity_id=entity_id,
        structure_id=structure_id,
        fact_set_id=fact_set_id,
      )
      session.add(rf)

  # autoflush=False: without this the first structure evaluated binds
  # against unflushed facts and skips every rule.
  session.flush()


def _persist_report_facts(
  session: Session,
  report_id: str,
  facts,
  entity_id: str,
  element_to_structures: dict[str, list[str]],
  structure_to_factset: dict[str, str],
) -> None:
  """Replace this report's facts; canonical (report_id NULL) sets are untouched."""
  session.execute(
    text(
      "DELETE FROM facts WHERE fact_set_id IN "
      "(SELECT id FROM fact_sets WHERE report_id = :report_id)"
    ),
    {"report_id": report_id},
  )
  _stamp_facts_into_sets(
    session, facts, entity_id, element_to_structures, structure_to_factset
  )


def _canonical_set_ids_in_window(
  session: Session, period_start: date, period_end: date
) -> list[str]:
  """Canonical statement set ids for exactly this period window.

  Window-scoped, not per-structure: a reclose after a reporting-style change
  must retire the old style's sets too. Excludes publication snapshots and
  scenario months.
  """
  return list(
    session.execute(
      text(
        "SELECT id FROM fact_sets "
        "WHERE period_start = :ps AND period_end = :pe "
        "  AND factset_type = 'report' "
        "  AND report_id IS NULL AND scenario_id IS NULL"
      ),
      {"ps": period_start, "pe": period_end},
    )
    .scalars()
    .all()
  )


def has_canonical_statement_sets(
  session: Session, *, period_start: date, period_end: date
) -> bool:
  """Whether the window already carries close-stamped canonical sets."""
  return bool(_canonical_set_ids_in_window(session, period_start, period_end))


def retract_canonical_statement_sets(
  session: Session, *, period_start: date, period_end: date
) -> list[str]:
  """Delete the window's canonical statement sets (reopen / replace path).

  VerificationResults have no FK to fact_sets, so they are swept first; facts
  cascade. Returns the retracted fact_set ids.
  """
  set_ids = _canonical_set_ids_in_window(session, period_start, period_end)
  if not set_ids:
    return []
  session.execute(
    delete(VerificationResult).where(VerificationResult.fact_set_id.in_(set_ids))
  )
  session.execute(text("DELETE FROM fact_sets WHERE id = ANY(:ids)"), {"ids": set_ids})
  session.flush()
  return set_ids


def stamp_canonical_statement_sets(
  session: Session,
  *,
  graph_id: str,
  period_start: date,
  period_end: date,
  actor_id: str,
) -> StatementStampResult:
  """Pivot the posted ledger and stamp the period's canonical statement sets.

  Statements only; disclosures and text blocks are publication concerns.
  Idempotent: the window's existing canonical sets are replaced.

  - **Soft-skip** (``stamped=False`` with a note): reporting isn't set up.
    The close proceeds.
  - **Hard fail** (:class:`StatementStampError`): reporting is configured
    but the stamp raised. The close must roll back rather than leave a hole
    in the statement series.

  Statement-rule verification afterwards is non-fatal; a failed check is a
  finding on the month (``rule_summary``), not a reason to refuse the close.
  """
  mapping = (
    session.query(Structure)
    .filter(Structure.block_type == "coa_mapping")
    .order_by(Structure.created_at.desc())
    .first()
  )
  if mapping is None:
    return StatementStampResult(stamped=False, note="no_coa_mapping")

  try:
    entity_id = _get_entity_id(session, graph_id)
  except NoEntityError:
    return StatementStampResult(stamped=False, note="no_entity")

  reporting_style_id = load_entity_reporting_style(session, entity_id)
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
    facts = generate_report_facts(
      session=session,
      taxonomy_id=taxonomy_row.id,
      mapping_id=mapping.id,
      periods=[PeriodSpec(start=period_start, end=period_end, label=period_label)],
      close_target_qname=close_target,
    )
    retract_canonical_statement_sets(
      session, period_start=period_start, period_end=period_end
    )
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
      session, facts, entity_id, element_to_structures, structure_to_factset
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
  "NoEntityError",
  "StatementStampError",
  "StatementStampResult",
  "retract_canonical_statement_sets",
  "stamp_canonical_statement_sets",
]
