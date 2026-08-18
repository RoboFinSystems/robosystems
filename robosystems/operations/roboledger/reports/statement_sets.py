"""Canonical statement FactSet production — shared by close and publication.

Statement FactSets (``factset_type='report'``) have two producers with
different ownership semantics:

- **Close (canonical).** Closing a fiscal period pivots the posted ledger
  through the active CoA mapping and stamps the month's statement sets
  with ``report_id NULL`` — the canonical, replaceable record of the
  closed month. Reclose replaces them (:func:`stamp_canonical_statement_sets`
  deletes-then-mints by period window); reopen retracts them
  (:func:`retract_canonical_statement_sets`). The statement-envelope,
  series, forecast, and metric readers bind these by
  ``(structure_id, factset_type, period)`` and never touch ``report_id``.
- **Publication (snapshot).** ``create_report`` mints its own
  ``report_id``-owned sets, frozen at filing time: a published Report keeps
  the numbers as generated even if the ledger is later reopened and
  restamped. ``delete_report`` / ``regenerate_report`` delete only by
  ``report_id`` and can never destroy canonical closed-month history.

The pivot/persist helpers live here so both producers share one
implementation; ``commands/reports.py`` re-exports them.

Import-cycle rule: this module must not import from
``operations/roboledger/commands/*`` other than the leaf ``_guards``
module (which imports nothing back). The close service and the fiscal
calendar commands import THIS module function-level, preserving their
lazy-import posture toward information-block machinery.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
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

  Distinct from the soft-skip path (tenant hasn't set up reporting —
  close proceeds without sets): this means the mapping/style resolved
  but the pivot or persist raised, so the close must roll back and the
  operator can fix the cause and re-run the close.
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
  # Aggregated statement-rule outcome across the stamped structures
  # (pass/fail/error/skipped); None when no statement rules exist or
  # the evaluation itself errored (non-fatal, logged).
  rule_summary: dict[str, int] | None = None


def _get_entity_id(session: Session, graph_id: str) -> str:
  """Get the earliest-created entity ID — the primary entity for single-entity graphs."""
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
  """Run rule evaluation for every structure that received report facts.

  Returns an aggregated rule_summary across all structures, or None if
  no structures have rules.
  """
  structures_with_facts: set[str] = set()
  for f in facts.facts:
    for sid in element_to_structures.get(f.element_id, ()):
      structures_with_facts.add(sid)
  # The rs-gaap-calculations DAG is invariant across structures within this
  # report run — load it once and share it (each structure still gets its own
  # local-arc overlay from a copy) rather than re-querying per structure.
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


# Downward-recursive taxonomy closure: the report's taxonomy plus every
# extension descending from it (extension taxonomies point at their parent
# via ``parent_taxonomy_id``). Scopes disclosure picking so one report
# never pulls in another framework's (or an abandoned extension's) notes.
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

  Disclosures are not composed by the Reporting Style — styles pin
  statement layouts only. A disclosure renders because the mapped
  ledger produced values for its concepts (the forward/Reportability
  direction): pick every active ``regulatory_disclosure`` structure
  owning at least one presentation arc whose MEMBER (``to``) endpoint
  is among the generated facts' elements. The member-only condition is
  deliberate: a note's total is typically a common library leaf that
  almost always has a fact, so an endpoint-OR would pick the note with
  an empty (structurally unfailable) member breakdown.

  Scoped to the report's taxonomy closure (the resolved taxonomy plus
  its descendant extensions) so a foreign framework's notes — or an
  abandoned extension taxonomy's — never join this report's render set.

  The library's disclosure-identity envelopes (``disclosures:*`` rows
  with no arcs) never match the presentation-arc requirement, and a
  tenant with no arc-bearing disclosure structures gets an empty list —
  the picker is inert until a disclosure structure with content exists.
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
  """Return (element_id→[structure_ids], structure_id→fact_set_id) for the
  Reporting Style composed for this graph, plus fact-picked disclosures.

  Walks ``reporting_style_networks`` to pick one Network per statement
  type, then enumerates each Network's association rows to map elements →
  structure ids. An element can appear in multiple structures (e.g.
  ``NetIncomeLoss`` is the bottom line of the Income Statement AND the
  first calc-child of the Cash Flow Operating rollup), and the fact must
  be stamped onto every owning structure's FactSet so each block's
  renderer can resolve its calc rollups locally. Pre-generates a
  fact_set_id ULID per picked structure.

  ``fact_element_ids`` (the elements the just-generated report facts
  touch) drives the disclosure half: disclosure structures are NOT
  style-composed — they join the render set when their concepts
  received facts (:func:`_pick_disclosure_structures`).

  Networks for statement types the Style doesn't compose are skipped
  silently — a Style is free to omit, say, the comprehensive_income
  Network. Statement types the renderer asks for that the Style
  *does* try to render but lacks a composition row for surface as
  ``NoNetworkForStatementTypeError`` at picker time (the caller wraps
  the loop and decides whether to fail closed).
  """
  element_to_structures: dict[str, list[str]] = {}
  structure_to_factset: dict[str, str] = {}
  picked_structure_ids: list[str] = []

  for statement_type in _RENDER_TARGET_STATEMENT_TYPES:
    try:
      network = get_render_network(session, reporting_style_id, statement_type)
    except NoNetworkForStatementTypeError:
      # Style doesn't compose this statement type — skip silently so a
      # Style that ships without (say) an equity Network still renders.
      # Safety net: the ``change-reporting-style`` command validates that
      # every required statement_type has a composition row before setting
      # ``entities.reporting_style_id``, so by the time this loop runs the
      # only `NoNetworkForStatementTypeError` we should see is for
      # genuinely optional types (e.g. a Style that deliberately omits
      # ``comprehensive_income``).
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

  # Map BOTH endpoints of every arc to the structure, not just
  # ``to_element_id``. A subtotal that sits at the top of its tree
  # (e.g. ``rs-gaap:Assets``, ``rs-gaap:LiabilitiesAndStockholdersEquity``)
  # appears only as a ``from_element_id`` (parent), so a to-only mapping
  # never stamps its persisted subtotal fact into the structure's FactSet
  # — and the balance-identity rule scoped to it can't bind. Abstract
  # parents carry no facts, so including ``from_element_id`` is harmless
  # for them and load-bearing for top-level subtotals.
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
  """Insert one FactSet row per picked Network — before facts are stamped.

  ``create_report`` creates the ``fact_sets`` row first, then stamps
  facts referencing its id. The period envelope comes from the report's
  ``periods`` (min start, max end) rather than being derived post-hoc
  from filtered facts. That keeps the dataflow forward and lets the FK
  ``facts.fact_set_id`` → ``fact_sets.id`` hold without orphan risk.

  ``report_id=None`` mints CANONICAL sets — the close-time record owned
  only by ``(structure_id, period)``; a non-None id mints a Report's
  publication snapshot.

  Every picked Network gets a FactSet row, even one whose Network the
  CoA hasn't reached yet (e.g., the demo's CF Network with no
  cash-flow CoA mappings). The envelope read
  (``ORDER BY period_end DESC LIMIT 1``) still resolves cleanly per
  structure; consumers can detect "no facts in this period" by the
  zero fact_count on the resulting envelope.
  """
  if not periods:
    return

  starts = [p.start for p in periods if getattr(p, "start", None) is not None]
  envelope_start = min(starts) if starts else None
  envelope_end = max(p.end for p in periods)

  # Report facts are pivoted from the posted ledger via the CoA→framework
  # mapping; one provenance descriptor per Network's FactSet. Duration
  # envelopes use ``start/end``; an instant-only envelope (no start) carries
  # the single end date rather than a leading-slash ``/end``.
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
  # Explicit flush so the new fact_sets rows hit the DB before the fact
  # INSERTs that reference them. SQLAlchemy's session-flush ordering is
  # by INSERT statement type, not by FK dependency.
  session.flush()


def _stamp_facts_into_sets(
  session: Session,
  facts,
  entity_id: str,
  element_to_structures: dict[str, list[str]],
  structure_to_factset: dict[str, str],
) -> None:
  """Persist generated report facts into their pre-created FactSets.

  Every persisted Fact is stamped with ``structure_id`` and
  ``fact_set_id`` — the FactSet is the sole parent pointer. Facts whose
  element isn't reached by any Network in the picked Reporting Style are
  skipped: there's no Network to render them through, so they can't be
  stamped with a structure_id, and the fact_set_id NOT NULL constraint
  would reject them.

  An element that appears in N structures (e.g. ``NetIncomeLoss`` =
  bottom-line of the IS AND first calc-child of CF Operating) gets one
  Fact row per owning structure, each pinned to that structure's
  FactSet. This is what lets the CF block's calc walker resolve
  NetIncome locally without cross-structure fact lookup.
  """
  for fact in facts.facts:
    for structure_id in element_to_structures.get(fact.element_id, ()):
      fact_set_id = structure_to_factset.get(structure_id)
      if fact_set_id is None:
        continue
      rf = Fact(
        element_id=fact.element_id,
        value=fact.value,
        period_start=fact.period_start,
        period_end=fact.period_end,
        period_type=fact.period_type,
        unit="USD",
        entity_id=entity_id,
        structure_id=structure_id,
        fact_set_id=fact_set_id,
      )
      session.add(rf)

  # Flush so every fact is visible to the rule-evaluation binds that
  # follow. The session runs ``autoflush=False``, and
  # ``_evaluate_report_structures`` iterates structures in
  # non-deterministic set order; without an explicit flush here the
  # FIRST structure evaluated would bind against unflushed facts and
  # spuriously ``skip`` every rule (the terminal flush inside the first
  # ``evaluate_rules_for_structure`` would only rescue the later ones).
  session.flush()


def _persist_report_facts(
  session: Session,
  report_id: str,
  facts,
  entity_id: str,
  element_to_structures: dict[str, list[str]],
  structure_to_factset: dict[str, str],
) -> None:
  """Clear any existing facts for this report and persist the new set.

  The publication path: DELETE by ``report_id`` (regeneration clears
  prior snapshots; canonical ``report_id NULL`` sets are untouchable
  through this path by construction), then stamp via
  :func:`_stamp_facts_into_sets`.
  """
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

  Window-scoped on purpose (not per-structure): a reclose after a
  reporting-style change must retire the OLD style's sets too, and
  reopen retraction can't know which structures a past close picked.
  The predicates exclude every other producer: publication snapshots
  (``report_id NOT NULL``), scenario months (``scenario_id NOT NULL``),
  and non-'report' factset types (schedule/metric/disclosure/custom).
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
  """Whether the window already carries close-stamped canonical sets.

  The plan-history backfill's idempotency check: months that answer True
  are already part of the statement series and are never touched again.
  """
  return bool(_canonical_set_ids_in_window(session, period_start, period_end))


def retract_canonical_statement_sets(
  session: Session, *, period_start: date, period_end: date
) -> list[str]:
  """Delete the window's canonical statement sets (reopen / replace path).

  Sweeps the sets' VerificationResults first: no DB-level FK ties results
  to fact_sets, so without the sweep they'd orphan invisibly and bleed into
  later reads. Facts cascade at the DB level
  (``facts.fact_set_id ON DELETE CASCADE``).

  Returns the retracted fact_set ids — empty when the window carries none,
  as after a soft-skipped close.
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

  The close-time producer: statements only (``taxonomy_id=None`` keeps
  the disclosure picker out — text-block snapshots and disclosure
  membership are publication concerns that stay in ``create_report``).

  The pivot runs over TWO periods — the prior calendar month plus the
  close month — so the indirect cash flow articulates: operating CF
  leaves derive from month-over-month BS deltas and the statement foots
  to the actual ΔCash via the reconciling plug, exactly the
  ``create_report`` / forecast doctrine. Only the close month's facts
  are stamped. A close month with no prior ledger activity deltas
  against zero balances — correct for a book's genuinely first month —
  and a backfill's clamped seed month deltas against the ledger's true
  prior-month balances even when that month was never itself stamped.

  Two-zone failure semantics:

  - **Soft-skip** (returns ``stamped=False`` with a note): the tenant
    hasn't set up reporting — no CoA mapping, no entity, no composed
    statement structures, or no rs-gaap taxonomy. The close proceeds;
    a later close (after mapping) stamps normally.
  - **Hard fail** (:class:`StatementStampError`): reporting IS
    configured but the pivot or persist raised. The close must roll
    back — a closed month without its canonical sets would silently
    hole the statement series — and the operator fixes and re-closes.

  Replace semantics: any canonical sets already in this window are
  retracted first (results swept, facts cascaded), making the stamp
  idempotent across reclose and retry-after-failure.

  Statement-rule verification runs after the stamp, pinned per minted
  set, and is NON-fatal (mirrors the close's schedule-rule pass): a
  failed identity check is a finding on the month, not a reason to
  refuse the close. Its outcome rides ``rule_summary``.
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
  # The prior month rides the pivot as the indirect-CF delta basis: the
  # working-capital derivation (``_derive_cash_flow_facts``) and the cash
  # foot (``_reconcile_operating_to_cash``) both no-op below two periods, so
  # a single-period stamp would mint a CF of NetIncome + DDA that never ties
  # to the actual cash movement. Only the close month's facts are stamped —
  # the prior month's are dropped after derivation, and its own canonical
  # sets, if any, are untouched.
  prior_end = period_start - timedelta(days=1)
  prior_start = prior_end.replace(day=1)
  try:
    close_target = load_close_target_concept(session, reporting_style_id)
    facts = generate_report_facts(
      session=session,
      taxonomy_id=taxonomy_row.id,
      mapping_id=mapping.id,
      periods=[
        PeriodSpec(start=prior_start, end=prior_end, label=prior_end.strftime("%Y-%m")),
        PeriodSpec(start=period_start, end=period_end, label=period_label),
      ],
      close_target_qname=close_target,
    )
    facts.facts = [f for f in facts.facts if f.period_end == period_end]
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
    # Under a savepoint: the evaluation writes VerificationResult rows, and a
    # database-level failure there must not abort the close's transaction
    # (the stamp above is already flushed; the close still has to commit).
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
