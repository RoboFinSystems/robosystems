"""Validate the curated rs-gaap framework — Mode A (structural correctness).

A host-run script (outside Docker, against the dockerized services via the
localhost env) that provisions a throwaway reference tenant, seeds the
default-pin library, and asserts the framework is internally sound across the
full set of renderable concepts and the three equity forms (CORP / PART / LLC)
— not just the slice a single demo chart of accounts exercises. It's the
front-end of the framework-completeness instrument
(``specs/framework-completeness-and-authoring.md`` §3) and the thing
``specs/rs-gaap-framework-alignment.md`` Phase 1 consumes.

Checks (all fact-free, deterministic; they name the offending element):

1. **reachability** — every concrete presentation leaf on a roll-up statement
   traces up the calc DAG to a canonical rs-gaap root. A leaf that doesn't
   reach a root renders but can never foot: it's mapped onto a dead branch, so
   its value never lands in any subtotal and the statement is wrong. This is
   the load-bearing check.
2. **consistency** — calc-reachable leaves that never appear in any
   presentation network (foots but doesn't render) are reported as warnings.
3. **no calc cycles** — the calculation DAG is acyclic.

These implement the ``rule_check_kind`` structural checks that ``auto_rules``
declares but the rule engine does not yet evaluate (taxonomy.md §9).

**Why there is no per-subtotal footing check.** It was investigated and
dropped: the renderer computes a *nesting* subtotal (e.g. Assets) as the plain
sum of its presentation children, so those foot by construction; *flat*
multi-step subtotals (GrossProfit, NetIncomeLoss) carry no presentation
children and are verified by the seeded RollUp rules, not by containment; and
the calc DAG is intentionally a superset for form-aware equity
(StockholdersEquity calc includes PartnersCapital/MembersEquity, zero on a
corp). A presentation-vs-calc child comparison therefore yields only benign
divergences, not render failures. The real-pipeline footing/identity proof is
the demo oracles (Harbinger, World Online — they foot and balance) plus the
RollUp rules.

Run it:
    just framework-validate                 # totals + table + annotated trees
    just framework-validate --summary        # terse: totals + table only
    UV_ENV_FILE=.env.local uv run python -m robosystems.scripts.framework_validate
"""

from __future__ import annotations

import argparse
import json
import sys
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.orm import Session

from robosystems.config.constants import ReportingStyleConstants
from robosystems.db.extensions import (
  _get_engine,
  _sanitize_schema,
  extensions_session,
  provision_tenant_schema,
)
from robosystems.operations.operators.implementations.mapping.constants import (
  FAC_TO_RS_GAAP_FALLBACK,
)
from robosystems.operations.roboledger.reads.taxonomies import (
  _COA_SOURCES,
  _load_calc_parents,
  _resolve_root_ids,
  is_target_reachable,
  list_unmapped_elements,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  _HierarchyNode,
  _load_reporting_structure,
)
from robosystems.operations.roboledger.reports.network_picker import (
  load_close_target_concept,
)

# The per-section "Other" catch-all leaves the MappingOperator falls back to
# when no specific rs-gaap leaf fits. An account landing here is a framework
# *breadth* signal — the curated set lacked a precise home. (Note: APIC is the
# equity fallback and also a legitimate leaf, so catch-all hits are flagged for
# review, not treated as failures.)
_CATCHALL_LEAVES: frozenset[str] = frozenset(FAC_TO_RS_GAAP_FALLBACK.values())

# ── Reference tenant ─────────────────────────────────────────────────────────

# Equity form → library-seeded Reporting Style id. This is the axis Mode A
# sweeps: the default family is form-pure (BSC / multi-step IS / indirect CF),
# and the equity form is the one place the framework varies per entity type.
REFERENCE_STYLES: dict[str, str] = {
  "CORP": ReportingStyleConstants.DEFAULT_STYLE_ID,
  "PART": ReportingStyleConstants.PARTNERSHIP_STYLE_ID,
  "LLC": ReportingStyleConstants.LLC_STYLE_ID,
}


@dataclass(frozen=True)
class ReferenceTenant:
  """A provisioned, library-seeded tenant the validator renders against."""

  graph_id: str
  styles: dict[str, str]  # equity form → reporting_style_id


@contextmanager
def reference_tenant(keep: bool = False) -> Generator[ReferenceTenant]:
  """Provision a disposable tenant seeded with the default-pin library.

  No platform ``Graph`` row is created: ``provision_tenant_schema`` falls back
  to the default framework pin when the graph is unknown, and the renderer
  takes a ``reporting_style_id`` directly — so the seeded style ids are all we
  need.

  Args:
      keep: leave the schema in place on exit (debugging). Default drops it.
  """
  graph_id = f"kg{uuid.uuid4().hex}"
  _sanitize_schema(graph_id)  # fail fast if the id pattern ever changes
  provision_tenant_schema(graph_id)
  try:
    yield ReferenceTenant(graph_id=graph_id, styles=dict(REFERENCE_STYLES))
  finally:
    if not keep:
      _drop_schema(graph_id)


def _drop_schema(graph_id: str) -> None:
  """Drop the tenant schema (DDL — immutability triggers guard rows, not DROP)."""
  schema = _sanitize_schema(graph_id)
  engine = _get_engine()
  with engine.connect() as conn:
    conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    conn.commit()


# ── Gap report ───────────────────────────────────────────────────────────────

ERROR = "error"
WARNING = "warning"

REACHABILITY = "reachability"  # renders but doesn't foot to a canonical root
CONSISTENCY = "consistency"  # calc-reachable but never presented
CYCLE = "cycle"  # calc DAG has a cycle
EQUITY = "equity"  # form earnings-home concept missing from the balance sheet
# ── package-integrity categories (Mode C: cross-package soundness) ──
DRIFT = "drift"  # a RollUp rule no longer matches the calc arcs it mirrors
REFERENTIAL = "referential"  # a non-FK ref (rule var / style concept) dangles
CLASSIFICATION = "classification"  # a renderable leaf lacks a primary EFS trait
BRIDGE = "bridge"  # fac→rs-gaap bridge integrity


@dataclass(frozen=True)
class Finding:
  """One framework defect surfaced by Mode A."""

  severity: str
  category: str
  message: str
  form: str | None = None  # CORP / PART / LLC, or None for framework-level
  statement: str | None = None  # balance_sheet / income_statement / ...
  element_qname: str | None = None

  def __str__(self) -> str:
    loc = "/".join(p for p in (self.form, self.statement) if p)
    where = f"[{loc}] " if loc else ""
    el = f" ({self.element_qname})" if self.element_qname else ""
    return f"{self.severity.upper():7} {self.category:12} {where}{self.message}{el}"


@dataclass
class StatementSummary:
  """What was checked for one (form, statement) pair — the visualization row."""

  form: str
  statement: str
  structure_name: str | None = None  # None → the style doesn't compose it
  arrangement: str | None = None  # arithmetic / roll_forward / ...
  n_leaves: int = 0
  n_reachable: int = 0
  n_subtotals: int = 0

  @property
  def composed(self) -> bool:
    return self.structure_name is not None

  @property
  def rollforward(self) -> bool:
    return self.arrangement == "roll_forward"


@dataclass
class FrameworkTotals:
  """Framework-wide counts — the surface that was validated."""

  elements: int = 0
  calc_arcs: int = 0
  rules: int = 0
  canonical_roots: int = 0
  renderable_leaves: int = 0  # distinct concrete leaves on roll-up statements
  reachable_leaves: int = 0  # of those, how many reach a canonical root
  off_hierarchy: int = 0  # calc-reachable leaves never presented


@dataclass
class GapReport:
  """The full Mode A result for one reference tenant."""

  findings: list[Finding] = field(default_factory=list)
  summary: list[StatementSummary] = field(default_factory=list)
  totals: FrameworkTotals | None = None
  equity_home: dict[str, tuple[str, bool]] = field(default_factory=dict)
  # form → (close-target qname, present-on-balance-sheet)
  renderable_leaf_ids: set[str] = field(default_factory=set)  # for package checks
  bs_leaf_ids: set[str] = field(default_factory=set)  # balance-sheet leaves only
  package_checks: dict[str, tuple[bool, str]] = field(default_factory=dict)
  # check name → (passed, one-line detail)

  def add(self, finding: Finding) -> None:
    self.findings.append(finding)

  @property
  def errors(self) -> list[Finding]:
    return [f for f in self.findings if f.severity == ERROR]

  @property
  def warnings(self) -> list[Finding]:
    return [f for f in self.findings if f.severity == WARNING]

  def ok(self) -> bool:
    """True iff no error-severity findings (the gate)."""
    return not self.errors

  def _uncomposed_statement_types(self) -> list[str]:
    """Renderable statement types that no form composes (e.g. comprehensive_income)."""
    composed = {s.statement for s in self.summary if s.composed}
    return [st for st in STATEMENT_TYPES if st not in composed]

  def format_text(self) -> str:
    lines: list[str] = ["Framework Mode A — rs-gaap structural validation", ""]

    if self.totals:
      t = self.totals
      lines += [
        f"  elements {t.elements} · calc arcs {t.calc_arcs} · "
        f"canonical roots {t.canonical_roots} · "
        f"seeded rules {t.rules} (not evaluated here)",
        f"  renderable leaves {t.renderable_leaves} · "
        f"reachable {t.reachable_leaves} · off-hierarchy {t.off_hierarchy}",
      ]
    if self.equity_home:
      parts = [
        f"{form}→{qname.replace('rs-gaap:', '')} {'ok' if ok else 'MISSING'}"
        for form, (qname, ok) in self.equity_home.items()
      ]
      lines.append("  equity home: " + " · ".join(parts))
    gap_types = self._uncomposed_statement_types()
    if gap_types:
      lines.append("  known coverage gaps (no form composes): " + ", ".join(gap_types))
    for name, (passed, detail) in self.package_checks.items():
      mark = "ok" if passed else "FAIL"
      lines.append(f"  package · {name}: {mark} ({detail})")
    if self.totals or self.equity_home or gap_types or self.package_checks:
      lines.append("")

    failed = {(f.form, f.statement) for f in self.errors if f.statement}
    if self.summary:
      lines.append(
        f"  {'form':<4}  {'statement':<20}  {'structure':<26}  "
        f"{'leaf':>4}  {'reach':>5}  {'sub':>3}  status"
      )
      for s in self.summary:
        if not s.composed:
          struct, leaf, reach, sub, status = "—", "-", "-", "-", "not composed"
        elif s.rollforward:
          struct = s.structure_name or ""
          leaf, reach, sub = str(s.n_leaves), "-", str(s.n_subtotals)
          status = "FAIL" if (s.form, s.statement) in failed else "roll-fwd"
        else:
          struct = s.structure_name or ""
          leaf, reach, sub = str(s.n_leaves), str(s.n_reachable), str(s.n_subtotals)
          status = "FAIL" if (s.form, s.statement) in failed else "ok"
        lines.append(
          f"  {s.form:<4}  {s.statement:<20}  {struct[:26]:<26}  "
          f"{leaf:>4}  {reach:>5}  {sub:>3}  {status}"
        )
      lines.append("")

    if self.findings:
      lines.append(f"{len(self.errors)} error(s), {len(self.warnings)} warning(s):")
      for f in sorted(self.findings, key=lambda x: (x.severity != ERROR, x.category)):
        lines.append(f"  {f}")
      lines.append("")

    lines.append(
      "PASS — no findings." if not self.findings else ("PASS" if self.ok() else "FAIL")
    )
    return "\n".join(lines)


# ── Structural checks ────────────────────────────────────────────────────────

# The five primary-statement types a Reporting Style may compose. A style that
# doesn't compose one (e.g. comprehensive_income) is skipped, not flagged.
STATEMENT_TYPES: tuple[str, ...] = (
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
  "comprehensive_income",
)


def run_structural(session: Session, styles: dict[str, str]) -> GapReport:
  """Run all Mode A checks against a provisioned reference tenant session."""
  report = GapReport()

  calc_parents = _load_calc_parents(session)
  root_ids = _resolve_root_ids(session)
  calc_children = _invert_calc_children(calc_parents)
  calc_reachable = _calc_reachable_set(calc_children, root_ids)

  _check_calc_cycles(calc_children, report)

  presentation_leaves: dict[str, str] = {}  # element_id → qname (all statements)
  rollup_leaf_ids: set[str] = set()  # distinct concrete leaves on roll-up stmts
  reachable_leaf_ids: set[str] = set()
  bs_leaf_ids: set[str] = set()  # balance-sheet leaves (where EFS class is needed)

  for form, style_id in styles.items():
    bs_leaf_qnames: set[str] = set()
    for stmt in STATEMENT_TYPES:
      _, structure_name, arrangement, hierarchy = _load_reporting_structure(
        session, stmt, style_id
      )
      summary = StatementSummary(form=form, statement=stmt)
      if not hierarchy:
        report.summary.append(summary)
        continue

      leaves = _concrete_leaves(hierarchy)
      summary.structure_name = structure_name
      summary.arrangement = arrangement
      summary.n_leaves = len(leaves)
      summary.n_subtotals = _count_subtotals(hierarchy)
      for node in leaves:
        presentation_leaves[node.element_id] = node.qname
      if stmt == "balance_sheet":
        bs_leaf_qnames = {node.qname for node in leaves}
        bs_leaf_ids |= {node.element_id for node in leaves}

      # Roll-forward statements (Statement of Changes in Equity) foot by an
      # opening+movements=closing identity, not by calc-DAG roll-up — their
      # movement leaves (dividends, OCI, contributions) legitimately don't
      # reach a calc root, so the reachability check doesn't apply.
      if arrangement != "roll_forward":
        summary.n_reachable = _check_reachability(
          session,
          form,
          stmt,
          leaves,
          calc_parents,
          root_ids,
          report,
          rollup_leaf_ids,
          reachable_leaf_ids,
        )

      report.summary.append(summary)

    _check_equity_home(session, form, style_id, bs_leaf_qnames, report)

  _check_consistency(session, presentation_leaves, calc_reachable, report)

  report.totals = FrameworkTotals(
    elements=_scalar(session, "SELECT count(*) FROM elements"),
    calc_arcs=_scalar(
      session,
      "SELECT count(*) FROM associations WHERE association_type = 'calculation'",
    ),
    rules=_scalar(session, "SELECT count(*) FROM rules"),
    canonical_roots=len(root_ids),
    renderable_leaves=len(rollup_leaf_ids),
    reachable_leaves=len(reachable_leaf_ids),
    off_hierarchy=sum(1 for f in report.warnings if f.category == CONSISTENCY),
  )
  report.renderable_leaf_ids = rollup_leaf_ids
  report.bs_leaf_ids = bs_leaf_ids
  return report


def _check_reachability(
  session: Session,
  form: str,
  stmt: str,
  leaves: list[_HierarchyNode],
  calc_parents: dict[str, set[str]],
  root_ids: set[str],
  report: GapReport,
  rollup_leaf_ids: set[str],
  reachable_leaf_ids: set[str],
) -> int:
  """Check every leaf reaches a canonical root; return the count that do."""
  n_reachable = 0
  for node in leaves:
    rollup_leaf_ids.add(node.element_id)
    if is_target_reachable(
      session, node.element_id, _calc_parents=calc_parents, _root_ids=root_ids
    ):
      n_reachable += 1
      reachable_leaf_ids.add(node.element_id)
    else:
      report.add(
        Finding(
          severity=ERROR,
          category=REACHABILITY,
          message="renderable leaf does not foot to a canonical root "
          "(dead calc branch)",
          form=form,
          statement=stmt,
          element_qname=node.qname,
        )
      )
  return n_reachable


def _check_equity_home(
  session: Session,
  form: str,
  style_id: str,
  bs_leaf_qnames: set[str],
  report: GapReport,
) -> None:
  """Cross-statement: the form's earnings-home concept renders on the BS.

  Net Income closes into a form-specific equity concept (``close target``:
  CORP→RetainedEarnings, PART→PartnersCapital, LLC→MembersEquity). For the
  balance sheet to foot, that concept must be a presented leaf in the BS equity
  section — otherwise NI lands on a concept the BS never shows and equity
  silently under-reports. This is the one check that exercises the form axis
  (the whole reason the three forms are swept).
  """
  close_target = load_close_target_concept(session, style_id)
  present = close_target in bs_leaf_qnames
  report.equity_home[form] = (close_target, present)
  if not present:
    report.add(
      Finding(
        severity=ERROR,
        category=EQUITY,
        message="form earnings-home concept is not a balance-sheet leaf — "
        "Net Income would not render on the BS for this form",
        form=form,
        statement="balance_sheet",
        element_qname=close_target,
      )
    )


def _concrete_leaves(hierarchy: list[_HierarchyNode]) -> list[_HierarchyNode]:
  """Concrete (non-abstract) leaf nodes of a presentation hierarchy.

  Abstract nodes are presentation scaffolding (headers/groupings) — they carry
  no value and are not expected to reach a calc root, so they're excluded.
  """
  leaves: list[_HierarchyNode] = []

  def walk(node: _HierarchyNode) -> None:
    if node.children:
      for child in node.children:
        walk(child)
    elif not node.is_abstract:
      leaves.append(node)

  for root in hierarchy:
    walk(root)
  return leaves


def _count_subtotals(hierarchy: list[_HierarchyNode]) -> int:
  """Count presentation subtotals (non-abstract nodes that have children)."""
  count = 0

  def walk(node: _HierarchyNode) -> None:
    nonlocal count
    if node.children and not node.is_abstract:
      count += 1
    for child in node.children:
      walk(child)

  for root in hierarchy:
    walk(root)
  return count


def _invert_calc_children(calc_parents: dict[str, set[str]]) -> dict[str, set[str]]:
  """Invert child→parents into parent→children."""
  children: dict[str, set[str]] = {}
  for child, parents in calc_parents.items():
    for parent in parents:
      children.setdefault(parent, set()).add(child)
  return children


def _calc_reachable_set(
  calc_children: dict[str, set[str]], root_ids: set[str]
) -> set[str]:
  """All element ids reachable from the canonical roots down the calc DAG."""
  visited: set[str] = set()
  frontier = set(root_ids)
  while frontier:
    visited |= frontier
    nxt: set[str] = set()
    for node in frontier:
      nxt |= calc_children.get(node, set())
    frontier = nxt - visited
  return visited


def _check_calc_cycles(calc_children: dict[str, set[str]], report: GapReport) -> None:
  """Flag any cycle in the calculation DAG (a calc DAG must be acyclic)."""
  WHITE, GREY, BLACK = 0, 1, 2
  color: dict[str, int] = {}

  def visit(node: str, path: list[str]) -> bool:
    color[node] = GREY
    for nxt in calc_children.get(node, set()):
      state = color.get(nxt, WHITE)
      if state == GREY:
        cycle = " → ".join([*path[path.index(nxt) :], nxt]) if nxt in path else nxt
        report.add(
          Finding(
            severity=ERROR,
            category=CYCLE,
            message=f"calculation DAG contains a cycle: {cycle}",
          )
        )
        return True
      if state == WHITE and visit(nxt, [*path, nxt]):
        return True
    color[node] = BLACK
    return False

  for start in list(calc_children):
    if color.get(start, WHITE) == WHITE and visit(start, [start]):
      return  # one cycle finding is enough to fail the check


def _check_consistency(
  session: Session,
  presentation_leaves: dict[str, str],
  calc_reachable: set[str],
  report: GapReport,
) -> None:
  """Report calc-reachable leaves that never appear in any presentation network.

  ``presentation - calc`` (renders-but-doesn't-foot) is covered by the
  reachability check. This is the opposite direction: a calc-reachable leaf
  that foots but never renders — a lower-severity authoring gap.
  """
  parent_ids = {
    row[0]
    for row in session.execute(
      text(
        "SELECT DISTINCT from_element_id FROM associations "
        "WHERE association_type = 'calculation'"
      )
    ).fetchall()
  }
  calc_leaves = calc_reachable - parent_ids
  off_hierarchy = calc_leaves - set(presentation_leaves)
  if not off_hierarchy:
    return
  qnames = _qnames_for(session, off_hierarchy)
  for element_id in sorted(off_hierarchy):
    report.add(
      Finding(
        severity=WARNING,
        category=CONSISTENCY,
        message="calc-reachable leaf is not presented in any statement "
        "(foots but does not render)",
        element_qname=qnames.get(element_id),
      )
    )


def _qnames_for(session: Session, element_ids: set[str]) -> dict[str, str]:
  if not element_ids:
    return {}
  rows = session.execute(
    text("SELECT id, qname FROM elements WHERE id = ANY(:ids)"),
    {"ids": list(element_ids)},
  ).fetchall()
  return {row[0]: row[1] for row in rows}


def _scalar(session: Session, sql: str) -> int:
  return int(session.execute(text(sql)).scalar() or 0)


# ── Mode C: package + bridge integrity (cross-package, against the tenant) ────
#
# These span packages, so the per-package seed tests in tests/taxonomy/ don't
# cover them. They run against the same loaded reference tenant as the
# structural checks (the packages are seeded into it), so they validate the
# library *as loaded* — catching seed→load drift, not just authored-file shape.


def run_package_integrity(session: Session, report: GapReport) -> None:
  """Cross-package soundness: rule↔calc, referential integrity, traits, bridge."""
  element_qnames = {
    r[0]
    for r in session.execute(
      text("SELECT qname FROM elements WHERE qname IS NOT NULL")
    ).fetchall()
  }
  _check_rollup_calc_drift(session, report)
  _check_referential_integrity(session, element_qnames, report)
  _check_classification_completeness(session, report)
  _check_bridge(session, element_qnames, report)


def _check_rollup_calc_drift(session: Session, report: GapReport) -> None:
  """Each seeded RollUp rule's children must equal its parent's calc arcs.

  The rollup-rules package is *generated* from rs-gaap-calculations; if calc is
  edited without regenerating, the loaded rule silently stops mirroring the
  DAG it claims to verify. Nothing else checks this on the real seeds.
  """
  calc_children: dict[str, set[str]] = {}
  for parent, child in session.execute(
    text(
      "SELECT p.qname, c.qname FROM associations a "
      "JOIN elements p ON p.id = a.from_element_id "
      "JOIN elements c ON c.id = a.to_element_id "
      "WHERE a.association_type = 'calculation'"
    )
  ).fetchall():
    calc_children.setdefault(parent, set()).add(child)

  rules = session.execute(
    text("SELECT rule_variables FROM rules WHERE rule_pattern = 'RollUp'")
  ).fetchall()
  drift = 0
  for (variables,) in rules:
    if not variables:
      continue
    parent_q = variables[0]["variable_qname"]
    rule_children = {v["variable_qname"] for v in variables[1:]}
    calc_kids = calc_children.get(parent_q, set())
    if rule_children != calc_kids:
      drift += 1
      report.add(
        Finding(
          severity=ERROR,
          category=DRIFT,
          message=(
            "RollUp rule no longer matches calc arcs "
            f"(calc-only={len(calc_kids - rule_children)}, "
            f"rule-only={len(rule_children - calc_kids)}) — regenerate rollup-rules"
          ),
          element_qname=parent_q,
        )
      )
  report.package_checks["rollup↔calc"] = (
    drift == 0,
    f"{len(rules)} RollUp rules vs calc arcs, {drift} drifted",
  )


def _check_referential_integrity(
  session: Session, element_qnames: set[str], report: GapReport
) -> None:
  """Non-FK references resolve: rule variable qnames + style close-targets.

  Arc endpoints are FK-enforced (a dangling one fails the library load), but
  rule variables (JSONB) and the reporting-style close-target (a qname string
  in metadata) are not — they can dangle silently.
  """
  dangling = 0
  for (variables,) in session.execute(
    text("SELECT rule_variables FROM rules")
  ).fetchall():
    for v in variables or []:
      q = v.get("variable_qname")
      if q and q not in element_qnames:
        dangling += 1
        report.add(
          Finding(
            severity=ERROR,
            category=REFERENTIAL,
            message="rule references a concept that doesn't exist",
            element_qname=q,
          )
        )
  for form, style_id in REFERENCE_STYLES.items():
    close_target = load_close_target_concept(session, style_id)
    if close_target not in element_qnames:
      dangling += 1
      report.add(
        Finding(
          severity=ERROR,
          category=REFERENTIAL,
          message=f"{form} reporting-style close target doesn't resolve",
          element_qname=close_target,
        )
      )
  report.package_checks["referential integrity"] = (
    dangling == 0,
    f"{dangling} dangling non-FK refs",
  )


def _check_classification_completeness(session: Session, report: GapReport) -> None:
  """Every *balance-sheet* leaf has a primary EFS classification trait.

  ``guard_rails._validate_balance_sheet`` assigns each BS line to
  asset/liability/equity by its ``elementsOfFinancialStatements`` trait to test
  Assets = Liabilities + Equity, so a BS leaf with none breaks that check.
  Scoped to BS deliberately: cash-flow movement leaves (Payments/Proceeds/…)
  and flat IS subtotals carry no EFS class and don't need one — requiring it
  there would be a false positive. (The existing ``LeafHasClassification`` is
  CoA-side.)
  """
  primary_efs = {
    r[0]
    for r in session.execute(
      text(
        "SELECT et.element_id FROM element_traits et "
        "JOIN traits t ON t.id = et.trait_id "
        "WHERE t.category = 'elementsOfFinancialStatements' AND et.is_primary"
      )
    ).fetchall()
  }
  missing = [e for e in report.bs_leaf_ids if e not in primary_efs]
  if missing:
    qnames = _qnames_for(session, set(missing))
    for element_id in missing:
      report.add(
        Finding(
          severity=ERROR,
          category=CLASSIFICATION,
          message="balance-sheet leaf has no primary EFS classification trait",
          element_qname=qnames.get(element_id),
        )
      )
  report.package_checks["EFS classification"] = (
    not missing,
    f"{len(report.bs_leaf_ids)} balance-sheet leaves, {len(missing)} unclassified",
  )


def _check_bridge(
  session: Session, element_qnames: set[str], report: GapReport
) -> None:
  """fac→rs-gaap bridge: report coverage + confirm fallback catch-alls exist.

  Deep bridge *correctness* needs FAC judgment (out of scope here); what's
  soundly checkable is that the catch-all leaves the bridge/fallback routes to
  actually exist as concepts — a missing one would make the fallback dangle.
  """
  arcs, sources = session.execute(
    text(
      "SELECT count(*), count(DISTINCT from_element_id) FROM associations "
      "WHERE association_type = 'equivalence'"
    )
  ).fetchone()
  missing_catchalls = sorted(q for q in _CATCHALL_LEAVES if q not in element_qnames)
  for q in missing_catchalls:
    report.add(
      Finding(
        severity=ERROR,
        category=BRIDGE,
        message="fallback catch-all leaf does not exist as a concept",
        element_qname=q,
      )
    )
  report.package_checks["fac→rs-gaap bridge"] = (
    not missing_catchalls,
    f"{arcs or 0} equivalence arcs, {sources or 0} fac sources, "
    f"{len(_CATCHALL_LEAVES)} catch-alls present",
  )


# ── Tree view (--tree) ───────────────────────────────────────────────────────


def render_trees(session: Session, styles: dict[str, str]) -> str:
  """Render each composed statement's presentation hierarchy, annotated.

  Each node is tagged abstract / subtotal / leaf, and roll-up leaves carry a
  reachability mark (``ok`` reaches a canonical root; ``UNREACHABLE`` doesn't).
  This is the "what was validated" picture for the eyes.
  """
  calc_parents = _load_calc_parents(session)
  root_ids = _resolve_root_ids(session)
  lines: list[str] = []

  for form, style_id in styles.items():
    for stmt in STATEMENT_TYPES:
      _, structure_name, arrangement, hierarchy = _load_reporting_structure(
        session, stmt, style_id
      )
      if not hierarchy:
        continue
      lines.append(f"{form} · {stmt} · {structure_name} [{arrangement}]")
      rollforward = arrangement == "roll_forward"

      def walk(node: _HierarchyNode, depth: int, is_rf: bool = rollforward) -> None:
        if node.is_abstract:
          tag = "abstract"
        elif node.children:
          tag = "subtotal"
        elif is_rf:
          tag = "movement"
        elif is_target_reachable(
          session, node.element_id, _calc_parents=calc_parents, _root_ids=root_ids
        ):
          tag = "leaf ok"
        else:
          tag = "leaf UNREACHABLE"
        name = node.qname.replace("rs-gaap:", "")
        lines.append(f"  {'  ' * depth}{name}  [{tag}]")
        for child in node.children:
          walk(child, depth + 1)

      for root in hierarchy:
        walk(root, 0)
      lines.append("")

  return "\n".join(lines)


# ── Mode B: CoA coverage on real graphs ──────────────────────────────────────
#
# Mode A validates the framework in isolation. Mode B asks the *breadth*
# question — does a real chart of accounts map cleanly onto the curated leaves,
# or fall into catch-alls / go unmapped? It measures the mappings that ALREADY
# exist on provisioned graphs (the demos render statements through them), so it
# is deterministic — no MappingOperator re-run, no credits, no LLM variance.
# The number it produces is "framework + operator coverage achieved on real
# data"; the catch-all and unmapped lists are the actionable framework-gap
# signal (review each: is it a missing leaf, or an account that needs none?).


@dataclass
class CoverageResult:
  """CoA mapping coverage for one provisioned graph."""

  label: str
  graph_id: str
  available: bool = True
  is_real_coa: bool = True  # False → the "CoA" is loaded taxonomy concepts
  total_coa: int = 0
  mapped: int = 0
  on_real_leaf: int = 0
  on_catchall: int = 0
  unmapped: int = 0
  catchall_accounts: list[str] = field(default_factory=list)  # "code name → target"
  unmapped_samples: list[str] = field(default_factory=list)

  @property
  def coverage_pct(self) -> float:
    return (self.mapped / self.total_coa * 100) if self.total_coa else 0.0


def measure_coverage(session: Session, label: str, graph_id: str) -> CoverageResult:
  """Bucket a graph's CoA accounts: real leaf vs catch-all vs unmapped."""
  total_coa = int(
    session.execute(
      text(
        "SELECT count(*) FROM elements WHERE source = ANY(:src) "
        "AND is_active AND NOT is_abstract"
      ),
      {"src": list(_COA_SOURCES)},
    ).scalar()
    or 0
  )

  # A real chart of accounts has accounts (null/code-based qnames), not
  # taxonomy concepts. If the "CoA" is dominated by taxonomy-namespaced qnames
  # (mini:/us-gaap:/…), this graph is a taxonomy→taxonomy projection (the
  # Seattle/World Online demos), not a CoA — its coverage % is meaningless as a
  # breadth signal, so we flag it rather than report a misleading number.
  taxonomy_coa = int(
    session.execute(
      text(
        "SELECT count(*) FROM elements WHERE source = ANY(:src) "
        "AND is_active AND NOT is_abstract AND ("
        "qname LIKE 'mini:%' OR qname LIKE 'us-gaap:%' OR qname LIKE 'rs-gaap:%' "
        "OR qname LIKE 'fac:%' OR qname LIKE 'ifrs:%')"
      ),
      {"src": list(_COA_SOURCES)},
    ).scalar()
    or 0
  )
  is_real_coa = total_coa > 0 and (taxonomy_coa / total_coa) < 0.5

  rows = session.execute(
    text(
      """
      SELECT src.id, src.code, src.name, tgt.qname AS target_qname
      FROM associations a
      JOIN elements src ON src.id = a.from_element_id
      JOIN elements tgt ON tgt.id = a.to_element_id
      WHERE a.association_type = 'mapping'
      """
    )
  ).fetchall()

  # An account may carry several mapping arcs; it counts as real-leaf if ANY
  # target is a real (non-catch-all) leaf, else catch-all.
  targets_by_account: dict[str, list[tuple[str | None, str | None, str]]] = {}
  for src_id, code, name, target_qname in rows:
    targets_by_account.setdefault(src_id, []).append((code, name, target_qname))

  on_real = 0
  catchall_accounts: list[str] = []
  for targets in targets_by_account.values():
    if any(tq not in _CATCHALL_LEAVES for _, _, tq in targets):
      on_real += 1
    else:
      code, name, tq = targets[0]
      label_str = f"{code or ''} {name or ''}".strip() or "(unnamed)"
      catchall_accounts.append(f"{label_str} → {tq.replace('rs-gaap:', '')}")

  unmapped = list_unmapped_elements(session, None)
  unmapped_samples = [
    f"{e.code or ''} {e.name or ''}".strip() or "(unnamed)" for e in unmapped
  ]

  return CoverageResult(
    label=label,
    graph_id=graph_id,
    is_real_coa=is_real_coa,
    total_coa=total_coa,
    mapped=len(targets_by_account),
    on_real_leaf=on_real,
    on_catchall=len(catchall_accounts),
    unmapped=len(unmapped),
    catchall_accounts=sorted(catchall_accounts),
    unmapped_samples=sorted(unmapped_samples),
  )


def load_config_graphs() -> dict[str, str]:
  """Read the loaded demo graphs (label → graph_id) from .local/config.json."""
  config_path = Path(__file__).resolve().parents[2] / ".local" / "config.json"
  if not config_path.exists():
    return {}
  data = json.loads(config_path.read_text())
  return {
    label: entry["graph_id"]
    for label, entry in data.get("graphs", {}).items()
    if entry.get("graph_id")
  }


def run_coverage(graphs: dict[str, str]) -> list[CoverageResult]:
  """Measure coverage for each provisioned graph (skips ones that error)."""
  results: list[CoverageResult] = []
  for label, graph_id in graphs.items():
    try:
      with extensions_session(graph_id) as session:
        results.append(measure_coverage(session, label, graph_id))
    except Exception as exc:  # graph deprovisioned / schema missing
      results.append(CoverageResult(label=label, graph_id=graph_id, available=False))
      print(f"  (skipped {label}: {type(exc).__name__})", file=sys.stderr)
  return results


def format_coverage(results: list[CoverageResult]) -> str:
  lines = [
    "Framework Mode B — CoA mapping coverage (existing mappings, deterministic)",
    "",
    f"  {'graph':<22}  {'CoA':>4}  {'map':>4}  {'real':>4}  "
    f"{'catch':>5}  {'unmap':>5}  coverage",
  ]
  for r in results:
    if not r.available:
      lines.append(f"  {r.label:<22}  (unavailable)")
    elif not r.is_real_coa:
      lines.append(
        f"  {r.label:<22}  {r.total_coa:>4}  "
        "— taxonomy projection (not a real CoA); coverage N/A"
      )
    else:
      lines.append(
        f"  {r.label:<22}  {r.total_coa:>4}  {r.mapped:>4}  {r.on_real_leaf:>4}  "
        f"{r.on_catchall:>5}  {r.unmapped:>5}  {r.coverage_pct:5.1f}%"
      )
  lines.append("")

  real = [r for r in results if r.available and r.is_real_coa]
  if not real:
    lines.append(
      "  No real chart-of-accounts graph found — only taxonomy projections. "
      "Load a real CoA (QB sync) to measure breadth."
    )
    return "\n".join(lines).rstrip()

  for r in real:
    if r.catchall_accounts:
      lines.append(f"  catch-all mappings — {r.label} (framework lacked a leaf):")
      lines += [f"    {a}" for a in r.catchall_accounts]
      lines.append("")
  for r in real:
    if r.unmapped_samples:
      shown = r.unmapped_samples[:15]
      more = len(r.unmapped_samples) - len(shown)
      lines.append(f"  unmapped accounts — {r.label} (review: gap or no-op?):")
      lines += [f"    {a}" for a in shown]
      if more > 0:
        lines.append(f"    … +{more} more")
      lines.append("")

  return "\n".join(lines).rstrip()


# ── Entry point ──────────────────────────────────────────────────────────────


def validate_framework(keep: bool = False) -> GapReport:
  """Provision a reference tenant and run the structural + package checks."""
  with reference_tenant(keep=keep) as rt:
    with extensions_session(rt.graph_id) as session:
      report = run_structural(session, rt.styles)
      run_package_integrity(session, report)
      return report


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(
    description="Validate the curated rs-gaap framework "
    "(Mode A: structure; --coverage for Mode B: CoA coverage on real graphs)."
  )
  parser.add_argument(
    "--coverage",
    action="store_true",
    help="Mode B: measure CoA mapping coverage on the graphs in "
    ".local/config.json (deterministic — reads existing mappings)",
  )
  parser.add_argument(
    "--summary",
    action="store_true",
    help="terse Mode A output: framework totals + the per-statement table only "
    "(omit the per-statement presentation trees, which print by default)",
  )
  parser.add_argument(
    "--keep",
    action="store_true",
    help="leave the throwaway reference tenant schema in place (debugging)",
  )
  args = parser.parse_args(argv)

  if args.coverage:
    graphs = load_config_graphs()
    if not graphs:
      print("No graphs found in .local/config.json — run a demo to load some.")
      return 1
    print(format_coverage(run_coverage(graphs)))
    return 0

  with reference_tenant(keep=args.keep) as rt:
    with extensions_session(rt.graph_id) as session:
      report = run_structural(session, rt.styles)
      run_package_integrity(session, report)
      if not args.summary:
        print(render_trees(session, rt.styles))
        print()
      print(report.format_text())
  return 0 if report.ok() else 1


if __name__ == "__main__":
  sys.exit(main())
