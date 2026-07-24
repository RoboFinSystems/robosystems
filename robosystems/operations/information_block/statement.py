"""Handlers for the statement family of Information Block types.

Four block types — ``balance_sheet``, ``income_statement``,
``cash_flow_statement``, ``equity_statement`` — share one envelope
builder parameterised on the block_type string. Each corresponds to a
library-seeded Structure in ``public.structures``; seeds live in
:mod:`robosystems.taxonomy.seed` (``seed_reporting_taxonomy``).

Statements exercise the **compositional** construction mode: the
Structure exists before any tenant action; per-tenant facts come from
``create-report`` calls; the envelope materialises on GET by pulling
the library atoms together with the tenant's most-recent report facts.

Statements aren't created via ``create-information-block``; the
``dispatch_create``/``update``/``delete`` handlers in the registry
entry are the not-implemented stubs built by
``make_not_implemented_handler``.

**Rendering projection.** As of Plan B (2026-04-25) the envelope
populates ``view.rendering`` with the server-computed statement grid
(rows + periods + validation). This replaces the legacy
``getStatement(reportId, blockType)`` REST path: frontend
``BlockView`` consumes ``envelope.view.rendering`` directly, no
client-side rollup or hierarchy walk needed. The pure in-memory rollup
helpers (``_build_rows``, ``_facts_to_balance_dict``, ``_natural_sign``)
are imported from
:mod:`robosystems.operations.roboledger.reports.fact_grid`; the
hierarchy + classifications are derived from the already-loaded
envelope atoms, and calculations compose cross-structure for
``arithmetic`` blocks (see ``_build_statement_rendering``).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from functools import partial
from typing import TYPE_CHECKING

from sqlalchemy import select, text

from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  RenderingLite,
  RenderingPeriodLite,
  RenderingRowLite,
  StatementMechanics,
  ValidationLite,
  ViewProjections,
)
from robosystems.models.extensions import Association, Element
from robosystems.models.extensions.roboledger import Fact
from robosystems.operations.information_block.envelope import (
  association_to_connection,
  elements_to_lites,
  fact_to_lite,
  load_base_envelope_atoms,
  load_disclosure_id_for_structure,
  load_statement_fact_set_series,
)
from robosystems.operations.roboledger.reports.fact_grid import (
  FactRow,
  PeriodSpec,
  ReportFact,
  _Balance,  # type: ignore[reportPrivateUsage]
  _build_rows,  # type: ignore[reportPrivateUsage]
  _collect_hierarchy_element_ids,  # type: ignore[reportPrivateUsage]
  _HierarchyNode,  # type: ignore[reportPrivateUsage]
  _load_calculations,  # type: ignore[reportPrivateUsage]
)
from robosystems.operations.roboledger.reports.guard_rails import validate_report

if TYPE_CHECKING:
  from sqlalchemy.orm import Session


# block_type → (display_name, display_plural)
STATEMENT_DISPLAY: dict[str, tuple[str, str]] = {
  "balance_sheet": ("Balance Sheet", "Balance Sheets"),
  "income_statement": ("Income Statement", "Income Statements"),
  "cash_flow_statement": ("Cash Flow Statement", "Cash Flow Statements"),
  "equity_statement": ("Equity Statement", "Equity Statements"),
  "comprehensive_income": (
    "Statement of Comprehensive Income",
    "Statements of Comprehensive Income",
  ),
}

# All five statement block types share the same sidebar category.
STATEMENT_CATEGORY = "Reporting"


def _build_statement_envelope(
  session: Session,
  structure_id: str,
  fact_set_id: str | None = None,
  scenario_id: str | None = None,
  series: bool = False,
  *,
  block_type: str,
) -> InformationBlockEnvelope | None:
  """Pack the Information Block envelope for a statement-family block.

  Returns ``None`` when the structure doesn't exist or is not the
  expected block_type — lets :func:`get_information_block` cleanly
  return nothing to the caller.

  **Read path (Plan B, Apr 2026).** Facts are loaded by `fact_set_id` —
  the canonical Block-instance pin per Charlie's PDF synonymy ("Block
  and Fact Set are synonyms"). ``load_base_envelope_atoms`` already
  loads the latest FactSet for this Structure (ordered by
  ``period_end`` desc, ``created_at`` desc); we filter facts by that
  FactSet's id. On the library sentinel and on tenant graphs with no
  generated reports the FactSet is null and ``facts`` comes back empty
  — the correct behaviour for both. This replaces the prior
  "latest Report touching these elements" heuristic; writes have
  stamped both ``report_id`` and ``fact_set_id`` since Phase gamma.1 so
  the switch is a strict simplification (one less query per envelope).

  **Scenario slice.** ``scenario_id=None`` binds actuals (the
  scenario-pinned latest-set read); a non-None value binds the
  scenario's latest computed forecast month and every rendered period
  column carries a ``"... (forecast)"`` label so the surface is honest
  about what it shows.

  **Series mode (F-4).** ``series=True`` binds the structure's WHOLE
  report-set series instead of one set — one rendered column per
  period, actuals-preferred at the seam when a scenario is selected
  (:func:`envelope.load_statement_fact_set_series`). This is the
  statement analog of the metric time series and the data spine of the
  Plan grid. Facts are windowed per set to the set's own period so an
  annual report's comparative facts can't leak into a monthly column.
  An explicit ``fact_set_id`` pin bypasses series mode (a snapshot is
  a single set by definition).
  """
  atoms = load_base_envelope_atoms(
    session,
    structure_id,
    expected_block_type=block_type,
    fact_set_id=fact_set_id,
    scenario_id=scenario_id,
  )
  if atoms is None:
    return None

  structure = atoms.structure
  element_ids = atoms.element_ids

  series_mode = series and fact_set_id is None
  forecast_period_ends: set[date] = set()
  facts: list[Fact] = []
  if element_ids and series_mode:
    series_sets = load_statement_fact_set_series(session, structure_id, scenario_id)
    forecast_period_ends = {
      fs.period_end for fs in series_sets if fs.scenario_id is not None
    }
    if series_sets:
      all_facts = (
        session.execute(
          select(Fact).where(
            Fact.fact_set_id.in_([fs.id for fs in series_sets]),
            Fact.element_id.in_(element_ids),
          )
        )
        .scalars()
        .all()
      )
      # Window each fact to its OWN set's period — a set may carry
      # comparative-period facts (annual reports do) and those belong
      # to another column, not this one.
      window_by_set = {fs.id: (fs.period_start, fs.period_end) for fs in series_sets}
      for f in all_facts:
        window = window_by_set.get(f.fact_set_id or "")
        if window is None:
          continue
        set_start, set_end = window
        if f.period_end != set_end:
          continue
        if (
          f.period_start is not None
          and set_start is not None
          and f.period_start < set_start
        ):
          continue
        facts.append(f)
  elif element_ids and atoms.fact_set is not None:
    facts = list(
      session.execute(
        select(Fact).where(
          Fact.fact_set_id == atoms.fact_set.id,
          Fact.element_id.in_(element_ids),
        )
      )
      .scalars()
      .all()
    )

  # Mechanics are read from the typed ``artifact_mechanics`` column when
  # populated; library-seeded rows that haven't been enriched fall back
  # to an empty tagged body so the discriminated union still validates.
  if structure.artifact_mechanics:
    mechanics = StatementMechanics.model_validate(structure.artifact_mechanics)
  else:
    mechanics = StatementMechanics(kind="statement_renderer")

  # Compute the Rendering view projection from already-loaded atoms +
  # the loaded facts. Adds one classification lookup query; everything
  # else (hierarchy, calculations, root order) is derived in-memory.
  rendering = _build_statement_rendering(
    session,
    elements=atoms.elements,
    associations=atoms.associations,
    facts=facts,
    structure_id=structure.id,
    block_type=block_type,
    concept_arrangement=structure.concept_arrangement,
  )

  # Stamp forecast columns — label + machine-readable flag — so tables
  # and chart axes read honestly. Series mode marks exactly the columns
  # whose winning set is a scenario set (the seam falls out of the
  # actuals-preferred collapse); single-set scenario mode marks every
  # column of the bound forecast month.
  if series_mode and forecast_period_ends:
    rendering = rendering.model_copy(
      update={
        "periods": [
          p.model_copy(
            update={"label": _forecast_period_label(p.end), "forecast": True}
          )
          if p.end in forecast_period_ends
          else p
          for p in rendering.periods
        ]
      }
    )
  elif (
    scenario_id is not None
    and atoms.fact_set is not None
    and atoms.fact_set.scenario_id == scenario_id
  ):
    rendering = rendering.model_copy(
      update={
        "periods": [
          p.model_copy(
            update={"label": _forecast_period_label(p.end), "forecast": True}
          )
          for p in rendering.periods
        ]
      }
    )

  # Statement-family types carry fixed display strings; block types that
  # share this builder without an entry (regulatory_disclosure) display
  # as the structure's own name — a disclosure note is named by its
  # author/taxonomy, not by its type.
  display_name, _display_plural = STATEMENT_DISPLAY.get(
    block_type, (structure.name, structure.name)
  )
  disclosure_id = load_disclosure_id_for_structure(session, structure.id)
  _elements_by_id = {e.id: e for e in atoms.elements}
  return InformationBlockEnvelope(
    id=structure.id,
    block_type=block_type,
    name=structure.name,
    display_name=display_name,
    category=STATEMENT_CATEGORY,
    taxonomy_id=structure.taxonomy_id,
    taxonomy_name=atoms.taxonomy_name,
    disclosure_id=disclosure_id,
    information_model=InformationModelResponse(
      concept_arrangement=structure.concept_arrangement or "roll_up",
      member_arrangement=structure.member_arrangement or "whole_part",
    ),
    artifact=ArtifactResponse(
      topic=structure.description,
      renderer_note=structure.renderer_note,
      template=None,
      mechanics=mechanics,
    ),
    elements=elements_to_lites(session, atoms.elements),
    connections=[
      association_to_connection(a, atoms.classifications_by_assoc.get(a.id, []))
      for a in atoms.associations
    ],
    facts=[fact_to_lite(f, _elements_by_id) for f in facts],
    rules=atoms.rules,
    fact_set=atoms.fact_set,
    verification_results=atoms.verification_results,
    verification_summary=atoms.verification_summary,
    view=ViewProjections(rendering=rendering),
  )


# ── Rendering projection — server-side computed at envelope build ─────────


def _forecast_period_label(period_end: date) -> str:
  """Display-ready column label for a forecast period — ``"Mar 2027 (forecast)"``.

  The full label (month + marker), not a bare suffix, so any consumer —
  table header, chart axis — can use it verbatim without composing.
  Actual columns keep ``label=None`` and format their own dates as today.
  """
  return f"{period_end.strftime('%b %Y')} (forecast)"


def _build_statement_rendering(
  session: Session,
  *,
  elements: list[Element],
  associations: list[Association],
  facts: list[Fact],
  structure_id: str,
  block_type: str,
  concept_arrangement: str | None = None,
) -> RenderingLite:
  """Compute the Rendering view projection for a statement-family block.

  Reuses the pure in-memory rollup logic from
  :mod:`robosystems.operations.roboledger.reports.fact_grid` (``_build_rows``)
  and the guard-rail checks from
  :mod:`robosystems.operations.roboledger.reports.guard_rails`
  (``validate_report``). The hierarchy is derived from the already-loaded
  ``associations``; classifications are fetched in a single trait-axis
  query. Calculations follow the block's concept arrangement:
  ``arithmetic`` composes the calc DAG cross-structure in one query
  (calc arcs live in sibling calculation structures, not in the
  presentation block's own associations); other CAPs derive them from
  the in-envelope associations with no extra SQL.

  Empty-fact case: returns ``RenderingLite`` with an empty ``rows`` list
  and an empty ``periods`` list — the envelope still validates and the
  frontend renders an empty state.
  """
  # 1) Empty-fact short-circuit — no rendering work to do, no
  # classification query needed.
  if not facts:
    return RenderingLite(rows=[], periods=[], validation=None, unmapped_count=0)

  # 2) Look up element metadata + per-element classification (FASB
  # elementsOfFinancialStatements trait axis). Single batched query.
  element_ids = [e.id for e in elements]
  classification_by_id = _load_element_classifications(session, element_ids)
  elements_by_id = {e.id: e for e in elements}

  # 3) Derive periods from facts. Order by (period_end, period_start) so
  # comparative columns appear in chronological order.
  period_keys: set[tuple[date, date]] = set()
  for f in facts:
    pe = f.period_end
    ps = f.period_start if f.period_start is not None else pe
    period_keys.add((ps, pe))
  if not period_keys:
    return RenderingLite(rows=[], periods=[], validation=None, unmapped_count=0)

  ordered_periods = sorted(period_keys, key=lambda pk: (pk[1], pk[0]))
  periods: list[PeriodSpec] = [
    PeriodSpec(start=ps, end=pe, label="") for ps, pe in ordered_periods
  ]

  # 3) Convert envelope Fact ORM rows to ReportFact dataclasses (the
  # shape ``_build_rows`` consumes downstream). Facts already carry
  # natural-sign values from generate_report_facts at write time, so
  # _build_rows is called with pre_signed=True.
  report_facts: list[ReportFact] = []
  for f in facts:
    elem = elements_by_id.get(f.element_id)
    if elem is None:
      continue
    if f.value is None:
      # Nonnumeric (text-block) facts have no place in the numeric grid.
      continue
    pe = f.period_end
    ps = f.period_start if f.period_start is not None else pe
    report_facts.append(
      ReportFact(
        element_id=f.element_id,
        element_qname=elem.qname or "",
        element_name=elem.name,
        classification=classification_by_id.get(f.element_id, ""),
        balance_type=elem.balance_type or "debit",
        value=float(f.value),
        period_start=ps,
        period_end=pe,
        period_type=f.period_type,
      )
    )

  # 4) Build hierarchy from already-loaded associations.
  hierarchy = _build_hierarchy_from_atoms(
    structure_id, elements_by_id, classification_by_id, associations
  )
  if not hierarchy:
    return RenderingLite(rows=[], periods=[], validation=None, unmapped_count=0)

  # Calculation arcs — mirrors ``render_structure_view``. ``arithmetic``
  # Disclosures keep presentation and calculation in SEPARATE structures
  # (e.g. the rs-gaap Multi-step presentation block carries only
  # presentation arcs; its calc DAG lives in the sibling "… calculation"
  # structure), so the block's own associations can't identify Gross
  # Profit / Operating Income as calc-target subtotals. Compose the calc
  # DAG cross-structure in that case; other CAPs keep the in-envelope
  # associations (single-structure convention, no extra SQL).
  if concept_arrangement == "arithmetic":
    calculations = _load_calculations(
      session, element_ids=_collect_hierarchy_element_ids(hierarchy)
    )
  else:
    calculations = _calculations_from_associations(associations)

  # 5) Per-period balance dicts (one per column).
  period_balances = [
    _facts_to_balance_dict_for_period(report_facts, p.start, p.end) for p in periods
  ]

  # 6) The pure in-memory rollup walker — produces FactRow per row with
  # depth + is_subtotal + per-period values.
  rows: list[FactRow] = _build_rows(
    hierarchy, period_balances, calculations, pre_signed=True
  )

  # 7) Validation — guard-rail checks on the rendered rows.
  validation_result = validate_report(block_type, rows)

  return RenderingLite(
    rows=[
      RenderingRowLite(
        element_id=r.element_id,
        element_qname=r.element_qname,
        element_name=r.element_name,
        classification=r.classification or None,
        balance_type=r.balance_type,
        values=list(r.values),
        is_subtotal=r.is_subtotal,
        depth=r.depth,
      )
      for r in rows
    ],
    periods=[
      RenderingPeriodLite(start=p.start, end=p.end, label=p.label or None)
      for p in periods
    ],
    validation=ValidationLite(
      passed=validation_result.passed,
      checks=list(validation_result.checks),
      failures=list(validation_result.failures),
      warnings=list(validation_result.warnings),
    ),
    unmapped_count=0,
  )


def _facts_to_balance_dict_for_period(
  facts: list[ReportFact],
  period_start: date,
  period_end: date,
) -> dict[str, _Balance]:
  """Build a `_Balance` dict for one period from in-memory `ReportFact` rows.

  Mirrors :func:`fact_grid._facts_to_balance_dict` but lives here so the
  envelope rendering path stays self-contained. Facts are already
  natural-signed by ``generate_report_facts``, so ``balance_type`` is
  set to "debit" to keep ``_build_rows`` from re-applying sign
  conversion (it dispatches on balance_type when ``pre_signed=False``;
  with ``pre_signed=True`` the balance is returned as-is).
  """
  balances: dict[str, _Balance] = {}
  for fact in facts:
    if fact.period_start == period_start and fact.period_end == period_end:
      balances[fact.element_id] = _Balance(
        element_id=fact.element_id,
        qname=fact.element_qname,
        name=fact.element_name,
        classification=fact.classification,
        balance_type="debit",
        total_debits=0.0,
        total_credits=0.0,
        net_balance=fact.value,
      )
  return balances


def _build_hierarchy_from_atoms(
  structure_id: str,
  elements_by_id: dict[str, Element],
  classification_by_id: dict[str, str],
  associations: list[Association],
) -> list[_HierarchyNode]:
  """Build the presentation hierarchy from already-loaded atoms.

  Replaces the SQL-heavy ``fact_grid._load_reporting_structure``: works
  off the associations + elements that ``load_base_envelope_atoms``
  already loaded, no extra queries.

  Two root-anchor conventions are supported:

  - **structure_id-rooted** (seed.py convention): presentation arcs with
    ``from_element_id == structure_id`` mark the top of the tree; the
    structure itself is the implicit root and its children become the
    rendered top-level rows.
  - **abstract-element-rooted** (XBRL standard, used by FAC / rs-gaap /
    rs-gaap-type-subtype reference taxonomies): an abstract element (e.g.
    ``fac:BalanceSheetAbstract``) is the top of the tree; the structure
    itself doesn't appear in any presentation arc. Detected by
    ``from_set - to_set`` — elements that appear as ``from_element_id``
    but never as ``to_element_id``.

  Children are walked depth-first via the parent → child mapping,
  sorted by ``order_value``.
  """
  # Presentation children grouped by parent_id (preserving order_value).
  children_by_parent: dict[str, list[tuple[float, str]]] = {}
  for a in associations:
    if a.association_type != "presentation":
      continue
    if a.from_element_id is None or a.to_element_id is None:
      continue
    order = a.order_value if a.order_value is not None else float("inf")
    children_by_parent.setdefault(a.from_element_id, []).append(
      (order, a.to_element_id)
    )

  for parent_id in children_by_parent:
    children_by_parent[parent_id].sort(key=lambda pair: pair[0])

  # Roots = children of the structure_id anchor (seed.py convention).
  root_ids = [child_id for _, child_id in children_by_parent.get(structure_id, [])]

  # Fallback: structures from FAC / rs-gaap / rs-gaap-type-subtype reference
  # taxonomies anchor at an abstract element instead of the structure_id.
  # Detect roots via the standard XBRL convention — elements that appear
  # as ``from_element_id`` but never as ``to_element_id``.
  if not root_ids:
    from_ids = set(children_by_parent.keys())
    to_ids = {c for children in children_by_parent.values() for _, c in children}
    root_ids = list(from_ids - to_ids)

  # Sort roots by qname so the visible order is deterministic and
  # alphabetic — this puts ``rs-gaap:Assets`` before
  # ``rs-gaap:LiabilitiesAndStockholdersEquity`` on the BS, matching
  # standard financial-statement convention. Element IDs are random
  # UUIDs and would otherwise sort by hash.
  root_ids.sort(
    key=lambda rid: (
      elements_by_id[rid].qname
      if rid in elements_by_id and elements_by_id[rid].qname
      else rid
    )
  )

  # Render each element at most once globally per structure walk. The
  # rs-gaap-presentation hierarchy is a DAG (a concept may have
  # multiple parents — e.g. "Cash" rolls up under both Current Assets
  # and the Cash Flow reconciliation). Without global dedup the walk
  # would expand a shared subtree under each parent, producing
  # exponential row counts and double-counting facts at render time.
  # The first parent that reaches a node owns it; subsequent parents
  # treat the node as already-rendered. Mirrors the same fix in
  # ``fact_grid._build_tree``.
  emitted: set[str] = set()

  def _make_node(element_id: str, depth: int) -> _HierarchyNode | None:
    if element_id in emitted:
      return None
    emitted.add(element_id)
    elem = elements_by_id.get(element_id)
    node = _HierarchyNode(
      element_id=element_id,
      qname=(elem.qname if elem is not None else None) or "",
      name=elem.name if elem is not None else "",
      classification=classification_by_id.get(element_id, ""),
      balance_type=(elem.balance_type if elem is not None else None) or "debit",
      is_abstract=bool(elem.is_abstract) if elem is not None else False,
      depth=depth,
    )
    for _order, child_id in children_by_parent.get(element_id, []):
      # Skip self-references (defensive — root anchors live above).
      if child_id == element_id:
        continue
      child_node = _make_node(child_id, depth + 1)
      if child_node is not None:
        node.children.append(child_node)
    return node

  roots: list[_HierarchyNode] = []
  for rid in root_ids:
    root_node = _make_node(rid, 0)
    if root_node is not None:
      roots.append(root_node)
  return roots


def _calculations_from_associations(
  associations: list[Association],
) -> dict[str, list[tuple[str, float]]]:
  """Project calculation associations into the dict shape `_build_rows` expects.

  Mirrors :func:`fact_grid._load_calculations` but works off the already-
  loaded associations list — no SQL.
  """
  calculations: dict[str, list[tuple[str, float]]] = {}
  ordered = sorted(
    associations,
    key=lambda a: a.order_value if a.order_value is not None else float("inf"),
  )
  for a in ordered:
    if a.association_type != "calculation":
      continue
    if a.from_element_id is None or a.to_element_id is None:
      continue
    weight = a.weight if a.weight is not None else 1.0
    calculations.setdefault(a.from_element_id, []).append((a.to_element_id, weight))
  return calculations


def _load_element_classifications(
  session: Session, element_ids: list[str]
) -> dict[str, str]:
  """Fetch primary FASB elementsOfFinancialStatements trait per element.

  Returns ``{element_id: identifier}`` where identifier is one of
  ``'asset'`` / ``'liability'`` / ``'equity'`` / ``'revenue'`` /
  ``'expense'`` (the FASB SFAC 6 axis). Elements with no primary trait
  on this axis are absent from the dict — callers default to the empty
  string for the resulting :class:`RenderingRowLite.classification`.

  Single batched query; mirrors the LEFT JOIN that the legacy
  :func:`fact_grid._load_reporting_structure` does inline, but separates
  the trait lookup from the hierarchy walk.
  """
  if not element_ids:
    return {}
  placeholders = ", ".join(f":e{i}" for i in range(len(element_ids)))
  params = {f"e{i}": eid for i, eid in enumerate(element_ids)}
  rows = session.execute(
    text(f"""
      SELECT et.element_id, cls.identifier
      FROM element_traits et
      JOIN classifications cls
        ON cls.id = et.trait_id
        AND cls.category = 'elementsOfFinancialStatements'
      WHERE et.is_primary = TRUE
        AND et.element_id IN ({placeholders})
    """),
    params,
  ).all()
  return {row.element_id: row.identifier for row in rows}


def make_statement_handlers(
  block_type: str,
) -> Callable[..., InformationBlockEnvelope | None]:
  """Build the envelope handler for one statement type.

  ``functools.partial`` binds the ``block_type`` keyword on the envelope
  builder so the registry entry holds a callable matching
  :class:`BlockTypeRegistryEntry.dispatch_build_envelope`'s signature
  ``(session, structure_id, fact_set_id?=None) -> envelope | None``.
  The ``fact_set_id`` arg is used by Report Block rehydration to pin
  the envelope to a specific FactSet snapshot.

  The create / update / delete handlers for statement block types are
  not built here — the registry installs not-implemented stubs via
  ``make_not_implemented_handler`` for those slots.
  """
  return partial(_build_statement_envelope, block_type=block_type)


__all__ = [
  "STATEMENT_CATEGORY",
  "STATEMENT_DISPLAY",
  "make_statement_handlers",
]
