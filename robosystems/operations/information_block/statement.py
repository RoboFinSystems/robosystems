"""Envelope builder for the statement family of Information Block types.

The statement block types share one builder parameterised on block_type.
Their Structures are library-seeded; tenant facts come from report sets, and
the envelope carries a server-computed ``view.rendering`` grid (rows,
periods, validation). Statements aren't created via
``create-information-block``: their create/update/delete slots are
not-implemented stubs.
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
  window_series_sets,
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
  series_history: int | None = None,
  series_forecast: int | None = None,
  *,
  block_type: str,
) -> InformationBlockEnvelope | None:
  """Pack the Information Block envelope for a statement-family block.

  ``None`` when the structure doesn't exist or isn't ``block_type``.
  Without ``series``, facts come from one set: ``fact_set_id`` if pinned,
  else the latest set (a scenario's latest forecast month when
  ``scenario_id`` is given, with columns labelled as forecast).

  ``series=True`` (ignored when ``fact_set_id`` is pinned) renders one
  column per period across the whole report-set series, actuals preferred
  at the seam. ``series_history`` / ``series_forecast`` trim it to the last
  N actual and first N forecast columns before facts load; ``None`` is
  unbounded.
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
    series_sets = window_series_sets(series_sets, series_history, series_forecast)
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
      # Window each fact to its own set's period: annual sets carry
      # comparative-period facts that belong to other columns.
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

  # Un-enriched library rows have no mechanics; an empty tagged body keeps
  # the discriminated union valid.
  if structure.artifact_mechanics:
    mechanics = StatementMechanics.model_validate(structure.artifact_mechanics)
  else:
    mechanics = StatementMechanics(kind="statement_renderer")

  rendering = _build_statement_rendering(
    session,
    elements=atoms.elements,
    associations=atoms.associations,
    facts=facts,
    structure_id=structure.id,
    block_type=block_type,
    concept_arrangement=structure.concept_arrangement,
  )

  # Mark forecast columns: in series mode, those whose winning set is a
  # scenario set; in single-set scenario mode, every column.
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

  # Types without a display entry (regulatory_disclosure) use the
  # structure's own name.
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


def _forecast_period_label(period_end: date) -> str:
  """Full column label, e.g. ``"Mar 2027 (forecast)"``; actual columns keep
  ``label=None``."""
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
  """Compute the Rendering view projection for a statement-family block,
  using ``fact_grid``'s rollup and ``guard_rails``' validation. No facts
  yields empty rows and periods."""
  if not facts:
    return RenderingLite(rows=[], periods=[], validation=None, unmapped_count=0)

  element_ids = [e.id for e in elements]
  classification_by_id = _load_element_classifications(session, element_ids)
  elements_by_id = {e.id: e for e in elements}

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

  # Facts are natural-signed at write time, hence pre_signed=True below.
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

  hierarchy = _build_hierarchy_from_atoms(
    structure_id, elements_by_id, classification_by_id, associations
  )
  if not hierarchy:
    return RenderingLite(rows=[], periods=[], validation=None, unmapped_count=0)

  # ``arithmetic`` disclosures keep their calc arcs in a sibling
  # calculation structure, so compose the calc DAG cross-structure; other
  # CAPs carry calc arcs in the block's own associations.
  if concept_arrangement == "arithmetic":
    calculations = _load_calculations(
      session, element_ids=_collect_hierarchy_element_ids(hierarchy)
    )
  else:
    calculations = _calculations_from_associations(associations)

  period_balances = [
    _facts_to_balance_dict_for_period(report_facts, p.start, p.end) for p in periods
  ]

  rows: list[FactRow] = _build_rows(
    hierarchy, period_balances, calculations, pre_signed=True
  )

  # Columns have no display label here, so findings name them by period end.
  validation_result = validate_report(
    block_type, rows, period_labels=[p.end.isoformat() for p in periods]
  )

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
      status=validation_result.status,
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
  """Build a `_Balance` dict for one period from natural-signed facts.

  Multiple facts on one element sum rather than overwrite: the stamped CF
  carries a derived ΔWC fact plus the cash-reconciliation plug on the same
  operating leaf, and dropping the plug stops the section footing.
  """
  balances: dict[str, _Balance] = {}
  for fact in facts:
    if fact.period_start == period_start and fact.period_end == period_end:
      existing = balances.get(fact.element_id)
      if existing is None:
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
      else:
        existing.net_balance += fact.value
  return balances


def _build_hierarchy_from_atoms(
  structure_id: str,
  elements_by_id: dict[str, Element],
  classification_by_id: dict[str, str],
  associations: list[Association],
) -> list[_HierarchyNode]:
  """Build the presentation hierarchy from already-loaded atoms, no SQL.

  Roots are either the children of an arc anchored at ``structure_id``
  (seed.py convention) or, failing that, the XBRL-standard abstract roots
  (elements that are arc sources but never targets).
  """
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

  root_ids = [child_id for _, child_id in children_by_parent.get(structure_id, [])]

  if not root_ids:
    from_ids = set(children_by_parent.keys())
    to_ids = {c for children in children_by_parent.values() for _, c in children}
    root_ids = list(from_ids - to_ids)

  # Sort roots by qname for a deterministic order (puts Assets before
  # LiabilitiesAndStockholdersEquity on the BS).
  root_ids.sort(
    key=lambda rid: (
      elements_by_id[rid].qname
      if rid in elements_by_id and elements_by_id[rid].qname
      else rid
    )
  )

  # The presentation hierarchy is a DAG; render each element once (first
  # parent wins) or shared subtrees expand under every parent and
  # double-count.
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
  """Project calculation associations into the dict shape `_build_rows` expects."""
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
  """Primary FASB elementsOfFinancialStatements (SFAC 6) trait per element,
  e.g. ``'asset'``; elements with none are absent."""
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
  """Build the envelope handler for one statement type."""
  return partial(_build_statement_envelope, block_type=block_type)


__all__ = [
  "STATEMENT_CATEGORY",
  "STATEMENT_DISPLAY",
  "make_statement_handlers",
]
