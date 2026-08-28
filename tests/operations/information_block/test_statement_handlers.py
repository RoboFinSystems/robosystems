"""Statement handler tests — dispatch_create + dispatch_build_envelope.

Four statement block types (balance_sheet, income_statement,
cash_flow_statement, equity_statement) share one handler body
parameterised on block_type via ``make_statement_handlers``. These
tests exercise both dispatch points with a mocked SQLAlchemy session.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock

import pytest

from robosystems.operations.information_block import statement as statement_handlers
from robosystems.operations.information_block.registry import REGISTRY


def _exec_result(
  *,
  scalars_all: list[Any] | None = None,
  scalar: Any = None,
  all_rows: list[Any] | None = None,
) -> MagicMock:
  """Build a MagicMock shaped like a SQLAlchemy Result object.

  ``all_rows`` covers the ``.all()`` path used by the association-
  classifications loader, which queries tuples rather than scalars.
  """
  result = MagicMock()
  if scalars_all is not None:
    result.scalars.return_value.all.return_value = scalars_all
  if all_rows is not None:
    result.all.return_value = all_rows
  if scalar is not None:
    result.scalar.return_value = scalar
  else:
    result.scalar.return_value = None
  return result


def _make_statement_structure(
  *,
  structure_id: str,
  block_type: str,
  name: str,
  description: str | None = None,
  taxonomy_id: str = "tax_usgaap",
  artifact_mechanics: dict[str, Any] | None = None,
  concept_arrangement: str | None = "roll_up",
  member_arrangement: str | None = "whole_part",
) -> MagicMock:
  """Shape a MagicMock like a Structure row for a statement block.

  Pins every field the envelope builder touches so MagicMock's
  auto-attribute behaviour doesn't inject unwanted truthy values.
  """
  structure = MagicMock()
  structure.id = structure_id
  structure.block_type = block_type
  structure.name = name
  structure.description = description
  structure.taxonomy_id = taxonomy_id
  structure.artifact_mechanics = (
    artifact_mechanics
    if artifact_mechanics is not None
    else {"kind": "statement_renderer"}
  )
  structure.concept_arrangement = concept_arrangement
  structure.member_arrangement = member_arrangement
  structure.renderer_note = None
  return structure


class TestCreate:
  def test_create_raises_not_implemented_with_create_report_pointer(self) -> None:
    entry = REGISTRY["balance_sheet"]
    with pytest.raises(NotImplementedError) as exc:
      entry.dispatch_create(MagicMock(), MagicMock(), "usr_test")
    assert "create-report" in str(exc.value)

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_every_statement_type_raises_not_implemented_on_create(
    self, block_type: str
  ) -> None:
    """All four statement block types share the same 501-producing create handler."""
    entry = REGISTRY[block_type]
    with pytest.raises(NotImplementedError, match="create-report"):
      entry.dispatch_create(MagicMock(), MagicMock(), "usr_test")


class TestUpdate:
  def test_update_raises_not_implemented(self) -> None:
    entry = REGISTRY["balance_sheet"]
    with pytest.raises(NotImplementedError) as exc:
      entry.dispatch_update(MagicMock(), MagicMock(), "usr_test")
    # Message points callers at the actionable workaround — regenerate the
    # underlying Report — until standalone update ships.
    assert "regenerate-report" in str(exc.value)

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_every_statement_type_raises_not_implemented_on_update(
    self, block_type: str
  ) -> None:
    entry = REGISTRY[block_type]
    with pytest.raises(NotImplementedError):
      entry.dispatch_update(MagicMock(), MagicMock(), "usr_test")


class TestDelete:
  def test_delete_raises_not_implemented(self) -> None:
    entry = REGISTRY["balance_sheet"]
    with pytest.raises(NotImplementedError) as exc:
      entry.dispatch_delete(MagicMock(), MagicMock(), "usr_test")
    assert "library-seeded" in str(exc.value)

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_every_statement_type_raises_not_implemented_on_delete(
    self, block_type: str
  ) -> None:
    entry = REGISTRY[block_type]
    with pytest.raises(NotImplementedError):
      entry.dispatch_delete(MagicMock(), MagicMock(), "usr_test")


class TestBuildEnvelope:
  def test_returns_none_when_structure_missing(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    build = statement_handlers.make_statement_handlers("balance_sheet")
    assert build(session, "struct_missing") is None

  def test_returns_none_when_structure_is_wrong_block_type(self) -> None:
    """A ``schedule`` structure must not surface through the BS handler."""
    session = MagicMock()
    structure = MagicMock()
    structure.block_type = "schedule"
    session.get.return_value = structure
    build = statement_handlers.make_statement_handlers("balance_sheet")
    assert build(session, "struct_other") is None

  def test_returns_envelope_with_empty_facts_when_no_reports_exist(self) -> None:
    """Library sentinel path: seeded atoms present, no tenant reports."""
    session = MagicMock()
    structure = _make_statement_structure(
      structure_id="struct_balance_sheet",
      block_type="balance_sheet",
      name="Balance Sheet",
      description="Assets + Liabilities + Equity",
    )
    session.get.return_value = structure
    # Query order (no associations → no elements or classifications queries):
    # fact_set (validated first so a stale pin doesn't pay for atom loads)
    # → taxonomy name → associations → rules → verification_results
    session.execute.side_effect = [
      _exec_result(scalar=None),  # latest fact set → None
      _exec_result(scalar="US GAAP"),  # taxonomy name
      _exec_result(scalars_all=[]),  # associations
      _exec_result(scalars_all=[]),  # rules
      _exec_result(scalars_all=[]),  # verification results
    ]

    build = statement_handlers.make_statement_handlers("balance_sheet")
    envelope = build(session, "struct_balance_sheet")

    assert envelope is not None
    assert envelope.id == "struct_balance_sheet"
    assert envelope.block_type == "balance_sheet"
    assert envelope.name == "Balance Sheet"
    assert envelope.display_name == "Balance Sheet"
    assert envelope.category == "Reporting"
    assert envelope.taxonomy_id == "tax_usgaap"
    assert envelope.taxonomy_name == "US GAAP"
    assert envelope.information_model.concept_arrangement == "roll_up"
    assert envelope.information_model.member_arrangement == "whole_part"
    assert envelope.artifact.topic == "Assets + Liabilities + Equity"
    assert envelope.artifact.mechanics.kind == "statement_renderer"
    assert envelope.elements == []
    assert envelope.connections == []
    assert envelope.facts == []
    assert envelope.rules == []
    assert envelope.dimensions == []
    assert envelope.fact_set is None
    assert envelope.verification_results == []

  def test_loads_elements_and_associations_from_library_seed(self) -> None:
    session = MagicMock()
    structure = _make_statement_structure(
      structure_id="struct_income_statement",
      block_type="income_statement",
      name="Income Statement",
    )
    session.get.return_value = structure

    association = MagicMock()
    association.id = "assoc_1"
    association.from_element_id = "elem_revenue"
    association.to_element_id = "elem_sales"
    association.association_type = "presentation"
    association.arcrole = "http://…/parent-child"
    association.order_value = 1.0
    association.weight = None

    element_revenue = MagicMock()
    element_revenue.id = "elem_revenue"
    element_revenue.qname = "us-gaap:Revenues"
    element_revenue.name = "Revenues"
    element_revenue.code = None
    element_revenue.element_type = "abstract"
    element_revenue.is_abstract = True
    element_revenue.is_monetary = True
    element_revenue.balance_type = "credit"
    element_revenue.period_type = "duration"
    element_revenue.item_type = None

    element_sales = MagicMock()
    element_sales.id = "elem_sales"
    element_sales.qname = "us-gaap:SalesRevenueNet"
    element_sales.name = "Sales"
    element_sales.code = None
    element_sales.element_type = "concept"
    element_sales.is_abstract = False
    element_sales.is_monetary = True
    element_sales.balance_type = "credit"
    element_sales.period_type = "duration"
    element_sales.item_type = None

    # Query order (1 association → elements + classifications queries run):
    # fact_set → taxonomy → associations → elements → rules → classifications →
    # verification_results → documentation labels
    session.execute.side_effect = [
      _exec_result(scalar=None),  # latest fact set → None
      _exec_result(scalar="US GAAP"),  # taxonomy name
      _exec_result(scalars_all=[association]),  # associations
      _exec_result(scalars_all=[element_revenue, element_sales]),  # elements
      _exec_result(scalars_all=[]),  # rules
      _exec_result(all_rows=[]),  # association classifications
      _exec_result(scalars_all=[]),  # verification results
      _exec_result(all_rows=[]),  # documentation labels
    ]

    build = statement_handlers.make_statement_handlers("income_statement")
    envelope = build(session, "struct_income_statement")

    assert envelope is not None
    assert envelope.block_type == "income_statement"
    assert len(envelope.connections) == 1
    assert envelope.connections[0].from_element_id == "elem_revenue"
    assert len(envelope.elements) == 2
    assert {e.id for e in envelope.elements} == {"elem_revenue", "elem_sales"}
    assert envelope.facts == []

  def test_facts_populated_from_latest_fact_set(self) -> None:
    """Plan B: envelope reads facts via the canonical fact_set_id pin.

    The previous behaviour scanned for the latest Report touching any of
    the structure's elements and filtered by report_id. After Plan B
    (Apr 2026) the read path uses the FactSet (= the Block instance per
    Charlie's PDF) loaded by ``load_latest_fact_set_for_structure``,
    eliminating one query and aligning with the canonical pin used by
    Report Block items.
    """
    session = MagicMock()
    structure = _make_statement_structure(
      structure_id="struct_balance_sheet",
      block_type="balance_sheet",
      name="Balance Sheet",
    )
    session.get.return_value = structure

    assoc = MagicMock()
    assoc.id = "assoc_1"
    assoc.from_element_id = "elem_assets"
    assoc.to_element_id = "elem_cash"
    assoc.association_type = "presentation"
    assoc.arcrole = None
    assoc.order_value = None
    assoc.weight = None

    element = MagicMock()
    element.id = "elem_cash"
    element.qname = "us-gaap:CashAndCashEquivalentsAtCarryingValue"
    element.name = "Cash"
    element.code = None
    element.element_type = "concept"
    element.is_abstract = False
    element.is_monetary = True
    element.balance_type = "debit"
    element.period_type = "instant"
    element.item_type = None

    fact_set = MagicMock()
    fact_set.id = "fset_balance_sheet_2026q1"
    fact_set.structure_id = "struct_balance_sheet"
    fact_set.period_start = date(2026, 1, 1)
    fact_set.period_end = date(2026, 3, 31)
    fact_set.factset_type = "report"
    fact_set.entity_id = "ent_demo"
    fact_set.report_id = "rep_latest"
    fact_set.scenario_id = None
    fact_set.provenance = {
      "origin": "pivot",
      "mapping_id": "map_1",
      "period": "2026-01-01/2026-03-31",
    }

    fact = MagicMock()
    fact.id = "fact_1"
    fact.element_id = "elem_cash"
    fact.value = 100_000.0
    fact.string_value = None
    fact.fact_type = "Numeric"
    fact.content_type = None
    fact.period_start = None
    fact.period_end = date(2026, 3, 31)
    fact.period_type = "instant"
    fact.unit = "USD"
    fact.fact_scope = "in_scope"
    fact.fact_set_id = "fset_balance_sheet_2026q1"

    # Query order (Plan B — fact_set_id read path; FactSet validated
    # first so the pin path can short-circuit on mismatch):
    # fact_set → taxonomy → associations → elements → rules →
    # assoc-classifications → verification_results →
    # facts (filtered by fact_set.id) →
    # element-classifications (rendering projection trait lookup) →
    # documentation labels
    session.execute.side_effect = [
      _exec_result(scalar=fact_set),  # latest fact set
      _exec_result(scalar="US GAAP"),  # taxonomy name
      _exec_result(scalars_all=[assoc]),  # associations
      _exec_result(scalars_all=[element]),  # elements
      _exec_result(scalars_all=[]),  # rules
      _exec_result(all_rows=[]),  # association classifications
      _exec_result(scalars_all=[]),  # verification results
      _exec_result(scalars_all=[fact]),  # facts (filtered by fact_set.id)
      _exec_result(all_rows=[]),  # element classifications (Plan B)
      _exec_result(all_rows=[]),  # documentation labels
    ]

    build = statement_handlers.make_statement_handlers("balance_sheet")
    envelope = build(session, "struct_balance_sheet")

    assert envelope is not None
    assert len(envelope.facts) == 1
    assert envelope.facts[0].id == "fact_1"
    assert envelope.facts[0].element_id == "elem_cash"
    assert envelope.facts[0].value == 100_000.0
    assert envelope.facts[0].period_end == date(2026, 3, 31)
    assert envelope.facts[0].fact_set_id == "fset_balance_sheet_2026q1"
    assert envelope.fact_set is not None
    assert envelope.fact_set.id == "fset_balance_sheet_2026q1"

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_display_metadata_is_block_type_specific(self, block_type: str) -> None:
    session = MagicMock()
    structure = _make_statement_structure(
      structure_id=f"struct_{block_type}",
      block_type=block_type,
      name=statement_handlers.STATEMENT_DISPLAY[block_type][0],
    )
    session.get.return_value = structure
    # No associations → no elements or classifications queries.
    session.execute.side_effect = [
      _exec_result(scalar=None),  # latest fact set → None
      _exec_result(scalar="US GAAP"),
      _exec_result(scalars_all=[]),  # associations
      _exec_result(scalars_all=[]),  # rules
      _exec_result(scalars_all=[]),  # verification results
    ]

    build = statement_handlers.make_statement_handlers(block_type)
    envelope = build(session, f"struct_{block_type}")

    assert envelope is not None
    assert envelope.block_type == block_type
    assert envelope.display_name == statement_handlers.STATEMENT_DISPLAY[block_type][0]
    assert envelope.category == "Reporting"
    assert envelope.artifact.mechanics.kind == "statement_renderer"


class TestBuildHierarchyFromAtoms:
  """Cover both presentation-tree root conventions in _build_hierarchy_from_atoms.

  The seed.py taxonomy anchors presentation arcs at ``from_element_id ==
  structure_id``; FAC / rs-gaap / rs-gaap-type-subtype reference taxonomies anchor
  at an abstract element (``fac:BalanceSheetAbstract``) and the structure
  itself never appears in any presentation arc. Both must produce a
  non-empty hierarchy or the rendering projection silently empties out.
  """

  @staticmethod
  def _assoc(*, from_id: str, to_id: str, order: float = 0.0) -> MagicMock:
    a = MagicMock()
    a.association_type = "presentation"
    a.from_element_id = from_id
    a.to_element_id = to_id
    a.order_value = order
    return a

  @staticmethod
  def _elem(element_id: str) -> MagicMock:
    e = MagicMock()
    e.id = element_id
    e.qname = f"qname:{element_id}"
    e.name = element_id
    e.balance_type = "debit"
    e.is_abstract = False
    return e

  def test_seed_convention_structure_id_root(self) -> None:
    """seed.py: presentation arcs anchor at from_element_id == structure_id."""
    structure_id = "struct_balance_sheet"
    elements_by_id = {eid: self._elem(eid) for eid in ("assets", "liabilities")}
    associations = [
      self._assoc(from_id=structure_id, to_id="assets"),
      self._assoc(from_id=structure_id, to_id="liabilities"),
    ]

    hierarchy = statement_handlers._build_hierarchy_from_atoms(
      structure_id, elements_by_id, {}, associations
    )

    assert len(hierarchy) == 2
    assert {n.element_id for n in hierarchy} == {"assets", "liabilities"}

  def test_fac_convention_abstract_element_root(self) -> None:
    """FAC / rs-gaap: presentation arcs anchor at an abstract element.

    The structure_id never appears in any arc; the root must be
    auto-detected as ``from_set - to_set``.
    """
    structure_id = "fac_balance_sheet_classified"
    abstract_root = "fac:BalanceSheetAbstract"
    elements_by_id = {
      eid: self._elem(eid) for eid in (abstract_root, "fac:Assets", "fac:CurrentAssets")
    }
    associations = [
      self._assoc(from_id=abstract_root, to_id="fac:Assets"),
      self._assoc(from_id="fac:Assets", to_id="fac:CurrentAssets"),
    ]

    hierarchy = statement_handlers._build_hierarchy_from_atoms(
      structure_id, elements_by_id, {}, associations
    )

    assert len(hierarchy) == 1
    assert hierarchy[0].element_id == abstract_root
    assert len(hierarchy[0].children) == 1
    assert hierarchy[0].children[0].element_id == "fac:Assets"
    assert len(hierarchy[0].children[0].children) == 1
    assert hierarchy[0].children[0].children[0].element_id == "fac:CurrentAssets"

  def test_no_presentation_arcs_returns_empty_hierarchy(self) -> None:
    hierarchy = statement_handlers._build_hierarchy_from_atoms(
      "struct_balance_sheet", {}, {}, []
    )
    assert hierarchy == []


class TestRenderingSkipsNonnumericFacts:
  def test_null_value_fact_does_not_crash_or_render(self) -> None:
    """A Nonnumeric fact (value=None) in the FactSet must be skipped by
    the numeric rendering path — before the guard, ``float(f.value)``
    raised TypeError."""
    from types import SimpleNamespace

    session = MagicMock()
    session.execute.return_value = _exec_result(all_rows=[])

    element = SimpleNamespace(
      id="elem_policy",
      qname="driftline:SignificantAccountingPoliciesTextBlock",
      name="Significant Accounting Policies",
      balance_type="debit",
    )
    text_fact = SimpleNamespace(
      element_id="elem_policy",
      value=None,
      period_start=date(2026, 1, 1),
      period_end=date(2026, 12, 31),
      period_type="duration",
    )

    rendering = statement_handlers._build_statement_rendering(
      session,
      elements=[element],  # type: ignore[list-item]
      associations=[],
      facts=[text_fact],  # type: ignore[list-item]
      structure_id="struct_note",
      block_type="regulatory_disclosure",
    )

    assert rendering.rows == []


class TestRenderingSumsDuplicateFacts:
  def test_two_facts_on_same_element_and_period_sum_into_one_cell(self) -> None:
    """The stamped CF can carry two facts on one operating leaf for the
    same month — the derived ΔWC value plus the cash-reconciliation plug
    from ``_reconcile_operating_to_cash``. The balance dict must sum them
    (mirroring ``fact_grid._facts_to_balance_dict``); overwriting drops
    the plug from the rendered cell while the stamped subtotal still
    includes it, so the operating section stops footing."""
    from types import SimpleNamespace

    session = MagicMock()
    session.execute.return_value = _exec_result(all_rows=[])

    structure_id = "struct_cf_indirect"
    element = SimpleNamespace(
      id="elem_other_wc",
      qname="rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet",
      name="Other operating capital, net",
      balance_type="debit",
      is_abstract=False,
    )
    association = MagicMock()
    association.association_type = "presentation"
    association.from_element_id = structure_id
    association.to_element_id = "elem_other_wc"
    association.order_value = 0.0

    def _fact(value: float) -> SimpleNamespace:
      return SimpleNamespace(
        element_id="elem_other_wc",
        value=value,
        period_start=date(2026, 3, 1),
        period_end=date(2026, 3, 31),
        period_type="duration",
      )

    rendering = statement_handlers._build_statement_rendering(
      session,
      elements=[element],  # type: ignore[list-item]
      associations=[association],
      facts=[_fact(-1000.00), _fact(-1819.25)],  # type: ignore[list-item]
      structure_id=structure_id,
      block_type="cash_flow_statement",
    )

    assert len(rendering.rows) == 1
    assert rendering.rows[0].values == [pytest.approx(-2819.25)]


class TestRenderingComposesCalcsCrossStructure:
  """Regression: rs-gaap ``arithmetic`` statement blocks carry only
  presentation arcs — their calc DAG lives in a sibling calculation
  structure. The rendering must compose calcs cross-structure so
  calc-target lines (Gross Profit, Net Income) are flagged
  ``is_subtotal`` and the guard-rail Net Income reconciliation doesn't
  double-count them as credit-nature leaves (Driftline FY2026:
  reported 8,785.70 vs implied 131,957.06)."""

  @staticmethod
  def _elem(element_id: str, qname: str, balance_type: str) -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(
      id=element_id,
      qname=qname,
      name=element_id,
      balance_type=balance_type,
      is_abstract=False,
    )

  @staticmethod
  def _fact(element_id: str, value: float) -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(
      element_id=element_id,
      value=value,
      period_start=date(2025, 7, 1),
      period_end=date(2026, 6, 30),
      period_type="duration",
    )

  @staticmethod
  def _presentation_arc(from_id: str, to_id: str, order: float) -> MagicMock:
    a = MagicMock()
    a.association_type = "presentation"
    a.from_element_id = from_id
    a.to_element_id = to_id
    a.order_value = order
    return a

  def _rendering(self, concept_arrangement: str | None) -> Any:
    from types import SimpleNamespace

    structure_id = "struct_is_multistep"
    elements = [
      self._elem("e_rev", "rs-gaap:Revenues", "credit"),
      self._elem("e_cogs", "rs-gaap:CostOfRevenue", "debit"),
      self._elem("e_gp", "rs-gaap:GrossProfit", "credit"),
      self._elem("e_ni", "rs-gaap:NetIncomeLoss", "credit"),
    ]
    # Presentation-only block: flat lines anchored at the structure_id,
    # no calculation associations in the envelope atoms.
    associations = [
      self._presentation_arc(structure_id, "e_rev", 0.0),
      self._presentation_arc(structure_id, "e_cogs", 1.0),
      self._presentation_arc(structure_id, "e_gp", 2.0),
      self._presentation_arc(structure_id, "e_ni", 3.0),
    ]
    # The report FactSet stamps a fact on EVERY line, subtotals included.
    facts = [
      self._fact("e_rev", 300.0),
      self._fact("e_cogs", 100.0),
      self._fact("e_gp", 200.0),
      self._fact("e_ni", 200.0),
    ]

    # session.execute #1: classification lookup (none mapped);
    # session.execute #2 (arithmetic only): cross-structure calc arcs —
    # GP = Revenues - CostOfRevenue; NI = GP.
    calc_rows = [
      SimpleNamespace(
        structure_id="struct_is_calc",
        from_element_id="e_gp",
        to_element_id="e_rev",
        weight=1.0,
      ),
      SimpleNamespace(
        structure_id="struct_is_calc",
        from_element_id="e_gp",
        to_element_id="e_cogs",
        weight=-1.0,
      ),
      SimpleNamespace(
        structure_id="struct_is_calc",
        from_element_id="e_ni",
        to_element_id="e_gp",
        weight=1.0,
      ),
    ]
    session = MagicMock()
    session.execute.side_effect = [_exec_result(all_rows=[]), calc_rows]

    return statement_handlers._build_statement_rendering(
      session,
      elements=elements,  # type: ignore[arg-type]
      associations=associations,  # type: ignore[arg-type]
      facts=facts,  # type: ignore[arg-type]
      structure_id=structure_id,
      block_type="income_statement",
      concept_arrangement=concept_arrangement,
    )

  def test_arithmetic_block_flags_calc_targets_and_validates(self) -> None:
    rendering = self._rendering("arithmetic")

    flags = {r.element_qname: r.is_subtotal for r in rendering.rows}
    assert flags["rs-gaap:GrossProfit"] is True
    assert flags["rs-gaap:NetIncomeLoss"] is True

    assert rendering.validation is not None
    assert rendering.validation.passed is True
    assert rendering.validation.status == "passed"
    assert not any("Net Income mismatch" in f for f in rendering.validation.failures)

  def test_non_arithmetic_block_keeps_envelope_associations(self) -> None:
    """roll_up blocks keep the single-structure convention: calcs come
    from the envelope's own associations (none here), no calc SQL."""
    rendering = self._rendering("roll_up")

    flags = {r.element_qname: r.is_subtotal for r in rendering.rows}
    assert flags["rs-gaap:GrossProfit"] is False


class TestSeriesMode:
  """F-4 statement-series projection — one column per report set,
  actuals-preferred at the seam, facts windowed to their own set."""

  @staticmethod
  def _element(element_id: str, qname: str, period_type: str = "instant") -> MagicMock:
    element = MagicMock()
    element.id = element_id
    element.qname = qname
    element.name = qname.split(":")[-1]
    element.code = None
    element.element_type = "concept"
    element.is_abstract = False
    element.is_monetary = True
    element.balance_type = "debit"
    element.period_type = period_type
    element.item_type = None
    return element

  @staticmethod
  def _assoc() -> MagicMock:
    assoc = MagicMock()
    assoc.id = "assoc_1"
    assoc.from_element_id = "elem_assets"
    assoc.to_element_id = "elem_cash"
    assoc.association_type = "presentation"
    assoc.arcrole = None
    assoc.order_value = None
    assoc.weight = None
    return assoc

  @staticmethod
  def _fact_set(
    fs_id: str,
    period_start: date,
    period_end: date,
    scenario_id: str | None = None,
  ) -> MagicMock:
    fs = MagicMock()
    fs.id = fs_id
    fs.structure_id = "struct_balance_sheet"
    fs.period_start = period_start
    fs.period_end = period_end
    fs.factset_type = "report"
    fs.entity_id = "ent_demo"
    fs.report_id = None
    fs.scenario_id = scenario_id
    fs.provenance = None
    return fs

  @staticmethod
  def _fact(
    fact_id: str,
    fact_set_id: str,
    period_end: date,
    value: float,
    period_start: date | None = None,
  ) -> MagicMock:
    fact = MagicMock()
    fact.id = fact_id
    fact.element_id = "elem_cash"
    fact.value = value
    fact.string_value = None
    fact.fact_type = "Numeric"
    fact.content_type = None
    fact.period_start = period_start
    fact.period_end = period_end
    fact.period_type = "instant"
    fact.unit = "USD"
    fact.fact_scope = "in_scope"
    fact.fact_set_id = fact_set_id
    return fact

  def _run_series(
    self,
    series_sets: list[MagicMock],
    facts: list[MagicMock],
    scenario_id: str | None,
    series_history: int | None = None,
    series_forecast: int | None = None,
  ):
    session = MagicMock()
    structure = _make_statement_structure(
      structure_id="struct_balance_sheet",
      block_type="balance_sheet",
      name="Balance Sheet",
    )
    session.get.return_value = structure
    latest = series_sets[-1] if series_sets else None
    session.execute.side_effect = [
      _exec_result(scalar=latest),  # latest set (atoms.fact_set)
      _exec_result(scalar="US GAAP"),  # taxonomy name
      _exec_result(scalars_all=[self._assoc()]),  # associations
      _exec_result(
        scalars_all=[self._element("elem_cash", "rs-gaap:Cash")]
      ),  # elements
      _exec_result(scalars_all=[]),  # rules
      _exec_result(all_rows=[]),  # association classifications
      _exec_result(scalars_all=[]),  # verification results
      _exec_result(scalars_all=series_sets),  # the series (F-4)
      _exec_result(scalars_all=facts),  # facts across the series sets
      _exec_result(all_rows=[]),  # element classifications
      _exec_result(all_rows=[]),  # documentation labels
    ]
    build = statement_handlers.make_statement_handlers("balance_sheet")
    return build(
      session,
      "struct_balance_sheet",
      scenario_id=scenario_id,
      series=True,
      series_history=series_history,
      series_forecast=series_forecast,
    )

  def test_one_column_per_set_with_seam_flags(self) -> None:
    may, june = date(2026, 5, 31), date(2026, 6, 30)
    sets = [
      self._fact_set("fs_may", date(2026, 5, 1), may),
      self._fact_set("fs_june", date(2026, 6, 1), june, scenario_id="struct_budget"),
    ]
    facts = [
      self._fact("f_may", "fs_may", may, 100_000.0),
      self._fact("f_june", "fs_june", june, 110_000.0),
    ]
    envelope = self._run_series(sets, facts, "struct_budget")

    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering is not None
    assert [p.end for p in rendering.periods] == [may, june]
    # The seam: actual column unmarked, forecast column labeled + flagged.
    assert rendering.periods[0].label is None
    assert rendering.periods[0].forecast is None
    assert rendering.periods[1].label == "Jun 2026 (forecast)"
    assert rendering.periods[1].forecast is True
    cash_row = next(r for r in rendering.rows if r.element_qname == "rs-gaap:Cash")
    assert cash_row.values == [100_000.0, 110_000.0]

  def test_facts_window_to_their_own_set(self) -> None:
    """An annual set carries comparative-period facts; they must not
    leak into other columns (nor mint phantom columns)."""
    may, june = date(2026, 5, 31), date(2026, 6, 30)
    sets = [
      self._fact_set("fs_may", date(2026, 5, 1), may),
      self._fact_set("fs_june", date(2026, 6, 1), june),
    ]
    facts = [
      self._fact("f_may", "fs_may", may, 100_000.0),
      self._fact("f_june", "fs_june", june, 110_000.0),
      # Comparative fact inside fs_june pointing at MAY — windowed out.
      self._fact("f_comparative", "fs_june", may, 999_999.0),
    ]
    envelope = self._run_series(sets, facts, None)

    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering is not None
    cash_row = next(r for r in rendering.rows if r.element_qname == "rs-gaap:Cash")
    assert cash_row.values == [100_000.0, 110_000.0]  # not 999_999

  def test_series_window_trims_to_seam_adjacent_columns(self) -> None:
    """series_history/series_forecast keep the last N actual + first N
    forecast columns — the server-side window. Facts for the trimmed sets
    never render."""
    ends = [date(2026, m, 28) for m in (3, 4, 5, 6, 7)]
    sets = [
      self._fact_set("fs_mar", date(2026, 3, 1), ends[0]),
      self._fact_set("fs_apr", date(2026, 4, 1), ends[1]),
      self._fact_set("fs_may", date(2026, 5, 1), ends[2]),
      self._fact_set("fs_jun", date(2026, 6, 1), ends[3], scenario_id="struct_budget"),
      self._fact_set("fs_jul", date(2026, 7, 1), ends[4], scenario_id="struct_budget"),
    ]
    facts = [
      self._fact(f"f_{fs.id}", fs.id, fs.period_end, 1_000.0 * (i + 1))
      for i, fs in enumerate(sets)
    ]
    envelope = self._run_series(
      sets, facts, "struct_budget", series_history=1, series_forecast=1
    )

    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering is not None
    assert [p.end for p in rendering.periods] == [ends[2], ends[3]]
    assert rendering.periods[1].forecast is True
    cash_row = next(r for r in rendering.rows if r.element_qname == "rs-gaap:Cash")
    assert cash_row.values == [3_000.0, 4_000.0]

  def test_fact_set_pin_bypasses_series(self) -> None:
    """A snapshot pin is a single set by definition — series is ignored."""
    may = date(2026, 5, 31)
    pinned = self._fact_set("fs_pinned", date(2026, 5, 1), may)
    session = MagicMock()
    structure = _make_statement_structure(
      structure_id="struct_balance_sheet",
      block_type="balance_sheet",
      name="Balance Sheet",
    )
    session.get.return_value = structure
    session.execute.side_effect = [
      _exec_result(scalar=pinned),  # fact set by id (pin validation)
      _exec_result(scalar="US GAAP"),  # taxonomy name
      _exec_result(scalars_all=[self._assoc()]),  # associations
      _exec_result(
        scalars_all=[self._element("elem_cash", "rs-gaap:Cash")]
      ),  # elements
      _exec_result(scalars_all=[]),  # rules
      _exec_result(all_rows=[]),  # association classifications
      _exec_result(scalars_all=[]),  # verification results
      _exec_result(
        scalars_all=[self._fact("f_may", "fs_pinned", may, 100_000.0)]
      ),  # facts by pinned set — NO series query in between
      _exec_result(all_rows=[]),  # element classifications
      _exec_result(all_rows=[]),  # documentation labels
    ]
    build = statement_handlers.make_statement_handlers("balance_sheet")
    envelope = build(
      session, "struct_balance_sheet", "fs_pinned", scenario_id=None, series=True
    )

    assert envelope is not None
    rendering = envelope.view.rendering
    assert rendering is not None
    assert len(rendering.periods) == 1
