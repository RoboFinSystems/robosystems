"""Unit tests for the bundle producer helpers (``bundle.py``).

Covers the projection / dedup logic that turns DB rows into
:class:`StatementBundle` content. The full DB-integrated
``build_report_bundle`` is exercised end-to-end by
``just demo-roboledger`` (lands the bundle at S3) and by the
cross-encoder + JSON-LD / XBRL test modules; here we focus on the
unit-level transforms that are easiest to break.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pytest

from robosystems.operations.serialization.bundle import (
  BundleElement,
  BundleFact,
  BundleLinkbases,
  BundlePeriod,
  BundleUnit,
  EntityMeta,
  FrameworkPin,
  PeriodMeta,
  ReportMeta,
  StatementBundle,
  _associations_to_linkbases,
  _element_to_bundle,
  _fact_to_bundle,
  _mint_periods,
  _mint_units,
  _period_metas_for_report,
)

# ── Fake ORM rows ──────────────────────────────────────────────────────


@dataclass
class _FakeElement:
  id: str
  qname: str
  name: str
  namespace: str | None = None
  description: str | None = None
  balance_type: str | None = "debit"
  period_type: str = "instant"
  is_abstract: bool = False
  is_monetary: bool = True
  element_type: str = "concept"
  substitution_group: str | None = None
  source: str = "rs-gaap"


@dataclass
class _FakeAssociation:
  structure_id: str
  from_element_id: str
  to_element_id: str
  association_type: str = "presentation"
  arcrole: str | None = None
  order_value: float | None = None
  weight: float | None = None
  metadata_: dict | None = None


@dataclass
class _FakeStructure:
  id: str
  name: str
  block_type: str | None = "balance_sheet"
  metadata_: dict | None = None


@dataclass
class _FakeFact:
  id: str
  element_id: str
  period_start: date | None
  period_end: date
  period_type: str
  unit: str = "USD"
  value: float = 100.0
  fact_set_id: str | None = "fs_01"
  structure_id: str | None = "struct_01"


@dataclass
class _FakePeriod:
  start: date | None
  end: date


@dataclass
class _FakeFactSet:
  period_start: date | None
  period_end: date


@dataclass
class _FakeReport:
  period_start: date | None = None
  period_end: date | None = None
  periods: list[dict] | None = None


# ── _element_to_bundle ─────────────────────────────────────────────────


class TestElementToBundle:
  def test_projects_xbrl_intrinsic_attributes(self) -> None:
    e = _FakeElement(
      id="elem_01",
      qname="rs-gaap:Assets",
      name="Assets",
      namespace="http://...",
      description="Total Assets",
      balance_type="debit",
      period_type="instant",
      is_monetary=True,
      source="rs-gaap",
    )
    b = _element_to_bundle(e)
    assert b.id == "elem_01"
    assert b.qname == "rs-gaap:Assets"
    assert b.name == "Assets"
    assert b.label == "Total Assets"
    assert b.balance_type == "debit"
    assert b.period_type == "instant"
    assert b.is_monetary is True
    assert b.source == "rs-gaap"

  def test_falls_back_to_name_when_qname_missing(self) -> None:
    """Library-seeded concepts without a qname surface as their bare
    name so the encoder can still construct a fallback URI."""
    e = _FakeElement(id="elem_01", qname="", name="LocalConcept")
    b = _element_to_bundle(e)
    assert b.qname == "LocalConcept"

  def test_drops_invalid_balance_type(self) -> None:
    """Unknown balance values get normalized to None so the
    Pydantic model's Literal type doesn't reject the row."""
    e = _FakeElement(
      id="elem_01", qname="rs-gaap:Assets", name="Assets", balance_type="other"
    )
    b = _element_to_bundle(e)
    assert b.balance_type is None


# ── _associations_to_linkbases ────────────────────────────────────────


class TestAssociationsToLinkbases:
  def test_presentation_arcs_land_in_presentation_links(self) -> None:
    structures = {
      "struct_01": _FakeStructure(
        id="struct_01",
        name="Balance Sheet",
        block_type="balance_sheet",
        metadata_={"role_uri": "http://robosystems.ai/role/BS"},
      )
    }
    assocs = [
      _FakeAssociation(
        structure_id="struct_01",
        from_element_id="elem_A",
        to_element_id="elem_B",
        association_type="presentation",
        arcrole="http://www.xbrl.org/2003/arcrole/parent-child",
        order_value=1.0,
      )
    ]
    lbs = _associations_to_linkbases(assocs, structures)
    assert len(lbs.presentation_links) == 1
    assert len(lbs.calculation_links) == 0
    assert len(lbs.definition_links) == 0
    link = lbs.presentation_links[0]
    assert link.link_type == "presentationLink"
    assert link.role_uri == "http://robosystems.ai/role/BS"
    assert link.block_type == "balance_sheet"
    assert len(link.arcs) == 1
    assert link.arcs[0].arc_type == "presentationArc"
    assert link.arcs[0].from_qname == "elem_A"
    assert link.arcs[0].to_qname == "elem_B"

  def test_calculation_arcs_land_in_calculation_links_with_weight(self) -> None:
    structures = {"s_01": _FakeStructure(id="s_01", name="BS", metadata_={})}
    assocs = [
      _FakeAssociation(
        structure_id="s_01",
        from_element_id="A",
        to_element_id="B",
        association_type="calculation",
        weight=1.0,
        order_value=1.0,
      )
    ]
    lbs = _associations_to_linkbases(assocs, structures)
    assert len(lbs.calculation_links) == 1
    arc = lbs.calculation_links[0].arcs[0]
    assert arc.arc_type == "calculationArc"
    assert arc.weight == 1.0

  def test_presentation_arc_weight_dropped(self) -> None:
    """Weight on a presentation arc is meaningless and would confuse
    XBRL processors — the grouping pass strips it for presentation /
    definition arcs."""
    structures = {"s_01": _FakeStructure(id="s_01", name="BS", metadata_={})}
    assocs = [
      _FakeAssociation(
        structure_id="s_01",
        from_element_id="A",
        to_element_id="B",
        association_type="presentation",
        weight=99.0,
      )
    ]
    lbs = _associations_to_linkbases(assocs, structures)
    assert lbs.presentation_links[0].arcs[0].weight is None

  def test_definition_arcs_collect_under_definition_links(self) -> None:
    """Several association_type values map to the definition linkbase
    (general-special, equivalence, essence-alias)."""
    structures = {"s_01": _FakeStructure(id="s_01", name="BS", metadata_={})}
    assocs = [
      _FakeAssociation(
        structure_id="s_01",
        from_element_id="A",
        to_element_id="B",
        association_type="general-special",
      ),
      _FakeAssociation(
        structure_id="s_01",
        from_element_id="C",
        to_element_id="D",
        association_type="equivalence",
      ),
    ]
    lbs = _associations_to_linkbases(assocs, structures)
    assert len(lbs.definition_links) == 1
    assert len(lbs.definition_links[0].arcs) == 2

  def test_unknown_association_type_skipped(self) -> None:
    """An unmapped association_type doesn't crash the bundler — it
    drops the arc so the linkbase output stays well-formed."""
    structures = {"s_01": _FakeStructure(id="s_01", name="BS", metadata_={})}
    assocs = [
      _FakeAssociation(
        structure_id="s_01",
        from_element_id="A",
        to_element_id="B",
        association_type="something-weird",
      )
    ]
    lbs = _associations_to_linkbases(assocs, structures)
    assert lbs.presentation_links == []
    assert lbs.calculation_links == []
    assert lbs.definition_links == []

  def test_arcs_sorted_within_link(self) -> None:
    """Output is deterministic: arcs sort by ``(order, from, to)``
    so the JSON-LD diff stays stable across runs."""
    structures = {"s_01": _FakeStructure(id="s_01", name="BS", metadata_={})}
    assocs = [
      _FakeAssociation(
        structure_id="s_01",
        from_element_id="A",
        to_element_id="Z",
        association_type="presentation",
        order_value=3.0,
      ),
      _FakeAssociation(
        structure_id="s_01",
        from_element_id="A",
        to_element_id="B",
        association_type="presentation",
        order_value=1.0,
      ),
      _FakeAssociation(
        structure_id="s_01",
        from_element_id="A",
        to_element_id="C",
        association_type="presentation",
        order_value=2.0,
      ),
    ]
    lbs = _associations_to_linkbases(assocs, structures)
    orders = [arc.order_value for arc in lbs.presentation_links[0].arcs]
    assert orders == [1.0, 2.0, 3.0]

  def test_default_arcrole_used_when_db_row_lacks_one(self) -> None:
    """The DB may not stamp an explicit arcrole on presentation
    arcs — the grouping pass falls back to the standard parent-child
    URI so XBRL processors don't reject the arc."""
    structures = {"s_01": _FakeStructure(id="s_01", name="BS", metadata_={})}
    assocs = [
      _FakeAssociation(
        structure_id="s_01",
        from_element_id="A",
        to_element_id="B",
        association_type="presentation",
        arcrole=None,
      )
    ]
    lbs = _associations_to_linkbases(assocs, structures)
    assert (
      lbs.presentation_links[0].arcs[0].arcrole
      == "http://www.xbrl.org/2003/arcrole/parent-child"
    )


# ── _mint_periods ─────────────────────────────────────────────────────


class TestMintPeriods:
  def test_unique_period_combos_get_unique_period_ids(self) -> None:
    facts = [
      _FakeFact(
        id="f1",
        element_id="A",
        period_start=None,
        period_end=date(2024, 12, 31),
        period_type="instant",
      ),
      _FakeFact(
        id="f2",
        element_id="A",
        period_start=None,
        period_end=date(2023, 12, 31),
        period_type="instant",
      ),
    ]
    periods, fact_to_ref = _mint_periods(facts)
    assert len(periods) == 2
    assert periods[0].id == "p_1"
    assert periods[1].id == "p_2"
    assert fact_to_ref["f1"] == "p_1"
    assert fact_to_ref["f2"] == "p_2"

  def test_repeated_period_combo_dedupes_to_single_period(self) -> None:
    facts = [
      _FakeFact(
        id="f1",
        element_id="A",
        period_start=None,
        period_end=date(2024, 12, 31),
        period_type="instant",
      ),
      _FakeFact(
        id="f2",
        element_id="B",
        period_start=None,
        period_end=date(2024, 12, 31),
        period_type="instant",
      ),
    ]
    periods, fact_to_ref = _mint_periods(facts)
    assert len(periods) == 1
    assert fact_to_ref["f1"] == fact_to_ref["f2"] == "p_1"

  def test_instant_vs_duration_get_separate_periods(self) -> None:
    """Same end date, different period_type → different period nodes.
    The XBRL encoder later derives contexts whose period element shape
    matches what's declared on the concept."""
    facts = [
      _FakeFact(
        id="f1",
        element_id="A",
        period_start=None,
        period_end=date(2024, 12, 31),
        period_type="instant",
      ),
      _FakeFact(
        id="f2",
        element_id="B",
        period_start=date(2024, 1, 1),
        period_end=date(2024, 12, 31),
        period_type="duration",
      ),
    ]
    periods, _ = _mint_periods(facts)
    assert len(periods) == 2
    assert {p.period_type for p in periods} == {"instant", "duration"}

  def test_instant_period_type_trumps_populated_period_start(self) -> None:
    """Regression: BS facts arrive with ``period_type='instant'`` but
    a populated ``period_start`` (the fact-stamping side carries
    period bounds even on instants). The dedup must honour the
    declared period_type — emitting BS facts under a duration period
    would break XBRL conformance (concept declares
    ``periodType='instant'``; processors reject mismatches).

    Found by the Seattle Method round-trip vs. Charlie Hoffman's
    reference instance. Producer coerces instant facts' ``period_start``
    to ``None`` regardless of what the DB row carries."""
    facts = [
      _FakeFact(
        id="bs_fact",
        element_id="Assets",
        period_start=date(2024, 1, 1),  # populated despite period_type=instant
        period_end=date(2024, 3, 31),
        period_type="instant",
      ),
      _FakeFact(
        id="is_fact",
        element_id="Revenue",
        period_start=date(2024, 1, 1),
        period_end=date(2024, 3, 31),
        period_type="duration",
      ),
    ]
    periods, fact_to_ref = _mint_periods(facts)
    assert len(periods) == 2
    by_kind = {p.period_type: p for p in periods}
    assert by_kind["instant"].period_start is None
    assert by_kind["instant"].period_end == date(2024, 3, 31)
    assert by_kind["duration"].period_start == date(2024, 1, 1)
    assert by_kind["duration"].period_end == date(2024, 3, 31)
    assert by_kind["instant"].id == fact_to_ref["bs_fact"]
    assert by_kind["duration"].id == fact_to_ref["is_fact"]


# ── _mint_units ───────────────────────────────────────────────────────


class TestMintUnits:
  def test_currency_code_gets_iso4217_prefix(self) -> None:
    facts = [
      _FakeFact(
        id="f1",
        element_id="A",
        period_start=None,
        period_end=date(2024, 12, 31),
        period_type="instant",
        unit="USD",
      )
    ]
    units, _ = _mint_units(facts)
    assert len(units) == 1
    assert units[0].measure == "iso4217:USD"
    assert units[0].id == "u_USD"

  def test_duplicate_units_dedup(self) -> None:
    facts = [
      _FakeFact(
        id="f1",
        element_id="A",
        period_start=None,
        period_end=date(2024, 12, 31),
        period_type="instant",
        unit="USD",
      ),
      _FakeFact(
        id="f2",
        element_id="B",
        period_start=None,
        period_end=date(2024, 12, 31),
        period_type="instant",
        unit="USD",
      ),
    ]
    units, fact_to_ref = _mint_units(facts)
    assert len(units) == 1
    assert fact_to_ref["f1"] == fact_to_ref["f2"]

  def test_multiple_currencies_get_distinct_units(self) -> None:
    facts = [
      _FakeFact(
        id="f1",
        element_id="A",
        period_start=None,
        period_end=date(2024, 12, 31),
        period_type="instant",
        unit="USD",
      ),
      _FakeFact(
        id="f2",
        element_id="B",
        period_start=None,
        period_end=date(2024, 12, 31),
        period_type="instant",
        unit="EUR",
      ),
    ]
    units, _ = _mint_units(facts)
    measures = sorted(u.measure for u in units)
    assert measures == ["iso4217:EUR", "iso4217:USD"]


# ── _fact_to_bundle ───────────────────────────────────────────────────


class TestFactToBundle:
  def test_projects_value_and_refs(self) -> None:
    f = _FakeFact(
      id="fact_01",
      element_id="elem_A",
      period_start=None,
      period_end=date(2024, 12, 31),
      period_type="instant",
      value=295_000_000.0,
    )
    element = _FakeElement(id="elem_A", qname="rs-gaap:Assets", name="Assets")
    b = _fact_to_bundle(f, element, period_ref="p_1", unit_ref="u_USD", entity_ref="e")
    assert b.id == "fact_01"
    assert b.element_qname == "rs-gaap:Assets"
    assert b.period_ref == "p_1"
    assert b.unit_ref == "u_USD"
    assert b.entity_ref == "e"
    assert b.value == 295_000_000.0
    assert b.decimals == "INF"

  def test_missing_element_falls_back_to_element_id_for_qname(self) -> None:
    """When the element row isn't in the bundle's slice (rare; only
    happens during partial test fixtures), use the element_id so the
    encoder still has something to tag the fact with."""
    f = _FakeFact(
      id="fact_01",
      element_id="elem_lone",
      period_start=None,
      period_end=date(2024, 12, 31),
      period_type="instant",
    )
    b = _fact_to_bundle(
      f, element=None, period_ref="p_1", unit_ref="u_USD", entity_ref="e"
    )
    assert b.element_qname == "elem_lone"


# ── _period_metas_for_report ──────────────────────────────────────────


class TestPeriodMetasForReport:
  def test_prefers_fact_set_periods_when_present(self) -> None:
    report = _FakeReport()
    fact_sets = [
      _FakeFactSet(period_start=date(2024, 1, 1), period_end=date(2024, 12, 31)),
      _FakeFactSet(period_start=date(2023, 1, 1), period_end=date(2023, 12, 31)),
    ]
    metas = _period_metas_for_report(report, fact_sets)
    # Sorted ascending by (start, end)
    assert metas[0].start == date(2023, 1, 1)
    assert metas[1].start == date(2024, 1, 1)

  def test_falls_back_to_report_periods_json_when_no_fact_sets(self) -> None:
    report = _FakeReport(
      periods=[
        {"start": "2024-01-01", "end": "2024-12-31", "label": "FY24"},
        {"start": "2023-01-01", "end": "2023-12-31", "label": "FY23"},
      ]
    )
    metas = _period_metas_for_report(report, fact_sets=[])
    assert len(metas) == 2
    assert metas[0].label == "FY24"

  def test_falls_back_to_report_period_columns_when_periods_json_empty(self) -> None:
    report = _FakeReport(period_start=date(2024, 1, 1), period_end=date(2024, 12, 31))
    metas = _period_metas_for_report(report, fact_sets=[])
    assert len(metas) == 1
    assert metas[0].start == date(2024, 1, 1)

  def test_empty_when_nothing_to_infer(self) -> None:
    metas = _period_metas_for_report(_FakeReport(), fact_sets=[])
    assert metas == []

  def test_instant_period_type_inferred_from_null_start(self) -> None:
    fact_sets = [_FakeFactSet(period_start=None, period_end=date(2024, 12, 31))]
    metas = _period_metas_for_report(_FakeReport(), fact_sets)
    assert metas[0].period_type == "instant"


# ── StatementBundle Pydantic shape ────────────────────────────────────


class TestStatementBundleValidation:
  def _minimal_args(self) -> dict:
    return {
      "entity": EntityMeta(id="e", name="X"),
      "periods": [
        PeriodMeta(start=date(2024, 1, 1), end=date(2024, 12, 31), label="FY")
      ],
      "reporting_style": "STYLE",
      "framework_pins": [FrameworkPin(framework="rs-gaap", version="v1")],
      "schema_concepts": [],
      "linkbases": BundleLinkbases(),
      "period_nodes": [],
      "units": [],
      "facts": [],
      "ib_envelopes": [],
      "mode": "report",
      "report_meta": ReportMeta(
        report_id="r", generation_count=0, filing_status="draft"
      ),
    }

  def test_report_mode_requires_report_meta_only(self) -> None:
    bundle = StatementBundle(**self._minimal_args())
    assert bundle.mode == "report"
    assert bundle.report_meta is not None
    assert bundle.live_meta is None

  def test_filing_status_constrained_to_known_values(self) -> None:
    """``ReportMeta.filing_status`` is a Pydantic Literal — invalid
    values get caught at construction time, before the bundle even
    composes them."""
    with pytest.raises(Exception):
      ReportMeta(
        report_id="r",
        generation_count=0,
        filing_status="nonsense",  # type: ignore[arg-type]
      )

  def test_framework_pins_serialize_as_object_list(self) -> None:
    """Pin entries should dump to ``{framework, version}`` dicts —
    that's the RDF-friendly shape from the spec."""
    bundle = StatementBundle(**self._minimal_args())
    dumped = bundle.model_dump()
    assert dumped["framework_pins"] == [{"framework": "rs-gaap", "version": "v1"}]


# ── Dispatch (covers operations/serialization/__init__.py) ─────────────


class TestDispatch:
  def test_rdf_jsonld_dispatch_returns_str(self) -> None:
    from robosystems.operations.serialization import RdfFlavor, serialize_to_rdf

    bundle = _build_min_bundle()
    out = serialize_to_rdf(bundle, RdfFlavor.JSONLD)
    assert isinstance(out, str)
    assert '"@context"' in out

  def test_rdf_default_flavor_is_jsonld(self) -> None:
    """``serialize_to_rdf(bundle)`` without explicit flavor should
    behave identically to passing ``RdfFlavor.JSONLD``."""
    from robosystems.operations.serialization import RdfFlavor, serialize_to_rdf

    bundle = _build_min_bundle()
    default = serialize_to_rdf(bundle)
    explicit = serialize_to_rdf(bundle, RdfFlavor.JSONLD)
    assert default == explicit

  def test_unknown_rdf_flavor_raises_value_error(self) -> None:
    from robosystems.operations.serialization.rdf import serialize_to_rdf as dispatch

    # Forge an enum-like value that isn't JSONLD
    class _BogusFlavor:
      value = "bogus"

    with pytest.raises(ValueError, match="Unsupported RDF flavor"):
      dispatch(_build_min_bundle(), _BogusFlavor())  # type: ignore[arg-type]

  def test_xbrl_default_flavor_is_xbrl_21(self) -> None:
    from robosystems.operations.serialization import XbrlFlavor, serialize_to_xbrl

    bundle = _build_min_bundle()
    default = serialize_to_xbrl(bundle)
    explicit = serialize_to_xbrl(bundle, XbrlFlavor.XBRL_2_1)
    # Identical zip bytes — both encoders are deterministic
    assert default == explicit

  def test_unknown_xbrl_flavor_raises_value_error(self) -> None:
    from robosystems.operations.serialization.xbrl import (
      serialize_to_xbrl as dispatch,
    )

    class _BogusFlavor:
      value = "bogus"

    with pytest.raises(ValueError, match="Unsupported XBRL flavor"):
      dispatch(_build_min_bundle(), _BogusFlavor())  # type: ignore[arg-type]


def _build_min_bundle() -> StatementBundle:
  return StatementBundle(
    entity=EntityMeta(id="e", name="X"),
    periods=[PeriodMeta(start=date(2024, 1, 1), end=date(2024, 12, 31), label="FY")],
    reporting_style="STYLE",
    framework_pins=[FrameworkPin(framework="rs-gaap", version="v1")],
    schema_concepts=[
      BundleElement(
        id="A",
        qname="rs-gaap:Assets",
        name="Assets",
        period_type="instant",
        is_monetary=True,
        balance_type="debit",
        source="rs-gaap",
      )
    ],
    linkbases=BundleLinkbases(),
    period_nodes=[
      BundlePeriod(
        id="p_1",
        period_start=None,
        period_end=date(2024, 12, 31),
        period_type="instant",
      )
    ],
    units=[BundleUnit(id="u_USD", measure="iso4217:USD")],
    facts=[
      BundleFact(
        id="f1",
        element_id="A",
        element_qname="rs-gaap:Assets",
        value=100.0,
        period_ref="p_1",
        unit_ref="u_USD",
        entity_ref="e",
      )
    ],
    ib_envelopes=[],
    mode="report",
    report_meta=ReportMeta(report_id="r", generation_count=0, filing_status="draft"),
  )


# ── Flavor enum smoke checks (covers flavors.py) ──────────────────────


class TestFlavorEnums:
  def test_rdf_flavor_value_is_lowercase_jsonld(self) -> None:
    from robosystems.operations.serialization import RdfFlavor

    assert RdfFlavor.JSONLD.value == "jsonld"

  def test_xbrl_flavor_value_is_kebab_xbrl_2_1(self) -> None:
    from robosystems.operations.serialization import XbrlFlavor

    assert XbrlFlavor.XBRL_2_1.value == "xbrl-2.1"

  def test_flavors_are_string_enums(self) -> None:
    """StrEnum membership means ``flavor.value`` is the same string
    as the enum itself — important for ergonomic query-param dispatch
    in the download endpoint."""
    from robosystems.operations.serialization import RdfFlavor, XbrlFlavor

    assert str(RdfFlavor.JSONLD) == "jsonld"
    assert str(XbrlFlavor.XBRL_2_1) == "xbrl-2.1"
