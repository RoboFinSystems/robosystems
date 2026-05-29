"""Unit tests for the XBRL 2.1 emitter (``xbrl/xbrl_21.py``).

Covers each output file of the Phase 1b flat zip independently:

* ``instance.xml`` — XBRL root, schemaRef, contexts, units, facts
* ``report.xsd`` — concept declarations + linkbaseRefs
* ``report-pre.xml`` / ``-cal.xml`` / ``-def.xml`` — linkbase shape

Then verifies the cross-cutting concerns: zip layout, value
formatting, qname-to-id mapping for linkbase locators, and the
single-file dispatch entry point.
"""

from __future__ import annotations

import io
import zipfile
from datetime import date

import pytest
from lxml import etree

from robosystems.operations.serialization.bundle import (
  BundleArc,
  BundleContext,
  BundleElement,
  BundleFact,
  BundleLinkbaseLink,
  BundleLinkbases,
  BundleUnit,
  EntityMeta,
  FrameworkPin,
  PeriodMeta,
  ReportMeta,
  StatementBundle,
)
from robosystems.operations.serialization.xbrl.xbrl_21 import (
  serialize_to_xbrl_21,
)

# ── XML namespaces ─────────────────────────────────────────────────────

NS_XBRLI = "http://www.xbrl.org/2003/instance"
NS_LINK = "http://www.xbrl.org/2003/linkbase"
NS_XLINK = "http://www.w3.org/1999/xlink"
NS_XS = "http://www.w3.org/2001/XMLSchema"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
NS_RS_GAAP = "https://robosystems.ai/taxonomy/rs-gaap/v1/"


# ── Fixture bundles ────────────────────────────────────────────────────


def _bundle(
  *,
  with_arcs: bool = False,
  with_two_contexts: bool = False,
) -> StatementBundle:
  schema_concepts = [
    BundleElement(
      id="Assets",
      qname="rs-gaap:Assets",
      name="Assets",
      period_type="instant",
      is_monetary=True,
      balance_type="debit",
      source="rs-gaap",
    ),
  ]
  if with_arcs:
    schema_concepts.append(
      BundleElement(
        id="AssetsCurrent",
        qname="rs-gaap:AssetsCurrent",
        name="AssetsCurrent",
        period_type="instant",
        is_monetary=True,
        balance_type="debit",
        source="rs-gaap",
      )
    )

  linkbases = BundleLinkbases()
  if with_arcs:
    linkbases.presentation_links.append(
      BundleLinkbaseLink(
        link_type="presentationLink",
        role_uri="http://robosystems.ai/role/BS",
        structure_id="struct_01",
        structure_name="Balance Sheet",
        block_type="balance_sheet",
        arcs=[
          BundleArc(
            arc_type="presentationArc",
            arcrole="http://www.xbrl.org/2003/arcrole/parent-child",
            from_qname="rs-gaap:Assets",
            to_qname="rs-gaap:AssetsCurrent",
            order_value=1.0,
          )
        ],
      )
    )
    linkbases.calculation_links.append(
      BundleLinkbaseLink(
        link_type="calculationLink",
        role_uri="http://robosystems.ai/role/BS",
        structure_id="struct_01",
        structure_name="Balance Sheet",
        arcs=[
          BundleArc(
            arc_type="calculationArc",
            arcrole="http://www.xbrl.org/2003/arcrole/summation-item",
            from_qname="rs-gaap:Assets",
            to_qname="rs-gaap:AssetsCurrent",
            weight=1.0,
            order_value=1.0,
          )
        ],
      )
    )

  contexts = [
    BundleContext(
      id="ctx_1",
      entity_identifier="ent_01",
      period_start=None,
      period_end=date(2024, 12, 31),
      period_type="instant",
    )
  ]
  if with_two_contexts:
    contexts.append(
      BundleContext(
        id="ctx_2",
        entity_identifier="ent_01",
        period_start=date(2024, 1, 1),
        period_end=date(2024, 12, 31),
        period_type="duration",
      )
    )

  facts = [
    BundleFact(
      id="fact_01",
      element_id="Assets",
      element_qname="rs-gaap:Assets",
      value=295_183_000.0,
      context_ref="ctx_1",
      unit_ref="u_USD",
    )
  ]
  if with_arcs:
    facts.append(
      BundleFact(
        id="fact_02",
        element_id="AssetsCurrent",
        element_qname="rs-gaap:AssetsCurrent",
        value=148_000_000.0,
        context_ref="ctx_1",
        unit_ref="u_USD",
      )
    )

  return StatementBundle(
    entity=EntityMeta(id="ent_01", name="Test Co"),
    periods=[PeriodMeta(start=date(2024, 1, 1), end=date(2024, 12, 31), label="FY24")],
    reporting_style="BSC-CORP-IS02-CF1",
    framework_pins=[FrameworkPin(framework="rs-gaap", version="v1")],
    schema_concepts=schema_concepts,
    linkbases=linkbases,
    contexts=contexts,
    units=[BundleUnit(id="u_USD", measure="iso4217:USD")],
    facts=facts,
    ib_envelopes=[],
    mode="report",
    report_meta=ReportMeta(
      report_id="rpt_test", generation_count=1, filing_status="draft"
    ),
  )


def _zip_open(zip_bytes: bytes) -> zipfile.ZipFile:
  return zipfile.ZipFile(io.BytesIO(zip_bytes))


def _parse(zip_bytes: bytes, name: str) -> etree._Element:
  zf = _zip_open(zip_bytes)
  return etree.fromstring(zf.read(name))


# ── Zip layout ─────────────────────────────────────────────────────────


class TestZipLayout:
  def test_zip_contains_all_five_phase_1b_files(self) -> None:
    zip_bytes = serialize_to_xbrl_21(_bundle(with_arcs=True))
    zf = _zip_open(zip_bytes)
    assert set(zf.namelist()) == {
      "instance.xml",
      "report.xsd",
      "report-pre.xml",
      "report-cal.xml",
      "report-def.xml",
    }

  def test_zip_is_deflated_not_stored(self) -> None:
    """Verify the zip uses ZIP_DEFLATED for compression so payloads
    stay small. The 5-file Phase 1b structure shouldn't approach
    even 100KB after compression for typical bundles."""
    zip_bytes = serialize_to_xbrl_21(_bundle(with_arcs=True))
    zf = _zip_open(zip_bytes)
    for info in zf.infolist():
      assert info.compress_type == zipfile.ZIP_DEFLATED

  def test_dispatch_via_serialize_to_xbrl_routes_to_xbrl_21(self) -> None:
    """The flavor-dispatch entry point hands off to ``xbrl_21`` when
    the default ``XbrlFlavor.XBRL_2_1`` flavor is requested."""
    from robosystems.operations.serialization import (
      XbrlFlavor,
      serialize_to_xbrl,
    )

    a = serialize_to_xbrl(_bundle(), XbrlFlavor.XBRL_2_1)
    b = serialize_to_xbrl_21(_bundle())
    # Both should produce a zip with the same file set
    assert set(_zip_open(a).namelist()) == set(_zip_open(b).namelist())


# ── instance.xml ───────────────────────────────────────────────────────


class TestInstanceXml:
  def test_root_is_xbrli_xbrl(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "instance.xml")
    assert root.tag == f"{{{NS_XBRLI}}}xbrl"

  def test_root_declares_required_namespaces(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "instance.xml")
    for prefix, uri in (
      ("xbrli", NS_XBRLI),
      ("link", NS_LINK),
      ("xlink", NS_XLINK),
      ("xsi", NS_XSI),
      ("rs-gaap", NS_RS_GAAP),
    ):
      assert prefix in root.nsmap
      assert root.nsmap[prefix] == uri

  def test_xsi_schema_location_points_at_report_xsd(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "instance.xml")
    sl = root.get(f"{{{NS_XSI}}}schemaLocation") or ""
    assert "report.xsd" in sl
    # And the XBRL 2.1 instance schema reference is present too.
    assert "xbrl-instance-2003-12-31.xsd" in sl

  def test_schema_ref_present_with_xlink_attrs(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "instance.xml")
    schema_ref = root.find(f"{{{NS_LINK}}}schemaRef")
    assert schema_ref is not None
    assert schema_ref.get(f"{{{NS_XLINK}}}type") == "simple"
    assert schema_ref.get(f"{{{NS_XLINK}}}href") == "report.xsd"

  def test_instant_context_emits_instant_period_element(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "instance.xml")
    ctx = root.find(f"{{{NS_XBRLI}}}context[@id='ctx_1']")
    assert ctx is not None
    period = ctx.find(f"{{{NS_XBRLI}}}period")
    assert period is not None
    instant = period.find(f"{{{NS_XBRLI}}}instant")
    assert instant is not None
    assert instant.text == "2024-12-31"
    # No start/end on an instant period.
    assert period.find(f"{{{NS_XBRLI}}}startDate") is None
    assert period.find(f"{{{NS_XBRLI}}}endDate") is None

  def test_duration_context_emits_start_and_end(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle(with_two_contexts=True)), "instance.xml")
    ctx = root.find(f"{{{NS_XBRLI}}}context[@id='ctx_2']")
    assert ctx is not None
    period = ctx.find(f"{{{NS_XBRLI}}}period")
    assert period is not None
    start = period.find(f"{{{NS_XBRLI}}}startDate")
    end = period.find(f"{{{NS_XBRLI}}}endDate")
    assert start is not None and start.text == "2024-01-01"
    assert end is not None and end.text == "2024-12-31"
    assert period.find(f"{{{NS_XBRLI}}}instant") is None

  def test_entity_identifier_carries_scheme_attribute(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "instance.xml")
    ctx = root.find(f"{{{NS_XBRLI}}}context[@id='ctx_1']")
    assert ctx is not None
    entity = ctx.find(f"{{{NS_XBRLI}}}entity")
    assert entity is not None
    identifier = entity.find(f"{{{NS_XBRLI}}}identifier")
    assert identifier is not None
    assert identifier.get("scheme") == "http://robosystems.ai/entity"
    assert identifier.text == "ent_01"

  def test_unit_emits_measure_with_iso4217_prefix(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "instance.xml")
    unit = root.find(f"{{{NS_XBRLI}}}unit[@id='u_USD']")
    assert unit is not None
    measure = unit.find(f"{{{NS_XBRLI}}}measure")
    assert measure is not None
    assert measure.text == "iso4217:USD"

  def test_fact_element_uses_concept_qname_as_tag(self) -> None:
    """The XBRL "element name IS the type tag" pattern — facts are
    tagged with the concept qname, not a generic xbrli:fact element."""
    root = _parse(serialize_to_xbrl_21(_bundle()), "instance.xml")
    assets_fact = root.find(f"{{{NS_RS_GAAP}}}Assets")
    assert assets_fact is not None
    assert assets_fact.get("contextRef") == "ctx_1"
    assert assets_fact.get("unitRef") == "u_USD"
    assert assets_fact.get("decimals") == "INF"
    assert assets_fact.text == "295183000"

  def test_integer_fact_renders_without_decimal_point(self) -> None:
    """Per ``_format_value`` — whole-currency values strip the
    trailing ``.0`` for compactness."""
    root = _parse(serialize_to_xbrl_21(_bundle()), "instance.xml")
    fact = root.find(f"{{{NS_RS_GAAP}}}Assets")
    assert fact is not None and fact.text is not None
    assert "." not in fact.text

  def test_non_integer_fact_preserves_precision(self) -> None:
    bundle = _bundle()
    bundle.facts[0].value = 1.234
    root = _parse(serialize_to_xbrl_21(bundle), "instance.xml")
    fact = root.find(f"{{{NS_RS_GAAP}}}Assets")
    assert fact is not None and fact.text == "1.234"

  def test_negative_fact_preserves_sign(self) -> None:
    bundle = _bundle()
    bundle.facts[0].value = -8_400_000.0
    root = _parse(serialize_to_xbrl_21(bundle), "instance.xml")
    fact = root.find(f"{{{NS_RS_GAAP}}}Assets")
    assert fact is not None and fact.text == "-8400000"


# ── report.xsd ─────────────────────────────────────────────────────────


class TestReportXsd:
  def test_target_namespace_is_rs_gaap(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "report.xsd")
    assert root.get("targetNamespace") == NS_RS_GAAP

  def test_imports_xbrli(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "report.xsd")
    imp = root.find(f"{{{NS_XS}}}import")
    assert imp is not None
    assert imp.get("namespace") == NS_XBRLI

  def test_concept_declared_as_xs_element(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "report.xsd")
    elements = root.findall(f"{{{NS_XS}}}element[@name='Assets']")
    assert len(elements) == 1

  def test_concept_carries_xbrli_attributes(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle()), "report.xsd")
    el = root.find(f"{{{NS_XS}}}element[@name='Assets']")
    assert el is not None
    assert el.get("type") == "xbrli:monetaryItemType"
    assert el.get("substitutionGroup") == "xbrli:item"
    assert el.get("abstract") == "false"
    assert el.get("nillable") == "true"
    assert el.get(f"{{{NS_XBRLI}}}periodType") == "instant"
    assert el.get(f"{{{NS_XBRLI}}}balance") == "debit"

  def test_non_monetary_concept_gets_string_type(self) -> None:
    bundle = _bundle()
    bundle.schema_concepts[0].is_monetary = False
    root = _parse(serialize_to_xbrl_21(bundle), "report.xsd")
    el = root.find(f"{{{NS_XS}}}element[@name='Assets']")
    assert el is not None
    assert el.get("type") == "xbrli:stringItemType"

  def test_abstract_concept_marked_abstract_true(self) -> None:
    bundle = _bundle()
    bundle.schema_concepts[0].is_abstract = True
    root = _parse(serialize_to_xbrl_21(bundle), "report.xsd")
    el = root.find(f"{{{NS_XS}}}element[@name='Assets']")
    assert el is not None
    assert el.get("abstract") == "true"

  def test_abstract_concept_does_not_carry_balance(self) -> None:
    """Regression: per XBRL 2.1 §5.1.1.2 ``xbrli:balance`` is invalid
    on abstract concepts (and on non-monetary types). Arelle reports
    ``xbrl.5.1.1.2 Item ... may not have a balance`` when we leak it.
    Abstract concepts in our taxonomy (e.g. ``IncomeStatementAbstract``)
    historically inherited a ``balance_type`` from the framework row
    even though they're abstract — suppress emission."""
    bundle = _bundle()
    bundle.schema_concepts[0].is_abstract = True
    bundle.schema_concepts[0].balance_type = "credit"  # would leak otherwise
    root = _parse(serialize_to_xbrl_21(bundle), "report.xsd")
    el = root.find(f"{{{NS_XS}}}element[@name='Assets']")
    assert el is not None
    assert el.get(f"{{{NS_XBRLI}}}balance") is None

  def test_non_monetary_concept_does_not_carry_balance(self) -> None:
    """Regression companion: ``xbrli:balance`` only applies to monetary
    item types. A string-typed concept with a stray ``balance_type``
    must not emit it."""
    bundle = _bundle()
    bundle.schema_concepts[0].is_monetary = False
    bundle.schema_concepts[0].balance_type = "debit"
    root = _parse(serialize_to_xbrl_21(bundle), "report.xsd")
    el = root.find(f"{{{NS_XS}}}element[@name='Assets']")
    assert el is not None
    assert el.get(f"{{{NS_XBRLI}}}balance") is None

  def test_concept_id_uses_qname_derived_ncname(self) -> None:
    """Regression: ``xs:element/@id`` must be a valid ``xs:ID``
    (NCName) — starts with letter or underscore, no spaces or colons.
    Bundle elements often arrive with a raw ULID for ``concept.id``
    (e.g. ``elem_01K…`` or ``488ac9be-…``) which fails the type when
    raw, and was producing Arelle ``MissingLoc`` errors at validation
    time because linkbase locators already used qname-derived ids
    (``rs-gaap_Assets``) while the schema declared digit-leading ULIDs.
    The emitter now derives ``id`` from the qname so schema and
    locators agree."""
    bundle = _bundle()
    bundle.schema_concepts[0].id = "488ac9be-588e-5ffa-92c9-a06"  # ULID-shape
    root = _parse(serialize_to_xbrl_21(bundle), "report.xsd")
    el = root.find(f"{{{NS_XS}}}element[@name='Assets']")
    assert el is not None
    # id is the qname-derived NCName, NOT the raw ULID from concept.id
    assert el.get("id") == "rs-gaap_Assets"

  def test_concept_name_uses_qname_local_not_display_name(self) -> None:
    """Regression: ``xs:element/@name`` must be a valid NCName — no
    spaces, commas, or parentheses. The bundle's ``concept.name`` is
    often the human-readable label (e.g. ``"Treasury Stock, Value"``)
    which is NOT an NCName. The emitter uses the qname's local part
    (``TreasuryStockValue``) which is always a valid NCName by
    construction."""
    bundle = _bundle()
    bundle.schema_concepts[0].qname = "rs-gaap:TreasuryStockValue"
    bundle.schema_concepts[0].name = "Treasury Stock, Value"  # NOT an NCName
    root = _parse(serialize_to_xbrl_21(bundle), "report.xsd")
    el = root.find(f"{{{NS_XS}}}element[@name='TreasuryStockValue']")
    assert el is not None
    # The display name doesn't leak through
    assert el.get("name") != "Treasury Stock, Value"

  def test_linkbase_refs_present_when_arcs_present(self) -> None:
    """Per XBRL 2.1 §5.1.2, ``link:linkbaseRef`` must live inside the
    schema's ``xs:annotation/xs:appinfo`` block — emitting it directly
    under ``xs:schema`` produces an ``xbrl.5.1.2.linkbaseRefLocation``
    load error in Arelle."""
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report.xsd")
    refs = root.findall(
      f"{{{NS_XS}}}annotation/{{{NS_XS}}}appinfo/{{{NS_LINK}}}linkbaseRef"
    )
    hrefs = {r.get(f"{{{NS_XLINK}}}href") for r in refs}
    assert "report-pre.xml" in hrefs
    assert "report-cal.xml" in hrefs
    # Belt-and-braces: nothing emitted at the schema root either.
    assert root.findall(f"{{{NS_LINK}}}linkbaseRef") == []

  def test_no_linkbase_ref_when_no_arcs_emitted(self) -> None:
    """When a bundle has no arcs in a given linkbase, we omit the
    linkbaseRef to keep the schema clean — XBRL processors don't
    need a pointer at an empty linkbase."""
    root = _parse(serialize_to_xbrl_21(_bundle()), "report.xsd")
    refs = root.findall(f".//{{{NS_LINK}}}linkbaseRef")
    hrefs = {r.get(f"{{{NS_XLINK}}}href") for r in refs}
    assert "report-pre.xml" not in hrefs
    assert "report-cal.xml" not in hrefs


# ── Linkbases ──────────────────────────────────────────────────────────


class TestPresentationLinkbase:
  def test_root_is_link_linkbase(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-pre.xml")
    assert root.tag == f"{{{NS_LINK}}}linkbase"

  def test_role_ref_declared_for_elr(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-pre.xml")
    role_refs = root.findall(f"{{{NS_LINK}}}roleRef")
    assert len(role_refs) == 1
    assert role_refs[0].get("roleURI") == "http://robosystems.ai/role/BS"

  def test_presentation_link_emitted_per_elr(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-pre.xml")
    links = root.findall(f"{{{NS_LINK}}}presentationLink")
    assert len(links) == 1
    assert links[0].get(f"{{{NS_XLINK}}}role") == "http://robosystems.ai/role/BS"
    assert links[0].get(f"{{{NS_XLINK}}}type") == "extended"

  def test_locators_emitted_for_each_endpoint(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-pre.xml")
    link = root.find(f"{{{NS_LINK}}}presentationLink")
    assert link is not None
    locs = link.findall(f"{{{NS_LINK}}}loc")
    # One arc with from + to → two locators (one per distinct endpoint)
    assert len(locs) == 2
    labels = {loc.get(f"{{{NS_XLINK}}}label") for loc in locs}
    assert "rs-gaap_Assets" in labels
    assert "rs-gaap_AssetsCurrent" in labels

  def test_loc_href_points_at_report_xsd(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-pre.xml")
    link = root.find(f"{{{NS_LINK}}}presentationLink")
    assert link is not None
    locs = link.findall(f"{{{NS_LINK}}}loc")
    for loc in locs:
      href = loc.get(f"{{{NS_XLINK}}}href") or ""
      assert href.startswith("report.xsd#")

  def test_arc_carries_xlink_arcrole_and_endpoints(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-pre.xml")
    arc = root.find(f"{{{NS_LINK}}}presentationLink/{{{NS_LINK}}}presentationArc")
    assert arc is not None
    assert (
      arc.get(f"{{{NS_XLINK}}}arcrole")
      == "http://www.xbrl.org/2003/arcrole/parent-child"
    )
    assert arc.get(f"{{{NS_XLINK}}}from") == "rs-gaap_Assets"
    assert arc.get(f"{{{NS_XLINK}}}to") == "rs-gaap_AssetsCurrent"
    assert arc.get("order") == "1"


class TestCalculationLinkbase:
  def test_calculation_arc_carries_weight(self) -> None:
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-cal.xml")
    arc = root.find(f"{{{NS_LINK}}}calculationLink/{{{NS_LINK}}}calculationArc")
    assert arc is not None
    assert arc.get("weight") == "1"

  def test_presentation_linkbase_does_not_emit_weight(self) -> None:
    """Weight is calc-only — make sure it doesn't sneak into the
    presentation linkbase (XBRL processors flag this as a defect)."""
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-pre.xml")
    arc = root.find(f"{{{NS_LINK}}}presentationLink/{{{NS_LINK}}}presentationArc")
    assert arc is not None
    assert arc.get("weight") is None


class TestDefinitionLinkbase:
  def test_empty_definition_linkbase_still_well_formed(self) -> None:
    """No definition arcs in the test bundle — but the file should
    still be valid XML the way XBRL processors expect."""
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-def.xml")
    assert root.tag == f"{{{NS_LINK}}}linkbase"
    assert root.findall(f"{{{NS_LINK}}}definitionLink") == []


# ── Cross-cutting helpers ──────────────────────────────────────────────


class TestQnameAndIdMapping:
  def test_qname_to_id_swaps_colon_for_underscore(self) -> None:
    """Schema id refs (``report.xsd#rs-gaap_Assets``) can't carry
    colons in their fragment. ``_qname_to_id`` swaps the separator."""
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-pre.xml")
    loc = root.find(
      f"{{{NS_LINK}}}presentationLink/{{{NS_LINK}}}loc[@{{{NS_XLINK}}}label='rs-gaap_Assets']"
    )
    assert loc is not None

  def test_locator_label_and_arc_endpoints_agree(self) -> None:
    """The XLink labels on locators must match the from/to refs on
    arcs — otherwise XBRL processors reject the linkbase."""
    root = _parse(serialize_to_xbrl_21(_bundle(with_arcs=True)), "report-pre.xml")
    link = root.find(f"{{{NS_LINK}}}presentationLink")
    assert link is not None
    loc_labels = {
      loc.get(f"{{{NS_XLINK}}}label") for loc in link.findall(f"{{{NS_LINK}}}loc")
    }
    arc = link.find(f"{{{NS_LINK}}}presentationArc")
    assert arc is not None
    assert arc.get(f"{{{NS_XLINK}}}from") in loc_labels
    assert arc.get(f"{{{NS_XLINK}}}to") in loc_labels


@pytest.mark.parametrize(
  "value,expected",
  [
    (1_000_000.0, "1000000"),
    (1.0, "1"),
    (-5.0, "-5"),
    (1.234, "1.234"),
    (0.5, "0.5"),
    (0.0, "0"),
    (1e10, "10000000000"),  # large integer-valued float still strips decimal
  ],
)
def test_format_value(value: float, expected: str) -> None:
  """``_format_value`` strips trailing zeros from integer-valued
  floats; non-integers keep their full precision."""
  from robosystems.operations.serialization.xbrl.xbrl_21 import _format_value

  assert _format_value(value) == expected
