"""Tests for the ``StatementBundle`` → xbrlkit ``XbrlModel`` bridge (``model.py``).

The bridge is the waist every xbrlkit projection reads through, so these pin
the mapping itself — concepts and headings, the item-type table, the
SEC-shaped network definitions, the period and value literals, the text
blocks — independently of any one flavor's output.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from xbrlkit.namespaces import ENTITY_SCHEME

from robosystems.operations.serialization.bundle import (
  _STATEMENT_BLOCK_TYPES,
  BundleArc,
  BundleElement,
  BundleFact,
  BundleLinkbaseLink,
  BundleLinkbases,
  BundlePeriod,
  BundleUnit,
  EntityMeta,
  FrameworkPin,
  PeriodMeta,
  ReportMeta,
  StatementBundle,
  namespace_uri_for,
)
from robosystems.operations.serialization.model import (
  ITEM_TYPE_TO_XBRL,
  STATEMENT_TITLES,
  bundle_to_xbrl_model,
  lexical_value,
  markdown_to_html,
  network_definition,
  report_identifier,
)
from robosystems.operations.serialization.xbrl.xbrl_21 import _format_value

RS_GAAP = "https://robosystems.ai/taxonomy/rs-gaap/v1/"
PARENT_CHILD = "http://www.xbrl.org/2003/arcrole/parent-child"
SUMMATION = "http://www.xbrl.org/2003/arcrole/summation-item"
MARKDOWN = "# Policies\n\nWe *do* things <b>x</b>"


def _element(qname: str, **overrides: object) -> BundleElement:
  fields: dict[str, object] = {
    "id": qname.split(":")[-1],
    "qname": qname,
    "namespace": qname.split(":")[0],
    "name": qname.split(":")[-1],
    "period_type": "instant",
    "is_monetary": True,
    "balance_type": "debit",
    "source": "rs-gaap",
  }
  fields.update(overrides)
  return BundleElement(**fields)  # type: ignore[arg-type]


def _bundle(**overrides: object) -> StatementBundle:
  """A report with a balance sheet, one note, and every value domain."""
  concepts = [
    _element("rs-gaap:AssetsAbstract", is_abstract=True, element_type="abstract"),
    _element("rs-gaap:Assets", label="Assets"),
    _element("rs-gaap:AssetsCurrent", label="Current assets"),
    _element("rs-gaap:SharesOutstanding", item_type="shares", balance_type=None),
    _element(
      "rs-gaap:DaysSalesOutstanding",
      item_type="days",
      period_type="duration",
      balance_type=None,
    ),
    _element(
      "rs-gaap:PoliciesTextBlock",
      item_type="textBlock",
      period_type="duration",
      is_monetary=False,
      balance_type=None,
    ),
    _element(
      "rs-gaap:Commentary",
      item_type="string",
      period_type="duration",
      is_monetary=False,
      balance_type=None,
    ),
    _element("custom:Widgets", period_type="duration", source="custom"),
  ]
  presentation = BundleLinkbaseLink(
    link_type="presentationLink",
    role_uri="http://robosystems.ai/role/BS",
    structure_id="struct_bs",
    structure_name="Balance Sheet — Classified",
    block_type="balance_sheet",
    arcs=[
      BundleArc(
        arc_type="presentationArc",
        arcrole=PARENT_CHILD,
        from_qname="rs-gaap:AssetsAbstract",
        to_qname="rs-gaap:Assets",
        order_value=1.0,
      ),
      BundleArc(
        arc_type="presentationArc",
        arcrole=PARENT_CHILD,
        from_qname="rs-gaap:Assets",
        to_qname="rs-gaap:AssetsCurrent",
        order_value=1.0,
      ),
    ],
  )
  calculation = BundleLinkbaseLink(
    link_type="calculationLink",
    role_uri="http://robosystems.ai/role/BS",
    structure_id="struct_bs",
    structure_name="Balance Sheet — Classified",
    block_type="balance_sheet",
    arcs=[
      BundleArc(
        arc_type="calculationArc",
        arcrole=SUMMATION,
        from_qname="rs-gaap:Assets",
        to_qname="rs-gaap:AssetsCurrent",
        order_value=1.0,
        weight=1.0,
      )
    ],
  )
  note = BundleLinkbaseLink(
    link_type="presentationLink",
    role_uri="",
    structure_id="struct_note1",
    structure_name="Significant Accounting Policies",
    block_type="regulatory_disclosure",
    arcs=[
      BundleArc(
        arc_type="presentationArc",
        arcrole=PARENT_CHILD,
        from_qname="rs-gaap:PoliciesTextBlock",
        to_qname="rs-gaap:Commentary",
        order_value=1.0,
      )
    ],
  )
  definition = BundleLinkbaseLink(
    link_type="definitionLink",
    role_uri="http://robosystems.ai/role/BS",
    structure_id="struct_bs",
    structure_name="Balance Sheet — Classified",
    arcs=[
      BundleArc(
        arc_type="definitionArc",
        arcrole="http://www.xbrl.org/2003/arcrole/general-special",
        from_qname="rs-gaap:Assets",
        to_qname="custom:Widgets",
      )
    ],
  )
  facts = [
    BundleFact(
      id="f_assets",
      element_id="Assets",
      element_qname="rs-gaap:Assets",
      value=295_183_000.0,
      period_ref="p_instant",
      unit_ref="u_USD",
      entity_ref="ent_01",
    ),
    BundleFact(
      id="f_current",
      element_id="AssetsCurrent",
      element_qname="rs-gaap:AssetsCurrent",
      value=148_000_000.0,
      period_ref="p_instant",
      unit_ref="u_USD",
      entity_ref="ent_01",
      decimals="-3",
    ),
    BundleFact(
      id="f_shares",
      element_id="SharesOutstanding",
      element_qname="rs-gaap:SharesOutstanding",
      value=1_000_000.0,
      period_ref="p_instant",
      unit_ref="u_shares",
      entity_ref="ent_01",
    ),
    BundleFact(
      id="f_widgets",
      element_id="Widgets",
      element_qname="custom:Widgets",
      value=12.5,
      period_ref="p_duration",
      unit_ref="u_USD",
      entity_ref="ent_01",
    ),
    BundleFact(
      id="f_policies",
      element_id="PoliciesTextBlock",
      element_qname="rs-gaap:PoliciesTextBlock",
      text_value=MARKDOWN,
      fact_type="Nonnumeric",
      content_type="text/markdown",
      period_ref="p_duration",
      entity_ref="ent_01",
    ),
    BundleFact(
      id="f_commentary",
      element_id="Commentary",
      element_qname="rs-gaap:Commentary",
      text_value="As reported.",
      fact_type="Nonnumeric",
      period_ref="p_duration",
      entity_ref="ent_01",
    ),
  ]
  fields: dict[str, object] = {
    "entity": EntityMeta(
      id="ent_01", name="Test Co", legal_name="Test Co LLC", ein="12-3456789"
    ),
    "periods": [
      PeriodMeta(start=date(2024, 1, 1), end=date(2024, 12, 31), label="FY24")
    ],
    "reporting_style": "BSC-CORP-IS02-CF1",
    "framework_pins": [FrameworkPin(framework="rs-gaap", version="v1")],
    "schema_concepts": concepts,
    "linkbases": BundleLinkbases(
      presentation_links=[note, presentation],
      calculation_links=[calculation],
      definition_links=[definition],
    ),
    "period_nodes": [
      BundlePeriod(
        id="p_instant", period_end=date(2024, 12, 31), period_type="instant"
      ),
      BundlePeriod(
        id="p_duration",
        period_start=date(2024, 1, 1),
        period_end=date(2024, 12, 31),
        period_type="duration",
      ),
    ],
    "units": [
      BundleUnit(id="u_USD", measure="iso4217:USD"),
      BundleUnit(id="u_shares", measure="shares"),
    ],
    "facts": facts,
    "structure_display_order": {"struct_note1": 100},
    "mode": "report",
    "report_meta": ReportMeta(
      report_id="rpt_test",
      generation_count=2,
      filing_status="filed",
      filed_at=datetime(2025, 2, 14, 15, 0, tzinfo=UTC),
    ),
  }
  fields.update(overrides)
  return StatementBundle(**fields)  # type: ignore[arg-type]


@pytest.mark.unit
def test_statement_order_mirrors_the_producer() -> None:
  """The titles' key order is the sort order — it must match the bundle's."""
  assert tuple(STATEMENT_TITLES) == _STATEMENT_BLOCK_TYPES


@pytest.mark.unit
def test_concepts_carry_type_balance_namespace_and_the_standard_label() -> None:
  model = bundle_to_xbrl_model(_bundle())
  assets = model.concepts["rs-gaap:Assets"]
  assert assets.namespace == RS_GAAP
  assert assets.name == "Assets"
  assert assets.item_type == "monetaryItemType"
  assert assets.period_type == "instant"
  assert assets.balance == "debit"
  assert assets.is_numeric and not assets.is_abstract and assets.nillable
  assert [(lb.value, lb.role, lb.language) for lb in assets.labels] == [
    ("Assets", "http://www.xbrl.org/2003/role/label", "en")
  ]
  assert model.concepts["rs-gaap:AssetsAbstract"].is_abstract


@pytest.mark.unit
@pytest.mark.parametrize(
  ("item_type", "is_monetary", "expected"),
  [
    ("monetary", True, "monetaryItemType"),
    ("shares", False, "sharesItemType"),
    ("percent", False, "percentItemType"),
    ("textBlock", False, "textBlockItemType"),
    ("string", False, "stringItemType"),
    ("date", False, "dateItemType"),
    ("boolean", False, "booleanItemType"),
    ("decimal", False, "decimalItemType"),
    ("integer", False, "integerItemType"),
    ("ratio", False, "pureItemType"),
    ("multiple", False, "pureItemType"),
    ("days", False, "decimalItemType"),
    (None, True, "monetaryItemType"),
    (None, False, "stringItemType"),
  ],
)
def test_item_type_table(
  item_type: str | None, is_monetary: bool, expected: str
) -> None:
  """Every wire item type maps; an untyped element follows the XBRL 2.1 rule."""
  element = _element("rs-gaap:X", item_type=item_type, is_monetary=is_monetary)
  model = bundle_to_xbrl_model(_bundle(schema_concepts=[element], facts=[]))
  assert model.concepts["rs-gaap:X"].item_type == expected


@pytest.mark.unit
def test_every_wire_item_type_is_in_the_table() -> None:
  assert set(ITEM_TYPE_TO_XBRL) >= {
    "monetary",
    "string",
    "date",
    "boolean",
    "shares",
    "decimal",
    "integer",
    "textBlock",
    "ratio",
    "percent",
    "multiple",
    "days",
  }


@pytest.mark.unit
def test_text_concepts_are_flagged_for_language_and_text_blocks() -> None:
  model = bundle_to_xbrl_model(_bundle())
  block = model.concepts["rs-gaap:PoliciesTextBlock"]
  assert block.is_textblock and block.is_text_fact and not block.is_numeric
  commentary = model.concepts["rs-gaap:Commentary"]
  assert commentary.is_text_fact and not commentary.is_textblock
  assert model.concepts["rs-gaap:SharesOutstanding"].is_shares


@pytest.mark.unit
def test_a_custom_prefix_is_minted_and_a_declared_namespace_wins() -> None:
  model = bundle_to_xbrl_model(_bundle())
  assert model.concepts["custom:Widgets"].namespace == (
    "https://robosystems.ai/taxonomy/custom/"
  )
  declared = _element(
    "custom:Widgets", namespace_uri="https://tenant.example/ontology#"
  )
  model = bundle_to_xbrl_model(_bundle(schema_concepts=[declared], facts=[]))
  assert (
    model.concepts["custom:Widgets"].namespace == "https://tenant.example/ontology#"
  )


@pytest.mark.unit
def test_namespace_uri_for_prefers_the_concept_iri_then_the_context() -> None:
  assert (
    namespace_uri_for("rs-gaap", concept_iri=f"{RS_GAAP}Assets", local_name="Assets")
    == RS_GAAP
  )
  assert (
    namespace_uri_for(
      "custom", concept_iri="https://t.example/o#Widgets", local_name="Widgets"
    )
    == "https://t.example/o#"
  )
  assert namespace_uri_for("rs-gaap") == RS_GAAP
  assert namespace_uri_for("fac") == "http://www.xbrlsite.com/fac#"
  assert namespace_uri_for("custom") == "https://robosystems.ai/taxonomy/custom/"
  assert namespace_uri_for(None) == "https://robosystems.ai/taxonomy/unqualified/"


@pytest.mark.unit
def test_periods_keep_inclusive_dates_with_no_start_on_an_instant() -> None:
  model = bundle_to_xbrl_model(_bundle())
  by_id = {p.id: p for p in model.periods}
  assert (by_id["p_instant"].start, by_id["p_instant"].end) == (
    None,
    date(2024, 12, 31),
  )
  assert (by_id["p_duration"].start, by_id["p_duration"].end) == (
    date(2024, 1, 1),
    date(2024, 12, 31),
  )


@pytest.mark.unit
def test_numeric_facts_carry_the_xbrl_lexical_form() -> None:
  model = bundle_to_xbrl_model(_bundle())
  by_id = {f.id: f for f in model.facts}
  assets = by_id["f_assets"]
  assert assets.value_str == "295183000"
  assert assets.numeric_value == 295_183_000.0
  assert assets.decimals is None  # INF — infinitely precise, omitted downstream
  assert (assets.unit_id, assets.period_id, assets.entity_cik) == (
    "u_USD",
    "p_instant",
    "ent_01",
  )
  assert by_id["f_current"].decimals == "-3"
  assert by_id["f_widgets"].value_str == "12.5"


@pytest.mark.unit
@pytest.mark.parametrize("value", [295_183_000.0, 12.5, -3.0, 0.0, 1e15, 0.001])
def test_lexical_value_matches_the_xbrl_emitter(value: float) -> None:
  assert lexical_value(value) == _format_value(value)


@pytest.mark.unit
def test_markdown_notes_become_html_with_raw_html_escaped() -> None:
  model = bundle_to_xbrl_model(_bundle())
  policies = next(f for f in model.facts if f.id == "f_policies")
  assert policies.value_kind == "text"
  assert policies.unit_id is None
  assert policies.language == "en"
  assert policies.value_str is not None
  assert "<h1>Policies</h1>" in policies.value_str
  assert "<em>do</em>" in policies.value_str
  assert "&lt;b&gt;x&lt;/b&gt;" in policies.value_str
  assert "<b>" not in policies.value_str
  assert markdown_to_html("plain") == "<p>plain</p>"


@pytest.mark.unit
def test_plain_text_passes_through_and_a_missing_narrative_is_nil() -> None:
  model = bundle_to_xbrl_model(_bundle())
  commentary = next(f for f in model.facts if f.id == "f_commentary")
  assert commentary.value_str == "As reported."
  nil = BundleFact(
    id="f_nil",
    element_id="PoliciesTextBlock",
    element_qname="rs-gaap:PoliciesTextBlock",
    fact_type="Nonnumeric",
    period_ref="p_duration",
    entity_ref="ent_01",
  )
  model = bundle_to_xbrl_model(_bundle(facts=[nil]))
  assert model.facts[0].is_nil and model.facts[0].value_str is None


@pytest.mark.unit
def test_entity_and_filing_identity() -> None:
  model = bundle_to_xbrl_model(_bundle())
  assert (model.entity.cik, model.entity.scheme) == ("ent_01", ENTITY_SCHEME)
  assert model.entity.scheme == "http://robosystems.ai/entity"
  assert (model.entity.name, model.entity.legal_name, model.entity.ein) == (
    "Test Co",
    "Test Co LLC",
    "12-3456789",
  )
  assert model.filing.accession == "rpt_test"
  assert model.filing.cik == "ent_01"
  assert model.filing.filing_date == date(2025, 2, 14)
  assert model.filing.report_uri == "https://robosystems.ai/report/rpt_test"
  assert model.filing.is_inline_xbrl is False
  assert RS_GAAP in model.filing.taxonomy_namespaces


@pytest.mark.unit
def test_report_identifier_falls_back_to_the_live_snapshot() -> None:
  assert report_identifier(_bundle()) == "rpt_test"
  live = _bundle(
    mode="live",
    report_meta=None,
    live_meta={"snapshot_at": datetime(2025, 3, 1, 12, 30, tzinfo=UTC)},
  )
  assert report_identifier(live) == "live-20250301T123000Z"
  assert bundle_to_xbrl_model(live).filing.filing_date is None


@pytest.mark.unit
def test_networks_are_sec_shaped_statements_first_then_notes() -> None:
  model = bundle_to_xbrl_model(_bundle())
  assert [(n.kind, n.definition) for n in model.networks] == [
    ("presentation", "0001 - Statement - Balance Sheet"),
    ("calculation", "0001 - Statement - Balance Sheet"),
    ("presentation", "0100 - Disclosure - Significant Accounting Policies"),
  ]
  statement, calc, note = model.networks
  # The raw structure name is kept beside the composed definition.
  assert statement.documentation == "Balance Sheet — Classified"
  assert statement.role_uri == "http://robosystems.ai/role/BS"
  # A tenant structure with no role gets one minted from its stable id.
  assert note.role_uri == "https://robosystems.ai/role/struct_note1"
  assert note.documentation is None
  # Definition links are not bridged.
  assert not [n for n in model.networks if n.kind == "definition"]
  # Roots: sources that are never a target within the link.
  roots = {arc.from_qname for arc in statement.arcs if arc.is_root}
  assert roots == {"rs-gaap:AssetsAbstract"}
  assert calc.arcs[0].weight == 1.0
  assert all(arc.weight is None for arc in statement.arcs)
  assert statement.arcs[0].arcrole == PARENT_CHILD
  assert statement.arcs[0].order == 1.0


@pytest.mark.unit
def test_network_definition_is_verbatim_when_unshaped() -> None:
  link = BundleLinkbaseLink(
    link_type="presentationLink",
    role_uri="",
    structure_id="struct_x",
    structure_name="Segment Rollforward",
    block_type="schedule",
  )
  assert network_definition(link, {}) == ("Segment Rollforward", None)
  assert network_definition(link, {"struct_x": 103}) == (
    "0103 - Disclosure - Segment Rollforward",
    None,
  )
  nameless = link.model_copy(update={"structure_name": ""})
  assert network_definition(nameless, {}) == ("struct_x", None)
  statement = link.model_copy(
    update={
      "block_type": "equity_statement",
      "structure_name": "Statement of Changes in Equity",
    }
  )
  # The raw name only rides as documentation when it differs from the title.
  assert network_definition(statement, {}) == (
    "0004 - Statement - Statement of Changes in Equity",
    None,
  )
