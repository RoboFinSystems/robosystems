"""Cross-encoder fact-equivalence assertion.

The v1.0 ontology claim is: *one bundle, two encoders, one fact set*.
The JSON-LD encoder and the XBRL 2.1 emitter share the same
:class:`StatementBundle` and produce semantically equivalent output —
every fact in the source emerges in both projections with matching
``(concept, period, unit, value, decimals)``.

These tests build synthetic bundles, run both encoders, parse the
outputs back, and assert the round-tripped fact sets are identical.
Validates the architectural claim without depending on the Seattle
Method reference (which is the next testing milestone — Task #22).
"""

from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from datetime import date

import pytest
import rdflib
from lxml import etree
from rdflib import Graph, Namespace, URIRef

from robosystems.operations.serialization import (
  RdfFlavor,
  XbrlFlavor,
  serialize_to_rdf,
  serialize_to_xbrl,
)
from robosystems.operations.serialization.bundle import (
  BundleContext,
  BundleElement,
  BundleFact,
  BundleLinkbases,
  BundleUnit,
  EntityMeta,
  FrameworkPin,
  PeriodMeta,
  ReportMeta,
  StatementBundle,
)

# ── Namespaces (for parsing back the encoder outputs) ────────────────────

XBRLI = Namespace("http://www.xbrl.org/2003/instance#")
XBRLI_XML = "http://www.xbrl.org/2003/instance"  # XML form (no trailing #)
LINK_XML = "http://www.xbrl.org/2003/linkbase"
RDF = rdflib.RDF
RS_GAAP = Namespace("https://robosystems.ai/taxonomy/rs-gaap/v1/")


# ── Test fixture helpers ─────────────────────────────────────────────────


def _make_bundle(
  *,
  facts: list[tuple[str, float, str, str]],
  contexts: list[tuple[str, date | None, date, str]],
  units: list[tuple[str, str]],
  concepts: list[tuple[str, str, bool]] | None = None,
) -> StatementBundle:
  """Build a minimal valid bundle for cross-encoder testing.

  ``facts`` is a list of ``(qname, value, context_ref, unit_ref)``
  tuples; ``contexts`` is ``(id, period_start, period_end, period_type)``;
  ``units`` is ``(id, measure)``; ``concepts`` (optional) overrides the
  default monetary debit-balance concept declarations.
  """
  if concepts is None:
    # Derive one concept per unique fact qname; default monetary + debit
    seen: set[str] = set()
    concept_list = []
    for qname, _, _, _ in facts:
      if qname not in seen:
        seen.add(qname)
        concept_list.append((qname, "instant", True))
  else:
    concept_list = concepts

  schema_concepts = [
    BundleElement(
      id=qname.split(":")[-1],
      qname=qname,
      name=qname.split(":")[-1],
      period_type=period_type,  # type: ignore[arg-type]
      is_monetary=is_monetary,
      balance_type="debit",
      source="rs-gaap",
    )
    for qname, period_type, is_monetary in concept_list
  ]

  return StatementBundle(
    entity=EntityMeta(id="ent_01", name="Test Co"),
    periods=[PeriodMeta(start=date(2024, 1, 1), end=date(2024, 12, 31), label="FY24")],
    reporting_style="BSC-CORP-IS02-CF1",
    framework_pins=[FrameworkPin(framework="rs-gaap", version="v1")],
    schema_concepts=schema_concepts,
    linkbases=BundleLinkbases(),
    contexts=[
      BundleContext(
        id=cid,
        entity_identifier="ent_01",
        period_start=p_start,
        period_end=p_end,
        period_type=p_type,  # type: ignore[arg-type]
      )
      for cid, p_start, p_end, p_type in contexts
    ],
    units=[BundleUnit(id=uid, measure=measure) for uid, measure in units],
    facts=[
      BundleFact(
        id=f"fact_{idx + 1:02d}",
        element_id=qname.split(":")[-1],
        element_qname=qname,
        value=value,
        context_ref=cref,
        unit_ref=uref,
      )
      for idx, (qname, value, cref, uref) in enumerate(facts)
    ],
    ib_envelopes=[],
    mode="report",
    report_meta=ReportMeta(
      report_id="rpt_test", generation_count=1, filing_status="draft"
    ),
  )


# ── Fact-set extraction from encoded outputs ─────────────────────────────


@dataclass(frozen=True)
class _FactTuple:
  """The cross-encoder equivalence atom."""

  concept_qname: str
  period_signature: tuple[str, ...]
  unit_measure: str
  value: float
  decimals: str


def _facts_from_jsonld(bundle: StatementBundle, jsonld_str: str) -> set[_FactTuple]:
  """Parse the JSON-LD output back into a fact set.

  Uses rdflib to read triples — same library that built the graph,
  so we know any encoder-decoder mismatch is on us, not on the parser.
  """
  g = Graph()
  g.parse(data=jsonld_str, format="json-ld")

  # Index contexts (xbrli:context → period sub-blob) and units
  # (xbrli:unit → measure) so facts can resolve their refs.
  context_signature: dict[URIRef, tuple[str, ...]] = {}
  for ctx_uri in g.subjects(RDF.type, XBRLI.context):
    period_uri = next(g.objects(ctx_uri, XBRLI.period), None)
    if period_uri is None:
      continue
    sig = _period_signature_from_rdf(g, period_uri)
    if isinstance(ctx_uri, URIRef):
      context_signature[ctx_uri] = sig

  unit_measure: dict[URIRef, str] = {}
  for unit_uri in g.subjects(RDF.type, XBRLI.unit):
    measure = next(g.objects(unit_uri, XBRLI.measure), None)
    if isinstance(unit_uri, URIRef) and measure is not None:
      unit_measure[unit_uri] = _qname_for_uri(str(measure))

  # Resolve each fact: typed by concept URI; references contextRef + unitRef
  facts: set[_FactTuple] = set()
  fact_uris = set()
  for fact_uri in g.objects(_root_uri_for_bundle(bundle), XBRLI.item):
    if isinstance(fact_uri, URIRef):
      fact_uris.add(fact_uri)

  for fact_uri in fact_uris:
    type_uri = next(g.objects(fact_uri, RDF.type), None)
    ctx_ref = next(g.objects(fact_uri, XBRLI.contextRef), None)
    unit_ref = next(g.objects(fact_uri, XBRLI.unitRef), None)
    value = next(g.objects(fact_uri, RDF.value), None)
    decimals = next(g.objects(fact_uri, XBRLI.decimals), None)
    if any(x is None for x in (type_uri, ctx_ref, unit_ref, value, decimals)):
      continue
    assert isinstance(ctx_ref, URIRef)
    assert isinstance(unit_ref, URIRef)
    facts.add(
      _FactTuple(
        concept_qname=_qname_for_uri(str(type_uri)),
        period_signature=context_signature.get(ctx_ref, ()),
        unit_measure=unit_measure.get(unit_ref, ""),
        value=float(value),
        decimals=str(decimals),
      )
    )
  return facts


def _facts_from_xbrl(xbrl_zip: bytes) -> set[_FactTuple]:
  """Parse the XBRL zip back into a fact set.

  Walks ``instance.xml`` for contexts, units, and concept-typed facts;
  resolves contextRef → period signature, unitRef → measure.
  """
  with zipfile.ZipFile(io.BytesIO(xbrl_zip)) as zf:
    instance_xml = zf.read("instance.xml")
  root = etree.fromstring(instance_xml)

  context_signature: dict[str, tuple[str, ...]] = {}
  for ctx in root.findall(f"{{{XBRLI_XML}}}context"):
    cid = ctx.get("id")
    period_el = ctx.find(f"{{{XBRLI_XML}}}period")
    if cid is None or period_el is None:
      continue
    instant = period_el.find(f"{{{XBRLI_XML}}}instant")
    start = period_el.find(f"{{{XBRLI_XML}}}startDate")
    end = period_el.find(f"{{{XBRLI_XML}}}endDate")
    if instant is not None and instant.text:
      context_signature[cid] = ("instant", instant.text)
    elif start is not None and end is not None and start.text and end.text:
      context_signature[cid] = ("duration", start.text, end.text)

  unit_measure: dict[str, str] = {}
  for unit in root.findall(f"{{{XBRLI_XML}}}unit"):
    uid = unit.get("id")
    measure = unit.find(f"{{{XBRLI_XML}}}measure")
    if uid is None or measure is None or measure.text is None:
      continue
    unit_measure[uid] = measure.text.strip()

  # Facts are any element with contextRef + unitRef attributes (not
  # context/unit/schemaRef themselves).
  facts: set[_FactTuple] = set()
  for el in root:
    ctx_ref = el.get("contextRef")
    unit_ref = el.get("unitRef")
    if ctx_ref is None or unit_ref is None or el.text is None:
      continue
    # Recover the concept qname from the namespaced tag
    qname_uri = etree.QName(el.tag)
    concept_qname = _qname_from_namespace_and_local(
      qname_uri.namespace, qname_uri.localname, root.nsmap
    )
    facts.add(
      _FactTuple(
        concept_qname=concept_qname,
        period_signature=context_signature.get(ctx_ref, ()),
        unit_measure=unit_measure.get(unit_ref, ""),
        value=float(el.text.strip()),
        decimals=el.get("decimals") or "INF",
      )
    )
  return facts


# ── URI / qname helpers ───────────────────────────────────────────────────


_QNAME_BY_NAMESPACE: dict[str, str] = {
  "https://robosystems.ai/taxonomy/rs-gaap/v1/": "rs-gaap",
  "http://www.xbrl.org/2003/iso4217#": "iso4217",
  "http://www.xbrl.org/2003/iso4217": "iso4217",
  "http://www.xbrl.org/2003/instance#": "xbrli",
  "http://www.xbrl.org/2003/instance": "xbrli",
  "http://www.xbrlsite.com/fac#": "fac",
  "http://www.xbrlsite.com/fac": "fac",
}


def _qname_for_uri(uri: str) -> str:
  """Convert a full IRI to its compact qname using known namespaces."""
  for ns, prefix in _QNAME_BY_NAMESPACE.items():
    if uri.startswith(ns):
      return f"{prefix}:{uri[len(ns) :]}"
  return uri


def _qname_from_namespace_and_local(
  namespace: str | None, local: str, nsmap: dict[str, str] | None
) -> str:
  """Build a compact qname given the namespace + local + the doc nsmap."""
  if namespace is None:
    return local
  ns_with_hash = namespace if namespace.endswith("#") else namespace + "#"
  ns_no_hash = namespace.rstrip("#")
  for candidate in (namespace, ns_with_hash, ns_no_hash):
    prefix = _QNAME_BY_NAMESPACE.get(candidate)
    if prefix:
      return f"{prefix}:{local}"
  # Fall back to whatever prefix the XML declared
  if nsmap is not None:
    for prefix, uri in nsmap.items():
      if uri.rstrip("#") == ns_no_hash and prefix:
        return f"{prefix}:{local}"
  return local


def _period_signature_from_rdf(
  g: Graph, period_uri: rdflib.term.Node
) -> tuple[str, ...]:
  """Extract a (type, *dates) tuple from an xbrli:period rdflib node."""
  instant = next(g.objects(period_uri, XBRLI.instant), None)
  if instant is not None:
    return ("instant", str(instant))
  start = next(g.objects(period_uri, XBRLI.startDate), None)
  end = next(g.objects(period_uri, XBRLI.endDate), None)
  if start is not None and end is not None:
    return ("duration", str(start), str(end))
  return ()


def _root_uri_for_bundle(bundle: StatementBundle) -> URIRef:
  if bundle.report_meta is not None:
    return URIRef(f"https://robosystems.ai/report/{bundle.report_meta.report_id}")
  raise ValueError("Bundle has no report identity to root URI")


# ── The actual assertions ───────────────────────────────────────────────


def _assert_equivalent(bundle: StatementBundle) -> None:
  """Run both encoders and assert the parsed fact sets match."""
  jsonld_str = serialize_to_rdf(bundle, RdfFlavor.JSONLD)
  xbrl_zip = serialize_to_xbrl(bundle, XbrlFlavor.XBRL_2_1)

  jsonld_facts = _facts_from_jsonld(bundle, jsonld_str)
  xbrl_facts = _facts_from_xbrl(xbrl_zip)

  assert jsonld_facts == xbrl_facts, (
    "Cross-encoder fact-set mismatch:\n"
    f"  JSON-LD only: {sorted(jsonld_facts - xbrl_facts)}\n"
    f"  XBRL only:    {sorted(xbrl_facts - jsonld_facts)}\n"
    f"  JSON-LD count: {len(jsonld_facts)}\n"
    f"  XBRL count:    {len(xbrl_facts)}"
  )


# ── Tests ────────────────────────────────────────────────────────────────


def test_single_instant_fact_round_trips_to_both_encoders() -> None:
  """The smallest possible bundle — one fact, one context, one unit."""
  bundle = _make_bundle(
    facts=[("rs-gaap:Assets", 295_183_000.0, "ctx_1", "u_USD")],
    contexts=[("ctx_1", None, date(2024, 12, 31), "instant")],
    units=[("u_USD", "iso4217:USD")],
  )
  _assert_equivalent(bundle)


def test_duration_fact_with_period_range() -> None:
  """Income-statement-style fact — duration period."""
  bundle = _make_bundle(
    facts=[("rs-gaap:NetIncomeLoss", 12_345_000.0, "ctx_1", "u_USD")],
    contexts=[("ctx_1", date(2024, 1, 1), date(2024, 12, 31), "duration")],
    units=[("u_USD", "iso4217:USD")],
    concepts=[("rs-gaap:NetIncomeLoss", "duration", True)],
  )
  _assert_equivalent(bundle)


def test_multiple_contexts_share_one_concept() -> None:
  """Same concept reported across two periods (comparative
  statement). Tests that contextRef dedup + resolution holds across
  the two encoders."""
  bundle = _make_bundle(
    facts=[
      ("rs-gaap:Assets", 295_183_000.0, "ctx_1", "u_USD"),
      ("rs-gaap:Assets", 280_000_000.0, "ctx_2", "u_USD"),
    ],
    contexts=[
      ("ctx_1", None, date(2024, 12, 31), "instant"),
      ("ctx_2", None, date(2023, 12, 31), "instant"),
    ],
    units=[("u_USD", "iso4217:USD")],
  )
  _assert_equivalent(bundle)


def test_multiple_concepts_one_context() -> None:
  """Balance sheet shape — three concepts, all at the same instant."""
  bundle = _make_bundle(
    facts=[
      ("rs-gaap:Assets", 295_183_000.0, "ctx_1", "u_USD"),
      ("rs-gaap:Liabilities", 180_000_000.0, "ctx_1", "u_USD"),
      ("rs-gaap:StockholdersEquity", 115_183_000.0, "ctx_1", "u_USD"),
    ],
    contexts=[("ctx_1", None, date(2024, 12, 31), "instant")],
    units=[("u_USD", "iso4217:USD")],
  )
  _assert_equivalent(bundle)


def test_mixed_instant_and_duration_facts() -> None:
  """Real reports mix balance-sheet (instant) and income-statement
  (duration) facts — verify both period shapes round-trip in the
  same bundle."""
  bundle = _make_bundle(
    facts=[
      ("rs-gaap:Assets", 295_183_000.0, "ctx_eoy", "u_USD"),
      ("rs-gaap:NetIncomeLoss", 12_345_000.0, "ctx_fy", "u_USD"),
    ],
    contexts=[
      ("ctx_eoy", None, date(2024, 12, 31), "instant"),
      ("ctx_fy", date(2024, 1, 1), date(2024, 12, 31), "duration"),
    ],
    units=[("u_USD", "iso4217:USD")],
    concepts=[
      ("rs-gaap:Assets", "instant", True),
      ("rs-gaap:NetIncomeLoss", "duration", True),
    ],
  )
  _assert_equivalent(bundle)


def test_non_integer_value_round_trips_with_full_precision() -> None:
  """Decimal values (e.g., earnings per share) must round-trip
  without binary-float imprecision creeping in between the two
  encoders' decimal-formatting paths."""
  bundle = _make_bundle(
    facts=[("rs-gaap:EarningsPerShareBasic", 1.234, "ctx_1", "u_USD_per_share")],
    contexts=[("ctx_1", date(2024, 1, 1), date(2024, 12, 31), "duration")],
    units=[("u_USD_per_share", "iso4217:USD")],
    concepts=[("rs-gaap:EarningsPerShareBasic", "duration", True)],
  )
  _assert_equivalent(bundle)


def test_negative_value_round_trips() -> None:
  """Loss / liability-side facts often arrive negative. Both
  encoders must preserve sign."""
  bundle = _make_bundle(
    facts=[("rs-gaap:NetIncomeLoss", -8_400_000.0, "ctx_1", "u_USD")],
    contexts=[("ctx_1", date(2024, 1, 1), date(2024, 12, 31), "duration")],
    units=[("u_USD", "iso4217:USD")],
    concepts=[("rs-gaap:NetIncomeLoss", "duration", True)],
  )
  _assert_equivalent(bundle)


@pytest.mark.parametrize(
  "n_periods,n_concepts",
  [(1, 1), (1, 5), (3, 3), (4, 8)],
)
def test_grid_of_periods_and_concepts(n_periods: int, n_concepts: int) -> None:
  """Stress the dedup + cross-encoder symmetry with an N-by-M grid."""
  contexts = [
    (f"ctx_{i + 1}", None, date(2024 - i, 12, 31), "instant") for i in range(n_periods)
  ]
  concepts = [(f"rs-gaap:Concept{j + 1}", "instant", True) for j in range(n_concepts)]
  facts = [
    (
      f"rs-gaap:Concept{j + 1}",
      float(1_000 * (i + 1) * (j + 1)),
      f"ctx_{i + 1}",
      "u_USD",
    )
    for i in range(n_periods)
    for j in range(n_concepts)
  ]
  bundle = _make_bundle(
    facts=facts,
    contexts=contexts,
    units=[("u_USD", "iso4217:USD")],
    concepts=concepts,
  )
  _assert_equivalent(bundle)
