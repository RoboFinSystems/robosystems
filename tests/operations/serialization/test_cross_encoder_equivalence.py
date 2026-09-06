"""Cross-encoder fact-equivalence assertion (v1 graph-native).

The ontology claim: *one bundle, three encoders, one fact set*. The JSON-LD
encoder (graph-native: ``rs:Fact`` referencing ``rs:period``/``rs:unit``), the
XBRL 2.1 emitter (which re-derives ``<context>``) and the Tavi flavor (xbrlkit's
emitter, fed through the bundle → ``XbrlModel`` bridge) share the same
:class:`StatementBundle` and produce semantically equivalent output — every
fact emerges in all three projections with matching
``(concept, period, unit, value, decimals)``.
"""

from __future__ import annotations

import io
import json
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta

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
)

RS = Namespace("https://robosystems.ai/vocab/")
XBRLI = Namespace("http://www.xbrl.org/2003/instance#")
XBRLI_XML = "http://www.xbrl.org/2003/instance"
RDF = rdflib.RDF


# ── Fixture ──────────────────────────────────────────────────────────────


def _make_bundle(
  *,
  facts: list[tuple[str, float, str, str]],
  period_nodes: list[tuple[str, date | None, date, str]],
  units: list[tuple[str, str]],
) -> StatementBundle:
  """``facts``=(qname, value, period_ref, unit_ref); ``period_nodes``=
  (id, start, end, period_type); ``units``=(id, measure)."""
  seen: set[str] = set()
  concepts: list[BundleElement] = []
  for qname, _, _, _ in facts:
    if qname in seen:
      continue
    seen.add(qname)
    concepts.append(
      BundleElement(
        id=qname.split(":")[-1],
        qname=qname,
        name=qname.split(":")[-1],
        period_type="instant",
        is_monetary=True,
        balance_type="debit",
        source="rs-gaap",
      )
    )
  return StatementBundle(
    entity=EntityMeta(id="ent_01", name="Test Co"),
    periods=[PeriodMeta(start=date(2024, 1, 1), end=date(2024, 12, 31), label="FY24")],
    reporting_style="BSC-CORP-IS02-CF1",
    framework_pins=[FrameworkPin(framework="rs-gaap", version="v1")],
    schema_concepts=concepts,
    linkbases=BundleLinkbases(),
    period_nodes=[
      BundlePeriod(id=pid, period_start=ps, period_end=pe, period_type=pt)  # type: ignore[arg-type]
      for pid, ps, pe, pt in period_nodes
    ],
    units=[BundleUnit(id=uid, measure=m) for uid, m in units],
    facts=[
      BundleFact(
        id=f"fact_{i + 1:02d}",
        element_id=q.split(":")[-1],
        element_qname=q,
        value=v,
        period_ref=pref,
        unit_ref=uref,
        entity_ref="ent_01",
      )
      for i, (q, v, pref, uref) in enumerate(facts)
    ],
    ib_envelopes=[],
    mode="report",
    report_meta=ReportMeta(
      report_id="rpt_test", generation_count=1, filing_status="draft"
    ),
  )


@dataclass(frozen=True)
class _FactTuple:
  concept: str
  period: tuple[str, ...]
  unit: str
  value: float
  decimals: str


def _measure_local(m: str) -> str:
  return m.rsplit("#", 1)[-1].rsplit(":", 1)[-1].rsplit("/", 1)[-1]


def _qname_local(uri: str) -> str:
  return uri.rsplit("#", 1)[-1].rsplit("/", 1)[-1]


def _facts_from_jsonld(jsonld_str: str) -> set[_FactTuple]:
  g = Graph().parse(data=jsonld_str, format="json-ld")

  def _period_sig(p: URIRef) -> tuple[str, ...]:
    ptype = str(next(g.objects(p, XBRLI.periodType)))
    if ptype == "instant":
      return ("instant", str(next(g.objects(p, XBRLI.instant))))
    return (
      "duration",
      str(next(g.objects(p, XBRLI.startDate))),
      str(next(g.objects(p, XBRLI.endDate))),
    )

  out: set[_FactTuple] = set()
  for fact in g.subjects(RDF.type, RS.Fact):
    element = next(g.objects(fact, RS.element))
    period = next(g.objects(fact, RS.period))
    unit = next(g.objects(fact, RS.unit))
    measure = next(g.objects(unit, XBRLI.measure))
    out.add(
      _FactTuple(
        concept=_qname_local(str(element)),
        period=_period_sig(period),  # type: ignore[arg-type]
        unit=_measure_local(str(measure)),
        value=float(next(g.objects(fact, RS.numericValue))),
        decimals=str(next(g.objects(fact, RS.decimals))),
      )
    )
  return out


def _facts_from_xbrl(xbrl_zip: bytes) -> set[_FactTuple]:
  with zipfile.ZipFile(io.BytesIO(xbrl_zip)) as zf:
    root = etree.fromstring(zf.read("instance.xml"))

  ctx_sig: dict[str, tuple[str, ...]] = {}
  for ctx in root.findall(f"{{{XBRLI_XML}}}context"):
    cid = ctx.get("id")
    period = ctx.find(f"{{{XBRLI_XML}}}period")
    if cid is None or period is None:
      continue
    inst = period.find(f"{{{XBRLI_XML}}}instant")
    start = period.find(f"{{{XBRLI_XML}}}startDate")
    end = period.find(f"{{{XBRLI_XML}}}endDate")
    if inst is not None and inst.text:
      ctx_sig[cid] = ("instant", inst.text)
    elif start is not None and end is not None and start.text and end.text:
      ctx_sig[cid] = ("duration", start.text, end.text)

  unit_measure: dict[str, str] = {}
  for unit in root.findall(f"{{{XBRLI_XML}}}unit"):
    uid = unit.get("id")
    measure = unit.find(f"{{{XBRLI_XML}}}measure")
    if uid and measure is not None and measure.text:
      unit_measure[uid] = _measure_local(measure.text.strip())

  out: set[_FactTuple] = set()
  for el in root:
    cref = el.get("contextRef")
    uref = el.get("unitRef")
    if cref is None or uref is None or el.text is None:
      continue
    out.add(
      _FactTuple(
        concept=_qname_local(etree.QName(el.tag).localname),
        period=ctx_sig.get(cref, ()),
        unit=unit_measure.get(uref, ""),
        value=float(el.text.strip()),
        decimals=el.get("decimals") or "INF",
      )
    )
  return out


def _inclusive(moment: str) -> str:
  """A Tavi exclusive-end ``xs:dateTime`` back to the inclusive date it stands for."""
  return (datetime.fromisoformat(moment) - timedelta(days=1)).date().isoformat()


def _facts_from_tavi(tavi: bytes) -> set[_FactTuple]:
  document = json.loads(tavi)
  out: set[_FactTuple] = set()
  for fact in document["xbrlModel"]["facts"]:
    dims = fact["factDimensions"]
    if "xbrl:unit" not in dims:
      continue
    interval = dims["xbrl:period"]
    if "/" in interval:
      start, end = interval.split("/", 1)
      period = ("duration", start[:10], _inclusive(end))
    else:
      period = ("instant", _inclusive(interval))
    (value,) = fact["factValues"]
    out.add(
      _FactTuple(
        concept=dims["xbrl:concept"].rsplit(":", 1)[-1],
        period=period,
        unit=_measure_local(dims["xbrl:unit"]),
        value=float(value["value"]),
        decimals=str(value["decimals"]) if "decimals" in value else "INF",
      )
    )
  return out


def _both(bundle: StatementBundle) -> tuple[set[_FactTuple], set[_FactTuple]]:
  """The JSON-LD and XBRL projections, after asserting the Tavi matches both."""
  jsonld = serialize_to_rdf(bundle, RdfFlavor.JSONLD)
  xbrl = serialize_to_xbrl(bundle, XbrlFlavor.XBRL_2_1)
  tavi = serialize_to_xbrl(bundle, XbrlFlavor.TAVI)
  from_jsonld, from_xbrl = _facts_from_jsonld(jsonld), _facts_from_xbrl(xbrl)
  assert _facts_from_tavi(tavi) == from_jsonld
  assert _facts_from_tavi(tavi) == from_xbrl
  return from_jsonld, from_xbrl


# ── Tests ────────────────────────────────────────────────────────────────


class TestCrossEncoderEquivalence:
  def test_single_instant_fact(self) -> None:
    b = _make_bundle(
      facts=[("rs-gaap:Assets", 295_183_000.0, "p_1", "u_USD")],
      period_nodes=[("p_1", None, date(2024, 12, 31), "instant")],
      units=[("u_USD", "iso4217:USD")],
    )
    jl, xb = _both(b)
    assert jl == xb
    assert len(jl) == 1

  def test_multiple_periods(self) -> None:
    b = _make_bundle(
      facts=[
        ("rs-gaap:Assets", 100.0, "p_1", "u_USD"),
        ("rs-gaap:Revenues", 50.0, "p_2", "u_USD"),
      ],
      period_nodes=[
        ("p_1", None, date(2024, 12, 31), "instant"),
        ("p_2", date(2024, 1, 1), date(2024, 12, 31), "duration"),
      ],
      units=[("u_USD", "iso4217:USD")],
    )
    jl, xb = _both(b)
    assert jl == xb
    assert len(jl) == 2
    # both projections carry an instant and a duration period signature
    assert {f.period[0] for f in jl} == {"instant", "duration"}

  def test_value_and_concept_match(self) -> None:
    b = _make_bundle(
      facts=[("rs-gaap:Assets", 295_183_000.0, "p_1", "u_USD")],
      period_nodes=[("p_1", None, date(2024, 12, 31), "instant")],
      units=[("u_USD", "iso4217:USD")],
    )
    jl, xb = _both(b)
    fact = next(iter(jl))
    assert fact.concept == "Assets"
    assert fact.value == 295_183_000.0
    assert fact.unit == "USD"
    assert fact in xb
