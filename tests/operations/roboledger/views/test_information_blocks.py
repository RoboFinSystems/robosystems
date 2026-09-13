"""Tests for the information-block views (``disclosures`` + ``information-block``).

A section read whole wants the whole report in memory, and the view gets one
from where the platform holds it: the published holon in the public bucket for
a shared repository, the report bundle the exports build for a tenant. The
tests stand a fake bucket and a fake graph lookup in for the first, a bundle
fixture in for the second, and let xbrlkit's own tools run unmocked over both
— the seam under test is that the two substrates yield the model those tools
expect. The rest pins the seams around it: report resolution, the model
cache, fragments that could not be read, and the caps the request models
restate.
"""

from __future__ import annotations

import zlib
from datetime import date
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from xbrlkit.model import (
  Arc,
  Concept,
  DimQualifier,
  EntityIdentity,
  FilingMeta,
  Label,
  Network,
  Unit,
  XbrlFact,
  XbrlModel,
)
from xbrlkit.parse.ids import unit_id
from xbrlkit.periods import duration_period, instant_period
from xbrlkit.serialize import to_holon
from xbrlkit.serve import tools as xbrlkit_tools

from robosystems.models.api.extensions.reports import (
  INFORMATION_BLOCK_MAX_MEMBERS,
  INFORMATION_BLOCK_MAX_ROWS,
)
from robosystems.operations.roboledger.views import information_blocks as module
from robosystems.operations.roboledger.views.information_blocks import (
  BlockNotFoundError,
  ReportNotFoundError,
  ReportNotPublishedError,
  ReportSelectorError,
  load_report_model,
  query_disclosures,
  query_information_block,
  resolve_report,
  resolved_report_info,
)
from robosystems.operations.serialization.bundle import (
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
)

MODULE = "robosystems.operations.roboledger.views.information_blocks"
US_GAAP = "http://fasb.org/us-gaap/2024"
ACME = "http://www.acme.example/20241231"
XBRLI = "http://www.xbrl.org/2003/instance"
STANDARD = "http://www.xbrl.org/2003/role/label"
PARENT_CHILD = "http://www.xbrl.org/2003/arcrole/parent-child"
SUMMATION = "http://www.xbrl.org/2003/arcrole/summation-item"
DIM = "http://xbrl.org/int/dim/arcrole"
BALANCE_SHEET = "http://www.acme.example/role/BalanceSheet"
SEGMENTS = "http://www.acme.example/role/SegmentsDetails"
REPORT_URI = (
  "https://www.sec.gov/Archives/edgar/data/12345/000001234525000001/acme-20241231.htm"
)
USD_URI = "http://www.xbrl.org/2003/iso4217#USD"
ACCESSION = "0000012345-25-000001"
CIK = "0000012345"
REPORT_ID = "rpt-acme"
HOLON_KEY = f"2025/{CIK}/{ACCESSION}/holon.jsonld"
FRAGMENT_KEY = f"2025/{CIK}/{ACCESSION}/fact_abc.html"
FRAGMENT = "<p>The company reports one segment, Widgets, and sells them everywhere.</p>"


def _concept(qname: str, namespace: str, **kw: Any) -> Concept:
  local = qname.split(":", 1)[1]
  label = kw.pop("label", local)
  labels = kw.pop("labels", [Label(value=label, role=STANDARD)])
  return Concept(
    qname=qname, namespace=namespace, name=local, pref_label=label, labels=labels, **kw
  )


def _model() -> XbrlModel:
  """A fictional filer's 10-K, small enough to read whole in a test: a balance
  sheet that foots, a segment table with one breakdown, and one text block
  whose value is the URL of its fragment — as a published holon carries it."""
  instant = instant_period(date(2024, 12, 31))
  prior = instant_period(date(2023, 12, 31))
  year = duration_period(date(2024, 1, 1), date(2024, 12, 31))
  usd = Unit(id=unit_id(USD_URI), measure="iso4217:USD", uri=USD_URI)
  monetary: dict[str, Any] = {
    "is_numeric": True,
    "item_type": "monetaryItemType",
    "item_type_qname": "xbrli:monetaryItemType",
    "item_type_namespace": XBRLI,
    "nice_type": "Monetary",
  }
  concepts = {
    "us-gaap:AssetsAbstract": _concept(
      "us-gaap:AssetsAbstract", US_GAAP, is_abstract=True, label="Assets [Abstract]"
    ),
    "us-gaap:Assets": _concept(
      "us-gaap:Assets", US_GAAP, period_type="instant", balance="debit", **monetary
    ),
    "us-gaap:Cash": _concept(
      "us-gaap:Cash", US_GAAP, period_type="instant", balance="debit", **monetary
    ),
    "us-gaap:OtherAssets": _concept(
      "us-gaap:OtherAssets",
      US_GAAP,
      period_type="instant",
      balance="debit",
      label="Other assets",
      **monetary,
    ),
    "us-gaap:SegmentReportingDisclosureTextBlock": _concept(
      "us-gaap:SegmentReportingDisclosureTextBlock",
      US_GAAP,
      period_type="duration",
      is_textblock=True,
      is_text_fact=True,
      nice_type="TextBlock",
      label="Segment Reporting",
    ),
    "us-gaap:SegmentTable": _concept(
      "us-gaap:SegmentTable", US_GAAP, is_abstract=True, is_hypercube_item=True
    ),
    "us-gaap:StatementBusinessSegmentsAxis": _concept(
      "us-gaap:StatementBusinessSegmentsAxis",
      US_GAAP,
      is_abstract=True,
      is_dimension_item=True,
      nice_type="Axis",
    ),
    "us-gaap:SegmentDomain": _concept(
      "us-gaap:SegmentDomain", US_GAAP, is_abstract=True, is_domain_member=True
    ),
    "acme:WidgetsMember": _concept(
      "acme:WidgetsMember", ACME, is_domain_member=True, label="Widgets"
    ),
  }
  filer: dict[str, Any] = {"entity_cik": CIK, "entity_scheme": "http://www.sec.gov/CIK"}

  def fact(id_: str, qname: str, period: str, value: float, **kw: Any) -> XbrlFact:
    return XbrlFact(
      id=id_,
      concept_qname=qname,
      period_id=period,
      unit_id=usd.id,
      value_str=str(int(value)),
      numeric_value=value,
      decimals="-3",
      source_hash=id_,
      **filer,
      **kw,
    )

  facts = [
    fact("a1", "us-gaap:Assets", instant.id, 1000.0),
    fact("a2", "us-gaap:Cash", instant.id, 400.0),
    fact("a3", "us-gaap:OtherAssets", instant.id, 600.0),
    fact("a4", "us-gaap:Assets", prior.id, 900.0),
    fact("a5", "us-gaap:Cash", prior.id, 350.0),
    fact("a6", "us-gaap:OtherAssets", prior.id, 550.0),
    fact(
      "a7",
      "us-gaap:Cash",
      instant.id,
      250.0,
      dims=[
        DimQualifier(
          axis_qname="us-gaap:StatementBusinessSegmentsAxis",
          member_qname="acme:WidgetsMember",
          axis_type="segment",
        )
      ],
    ),
    XbrlFact(
      id="a8",
      concept_qname="us-gaap:SegmentReportingDisclosureTextBlock",
      period_id=year.id,
      value_str=f"https://public.example/{FRAGMENT_KEY}",
      value_kind="text",
      source_hash="a8",
      **filer,
    ),
  ]
  balance_sheet = "0000002 - Statement - Balance Sheet"
  segments = "0000010 - Disclosure - Segments (Details)"
  networks = [
    Network(
      role_uri=BALANCE_SHEET,
      definition=balance_sheet,
      kind="presentation",
      role_id="BalanceSheet",
      arcs=[
        Arc(
          from_qname="us-gaap:AssetsAbstract",
          to_qname="us-gaap:Cash",
          arcrole=PARENT_CHILD,
          order=1.0,
          is_root=True,
        ),
        Arc(
          from_qname="us-gaap:AssetsAbstract",
          to_qname="us-gaap:OtherAssets",
          arcrole=PARENT_CHILD,
          order=2.0,
        ),
        Arc(
          from_qname="us-gaap:AssetsAbstract",
          to_qname="us-gaap:Assets",
          arcrole=PARENT_CHILD,
          order=3.0,
          preferred_label="http://www.xbrl.org/2003/role/totalLabel",
        ),
      ],
    ),
    Network(
      role_uri=BALANCE_SHEET,
      definition=balance_sheet,
      kind="calculation",
      role_id="BalanceSheet",
      arcs=[
        Arc(
          from_qname="us-gaap:Assets",
          to_qname="us-gaap:Cash",
          arcrole=SUMMATION,
          order=1.0,
          weight=1.0,
          is_root=True,
        ),
        Arc(
          from_qname="us-gaap:Assets",
          to_qname="us-gaap:OtherAssets",
          arcrole=SUMMATION,
          order=2.0,
          weight=1.0,
        ),
      ],
    ),
    Network(
      role_uri=SEGMENTS,
      definition=segments,
      kind="presentation",
      role_id="SegmentsDetails",
      arcs=[
        Arc(
          from_qname="us-gaap:SegmentTable",
          to_qname="us-gaap:StatementBusinessSegmentsAxis",
          arcrole=PARENT_CHILD,
          order=1.0,
          is_root=True,
        ),
        Arc(
          from_qname="us-gaap:StatementBusinessSegmentsAxis",
          to_qname="us-gaap:SegmentDomain",
          arcrole=PARENT_CHILD,
          order=1.0,
        ),
        Arc(
          from_qname="us-gaap:SegmentDomain",
          to_qname="acme:WidgetsMember",
          arcrole=PARENT_CHILD,
          order=1.0,
        ),
        Arc(
          from_qname="us-gaap:SegmentTable",
          to_qname="us-gaap:Cash",
          arcrole=PARENT_CHILD,
          order=2.0,
        ),
        Arc(
          from_qname="us-gaap:SegmentTable",
          to_qname="us-gaap:SegmentReportingDisclosureTextBlock",
          arcrole=PARENT_CHILD,
          order=3.0,
        ),
      ],
    ),
    Network(
      role_uri=SEGMENTS,
      definition=segments,
      kind="definition",
      role_id="SegmentsDetails",
      arcs=[
        Arc(
          from_qname="us-gaap:Cash",
          to_qname="us-gaap:SegmentTable",
          arcrole=f"{DIM}/all",
          order=1.0,
          is_root=True,
        ),
        Arc(
          from_qname="us-gaap:SegmentTable",
          to_qname="us-gaap:StatementBusinessSegmentsAxis",
          arcrole=f"{DIM}/hypercube-dimension",
          order=1.0,
        ),
        Arc(
          from_qname="us-gaap:StatementBusinessSegmentsAxis",
          to_qname="us-gaap:SegmentDomain",
          arcrole=f"{DIM}/dimension-domain",
          order=1.0,
        ),
        Arc(
          from_qname="us-gaap:SegmentDomain",
          to_qname="acme:WidgetsMember",
          arcrole=f"{DIM}/domain-member",
          order=1.0,
        ),
      ],
    ),
  ]
  return XbrlModel(
    filing=FilingMeta(
      accession=ACCESSION,
      cik=CIK,
      form="10-K",
      filing_date=date(2025, 2, 5),
      fiscal_year_focus="2024",
      fiscal_period_focus="FY",
      report_date=date(2024, 12, 31),
      is_inline_xbrl=True,
      report_uri=REPORT_URI,
      extension_namespace=ACME,
    ),
    entity=EntityIdentity(cik=CIK, name="Acme Industrial Corp", ticker="ACME"),
    concepts=concepts,
    periods=[instant, prior, year],
    units=[usd],
    facts=facts,
    networks=networks,
  )


def _bundle() -> StatementBundle:
  """A tenant's balance sheet as the exports build it: one presentation link
  and its calculation link on one structure, facts pinned to it."""
  elements = [
    BundleElement(
      id="Assets",
      qname="rs-gaap:Assets",
      name="Assets",
      label="Assets",
      period_type="instant",
      balance_type="debit",
      source="rs-gaap",
    ),
    BundleElement(
      id="Cash",
      qname="rl:1000",
      name="Cash and equivalents",
      label="Cash and equivalents",
      period_type="instant",
      balance_type="debit",
      source="native",
    ),
    BundleElement(
      id="Other",
      qname="rl:1900",
      name="Other assets",
      label="Other assets",
      period_type="instant",
      balance_type="debit",
      source="native",
    ),
  ]
  arcs = [
    BundleArc(
      arc_type="presentationArc",
      arcrole=PARENT_CHILD,
      from_qname="rs-gaap:Assets",
      to_qname="rl:1000",
      order_value=1.0,
    ),
    BundleArc(
      arc_type="presentationArc",
      arcrole=PARENT_CHILD,
      from_qname="rs-gaap:Assets",
      to_qname="rl:1900",
      order_value=2.0,
    ),
  ]
  calcs = [
    BundleArc(
      arc_type="calculationArc",
      arcrole=SUMMATION,
      from_qname="rs-gaap:Assets",
      to_qname="rl:1000",
      order_value=1.0,
      weight=1.0,
    ),
    BundleArc(
      arc_type="calculationArc",
      arcrole=SUMMATION,
      from_qname="rs-gaap:Assets",
      to_qname="rl:1900",
      order_value=2.0,
      weight=1.0,
    ),
  ]

  def fact(id_: str, element: BundleElement, value: float) -> BundleFact:
    return BundleFact(
      id=id_,
      element_id=element.id,
      element_qname=element.qname,
      value=value,
      period_ref="p_1",
      unit_ref="u_USD",
      entity_ref="ent_01",
      decimals="2",
      fact_set_id="fs_1",
      structure_id="st_bs",
    )

  return StatementBundle(
    entity=EntityMeta(id="ent_01", name="Acme LLC"),
    periods=[PeriodMeta(start=date(2025, 1, 1), end=date(2025, 12, 31), label="FY25")],
    reporting_style="BSC-CORP-IS02-CF1",
    framework_pins=[FrameworkPin(framework="rs-gaap", version="v1")],
    schema_concepts=elements,
    linkbases=BundleLinkbases(
      presentation_links=[
        BundleLinkbaseLink(
          link_type="presentationLink",
          role_uri="",
          structure_id="st_bs",
          structure_name="Balance Sheet",
          block_type="balance_sheet",
          arcs=arcs,
        )
      ],
      calculation_links=[
        BundleLinkbaseLink(
          link_type="calculationLink",
          role_uri="",
          structure_id="st_bs",
          structure_name="Balance Sheet",
          block_type="balance_sheet",
          arcs=calcs,
        )
      ],
    ),
    period_nodes=[
      BundlePeriod(id="p_1", period_end=date(2025, 12, 31), period_type="instant")
    ],
    units=[BundleUnit(id="u_USD", measure="iso4217:USD")],
    facts=[
      fact("f1", elements[0], 1500.0),
      fact("f2", elements[1], 400.0),
      fact("f3", elements[2], 1100.0),
    ],
    ib_envelopes=[],
    mode="report",
    report_meta=ReportMeta(
      report_id=REPORT_ID, generation_count=1, filing_status="filed"
    ),
  )


class _FakeBucket:
  """``S3Client`` as the view uses it: ``download_string(bucket, key)``."""

  def __init__(self, objects: dict[str, str]) -> None:
    self.objects = objects
    self.reads: list[str] = []

  def download_string(self, bucket: str, key: str) -> str | None:
    self.reads.append(key)
    return self.objects.get(key)


class _FakeRedis:
  def __init__(self) -> None:
    self.store: dict[str, bytes] = {}
    self.ttls: dict[str, int] = {}

  async def get(self, key: str) -> bytes | None:
    return self.store.get(key)

  async def set(self, key: str, value: bytes, ex: int | None = None) -> None:
    self.store[key] = value
    if ex is not None:
      self.ttls[key] = ex


@pytest.fixture
def no_cache(monkeypatch: pytest.MonkeyPatch) -> None:
  monkeypatch.setattr(module, "_cache", lambda: None)


@pytest.fixture
def published(monkeypatch: pytest.MonkeyPatch):
  """The shared repository: the graph answers the filing's coordinates, the
  bucket holds its holon and one text-block fragment."""
  bucket = _FakeBucket({HOLON_KEY: to_holon(_model()), FRAGMENT_KEY: FRAGMENT})
  repository = AsyncMock()
  repository.execute_query = AsyncMock(
    return_value=[{"accession": ACCESSION, "filing_date": "2025-02-05", "cik": CIK}]
  )
  monkeypatch.setattr(module, "S3Client", lambda: bucket)
  monkeypatch.setattr(
    module, "get_graph_repository", AsyncMock(return_value=repository)
  )
  monkeypatch.setattr(module, "is_shared_repository_or_subgraph", lambda graph_id: True)
  monkeypatch.setattr(module.env, "PUBLIC_DATA_BUCKET", "public-data-test")
  return bucket


@pytest.fixture
def tenant(monkeypatch: pytest.MonkeyPatch):
  """A tenant graph: the exports' bundle builder answers with the fixture."""
  import robosystems.db.extensions as extensions_db
  import robosystems.operations.serialization as serialization

  session = MagicMock()
  session.__enter__ = MagicMock(return_value=session)
  session.__exit__ = MagicMock(return_value=False)
  monkeypatch.setattr(extensions_db, "extensions_session", lambda graph_id: session)
  builder = MagicMock(return_value=_bundle())
  monkeypatch.setattr(serialization, "build_report_bundle", builder)
  monkeypatch.setattr(
    module, "is_shared_repository_or_subgraph", lambda graph_id: False
  )
  return builder


# ── the shared repository: the published filing ─────────────────────────────


@pytest.mark.asyncio
@pytest.mark.unit
class TestPublishedFiling:
  async def test_disclosures_maps_the_filing(self, published, no_cache) -> None:
    out = await query_disclosures("sec", REPORT_ID)
    assert (out["graph_id"], out["report_id"]) == ("sec", REPORT_ID)
    assert [row["disclosure"] for row in out["disclosures"]] == [
      "Balance Sheet",
      "Segments",
    ]
    assert out["disclosures"][0]["levels"] == {"statement": 1}
    assert out["disclosures"][1]["text_blocks"] == 1
    # The holon and the fragment, nothing else, came out of the bucket.
    assert published.reads == [HOLON_KEY, FRAGMENT_KEY]

  async def test_a_familys_blocks_and_the_block_itself(
    self, published, no_cache
  ) -> None:
    fam = await query_disclosures("sec", REPORT_ID, topic="segments")
    [block] = fam["blocks"]
    assert block["id"] == "SegmentsDetails"
    assert block["axes"] == ["us-gaap:StatementBusinessSegmentsAxis"]
    # The fragment was inlined: the family's text entry and the block's
    # preview are the text, not a URL.
    assert block["text_blocks"][0]["chars"] == len(
      "The company reports one segment, Widgets, and sells them everywhere."
    )
    out = await query_information_block("sec", REPORT_ID, "SegmentsDetails")
    cash = next(row for row in out["rows"] if row["concept"] == "us-gaap:Cash")
    assert cash["members"] == {"acme:WidgetsMember": {"2024-12-31": 250.0}}
    [text] = out["text"]
    assert text["preview"].startswith("The company reports one segment")
    assert "external" not in text

  async def test_the_balance_sheet_foots(self, published, no_cache) -> None:
    out = await query_information_block("sec", REPORT_ID, "BalanceSheet")
    assert [row["label"] for row in out["rows"]] == [
      "Assets [Abstract]",
      "Cash",
      "Other assets",
      "Assets",
    ]
    [roll] = out["calculation"]
    assert (roll["total"], roll["foots"], roll["checked"]) == ("us-gaap:Assets", 2, 2)

  async def test_a_fragment_that_cannot_be_read_stays_marked(
    self, published, no_cache
  ) -> None:
    del published.objects[FRAGMENT_KEY]
    out = await query_information_block("sec", REPORT_ID, "SegmentsDetails")
    [text] = out["text"]
    assert text["external"] is True and "preview" not in text
    assert "search-documents" in out["note"]

  async def test_a_filing_without_artifacts_is_not_published(
    self, published, no_cache
  ) -> None:
    del published.objects[HOLON_KEY]
    with pytest.raises(
      ReportNotPublishedError, match="processed before its filing artifacts"
    ):
      await query_disclosures("sec", REPORT_ID)

  async def test_a_report_the_graph_does_not_hold_is_not_found(
    self, published, no_cache
  ) -> None:
    repository = await module.get_graph_repository("sec")
    repository.execute_query = AsyncMock(return_value=[])
    with pytest.raises(ReportNotFoundError, match="No report"):
      await query_disclosures("sec", "nope")

  async def test_an_unknown_block_or_topic_is_not_found(
    self, published, no_cache
  ) -> None:
    with pytest.raises(BlockNotFoundError):
      await query_information_block("sec", REPORT_ID, "Pensions")
    with pytest.raises(BlockNotFoundError, match="No disclosure matches"):
      await query_disclosures("sec", REPORT_ID, topic="pensions")

  async def test_a_fragment_url_maps_to_its_key_with_or_without_the_bucket(
    self,
  ) -> None:
    assert (
      module._fragment_key("https://public.example/2025/x/y/fact_1.html", "b")
      == "2025/x/y/fact_1.html"
    )
    assert (
      module._fragment_key("http://localhost:4566/b/2025/x/y/fact_1.html", "b")
      == "2025/x/y/fact_1.html"
    )


# ── a tenant graph: the ledger's report ─────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.unit
class TestTenantReport:
  async def test_disclosures_maps_the_ledgers_report(self, tenant, no_cache) -> None:
    out = await query_disclosures("kg1234567890abcdef", REPORT_ID)
    assert tenant.call_args.args[1:] == ("kg1234567890abcdef", REPORT_ID)
    [fam] = out["disclosures"]
    assert fam["disclosure"] == "Balance Sheet"
    assert fam["levels"] == {"statement": 1}
    assert fam["facts"] == 3

  async def test_the_block_reads_the_live_report_and_foots(
    self, tenant, no_cache
  ) -> None:
    fam = await query_disclosures("kg1234567890abcdef", REPORT_ID, topic="balance")
    [block] = fam["blocks"]
    out = await query_information_block("kg1234567890abcdef", REPORT_ID, block["id"])
    assert out["block"]["block_type"] == "balance_sheet"
    labels = [row["label"] for row in out["rows"]]
    assert labels == ["Assets", "Cash and equivalents", "Other assets"]
    assets = out["rows"][0]
    assert assets["values"] == {"2025-12-31": 1500.0}
    [roll] = out["calculation"]
    assert (roll["foots"], roll["checked"]) == (1, 1)

  async def test_a_report_the_ledger_does_not_hold_is_not_found(
    self, tenant, no_cache
  ) -> None:
    tenant.side_effect = LookupError("Report 'nope' not found")
    with pytest.raises(ReportNotFoundError, match="nope"):
      await query_disclosures("kg1234567890abcdef", "nope")


# ── the cache ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.unit
class TestCache:
  async def test_a_published_model_is_cached_by_report(
    self, published, monkeypatch
  ) -> None:
    redis = _FakeRedis()
    monkeypatch.setattr(module, "_cache", lambda: redis)
    model, cached = await load_report_model("sec", REPORT_ID)
    assert cached is False
    [key] = list(redis.store)
    assert key == f"ib:model:v{module.MODEL_CACHE_VERSION}:sec:{REPORT_ID}"
    assert redis.ttls[key] == module.MODEL_CACHE_TTL_SHARED_SECONDS
    assert b'"accession"' in zlib.decompress(redis.store[key])
    reads = len(published.reads)
    again, cached_again = await load_report_model("sec", REPORT_ID)
    assert cached_again is True
    assert len(published.reads) == reads
    assert again.model_dump() == model.model_dump()

  async def test_a_tenant_model_is_cached_for_less_time(
    self, tenant, monkeypatch
  ) -> None:
    redis = _FakeRedis()
    monkeypatch.setattr(module, "_cache", lambda: redis)
    await load_report_model("kg1234567890abcdef", REPORT_ID)
    assert list(redis.ttls.values()) == [module.MODEL_CACHE_TTL_TENANT_SECONDS]

  async def test_a_cache_outage_costs_a_read_not_the_call(
    self, published, monkeypatch
  ) -> None:
    broken = AsyncMock()
    broken.get = AsyncMock(side_effect=ConnectionError("valkey down"))
    broken.set = AsyncMock(side_effect=ConnectionError("valkey down"))
    monkeypatch.setattr(module, "_cache", lambda: broken)
    out = await query_disclosures("sec", REPORT_ID)
    assert out["count"] == 2


# ── report resolution ───────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.unit
class TestResolveReport:
  async def test_a_report_id_is_taken_as_given(self) -> None:
    assert await resolve_report("kg1234567890abcdef", report_id="rpt_1") == (
      "rpt_1",
      None,
    )

  async def test_a_tenant_needs_a_report_id(self) -> None:
    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=False),
      pytest.raises(ReportSelectorError, match="report_id is required"),
    ):
      await resolve_report("kg1234567890abcdef", ticker="ACME")

  async def test_a_shared_repo_needs_a_ticker_or_a_report_id(self) -> None:
    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True),
      pytest.raises(ReportSelectorError, match="ticker is required"),
    ):
      await resolve_report("sec")

  async def test_a_ticker_resolves_the_latest_filing(self) -> None:
    resolved = {
      "identifier": "rpt_abc",
      "form": "10-K",
      "filing_date": "2025-02-05",
      "fiscal_year": 2024,
      "fiscal_period": "FY",
    }
    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True),
      patch(
        "robosystems.adapters.sec.mcp.resolve_sec_report",
        new=AsyncMock(return_value=resolved),
      ) as resolve,
    ):
      report_id, info = await resolve_report(
        "sec", ticker="acme", fiscal_year=2024, period_type="annual"
      )
    assert (report_id, info) == ("rpt_abc", resolved)
    assert resolve.call_args.kwargs["ticker"] == "ACME"
    assert resolved_report_info(info) == {
      "report_id": "rpt_abc",
      "form": "10-K",
      "filing_date": "2025-02-05",
      "fiscal_year": 2024,
      "fiscal_period": "FY",
    }

  async def test_a_year_with_no_filing_is_not_found(self) -> None:
    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True),
      patch(
        "robosystems.adapters.sec.mcp.resolve_sec_report",
        new=AsyncMock(return_value=None),
      ),
      pytest.raises(ReportNotFoundError, match="ACME in fiscal year 2005"),
    ):
      await resolve_report("sec", ticker="ACME", fiscal_year=2005)


@pytest.mark.unit
def test_the_request_models_restate_xbrlkits_caps() -> None:
  assert (
    INFORMATION_BLOCK_MAX_ROWS == xbrlkit_tools.MAX_BLOCK_ROWS == module.MAX_BLOCK_ROWS
  )
  assert (
    INFORMATION_BLOCK_MAX_MEMBERS
    == xbrlkit_tools.MAX_BLOCK_MEMBERS_CAP
    == module.MAX_BLOCK_MEMBERS
  )
