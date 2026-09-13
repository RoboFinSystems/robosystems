"""Tests for the information-block views (``disclosures`` + ``information-block``).

The property under test is the seam: a report's slice comes out of the graph
through the repository interface, goes into xbrlkit's model, and xbrlkit's
own tools answer over it. So the main test runs the real slice queries
against a real single-filing LadybugDB built from a fixture model, through a
fake repository that speaks the repository's ``execute_query`` — no mock of
the Cypher, no mock of xbrlkit. The rest pins the seams around it: report
resolution, the model cache, the text-block handling for a graph that stores
text outside itself, and the caps the request models restate.
"""

from __future__ import annotations

import zlib
from datetime import date
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

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
from xbrlkit.serialize.lpg import build_lbug, to_graph_tables
from xbrlkit.serve import tools as xbrlkit_tools

from robosystems.models.api.extensions.reports import (
  INFORMATION_BLOCK_MAX_MEMBERS,
  INFORMATION_BLOCK_MAX_ROWS,
)
from robosystems.operations.roboledger.views import information_blocks as module
from robosystems.operations.roboledger.views.information_blocks import (
  BlockNotFoundError,
  ReportNotFoundError,
  ReportSelectorError,
  load_report_model,
  query_disclosures,
  query_information_block,
  resolve_report,
  resolved_report_info,
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


def _concept(qname: str, namespace: str, **kw: Any) -> Concept:
  local = qname.split(":", 1)[1]
  labels = kw.pop("labels", [Label(value=kw.pop("label", local), role=STANDARD)])
  return Concept(qname=qname, namespace=namespace, name=local, labels=labels, **kw)


def _model() -> XbrlModel:
  """A fictional filer's 10-K, small enough to read whole in a test: a balance
  sheet that foots, a segment table with one breakdown, and one text block."""
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
  filer: dict[str, Any] = {
    "entity_cik": "0000012345",
    "entity_scheme": "http://www.sec.gov/CIK",
  }

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
      value_str="<p>The company reports one segment, Widgets.</p>",
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
      cik="0000012345",
      form="10-K",
      filing_date=date(2025, 2, 5),
      fiscal_year_focus="2024",
      fiscal_period_focus="FY",
      report_date=date(2024, 12, 31),
      is_inline_xbrl=True,
      report_uri=REPORT_URI,
      extension_namespace=ACME,
    ),
    entity=EntityIdentity(cik="0000012345", name="Acme Industrial Corp", ticker="ACME"),
    concepts=concepts,
    periods=[instant, prior, year],
    units=[usd],
    facts=facts,
    networks=networks,
  )


class _LbugRepository:
  """The repository interface (``execute_query(cypher, params) -> rows``)
  over a single-filing LadybugDB — what the Graph API does over HTTP."""

  def __init__(self, path: Path) -> None:
    import ladybug as lbug

    self._db = lbug.Database(str(path), read_only=True)
    self._conn = lbug.Connection(self._db)
    self.calls: list[str] = []

  async def execute_query(self, cypher: str, params: dict[str, Any] | None = None):
    self.calls.append(cypher)
    result = self._conn.execute(cypher, parameters=dict(params or {}))
    names = result.get_column_names()
    return [dict(zip(names, row, strict=True)) for row in result.get_all()]

  def close(self) -> None:
    self._conn.close()
    self._db.close()


class _FakeRedis:
  """Enough of the async client for the model cache: get / set with ``ex``."""

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
def graph(tmp_path: Path):
  """A fake repository over a database built from the fixture, patched in
  where the view asks for the graph repository."""
  pytest.importorskip("ladybug")
  path = build_lbug(to_graph_tables(_model()), tmp_path / "acme.lbug")
  repository = _LbugRepository(path)
  with patch(f"{MODULE}.get_graph_repository", new=AsyncMock(return_value=repository)):
    yield repository
  repository.close()


# ── the seam: graph → xbrlkit → the two tools ─────────────────────────────


@pytest.mark.asyncio
@pytest.mark.unit
class TestOverTheGraph:
  async def test_disclosures_maps_the_report(self, graph, no_cache) -> None:
    out = await query_disclosures("sec", ACCESSION)
    assert out["graph_id"] == "sec"
    assert out["report_id"] == ACCESSION
    names = [row["disclosure"] for row in out["disclosures"]]
    assert names == ["Balance Sheet", "Segments"]
    balance = out["disclosures"][0]
    assert balance["levels"] == {"statement": 1}
    assert balance["category"] == "Statement"
    segments = out["disclosures"][1]
    assert segments["levels"] == {"details": 1}
    assert segments["text_blocks"] == 1
    # Every slice statement went through the repository, by report.
    assert len(graph.calls) >= 18

  async def test_disclosures_topic_lists_the_familys_blocks(
    self, graph, no_cache
  ) -> None:
    out = await query_disclosures("sec", ACCESSION, topic="segments")
    assert out["disclosure"] == "Segments"
    [block] = out["blocks"]
    assert block["id"] == "SegmentsDetails"
    assert block["axes"] == ["us-gaap:StatementBusinessSegmentsAxis"]

  async def test_disclosures_unknown_topic_is_not_found(self, graph, no_cache) -> None:
    with pytest.raises(BlockNotFoundError, match="No disclosure matches"):
      await query_disclosures("sec", ACCESSION, topic="pensions")

  async def test_information_block_reads_the_section_whole(
    self, graph, no_cache
  ) -> None:
    out = await query_information_block("sec", ACCESSION, "BalanceSheet")
    assert out["block"]["id"] == "BalanceSheet"
    assert out["block"]["kind"] == "balance_sheet"
    labels = [row["label"] for row in out["rows"]]
    assert labels == ["Assets [Abstract]", "Cash", "Other assets", "Assets"]
    assets = out["rows"][3]
    assert assets["values"] == {"2024-12-31": 1000.0, "2023-12-31": 900.0}
    # The calculation arcs foot on both columns.
    [roll] = out["calculation"]
    assert roll["total"] == "us-gaap:Assets"
    assert (roll["foots"], roll["checked"]) == (2, 2)
    assert "differences" not in roll

  async def test_information_block_breaks_out_the_sections_own_axis(
    self, graph, no_cache
  ) -> None:
    out = await query_information_block("sec", ACCESSION, "SegmentsDetails")
    [axis] = out["axes"]
    assert axis["axis"] == "us-gaap:StatementBusinessSegmentsAxis"
    assert [m["member"] for m in axis["members"]] == ["acme:WidgetsMember"]
    cash = next(row for row in out["rows"] if row["concept"] == "us-gaap:Cash")
    assert cash["members"] == {"acme:WidgetsMember": {"2024-12-31": 250.0}}
    # The text block stays inline here: this graph holds the text itself.
    [text] = out["text"]
    assert text["concept"] == "us-gaap:SegmentReportingDisclosureTextBlock"
    assert text["preview"].startswith("The company reports one segment")
    assert "external" not in text

  async def test_information_block_unknown_block_is_not_found(
    self, graph, no_cache
  ) -> None:
    with pytest.raises(BlockNotFoundError):
      await query_information_block("sec", ACCESSION, "Pensions")

  async def test_a_report_the_graph_does_not_hold_is_not_found(
    self, graph, no_cache
  ) -> None:
    with pytest.raises(ReportNotFoundError, match="No report"):
      await query_disclosures("sec", "0000000000-00-000000")

  async def test_the_model_is_cached_by_report(self, graph, monkeypatch) -> None:
    redis = _FakeRedis()
    monkeypatch.setattr(module, "_cache", lambda: redis)
    model, gaps, cached = await load_report_model("sec", ACCESSION)
    assert cached is False and gaps is not None
    [key] = list(redis.store)
    assert key == f"ib:model:v{module.MODEL_CACHE_VERSION}:sec:{ACCESSION}"
    assert redis.ttls[key] == module.MODEL_CACHE_TTL_SHARED_SECONDS
    # The cached copy is the model, compressed.
    assert b'"accession"' in zlib.decompress(redis.store[key])
    calls_before = len(graph.calls)
    again, gaps_again, cached_again = await load_report_model("sec", ACCESSION)
    assert cached_again is True and gaps_again is None
    assert len(graph.calls) == calls_before
    assert again.model_dump() == model.model_dump()

  async def test_a_tenant_model_is_cached_for_less_time(
    self, graph, monkeypatch
  ) -> None:
    redis = _FakeRedis()
    monkeypatch.setattr(module, "_cache", lambda: redis)
    with patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=False):
      await load_report_model("kg1234567890abcdef", ACCESSION)
    assert list(redis.ttls.values()) == [module.MODEL_CACHE_TTL_TENANT_SECONDS]

  async def test_a_cache_outage_costs_a_read_not_the_call(
    self, graph, monkeypatch
  ) -> None:
    broken = AsyncMock()
    broken.get = AsyncMock(side_effect=ConnectionError("valkey down"))
    broken.set = AsyncMock(side_effect=ConnectionError("valkey down"))
    monkeypatch.setattr(module, "_cache", lambda: broken)
    out = await query_disclosures("sec", ACCESSION)
    assert out["count"] == 2


# ── text held outside the graph ─────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.unit
class TestExternalText:
  async def test_a_text_block_stored_as_a_url_is_flagged_not_previewed(
    self, no_cache
  ) -> None:
    """The SEC graph stores a text block's value as a CDN URL."""
    model = _model()
    text_fact = next(f for f in model.facts if f.value_kind == "text")
    text_fact.value_str = "https://cdn.example/sec/text/a8.html"
    text_fact.content_type = "text/html"
    with patch(
      f"{MODULE}.load_report_model", new=AsyncMock(return_value=(model, None, True))
    ):
      out = await query_information_block("sec", ACCESSION, "SegmentsDetails")
    [text] = out["text"]
    assert text["external"] is True
    assert "preview" not in text and "chars" not in text
    assert text["concept"] == "us-gaap:SegmentReportingDisclosureTextBlock"
    assert "search-documents" in out["note"]

  async def test_the_familys_block_list_says_so_too(self, no_cache) -> None:
    model = _model()
    text_fact = next(f for f in model.facts if f.value_kind == "text")
    text_fact.value_str = "https://cdn.example/sec/text/a8.html"
    with patch(
      f"{MODULE}.load_report_model", new=AsyncMock(return_value=(model, None, True))
    ):
      out = await query_disclosures("sec", ACCESSION, topic="segments")
    [block] = out["blocks"]
    [text] = block["text_blocks"]
    assert text == {
      "concept": "us-gaap:SegmentReportingDisclosureTextBlock",
      "external": True,
    }


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
    assert report_id == "rpt_abc"
    assert info is resolved
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


# ── the caps the request models restate ────────────────────────────────────


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
