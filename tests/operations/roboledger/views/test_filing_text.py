"""The filing-text views (``describe-filing`` + ``search-text`` + ``read-text``).

A filing is found and read from the public bucket alone: the filer's catalog
or releases list names its folder, the folder holds its holon and documents.
xbrlkit's text tools run unmocked over the text the view assembles; a fake
bucket stands in for the CDN, and the graph appears only for a ``report_id``
named without a ticker. A tenant graph is refused: a ledger files no document.
"""

from __future__ import annotations

import json
import zlib
from datetime import date
from typing import Any
from unittest.mock import AsyncMock

import pytest
from xbrlkit.model import (
  Concept,
  EntityIdentity,
  FilingMeta,
  Label,
  XbrlFact,
  XbrlModel,
)
from xbrlkit.periods import duration_period
from xbrlkit.serialize import to_holon
from xbrlkit.serve import tools as xbrlkit_tools

from robosystems.models.api.extensions.reports import (
  READ_TEXT_MAX_LENGTH,
  SEARCH_TEXT_MAX_HITS,
  SEARCH_TEXT_MAX_WINDOW,
)
from robosystems.operations.roboledger.views import filing_text as module
from robosystems.operations.roboledger.views import information_blocks
from robosystems.operations.roboledger.views.filing_text import (
  QueryError,
  query_describe_filing,
  query_pattern,
  query_read_text,
  query_search_text,
  resolve_filing,
)
from robosystems.operations.roboledger.views.information_blocks import (
  ReportNotFoundError,
  ReportSelectorError,
)

CIK = "0000012345"
CDN = "https://cdn.example.com"
REPORT_ID = "rpt-acme"
ACCESSION = "0000012345-25-000001"
OLD = "0000012345-17-000070"
EIGHT_K = "0000012345-25-000077"
NEWER_FD = "0000012345-25-000090"
FOLDER = f"2025/{CIK}/{ACCESSION}"
OLD_FOLDER = f"2017/{CIK}/{OLD}"
EIGHT_K_FOLDER = f"2025/{CIK}/{EIGHT_K}"
POLICY = (
  "Revenue is recognized when control of the widgets transfers to the customer, "
  "which is generally on shipment, net of returns and volume rebates."
)
OLD_POLICY = (
  "In 2017 the company recognized revenue on delivery of each widget, with a "
  "reserve for returns estimated from three years of history."
)
DOCUMENT = (
  "<html><body><p>UNITED STATES SECURITIES AND EXCHANGE COMMISSION</p>"
  "<p>FORM 10-K</p><p>Indicate by check mark whether the registrant is a shell "
  "company. No.</p><p>Item 1. Business</p><p>Acme makes widgets. Our largest "
  "customer accounted for 14 percent of revenue, a customer concentration we "
  "watch closely.</p><p>Item 7. Management's Discussion and Analysis</p><p>"
  f"{POLICY}</p></body></html>"
)
COVER_8K = (
  "<html><body><p>FORM 8-K</p><p>Item 2.02 Results of Operations and Financial "
  "Condition. Acme issued a press release announcing its results, furnished "
  "as Exhibit 99.1.</p></body></html>"
)
RELEASE = (
  "<html><body><p>Exhibit 99.1</p><p>Acme reports record revenue and raises "
  "full year guidance for operating margin to thirty percent.</p></body></html>"
)


def _model(accession: str, filing_date: date, policy_value: str) -> XbrlModel:
  fy = filing_date.year - 1
  year = duration_period(date(fy, 1, 1), date(fy, 12, 31))
  concept = Concept(
    qname="us-gaap:RevenueRecognitionPolicyTextBlock",
    namespace="http://fasb.org/us-gaap/2024",
    name="RevenueRecognitionPolicyTextBlock",
    pref_label="Revenue Recognition",
    labels=[
      Label(value="Revenue Recognition", role="http://www.xbrl.org/2003/role/label")
    ],
    period_type="duration",
    is_textblock=True,
    is_text_fact=True,
  )
  return XbrlModel(
    filing=FilingMeta(
      accession=accession,
      cik=CIK,
      form="10-K",
      filing_date=filing_date,
      primary_document="acme-10k.htm",
    ),
    entity=EntityIdentity(cik=CIK, name="Acme Corp", ticker="ACME"),
    concepts={concept.qname: concept},
    periods=[year],
    facts=[
      XbrlFact(
        id="f-policy",
        entity_cik=CIK,
        entity_scheme="http://www.sec.gov/CIK",
        entity_identifier=CIK,
        concept_qname=concept.qname,
        period_id=year.id,
        value_kind="text",
        value_str=policy_value,
        raw_value=policy_value,
      )
    ],
  )


class _FakeBucket:
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


def _manifest(form: str, representations: list[dict[str, Any]], **extra: Any) -> str:
  return json.dumps({"form": form, "representations": representations, **extra})


def _filing(accession: str, filing_date: str, fiscal_year: int, kinds: list[str]):
  return {
    "accession": accession,
    "form": "10-K",
    "filing_date": filing_date,
    "fiscal_year": fiscal_year,
    "fiscal_period": "FY",
    "report_id": f"rpt-{accession}",
    # As the catalog writes them: each representation names its file.
    "representations": [
      {
        "kind": k,
        "name": {
          "holon": "holon.jsonld",
          "tavi": "tavi.json",
          "document": "acme-10k.htm",
        }[k],
      }
      for k in kinds
    ],
  }


def _release(accession: str, filing_date: str, items: list[str]):
  return {"accession": accession, "filing_date": filing_date, "items": items}


@pytest.fixture
def no_cache(monkeypatch: pytest.MonkeyPatch) -> None:
  monkeypatch.setattr(module, "_cache", lambda: None)


@pytest.fixture
def cdn(monkeypatch: pytest.MonkeyPatch):
  """The SEC repository's public bucket: one filer's catalog and releases,
  a current 10-K with its document, a 2017 10-K whose policy is a fragment,
  and an 8-K with its release."""
  fragment_url = f"{CDN}/{OLD_FOLDER}/fact_abc.html"
  holon = {"kind": "holon", "name": "holon.jsonld"}
  bucket = _FakeBucket(
    {
      "companies/acme.json": json.dumps(
        {
          "cik": CIK,
          "ticker": "ACME",
          "filings": [
            _filing(ACCESSION, "2025-02-05", 2024, ["tavi", "holon", "document"]),
            _filing(OLD, "2017-11-03", 2017, ["tavi", "holon"]),
          ],
        }
      ),
      f"current-reports/{CIK}.json": json.dumps(
        {
          "cik": CIK,
          "releases": [
            _release(NEWER_FD, "2025-03-01", ["7.01", "9.01"]),
            {
              **_release(EIGHT_K, "2025-01-30", ["2.02", "9.01"]),
              "document": "acme-8k.htm",
              "exhibits": {"EX-99.1": "ex991.htm"},
            },
          ],
        }
      ),
      f"{FOLDER}/manifest.json": _manifest(
        "10-K", [holon, {"kind": "document", "name": "acme-10k.htm"}]
      ),
      f"{FOLDER}/holon.jsonld": to_holon(
        _model(ACCESSION, date(2025, 2, 5), f"<p>{POLICY}</p>")
      ),
      f"{FOLDER}/acme-10k.htm": DOCUMENT,
      f"{OLD_FOLDER}/manifest.json": _manifest("10-K", [holon]),
      f"{OLD_FOLDER}/holon.jsonld": to_holon(
        _model(OLD, date(2017, 11, 3), fragment_url)
      ),
      f"{OLD_FOLDER}/fact_abc.html": f"<p>{OLD_POLICY}</p>",
      f"{EIGHT_K_FOLDER}/manifest.json": _manifest(
        "8-K",
        [
          {"kind": "exhibit", "name": "ex991.htm", "exhibit": "EX-99.1"},
          {"kind": "document", "name": "acme-8k.htm"},
        ],
        items=["2.02", "9.01"],
        report_date="2025-01-30",
        primary_document="acme-8k.htm",
        entity={"cik": CIK, "name": "Acme Corp", "ticker": "ACME"},
      ),
      f"{EIGHT_K_FOLDER}/acme-8k.htm": COVER_8K,
      f"{EIGHT_K_FOLDER}/ex991.htm": RELEASE,
    }
  )
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
  monkeypatch.setattr(information_blocks.env, "PUBLIC_DATA_BUCKET", "public-data-test")
  return bucket


@pytest.mark.unit
class TestQueryPattern:
  def test_words_across_any_spacing(self):
    assert query_pattern("customer concentration") == r"customer\s+concentration"

  def test_alternatives_and_stems(self):
    assert query_pattern("terminat* | going concern") == r"terminat\w*|going\s+concern"

  def test_regex_syntax_is_literal(self):
    assert query_pattern("(a+)+$") == r"\(a\+\)\+\$"

  def test_nothing_to_match(self):
    with pytest.raises(QueryError):
      query_pattern("  |  ")


@pytest.mark.asyncio
@pytest.mark.unit
class TestResolution:
  async def test_ticker_is_the_latest_annual_report(self, cdn):
    ref = await resolve_filing("sec", ticker="acme")
    assert (ref.accession, ref.cik, ref.filing_date) == (ACCESSION, CIK, "2025-02-05")
    assert ref.resolved and ref.resolved["fiscal_year"] == 2024
    links = ref.resolved["links"]
    assert links["holon"].endswith(f"/{FOLDER}/holon.jsonld")
    assert links["as_filed"].endswith(f"/{FOLDER}/acme-10k.htm")
    assert links["viewer"].startswith(module.env.VIEWER_URL.rstrip("/") + "/?url=")
    assert "holon.jsonld" in links["viewer"]
    assert links["edgar"].endswith(f"/12345/{ACCESSION.replace('-', '')}/")

  async def test_a_pre_inline_filing_links_what_its_folder_holds(self, cdn):
    ref = await resolve_filing("sec", ticker="ACME", fiscal_year=2017)
    assert ref.resolved
    assert "as_filed" not in ref.resolved["links"]
    assert ref.resolved["links"]["holon"].endswith(f"/{OLD_FOLDER}/holon.jsonld")

  async def test_any_processed_year(self, cdn):
    ref = await resolve_filing("sec", ticker="ACME", fiscal_year=2017)
    assert ref.accession == OLD

  async def test_a_report_by_accession(self, cdn):
    ref = await resolve_filing("sec", ticker="ACME", accession=OLD)
    assert ref.accession == OLD and ref.form == "10-K"

  async def test_latest_release_is_the_item_2_02(self, cdn):
    ref = await resolve_filing("sec", ticker="ACME", form="8-K")
    assert ref.accession == EIGHT_K and ref.form == "8-K"
    assert ref.resolved
    links = ref.resolved["links"]
    assert "viewer" not in links and "holon" not in links
    assert links["as_filed"].endswith(f"/{EIGHT_K_FOLDER}/acme-8k.htm")
    assert list(links["exhibits"]) == ["EX-99.1"]
    assert links["exhibits"]["EX-99.1"].endswith(f"/{EIGHT_K_FOLDER}/ex991.htm")
    assert links["edgar"].endswith(f"/12345/{EIGHT_K.replace('-', '')}/")
    recent = [r["accession"] for r in ref.resolved["recent_releases"]]
    assert recent == [NEWER_FD, EIGHT_K]

  async def test_a_release_by_accession(self, cdn):
    ref = await resolve_filing("sec", ticker="ACME", accession=NEWER_FD)
    assert ref.accession == NEWER_FD and ref.form == "8-K"

  async def test_a_year_reaches_a_release_older_than_the_listed_few(self, cdn):
    earlier = "0000012345-24-000090"
    listed = json.loads(cdn.objects[f"current-reports/{CIK}.json"])
    listed["releases"].append(_release(earlier, "2024-10-28", ["2.02", "9.01"]))
    cdn.objects[f"current-reports/{CIK}.json"] = json.dumps(listed)

    ref = await resolve_filing("sec", ticker="ACME", form="8-K", fiscal_year=2024)
    assert ref.accession == earlier
    assert ref.resolved
    assert [r["accession"] for r in ref.resolved["recent_releases"]] == [earlier]
    # Without a year the latest earnings release is still the pick.
    latest = await resolve_filing("sec", ticker="ACME", form="8-K")
    assert latest.accession == EIGHT_K
    with pytest.raises(ReportNotFoundError, match="filed in 2019"):
      await resolve_filing("sec", ticker="ACME", form="8-K", fiscal_year=2019)

  async def test_a_cik_reaches_a_filer_the_catalog_does_not_list(self, cdn):
    del cdn.objects["companies/acme.json"]
    ref = await resolve_filing("sec", ticker=CIK.lstrip("0"), form="8-K")
    assert (ref.accession, ref.cik, ref.form) == (EIGHT_K, CIK, "8-K")
    named = await resolve_filing("sec", ticker=CIK, accession=NEWER_FD)
    assert named.accession == NEWER_FD
    # Its annual and quarterly reports are listed by ticker only.
    with pytest.raises(ReportSelectorError, match="8-K"):
      await resolve_filing("sec", ticker=CIK)

  async def test_a_combined_release_is_read_where_it_was_published(self, cdn, no_cache):
    # A combined 8-K is published once, under its first registrant, and listed
    # under every registrant. Asked for through a co-registrant, it is read
    # from the folder its entry names, not from one built on the asking CIK.
    subsidiary = "0000099999"
    cdn.objects[f"current-reports/{subsidiary}.json"] = json.dumps(
      {
        "cik": subsidiary,
        "releases": [
          {
            **_release(EIGHT_K, "2025-01-30", ["2.02", "9.01"]),
            "folder": f"{CDN}/{EIGHT_K_FOLDER}/",
            "document": "acme-8k.htm",
            "exhibits": {"EX-99.1": "ex991.htm"},
          }
        ],
      }
    )
    ref = await resolve_filing("sec", ticker=subsidiary, form="8-K")
    assert ref.cik == CIK
    assert ref.resolved
    assert ref.resolved["links"]["as_filed"].endswith(f"/{EIGHT_K_FOLDER}/acme-8k.htm")
    described = await query_describe_filing("sec", ref)
    assert described["profile"]["text"] == "primary document"

  async def test_report_id_alone_reads_the_graphs_coordinates(self, cdn):
    ref = await resolve_filing("sec", report_id=REPORT_ID)
    assert (ref.report_id, ref.accession) == (REPORT_ID, ACCESSION)

  async def test_a_ticker_never_asks_the_graph(self, cdn):
    await resolve_filing("sec", ticker="ACME", form="8-K")
    await resolve_filing("sec", ticker="ACME", fiscal_year=2017)
    module.get_graph_repository.assert_not_awaited()

  @pytest.mark.parametrize(
    ("kwargs", "error"),
    [
      ({}, ReportSelectorError),
      ({"ticker": "ACME", "accession": "12345"}, ReportSelectorError),
      ({"ticker": "../x"}, ReportSelectorError),
      ({"ticker": "A" * 11}, ReportSelectorError),
      ({"report_id": REPORT_ID, "ticker": "ACME"}, ReportSelectorError),
      ({"report_id": REPORT_ID, "accession": ACCESSION}, ReportSelectorError),
      ({"report_id": REPORT_ID, "form": "8-K"}, ReportSelectorError),
      ({"ticker": "NOPE"}, ReportNotFoundError),
      ({"ticker": "ACME", "fiscal_year": 2001}, ReportNotFoundError),
      ({"ticker": "ACME", "accession": "0000099999-25-000001"}, ReportNotFoundError),
    ],
  )
  async def test_errors(self, cdn, kwargs, error):
    with pytest.raises(error):
      await resolve_filing("sec", **kwargs)

  async def test_class_tickers_resolve(self, cdn):
    cdn.objects["companies/brk.b.json"] = cdn.objects["companies/acme.json"]
    ref = await resolve_filing("sec", ticker="brk.b")
    assert ref.accession == ACCESSION

  async def test_a_tenant_graph_is_refused(self, monkeypatch):
    # A ledger files no document; its sections are disclosures / information-block.
    monkeypatch.setattr(
      module, "is_shared_repository_or_subgraph", lambda graph_id: False
    )
    with pytest.raises(ReportSelectorError, match="information-block"):
      await resolve_filing("kg1234567890abcdef", report_id=REPORT_ID)


@pytest.mark.asyncio
@pytest.mark.unit
class TestReportText:
  async def test_search_finds_what_only_the_document_says(self, cdn, no_cache):
    ref = await resolve_filing("sec", ticker="ACME")
    out = await query_search_text("sec", ref, "check mark")
    assert out["accession"] == ACCESSION
    assert out["text"] == "primary document"
    assert out["total"] == 1
    assert "shell company" in out["hits"][0]["text"]
    assert "pattern" not in out

  async def test_no_match_counts_the_words(self, cdn, no_cache):
    ref = await resolve_filing("sec", ticker="ACME")
    out = await query_search_text("sec", ref, "customer dependence")
    assert out["total"] == 0
    assert {"term": "dependence", "matches": 0} in out["terms"]

  async def test_read_pages_from_a_hit(self, cdn, no_cache):
    ref = await resolve_filing("sec", ticker="ACME")
    hit = (await query_search_text("sec", ref, "largest customer"))["hits"][0]
    page = await query_read_text("sec", ref, offset=hit["offset"], length=40)
    assert page["text"].startswith("largest customer")
    assert page["next_offset"] == hit["offset"] + 40

  async def test_an_old_filing_reads_its_text_blocks(self, cdn, no_cache):
    ref = await resolve_filing("sec", ticker="ACME", fiscal_year=2017)
    out = await query_search_text("sec", ref, "three years of history")
    assert out["text"] == "tagged text blocks"
    assert out["total"] == 1
    # The policy was a fragment URL in the holon; it was read in.
    assert f"{OLD_FOLDER}/fact_abc.html" in cdn.reads

  async def test_an_old_filing_with_a_fetched_document_keeps_its_blocks(
    self, cdn, no_cache
  ):
    # sec_filing_documents added the document; the blocks are still located
    # from their fragments, since a classic document carries no inline tags.
    cdn.objects[f"{OLD_FOLDER}/manifest.json"] = _manifest(
      "10-K",
      [
        {"kind": "holon", "name": "holon.jsonld"},
        {"kind": "document", "name": "old.htm"},
      ],
      is_inline_xbrl=False,
    )
    cdn.objects[f"{OLD_FOLDER}/old.htm"] = (
      "<html><body><p>FORM 10-K</p><p>Item 7. Management's Discussion</p>"
      f"<p>{OLD_POLICY}</p></body></html>"
    )
    ref = await resolve_filing("sec", ticker="ACME", fiscal_year=2017)
    out = await query_describe_filing("sec", ref)
    assert out["profile"]["text"] == "primary document"
    [block] = out["sections"]["text_blocks"]
    assert block["id"] == "us-gaap:RevenueRecognitionPolicyTextBlock"
    assert block["offset"] is not None

  async def test_an_instance_named_as_the_document_is_not_read(self, cdn, no_cache):
    cdn.objects[f"{OLD_FOLDER}/manifest.json"] = _manifest(
      "10-K",
      [
        {"kind": "holon", "name": "holon.jsonld"},
        {"kind": "document", "name": "acme-20161231.xml"},
      ],
      is_inline_xbrl=False,
    )
    cdn.objects[f"{OLD_FOLDER}/acme-20161231.xml"] = "<xbrl><context/></xbrl>"
    ref = await resolve_filing("sec", ticker="ACME", fiscal_year=2017)
    out = await query_search_text("sec", ref, "three years")
    assert out["text"] == "tagged text blocks"
    assert f"{OLD_FOLDER}/acme-20161231.xml" not in cdn.reads

  async def test_the_budget_is_checked_before_the_download(
    self, cdn, no_cache, monkeypatch
  ):
    from robosystems.operations.roboledger.views.information_blocks import (
      ReportTooLargeError,
    )

    monkeypatch.setattr(module, "DOCUMENT_BUDGET_CHARS", 100)
    cdn.objects[f"{FOLDER}/manifest.json"] = _manifest(
      "10-K",
      [
        {"kind": "holon", "name": "holon.jsonld"},
        {"kind": "document", "name": "acme-10k.htm", "bytes": 101},
      ],
    )
    ref = await resolve_filing("sec", ticker="ACME")
    with pytest.raises(ReportTooLargeError):
      await query_search_text("sec", ref, "widgets")
    assert f"{FOLDER}/acme-10k.htm" not in cdn.reads

  async def test_an_8ks_documents_are_budgeted_together(
    self, cdn, no_cache, monkeypatch
  ):
    from robosystems.operations.roboledger.views.information_blocks import (
      ReportTooLargeError,
    )

    monkeypatch.setattr(module, "DOCUMENT_BUDGET_CHARS", 150)
    cdn.objects[f"{EIGHT_K_FOLDER}/manifest.json"] = _manifest(
      "8-K",
      [
        {"kind": "exhibit", "name": "ex991.htm", "exhibit": "EX-99.1", "bytes": 100},
        {"kind": "document", "name": "acme-8k.htm", "bytes": 100},
      ],
      items=["2.02"],
    )
    ref = await resolve_filing("sec", ticker="ACME", form="8-K")
    with pytest.raises(ReportTooLargeError):
      await query_search_text("sec", ref, "guidance")
    assert not any(
      k.startswith(EIGHT_K_FOLDER) and k.endswith(".htm") for k in cdn.reads
    )

  async def test_describe_counts_facts_after_the_cache(self, cdn, monkeypatch):
    redis = _FakeRedis()
    monkeypatch.setattr(module, "_cache", lambda: redis)
    ref = await resolve_filing("sec", ticker="ACME")
    await query_search_text("sec", ref, "widgets")
    out = await query_describe_filing("sec", ref)
    assert out["profile"]["text"] == "primary document"
    assert out["filing"]["form"] == "10-K"
    assert out["counts"]["text_blocks"] == 1
    # The model it read is kept under information-block's own key, and a
    # second describe reads neither the manifest nor the holon again.
    assert (
      f"ib:model:v{information_blocks.MODEL_CACHE_VERSION}:sec:{ref.report_id}"
      in redis.store
    )
    reads = len(cdn.reads)
    again = await query_describe_filing("sec", ref)
    assert again["counts"] == out["counts"]
    assert len(cdn.reads) == reads

  async def test_cached_text_is_built_once(self, cdn, monkeypatch):
    redis = _FakeRedis()
    monkeypatch.setattr(module, "_cache", lambda: redis)
    ref = await resolve_filing("sec", ticker="ACME")
    first = await query_search_text("sec", ref, "customer concentration")
    reads = len(cdn.reads)
    again = await query_search_text("sec", ref, "customer concentration")
    assert again["hits"] == first["hits"]
    assert len(cdn.reads) == reads
    [key] = redis.store
    assert key == f"ft:text:v{module.TEXT_CACHE_VERSION}:sec:{ACCESSION}"
    assert redis.ttls[key] == module.TEXT_CACHE_TTL_SECONDS
    cached = json.loads(zlib.decompress(redis.store[key]))
    assert set(cached) == {"filing", "entity", "text", "sections", "has_document"}


@pytest.mark.asyncio
@pytest.mark.unit
class TestCurrentReport:
  async def test_the_release_is_searched_with_its_exhibits(self, cdn, no_cache):
    ref = await resolve_filing("sec", ticker="ACME", form="8-K")
    out = await query_search_text("sec", ref, "full year guidance")
    assert out["accession"] == EIGHT_K
    assert out["total"] == 1
    assert out["hits"][0]["section"] == "EX-99.1"

  async def test_the_8k_comes_first_then_its_exhibits(self, cdn, no_cache):
    ref = await resolve_filing("sec", ticker="ACME", form="8-K")
    out = await query_describe_filing("sec", ref)
    assert out["filing"]["form"] == "8-K"
    assert out["filing"]["items"]
    assert [i["label"] for i in out["sections"]["items"]] == ["Form 8-K", "EX-99.1"]


@pytest.mark.unit
def test_request_caps_match_xbrlkit():
  assert SEARCH_TEXT_MAX_WINDOW == xbrlkit_tools.MAX_WINDOW
  assert SEARCH_TEXT_MAX_HITS == xbrlkit_tools.MAX_HITS
  assert READ_TEXT_MAX_LENGTH == xbrlkit_tools.MAX_READ
