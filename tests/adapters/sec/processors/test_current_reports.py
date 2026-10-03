"""8-K selection: which filings EFTS names, which of their files are kept."""

import io
import zipfile

import pytest

from robosystems.adapters.sec.processors.current_reports import (
  CurrentReportHit,
  current_report_manifest,
  current_report_text,
  documents_in_zip,
  exhibit_index,
  exhibit_type,
  keeps_exhibit,
  missing_document,
)

COVER = (
  b"<html><body><ix:nonNumeric name='dei:DocumentType'>8-K</ix:nonNumeric>"
  b"<p>Item 2.02 Results of Operations and Financial Condition. On October 30 the "
  b"company issued a press release announcing its results for the quarter, "
  b"furnished as Exhibit 99.1.</p></body></html>"
)
RELEASE = (
  b"<html><body><p>Exhibit 99.1</p><p>Acme reports record fourth quarter revenue "
  b"of $10 billion, up 8 percent year over year, and raises full year guidance "
  b"for operating margin.</p><table><tr><td>Metric</td><td>Q4</td></tr>"
  b"<tr><td>Revenue</td><td>10.0</td></tr><tr><td>Margin</td><td>30%</td></tr></table>"
  b"</body></html>"
)
CREDIT_AGREEMENT = (
  b"<html><body><p>Exhibit 10.1</p><p>CREDIT AGREEMENT</p></body></html>"
)
SLIDES = b"<html><body><p>Q3 2025 EARNINGS PRESENTATION</p></body></html>"


def _zip(files: dict[str, bytes]) -> bytes:
  buf = io.BytesIO()
  with zipfile.ZipFile(buf, "w") as zf:
    for name, data in files.items():
      zf.writestr(name, data)
  return buf.getvalue()


def _efts_hit(**source) -> dict:
  base = {
    "ciks": ["0000320193"],
    "file_date": "2025-10-30",
    "items": ["2.02", "9.01"],
    "display_names": ["Apple Inc.  (AAPL)  (CIK 0000320193)"],
    "period_ending": "2025-10-30",
  }
  base.update(source)
  return {"_id": "0000320193-25-000077:aapl-20251030.htm", "_source": base}


@pytest.mark.unit
class TestCurrentReportHit:
  def test_parses_efts_hit(self):
    hit = CurrentReportHit.from_efts(_efts_hit())
    assert hit is not None
    assert hit.accession == "0000320193-25-000077"
    assert hit.cik == "0000320193"
    assert hit.items == ("2.02", "9.01")
    assert hit.primary_document == "aapl-20251030.htm"
    assert (hit.entity_name, hit.ticker) == ("Apple Inc.", "AAPL")
    assert hit.report_date == "2025-10-30"

  def test_pads_the_cik(self):
    hit = CurrentReportHit.from_efts(_efts_hit(ciks=["320193"]))
    assert hit is not None and hit.cik == "0000320193"

  def test_first_of_several_tickers(self):
    hit = CurrentReportHit.from_efts(
      _efts_hit(
        display_names=["Berkshire Hathaway Inc  (BRK-B, BRK-A)  (CIK 0001067983)"]
      )
    )
    assert hit is not None
    assert (hit.entity_name, hit.ticker) == ("Berkshire Hathaway Inc", "BRK-B")

  def test_filer_without_ticker(self):
    hit = CurrentReportHit.from_efts(
      _efts_hit(display_names=["Foo LLC  (CIK 0000000001)"])
    )
    assert hit is not None
    assert (hit.entity_name, hit.ticker) == ("Foo LLC", None)

  def test_none_without_a_filer(self):
    assert CurrentReportHit.from_efts({"_id": "x:y", "_source": {"ciks": []}}) is None

  def test_a_combined_filing_lists_every_registrant(self):
    hit = CurrentReportHit.from_efts(_efts_hit(ciks=["92122", "0000092122", "1000000"]))
    assert hit is not None
    assert hit.cik == "0000092122"
    assert hit.ciks == ("0000092122", "0001000000")
    assert hit.registrants() == ("0000092122", "0001000000")
    assert hit.registrants({"0001000000"}) == ("0001000000",)
    assert hit.registrants({"0000000009"}) == ()

  def test_wanted_items(self):
    hit = CurrentReportHit.from_efts(_efts_hit(items=["7.01", "9.01"]))
    assert hit is not None
    assert hit.wanted(["2.02", "7.01"])
    assert not hit.wanted(["2.02"])


@pytest.mark.unit
class TestExhibitType:
  @pytest.mark.parametrize(
    ("name", "head", "expected"),
    [
      ("ex99-1.htm", "", "EX-99.1"),
      ("ex99_1.htm", "", "EX-99.1"),
      ("vlto-20251028xex991.htm", "", "EX-99.1"),
      ("a8-kex991q4202509272025.htm", "", "EX-99.1"),
      ("q320258kex991pressrelease.htm", "", "EX-99.1"),
      ("ex99.htm", "", "EX-99"),
      ("ex101fetcreditfacility.htm", "", "EX-10.1"),
      ("ex1045-3m_ltip.htm", "", "EX-10.45"),
      ("a2024exhibit2110k.htm", "", "EX-21.10"),
      ("amrizeq32025pressrelease.htm", "Amrize Delivers Strong Third Quarter", None),
      # The heading wins over the name.
      ("release.htm", "Document Exhibit 99.2 Investor presentation", "EX-99.2"),
      # "ex" inside a word is not an exhibit number.
      ("text2.htm", "", None),
      ("index1.htm", "", None),
      ("annex1.htm", "", None),
    ],
  )
  def test_reads_heading_then_name(self, name, head, expected):
    assert exhibit_type(name, head) == expected

  def test_keeps_releases_and_the_unnamed(self):
    assert keeps_exhibit("EX-99.1")
    assert keeps_exhibit(None)
    assert not keeps_exhibit("EX-10.1")


@pytest.mark.unit
class TestDocumentsInZip:
  def test_primary_first_then_kept_exhibits(self):
    zip_bytes = _zip(
      {
        "aapl-20251030.htm": COVER,
        "ex991.htm": RELEASE,
        "ex101.htm": CREDIT_AGREEMENT,
        "slides.htm": SLIDES,
        "aapl-20251030.xsd": b"<schema/>",
        "aapl-20251030_lab.xml": b"<linkbase/>",
        "R1.htm": b"<html>viewer</html>",
        "logo.jpg": b"\xff\xd8",
      }
    )
    docs = documents_in_zip(zip_bytes, "aapl-20251030.htm")
    assert [(d.kind, d.name, d.exhibit) for d in docs] == [
      ("document", "aapl-20251030.htm", None),
      ("exhibit", "ex991.htm", "EX-99.1"),
      ("exhibit", "slides.htm", None),
    ]

  def test_primary_found_by_cover_page_when_unnamed(self):
    docs = documents_in_zip(_zip({"cover.htm": COVER, "ex99-1.htm": RELEASE}))
    assert docs[0].kind == "document" and docs[0].name == "cover.htm"

  def test_section_ids(self):
    docs = documents_in_zip(
      _zip({"c.htm": COVER, "ex99-1.htm": RELEASE, "s.htm": SLIDES})
    )
    assert [d.section_id for d in docs] == ["form_8k", "ex_99_1", "exhibit_s"]


INDEXED_COVER = (
  b"<html><body><ix:nonNumeric name='dei:DocumentType'>8-K</ix:nonNumeric>"
  b"<p>Item 9.01 Financial Statements and Exhibits.</p><table>"
  b"<tr><td>Exhibit No.</td><td>Description</td></tr>"
  b"<tr><td>2.1*</td><td><a href='projectclarets-spa.htm'>Share Purchase Deed</a></td></tr>"
  b'<tr><td>99.1</td><td><a href="ex991pr.htm">Press release</a></td></tr>'
  b"<tr><td>99.2</td><td><a href='deck.htm#page1'>Investor deck</a></td></tr>"
  b"<tr><td>104</td><td>Cover Page Interactive Data File</td></tr>"
  b"</table></body></html>"
)


@pytest.mark.unit
class TestExhibitIndex:
  def test_reads_each_linked_files_number(self):
    assert exhibit_index(INDEXED_COVER) == {
      "projectclarets-spa.htm": "EX-2.1",
      "ex991pr.htm": "EX-99.1",
      "deck.htm": "EX-99.2",
    }

  def test_the_index_outranks_the_file_itself(self):
    # An unnamed purchase deed and deck: only the 8-K's index says which is which.
    docs = documents_in_zip(
      _zip(
        {
          "cover.htm": INDEXED_COVER,
          "projectclarets-spa.htm": b"<html><p>SHARE PURCHASE DEED</p></html>",
          "ex991pr.htm": RELEASE,
          "deck.htm": SLIDES,
        }
      )
    )
    assert [(d.name, d.exhibit) for d in docs if d.kind == "exhibit"] == [
      ("ex991pr.htm", "EX-99.1"),
      ("deck.htm", "EX-99.2"),
    ]


@pytest.mark.unit
class TestCurrentReportText:
  def test_each_document_a_located_section(self):
    docs = documents_in_zip(_zip({"c.htm": COVER, "ex99-1.htm": RELEASE}))
    assembled = current_report_text(docs)
    assert [s.label for s in assembled.sections] == ["Form 8-K", "EX-99.1"]
    for section in assembled.sections:
      assert section.offset is not None
      body = assembled.text[section.offset : section.offset + section.chars]
      assert body.strip()
    release = assembled.sections[1]
    assert "record fourth quarter revenue" in assembled.text[release.offset :]
    # Tables come through as markdown pipes, as the index reads them.
    assert "| Revenue" in assembled.text


@pytest.mark.unit
class TestManifest:
  def test_shape(self):
    hit = CurrentReportHit.from_efts(_efts_hit())
    assert hit is not None
    reps = [{"kind": "document", "name": "aapl-20251030.htm"}]
    manifest = current_report_manifest(hit, reps, [], "https://cdn/2025/x/")
    assert manifest["form"] == "8-K"
    assert manifest["items"] == ["2.02", "9.01"]
    assert manifest["report_id"] is None
    assert manifest["entity"] == {
      "cik": "0000320193",
      "name": "Apple Inc.",
      "ticker": "AAPL",
    }
    assert manifest["representations"] == reps


@pytest.mark.unit
class TestMissingDocument:
  def test_pre_inline_without_document(self):
    manifest = {
      "primary_document": "a10-k2017.htm",
      "is_inline_xbrl": False,
      "representations": [{"kind": "holon"}, {"kind": "tavi"}],
    }
    assert missing_document(manifest) == "a10-k2017.htm"

  @pytest.mark.parametrize(
    "manifest",
    [
      None,
      {"primary_document": "x.htm", "is_inline_xbrl": True, "representations": []},
      {"primary_document": None, "is_inline_xbrl": False, "representations": []},
      {
        "primary_document": "x.htm",
        "is_inline_xbrl": False,
        "representations": [{"kind": "document"}],
      },
      # The XBRL instance named as the primary document is not a document.
      {
        "primary_document": "acme-20161231.xml",
        "is_inline_xbrl": False,
        "representations": [],
      },
    ],
  )
  def test_nothing_to_fetch(self, manifest):
    assert missing_document(manifest) is None
