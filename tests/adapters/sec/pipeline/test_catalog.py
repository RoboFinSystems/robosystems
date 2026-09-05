"""The filer catalog fold: processed Report/Entity rows plus manifests → JSON."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from robosystems.adapters.sec.pipeline.catalog import (
  build_company,
  build_index,
  corpus_partitions,
  filers,
  filings_by_cik,
  index_row,
  read_manifests,
  viewer_link,
)
from robosystems.config.storage.shared import (
  get_filing_artifact_key,
  get_filing_catalog_key,
)

MMM = "0000066740"
NVDA = "0001045810"
CDN = "https://data.example.com"


def _entities(*rows):
  return pd.DataFrame(
    rows,
    columns=[
      "identifier",
      "cik",
      "ticker",
      "name",
      "exchange",
      "sic",
      "sic_description",
      "updated_at",
      "partition",
    ],
  )


def _reports(*rows):
  return pd.DataFrame(
    rows,
    columns=[
      "identifier",
      "accession_number",
      "form",
      "filing_date",
      "report_date",
      "fiscal_year_focus",
      "fiscal_period_focus",
      "updated_at",
      "partition",
    ],
  )


def _links(*rows):
  return pd.DataFrame(rows, columns=["from", "to", "partition"])


@pytest.fixture
def corpus():
  entities = _entities(
    ("e-mmm", MMM, "MMM", "3M CO", "NYSE", "3841", "Surgical", "2025-02-05", "2025-Q1"),
    (
      "e-mmm",
      MMM,
      "MMM",
      "3M COMPANY",
      "NYSE",
      "3841",
      "Surgical",
      "2025-04-30",
      "2025-Q2",
    ),
    (
      "e-nvda",
      NVDA,
      "NVDA",
      "NVIDIA CORP",
      "Nasdaq",
      "3674",
      "Semis",
      "2025-02-26",
      "2025-Q1",
    ),
    (
      "e-priv",
      "0009999999",
      None,
      "PRIVATE CO",
      None,
      None,
      None,
      "2025-02-01",
      "2025-Q1",
    ),
  )
  reports = _reports(
    (
      "r-mmm-k",
      "0000066740-25-000006",
      "10-K",
      "2025-02-05",
      "2024-12-31",
      2024,
      "FY",
      "2025-02-05",
      "2025-Q1",
    ),
    # The same 10-K reprocessed into a later batch: the later row wins.
    (
      "r-mmm-k",
      "0000066740-25-000006",
      "10-K",
      "2025-02-05",
      "2024-12-31",
      2024,
      "FY",
      "2025-03-01",
      "2025-Q1",
    ),
    (
      "r-mmm-q",
      "0000066740-25-000020",
      "10-Q",
      "2025-04-30",
      "2025-03-31",
      2025,
      "Q1",
      "2025-04-30",
      "2025-Q2",
    ),
    (
      "r-mmm-8k",
      "0000066740-25-000021",
      "8-K",
      "2025-05-01",
      None,
      None,
      None,
      "2025-05-01",
      "2025-Q2",
    ),
    (
      "r-nvda-k",
      "0001045810-25-000023",
      "10-K",
      "2025-02-26",
      "2025-01-26",
      2025,
      "FY",
      "2025-02-26",
      "2025-Q1",
    ),
    (
      "r-priv-k",
      "0009999999-25-000001",
      "10-K",
      "2025-02-01",
      "2024-12-31",
      2024,
      "FY",
      "2025-02-01",
      "2025-Q1",
    ),
  )
  links = _links(
    ("e-mmm", "r-mmm-k", "2025-Q1"),
    ("e-mmm", "r-mmm-q", "2025-Q2"),
    ("e-mmm", "r-mmm-8k", "2025-Q2"),
    ("e-nvda", "r-nvda-k", "2025-Q1"),
    ("e-priv", "r-priv-k", "2025-Q1"),
  )
  return entities, reports, links


@pytest.mark.unit
class TestFold:
  def test_filers_keep_the_latest_entity_row_per_cik(self, corpus):
    entities, _, _ = corpus
    rows = filers(entities)
    assert set(rows) == {MMM, NVDA, "0009999999"}
    assert rows[MMM]["name"] == "3M COMPANY"
    assert rows[MMM]["ticker"] == "MMM"
    assert rows["0009999999"]["ticker"] is None

  def test_filings_by_cik_dedups_filters_forms_and_sorts_newest_first(self, corpus):
    entities, reports, links = corpus
    by_cik = filings_by_cik(reports, links, entities, ["10-K", "10-Q"])
    assert [f["accession"] for f in by_cik[MMM]] == [
      "0000066740-25-000020",
      "0000066740-25-000006",
    ]
    tenk = by_cik[MMM][1]
    assert tenk["form"] == "10-K"
    assert tenk["fiscal_year"] == 2024
    assert tenk["fiscal_period"] == "FY"
    assert tenk["report_id"] == "r-mmm-k"
    assert [f["accession"] for f in by_cik[NVDA]] == ["0001045810-25-000023"]
    assert "8-K" not in {f["form"] for fs in by_cik.values() for f in fs}

  def test_empty_inputs_fold_to_nothing(self):
    assert filers(_entities()) == {}
    assert filings_by_cik(_reports(), _links(), _entities(), ["10-K"]) == {}


@pytest.mark.unit
class TestDocuments:
  def test_build_company_lists_representations_and_viewer_links(self, corpus):
    entities, reports, links = corpus
    filer = filers(entities)[MMM]
    filings = filings_by_cik(reports, links, entities, ["10-K", "10-Q"])[MMM]
    holon_url = f"{CDN}/2025/{MMM}/0000066740-25-000006/holon.jsonld"
    tavi_url = f"{CDN}/2025/{MMM}/0000066740-25-000006/tavi.json"
    manifests = {
      "0000066740-25-000006": {
        "folder": f"{CDN}/2025/{MMM}/0000066740-25-000006/",
        "representations": [
          {
            "kind": "tavi",
            "name": "tavi.json",
            "media_type": "application/json",
            "bytes": 2,
            "url": tavi_url,
          },
          {
            "kind": "holon",
            "name": "holon.jsonld",
            "media_type": "application/ld+json",
            "bytes": 1,
            "url": holon_url,
          },
          {
            "kind": "document",
            "name": "mmm-20241231.htm",
            "media_type": "text/html",
            "bytes": 3,
            "url": f"{CDN}/2025/{MMM}/0000066740-25-000006/mmm-20241231.htm",
          },
        ],
      },
      # The 10-Q predates the artifacts: listed, nothing to open yet.
      "0000066740-25-000020": None,
    }

    doc = build_company(
      filer, filings, manifests, viewer_url="https://holon.robosystems.ai/"
    )

    assert doc["ticker"] == "MMM"
    assert doc["cik"] == MMM
    assert doc["name"] == "3M COMPANY"
    assert doc["source"] == "SEC EDGAR"
    assert [f["accession"] for f in doc["filings"]] == [
      "0000066740-25-000020",
      "0000066740-25-000006",
    ]
    tenq, tenk = doc["filings"]
    assert tenq["representations"] == []
    assert tenq["viewer"] == {}
    assert tenq["folder"] is None
    assert len(tenk["representations"]) == 3
    assert tenk["viewer"] == {
      "holon": viewer_link("https://holon.robosystems.ai", holon_url),
      "tavi": viewer_link("https://holon.robosystems.ai", tavi_url),
    }
    # The latest openable filing per form: the 10-Q has nothing to open.
    assert doc["latest"] == {"10-K": "0000066740-25-000006"}

  def test_viewer_link_encodes_the_url_parameter(self):
    link = viewer_link("https://holon.robosystems.ai", "https://cdn/a/b/holon.jsonld")
    assert (
      link
      == "https://holon.robosystems.ai/?url=https%3A%2F%2Fcdn%2Fa%2Fb%2Fholon.jsonld"
    )

  def test_build_index_sorts_by_ticker_with_the_newest_filing(self, corpus):
    entities, reports, links = corpus
    rows_by_cik = filers(entities)
    by_cik = filings_by_cik(reports, links, entities, ["10-K", "10-Q"])
    rows = [index_row(rows_by_cik[cik], by_cik[cik]) for cik in (NVDA, MMM)]

    index = build_index(rows)

    assert index["count"] == 2
    assert [c["ticker"] for c in index["companies"]] == ["MMM", "NVDA"]
    mmm = index["companies"][0]
    assert mmm["filings"] == 2
    assert mmm["latest"]["accession"] == "0000066740-25-000020"
    assert mmm["latest"]["form"] == "10-Q"

  def test_catalog_keys(self):
    assert get_filing_catalog_key("MMM") == "companies/mmm.json"
    assert get_filing_catalog_key(" brk.b ") == "companies/brk.b.json"


@pytest.mark.unit
class TestReads:
  def test_corpus_partitions_start_at_the_year(self):
    partitions = corpus_partitions(2025)
    assert partitions[0] == "2025-Q1"
    assert all(int(p[:4]) >= 2025 for p in partitions)

  def test_read_manifests_returns_none_for_a_missing_manifest(self):
    s3 = MagicMock()

    class NoSuchKey(Exception):
      pass

    s3.exceptions.NoSuchKey = NoSuchKey
    present = get_filing_artifact_key(
      "2025", MMM, "0000066740-25-000006", "manifest.json"
    )

    def get_object(Bucket, Key):
      if Key == present:
        return {"Body": MagicMock(read=lambda: b'{"representations": [1]}')}
      raise NoSuchKey()

    s3.get_object.side_effect = get_object
    filings = [
      {"cik": MMM, "accession": "0000066740-25-000006", "filing_date": "2025-02-05"},
      {"cik": MMM, "accession": "0000066740-25-000020", "filing_date": "2025-04-30"},
      {"cik": MMM, "accession": "no-date", "filing_date": None},
    ]

    manifests = read_manifests(s3, "public", filings, workers=2)

    assert manifests == {
      "0000066740-25-000006": {"representations": [1]},
      "0000066740-25-000020": None,
      "no-date": None,
    }
