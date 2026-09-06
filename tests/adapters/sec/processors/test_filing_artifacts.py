"""FilingArtifactWriter: what the processor publishes per filing, and how.

The model is synthetic (Arelle never runs); the S3 client is a mock that
records every upload, so the tests read the artifacts back from the calls.
"""

import json
import os
from datetime import date
from unittest.mock import MagicMock

import pytest
from xbrlkit.model import (
  Concept,
  EntityIdentity,
  FilingMeta,
  Label,
  Period,
  Unit,
  XbrlFact,
  XbrlModel,
)
from xbrlkit.serialize.lpg import graph_id

from robosystems.adapters.sec.processors.artifacts import (
  ARTIFACT_CACHE_CONTROL,
  MANIFEST_CACHE_CONTROL,
  FilingArtifactWriter,
  filing_coordinates,
  primary_document_path,
  with_external_values,
)

REPORT_URI = (
  "https://www.sec.gov/Archives/edgar/data/1045810/000104581024000029/nvda-20240128.htm"
)
CIK = "0001045810"
ACCESSION = "0001045810-24-000029"
FOLDER = f"2024/{CIK}/{ACCESSION}"
CDN = "https://data.example.com"
US_GAAP = "http://fasb.org/us-gaap/2023"
NVDA = "http://www.nvidia.com/20240128"
XBRLI = "http://www.xbrl.org/2003/instance"
STANDARD_LABEL = "http://www.xbrl.org/2003/role/label"
TEXT_BLOCK_HTML = "<p>" + "Revenue recognition policy. " * 20 + "</p>"
TEXT_FACT_HASH = "h-text"


def _concept(qname, namespace, **kwargs):
  name = qname.split(":")[1]
  return Concept(
    qname=qname,
    namespace=namespace,
    name=name,
    substitution_group="xbrli:item",
    substitution_group_namespace=XBRLI,
    labels=[Label(value=name, role=STANDARD_LABEL, language="en-US")],
    **kwargs,
  )


def _model(filing_date: date | None = date(2024, 2, 21)) -> XbrlModel:
  filing = FilingMeta(
    accession=ACCESSION,
    cik=CIK,
    form="10-K",
    filing_date=filing_date,
    report_date=date(2024, 1, 28),
    is_inline_xbrl=True,
    primary_document="nvda-20240128.htm",
    report_uri=REPORT_URI,
    extension_namespace=NVDA,
    fiscal_year_focus="2024",
    fiscal_period_focus="FY",
  )
  entity = EntityIdentity(cik=CIK, name="NVIDIA CORP", ticker="NVDA")
  concepts = {
    "us-gaap:Assets": _concept(
      "us-gaap:Assets",
      US_GAAP,
      period_type="instant",
      balance="debit",
      is_numeric=True,
      nice_type="Monetary",
    ),
    "nvda:RevenueRecognitionPolicyTextBlock": _concept(
      "nvda:RevenueRecognitionPolicyTextBlock",
      NVDA,
      period_type="duration",
      is_textblock=True,
      nice_type="Text Block",
    ),
  }
  periods = [
    Period(id="i-2024-01-28", period_type="instant", end=date(2024, 1, 28)),
    Period(
      id="d-fy2024",
      period_type="duration",
      start=date(2023, 1, 30),
      end=date(2024, 1, 28),
    ),
  ]
  units = [
    Unit(id="usd", measure="iso4217:USD", uri="http://www.xbrl.org/2003/iso4217#USD")
  ]
  context = {
    "entity_cik": CIK,
    "entity_scheme": "http://www.sec.gov/CIK",
    "entity_identifier": CIK,
  }
  facts = [
    XbrlFact(
      id="f-assets",
      concept_qname="us-gaap:Assets",
      period_id="i-2024-01-28",
      unit_id="usd",
      value_str="65728000000",
      raw_value="65728000000",
      source_hash="h-assets",
      numeric_value=65728000000.0,
      decimals="-6",
      **context,
    ),
    XbrlFact(
      id="f-text",
      concept_qname="nvda:RevenueRecognitionPolicyTextBlock",
      period_id="d-fy2024",
      value_str=TEXT_BLOCK_HTML,
      raw_value=TEXT_BLOCK_HTML,
      source_hash=TEXT_FACT_HASH,
      value_kind="text",
      **context,
    ),
  ]
  return XbrlModel(
    filing=filing,
    entity=entity,
    concepts=concepts,
    periods=periods,
    units=units,
    facts=facts,
  )


def _text_fact_graph_id() -> str:
  return graph_id("fact", f"{REPORT_URI}#fact-{TEXT_FACT_HASH}")


@pytest.fixture
def s3():
  client = MagicMock()
  client.upload_bytes.return_value = True
  client.upload_file.return_value = True
  return client


@pytest.fixture
def writer(s3):
  return FilingArtifactWriter(s3_client=s3, bucket="public", cdn_url=CDN)


def _uploads(s3) -> dict[str, tuple[bytes, str | None, str | None]]:
  """key → (body, content type, cache control) for every byte upload."""
  out = {}
  for call in s3.upload_bytes.call_args_list:
    data, _bucket, key = call.args
    out[key] = (data, call.kwargs.get("content_type"), call.kwargs.get("cache_control"))
  return out


def _write(writer, model=None, **kwargs):
  defaults = {
    "report_id": "rpt-1",
    "instance_path": None,
    "external_values": {},
    "processor_version": "test",
  }
  return writer.write(model or _model(), **{**defaults, **kwargs})


@pytest.mark.unit
class TestFilingArtifactWriter:
  def test_writes_holon_tavi_and_manifest_under_the_filing_folder(self, writer, s3):
    result = _write(writer)

    uploads = _uploads(s3)
    assert set(uploads) == {
      f"{FOLDER}/holon.jsonld",
      f"{FOLDER}/tavi.json",
      f"{FOLDER}/tavi.gaps.json",
      f"{FOLDER}/manifest.json",
    }
    assert uploads[f"{FOLDER}/holon.jsonld"][1] == "application/ld+json"
    assert uploads[f"{FOLDER}/tavi.json"][1] == "application/json"
    assert uploads[f"{FOLDER}/holon.jsonld"][2] == ARTIFACT_CACHE_CONTROL
    assert uploads[f"{FOLDER}/manifest.json"][2] == MANIFEST_CACHE_CONTROL

    assert result is not None
    assert result.prefix == FOLDER
    assert result.errors == []
    assert [r.kind for r in result.representations] == ["tavi", "holon"]

    manifest = json.loads(uploads[f"{FOLDER}/manifest.json"][0])
    assert manifest["accession"] == ACCESSION
    assert manifest["cik"] == CIK
    assert manifest["form"] == "10-K"
    assert manifest["filing_date"] == "2024-02-21"
    assert manifest["fiscal_year"] == "2024"
    assert manifest["entity"] == {"cik": CIK, "name": "NVIDIA CORP", "ticker": "NVDA"}
    assert manifest["report_id"] == "rpt-1"
    assert manifest["folder"] == f"{CDN}/{FOLDER}/"
    kinds = {r["kind"]: r for r in manifest["representations"]}
    assert kinds["holon"]["url"] == f"{CDN}/{FOLDER}/holon.jsonld"
    assert kinds["holon"]["bytes"] == len(uploads[f"{FOLDER}/holon.jsonld"][0])
    assert kinds["tavi"]["spec"].endswith("/compiled")
    assert kinds["tavi"]["gaps"] == f"{CDN}/{FOLDER}/tavi.gaps.json"

  def test_the_holon_carries_an_externalized_text_block_as_its_cdn_url(
    self, writer, s3
  ):
    url = f"{CDN}/{FOLDER}/fact_abc123.html"
    _write(writer, external_values={_text_fact_graph_id(): url})

    uploads = _uploads(s3)
    holon = uploads[f"{FOLDER}/holon.jsonld"][0].decode()
    tavi = uploads[f"{FOLDER}/tavi.json"][0].decode()
    assert url in holon
    assert TEXT_BLOCK_HTML not in holon
    # The draft has no external-value construct: the Tavi keeps the text inline.
    assert url not in tavi
    assert "Revenue recognition policy." in tavi

  def test_the_primary_document_is_copied_when_it_is_the_instance(
    self, writer, s3, tmp_path
  ):
    instance = tmp_path / "nvda-20240128.htm"
    instance.write_text("<html>filed</html>")

    result = _write(writer, instance_path=str(instance))

    assert result is not None
    document = [r for r in result.representations if r.kind == "document"]
    assert len(document) == 1
    assert document[0].name == "nvda-20240128.htm"
    assert document[0].media_type == "text/html"
    assert document[0].bytes == len("<html>filed</html>")
    args, kwargs = s3.upload_file.call_args
    assert args == (str(instance), "public", f"{FOLDER}/nvda-20240128.htm")
    assert kwargs["content_type"] == "text/html"
    assert kwargs["cache_control"] == ARTIFACT_CACHE_CONTROL

  def test_a_failed_upload_is_recorded_in_the_manifest_not_raised(self, writer, s3):
    def upload(data, bucket, key, **kwargs):
      return not key.endswith("tavi.json")

    s3.upload_bytes.side_effect = upload

    result = _write(writer)

    assert result is not None
    assert [r.kind for r in result.representations] == ["holon"]
    assert result.errors == ["tavi: upload failed"]
    manifest = json.loads(_uploads(s3)[f"{FOLDER}/manifest.json"][0])
    assert manifest["errors"] == ["tavi: upload failed"]
    assert [r["kind"] for r in manifest["representations"]] == ["holon"]

  def test_disabled_or_unconfigured_writer_writes_nothing(self, s3):
    off = FilingArtifactWriter(
      s3_client=s3, bucket="public", cdn_url=CDN, enabled=False
    )
    assert _write(off) is None
    no_bucket = FilingArtifactWriter(s3_client=s3, bucket=None, cdn_url=CDN)
    assert _write(no_bucket) is None
    no_client = FilingArtifactWriter(s3_client=None, bucket="public", cdn_url=CDN)
    assert _write(no_client) is None
    s3.upload_bytes.assert_not_called()

  def test_a_filing_without_a_filing_date_is_skipped(self, writer, s3):
    assert _write(writer, model=_model(filing_date=None)) is None
    s3.upload_bytes.assert_not_called()


@pytest.mark.unit
class TestHelpers:
  def test_filing_coordinates_use_the_filing_year(self):
    assert filing_coordinates(_model()) == ("2024", CIK, ACCESSION)
    assert filing_coordinates(_model(filing_date=None)) is None

  def test_with_external_values_replaces_only_the_matched_fact(self):
    url = "https://cdn/fact_1.html"
    swapped = with_external_values(_model(), {_text_fact_graph_id(): url})
    by_id = {f.id: f for f in swapped.facts}
    assert by_id["f-text"].value_str == url
    assert by_id["f-text"].raw_value == url
    assert by_id["f-assets"].value_str == "65728000000"

  def test_with_external_values_is_identity_when_nothing_moved(self):
    model = _model()
    assert with_external_values(model, {}) is model

  def test_primary_document_path_resolves_the_instance_and_ixds_members(self, tmp_path):
    from arelle.UrlUtil import IXDS_DOC_SEPARATOR, IXDS_SURROGATE

    primary = tmp_path / "nvda-20240128.htm"
    exhibit = tmp_path / "nvda-ex.htm"
    primary.write_text("p")
    exhibit.write_text("e")

    assert primary_document_path(str(primary), "nvda-20240128.htm") == str(primary)
    surrogate = os.path.join(str(tmp_path), IXDS_SURROGATE) + IXDS_DOC_SEPARATOR.join(
      [str(exhibit), str(primary)]
    )
    assert primary_document_path(surrogate, "nvda-20240128.htm") == str(primary)
    # A sibling the instance path does not name is still found by name.
    assert primary_document_path(str(exhibit), "nvda-20240128.htm") == str(primary)
    assert primary_document_path(str(primary), "missing.htm") is None
    assert primary_document_path(None, "nvda-20240128.htm") is None
    assert primary_document_path(str(primary), None) is None
