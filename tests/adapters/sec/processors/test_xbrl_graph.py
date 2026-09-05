"""XBRLGraphProcessor on xbrlkit's model and projection.

The parse is stubbed with a synthetic ``XbrlModel`` (Arelle never runs), so
these tests cover what the processor owns: the filing's coordinates from the
EDGAR metadata, the projected rows bound as DataFrames and written as
parquet, and the platform steps that run on top — externalization,
enrichment, classification — reading the rows the projection wrote.
"""

import os
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pytest
from arelle.UrlUtil import IXDS_DOC_SEPARATOR, IXDS_SURROGATE
from xbrlkit.model import (
  Arc,
  Concept,
  DimQualifier,
  EntityIdentity,
  FilingMeta,
  Label,
  Network,
  Period,
  Unit,
  XbrlFact,
  XbrlModel,
)
from xbrlkit.schema import node_table, rel_table
from xbrlkit.serialize.lpg import PARENT_CHILD, graph_id

from robosystems.adapters.sec.processors import xbrl_graph
from robosystems.adapters.sec.processors.xbrl_graph import (
  XBRL_GRAPH_PROCESSOR_VERSION,
  XBRLGraphProcessor,
)

MODULE = "robosystems.adapters.sec.processors.xbrl_graph"

SCHEMA_CONFIG = {
  "name": "SEC Database Schema",
  "description": "base + roboledger",
  "base_schema": "base",
  "extensions": ["roboledger"],
}

REPORT_URI = (
  "https://www.sec.gov/Archives/edgar/data/1045810/000104581024000029/nvda-20240128.htm"
)
CIK = "0001045810"
US_GAAP = "http://fasb.org/us-gaap/2023"
SRT = "http://fasb.org/srt/2023"
NVDA = "http://www.nvidia.com/20240128"
XBRLI = "http://www.xbrl.org/2003/instance"
STANDARD_LABEL = "http://www.xbrl.org/2003/role/label"
TEXT_BLOCK_HTML = "<p>" + "Revenue recognition policy. " * 80 + "</p>"


@pytest.fixture
def sec_filer():
  return {
    "cik": "1045810",
    "name": "NVIDIA CORP",
    "entity_name": "NVIDIA CORP",
    "ticker": "NVDA",
    "exchange": "Nasdaq",
    "sic": "3674",
    "sicDescription": "Semiconductors & Related Devices",
    "stateOfIncorporation": "DE",
    "fiscalYearEnd": "0128",
    "ein": "943177549",
    "entityType": "operating",
    "category": "Large accelerated filer",
    "website": "https://www.nvidia.com",
    "phone": "408-486-2000",
  }


@pytest.fixture
def sec_report():
  return {
    "accessionNumber": "0001045810-24-000029",
    "form": "10-K",
    "filingDate": "2024-02-21",
    "reportDate": "2024-01-28",
    "acceptanceDateTime": "2024-02-21T16:06:47.000Z",
    "primaryDocument": "nvda-20240128.htm",
    "isInlineXBRL": True,
  }


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


def _model(sec_report) -> XbrlModel:
  """A filing with three facts, one dimension and one presentation network."""
  filing = FilingMeta(
    accession=sec_report["accessionNumber"],
    cik=CIK,
    form="10-K",
    filing_date=date(2024, 2, 21),
    report_date=date(2024, 1, 28),
    acceptance_datetime="2024-02-21T16:06:47.000Z",
    is_inline_xbrl=True,
    primary_document="nvda-20240128.htm",
    report_uri=REPORT_URI,
    extension_namespace=NVDA,
    fiscal_year_focus="2024",
    fiscal_period_focus="FY",
    fiscal_year_end_month="1",
  )
  entity = EntityIdentity(cik=CIK, name="NVIDIA CORP", ticker="NVDA", ein="943177549")
  concepts = {
    "us-gaap:Assets": _concept(
      "us-gaap:Assets",
      US_GAAP,
      period_type="instant",
      balance="debit",
      is_numeric=True,
      nice_type="Monetary",
    ),
    "us-gaap:Revenues": _concept(
      "us-gaap:Revenues",
      US_GAAP,
      period_type="duration",
      balance="credit",
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
    "us-gaap:StatementLineItems": _concept(
      "us-gaap:StatementLineItems", US_GAAP, period_type="duration", is_abstract=True
    ),
    "srt:ProductOrServiceAxis": _concept(
      "srt:ProductOrServiceAxis",
      SRT,
      period_type="duration",
      is_abstract=True,
      is_dimension_item=True,
      nice_type="Axis",
    ),
    "srt:ProductMember": _concept(
      "srt:ProductMember",
      SRT,
      period_type="duration",
      is_abstract=True,
      is_domain_member=True,
    ),
  }
  periods = [
    Period(
      id="instant-2024-01-28",
      period_type="instant",
      end=date(2024, 1, 28),
      calendar_year=2024,
      calendar_quarter="Q1",
      calendar_period_key="2024-01-28",
    ),
    Period(
      id="duration-fy2024",
      period_type="duration",
      start=date(2023, 1, 30),
      end=date(2024, 1, 28),
      duration_type="annual",
      calendar_year=2024,
      calendar_quarter="FY",
      calendar_period_key="2024",
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
      period_id="instant-2024-01-28",
      unit_id="usd",
      value_str="65728000000",
      raw_value="65728000000",
      source_hash="a1",
      numeric_value=65728000000.0,
      decimals="-6",
      **context,
    ),
    XbrlFact(
      id="f-revenues",
      concept_qname="us-gaap:Revenues",
      period_id="duration-fy2024",
      unit_id="usd",
      value_str="47405000000",
      raw_value="47405000000",
      source_hash="a2",
      numeric_value=47405000000.0,
      decimals="-6",
      dims=[
        DimQualifier(
          axis_qname="srt:ProductOrServiceAxis",
          member_qname="srt:ProductMember",
          is_explicit=True,
          axis_type="segment",
        )
      ],
      **context,
    ),
    XbrlFact(
      id="f-policy",
      concept_qname="nvda:RevenueRecognitionPolicyTextBlock",
      period_id="duration-fy2024",
      value_str=TEXT_BLOCK_HTML,
      raw_value=TEXT_BLOCK_HTML,
      source_hash="a3",
      value_kind="text",
      **context,
    ),
  ]
  networks = [
    Network(
      role_uri=f"{NVDA}/role/BalanceSheet",
      definition="0001001 - Statement - CONSOLIDATED BALANCE SHEETS",
      kind="presentation",
      role_id="BalanceSheet",
      arcs=[
        Arc(
          from_qname="us-gaap:StatementLineItems",
          to_qname="us-gaap:Assets",
          arcrole=PARENT_CHILD,
          order=1.0,
          is_root=True,
        ),
        Arc(
          from_qname="us-gaap:StatementLineItems",
          to_qname="us-gaap:Revenues",
          arcrole=PARENT_CHILD,
          order=2.0,
          is_root=True,
        ),
      ],
    )
  ]
  return XbrlModel(
    filing=filing,
    entity=entity,
    concepts=concepts,
    periods=periods,
    units=units,
    facts=facts,
    networks=networks,
  )


@pytest.fixture
def model(sec_report):
  return _model(sec_report)


@pytest.fixture(autouse=True)
def platform_steps_off(monkeypatch):
  """Externalization, enrichment and classification are opt-in per test."""
  monkeypatch.setattr(xbrl_graph, "XBRL_EXTERNALIZE_LARGE_VALUES", False)
  monkeypatch.setattr(xbrl_graph, "XBRL_SEMANTIC_ENRICHMENT", False)
  monkeypatch.setattr(xbrl_graph, "XBRL_SKIP_TEXTBLOCK_FACTS", False)
  monkeypatch.setattr(
    "robosystems.adapters.sec.config.XBRL_ASSOCIATION_CLASSIFICATION", False
  )


def _instance(tmp_path: Path) -> str:
  instance = tmp_path / "nvda-20240128.htm"
  instance.write_text("<html/>")
  return str(instance)


def _processor(tmp_path, sec_filer, sec_report, **overrides) -> XBRLGraphProcessor:
  kwargs = {
    "report_uri": REPORT_URI,
    "entityId": "1045810",
    "sec_filer": sec_filer,
    "sec_report": sec_report,
    "output_dir": str(tmp_path / "out"),
    "schema_config": SCHEMA_CONFIG,
    "local_file_path": _instance(tmp_path),
  }
  kwargs.update(overrides)
  return XBRLGraphProcessor(**kwargs)


class _Arelle:
  """``ArelleClient`` and the ``ModelXbrl`` it returns, as one patch."""

  def __init__(self):
    self.client = MagicMock(name="ArelleClient")
    self.model_xbrl = MagicMock(name="ModelXbrl")
    self.client.controller.return_value = self.model_xbrl


def _run(processor: XBRLGraphProcessor, model: XbrlModel) -> _Arelle:
  arelle = _Arelle()
  with (
    patch(f"{MODULE}.ArelleClient", return_value=arelle.client),
    patch(f"{MODULE}.to_xbrl_model", return_value=model) as to_model,
  ):
    processor.process()
  arelle.to_model = to_model
  return arelle


def _read(processor: XBRLGraphProcessor, table: str):
  """The written parquet table, read through an open handle: resolving a
  path goes through pyarrow's filesystem registry, which a forked test
  worker can find already claimed by another library."""
  subdir = "nodes" if table[0].isupper() and not table.isupper() else "relationships"
  with open(processor.output_dir / subdir / f"{table}.parquet", "rb") as handle:
    return pq.read_table(handle)


def _rows(processor: XBRLGraphProcessor, table: str) -> list[dict]:
  return _read(processor, table).to_pylist()


def _columns(processor: XBRLGraphProcessor, table: str) -> list[str]:
  return _read(processor, table).column_names


@pytest.mark.unit
class TestConstruction:
  def test_version_is_xbrlkits(self):
    from xbrlkit.serialize.lpg import XBRL_GRAPH_PROCESSOR_VERSION as kit_version

    assert kit_version == XBRL_GRAPH_PROCESSOR_VERSION

  def test_schema_config_is_required(self, tmp_path):
    with pytest.raises(ValueError, match="Schema configuration is required"):
      XBRLGraphProcessor(report_uri=REPORT_URI, output_dir=str(tmp_path))

  def test_binds_an_empty_dataframe_per_schema_table(self, tmp_path, sec_filer):
    processor = _processor(tmp_path, sec_filer, None)
    assert processor.facts_df.empty
    assert list(processor.facts_df.columns) == list(node_table("Fact").columns)
    assert processor.report_facts_df.empty
    assert processor.entity_data is None and processor.report_data is None
    assert processor.failed is False


@pytest.mark.unit
class TestFilingCoordinates:
  def test_filing_meta_from_the_edgar_record(self, tmp_path, sec_filer, sec_report):
    meta = _processor(tmp_path, sec_filer, sec_report)._filing_meta()
    assert meta.accession == "0001045810-24-000029"
    assert meta.cik == CIK
    assert meta.form == "10-K"
    assert meta.filing_date == date(2024, 2, 21)
    assert meta.report_date == date(2024, 1, 28)
    assert meta.acceptance_datetime == "2024-02-21T16:06:47.000Z"
    assert meta.is_inline_xbrl is True
    assert meta.primary_document == "nvda-20240128.htm"
    assert meta.report_uri == REPORT_URI
    # The fiscal context is the parse's to fill from the DEI facts.
    assert meta.fiscal_year_focus is None

  def test_filing_meta_without_a_record(self, tmp_path, sec_filer):
    meta = _processor(tmp_path, sec_filer, None)._filing_meta()
    assert meta.accession == ""
    assert meta.form is None
    assert meta.filing_date is None
    assert meta.is_inline_xbrl is False

  def test_malformed_dates_become_null(self, tmp_path, sec_filer, sec_report):
    sec_report = {
      **sec_report,
      "filingDate": "21/02/2024",
      "reportDate": "",
      "acceptanceDateTime": "yesterday",
    }
    meta = _processor(tmp_path, sec_filer, sec_report)._filing_meta()
    assert meta.filing_date is None
    assert meta.report_date is None
    assert meta.acceptance_datetime is None

  def test_entity_identity_from_the_submissions_header(
    self, tmp_path, sec_filer, sec_report
  ):
    entity = _processor(tmp_path, sec_filer, sec_report)._entity_identity()
    assert entity.cik == CIK
    assert entity.name == "NVIDIA CORP" and entity.legal_name == "NVIDIA CORP"
    assert entity.ein == "943177549"
    assert entity.ticker == "NVDA" and entity.exchange == "Nasdaq"
    assert entity.sic == "3674"
    assert entity.sic_description == "Semiconductors & Related Devices"
    assert entity.state_of_incorporation == "DE"
    assert entity.fiscal_year_end == "0128"
    assert entity.entity_type == "operating"
    assert entity.category == "Large accelerated filer"
    assert entity.website == "https://www.nvidia.com"
    assert entity.phone == "408-486-2000"

  def test_header_cik_wins_over_entity_id_and_is_padded(self, tmp_path):
    processor = _processor(tmp_path, {"cik": "320193"}, None, entityId="0000000001")
    assert processor._normalized_cik() == "0000320193"
    assert processor._entity_identity().cik == "0000320193"

  def test_entity_id_alone_is_enough(self, tmp_path):
    processor = _processor(tmp_path, None, None, entityId="320193")
    assert processor._normalized_cik() == "0000320193"

  def test_a_cik_is_required(self, tmp_path):
    processor = _processor(tmp_path, {}, None, entityId=None)
    with pytest.raises(ValueError, match="CIK"):
      processor._entity_identity()

  def test_empty_header_strings_are_kept_except_the_website(self, tmp_path):
    filer = {
      "cik": "1",
      "name": "X",
      "ein": "",
      "sic": "",
      "stateOfIncorporation": "",
      "website": "",
      "investorWebsite": "https://ir.example.com",
    }
    entity = _processor(tmp_path, filer, None)._entity_identity()
    assert entity.ein is None
    assert entity.sic == "" and entity.state_of_incorporation == ""
    assert entity.website == "https://ir.example.com"
    assert (
      _processor(tmp_path, {"cik": "1", "website": ""}, None)._entity_identity().website
      is None
    )

  def test_numeric_header_values_become_strings(self, tmp_path):
    entity = _processor(
      tmp_path, {"cik": 1045810, "ein": 943177549, "sic": 3674}, None
    )._entity_identity()
    assert entity.cik == CIK
    assert entity.ein == "943177549" and entity.sic == "3674"


@pytest.mark.unit
class TestInstancePath:
  def test_local_file_path(self, tmp_path, sec_filer):
    processor = _processor(tmp_path, sec_filer, None)
    assert processor._resolve_instance_path() == processor.local_file_path

  def test_file_uri_when_no_local_path(self, tmp_path, sec_filer):
    instance = _instance(tmp_path)
    processor = _processor(
      tmp_path, sec_filer, None, report_uri=f"file://{instance}", local_file_path=None
    )
    assert processor._resolve_instance_path() == instance

  def test_http_uri_without_local_path_fails(self, tmp_path, sec_filer):
    processor = _processor(tmp_path, sec_filer, None, local_file_path=None)
    assert processor._resolve_instance_path() is None

  def test_missing_file_fails(self, tmp_path, sec_filer):
    processor = _processor(
      tmp_path, sec_filer, None, local_file_path=str(tmp_path / "missing.htm")
    )
    assert processor._resolve_instance_path() is None

  def test_inline_document_set_checks_every_member(self, tmp_path, sec_filer):
    first = _instance(tmp_path)
    second = tmp_path / "nvda-20240128_d2.htm"
    second.write_text("<html/>")
    surrogate = os.path.join(str(tmp_path), IXDS_SURROGATE) + IXDS_DOC_SEPARATOR.join(
      [first, str(second)]
    )
    processor = _processor(tmp_path, sec_filer, None, local_file_path=surrogate)
    assert processor._resolve_instance_path() == surrogate

    second.unlink()
    assert processor._resolve_instance_path() is None

  def test_failed_filing_writes_nothing(self, tmp_path, sec_filer, sec_report, model):
    processor = _processor(
      tmp_path, sec_filer, sec_report, local_file_path=str(tmp_path / "missing.htm")
    )
    with patch(f"{MODULE}.ArelleClient") as arelle:
      processor.process()
    assert processor.failed is True
    arelle.assert_not_called()
    assert not (processor.output_dir / "nodes").exists()


@pytest.mark.unit
class TestProjection:
  def test_writes_every_projected_table(self, tmp_path, sec_filer, sec_report, model):
    processor = _processor(tmp_path, sec_filer, sec_report)
    arelle = _run(processor, model)

    arelle.client.controller.assert_called_once_with(processor.local_file_path)
    arelle.to_model.assert_called_once()
    assert arelle.to_model.call_args.kwargs["entity"].cik == CIK

    assert len(_rows(processor, "Entity")) == 1
    assert len(_rows(processor, "Report")) == 1
    assert len(_rows(processor, "Fact")) == 3
    assert len(_rows(processor, "Element")) == 6
    assert len(_rows(processor, "Label")) == 6
    assert len(_rows(processor, "Period")) == 2
    assert len(_rows(processor, "Unit")) == 1
    assert len(_rows(processor, "Dimension")) == 1
    assert len(_rows(processor, "Taxonomy")) == 1
    assert len(_rows(processor, "Structure")) == 1
    assert len(_rows(processor, "Association")) == 2
    assert len(_rows(processor, "REPORT_HAS_FACT")) == 3
    assert len(_rows(processor, "FACT_HAS_ELEMENT")) == 3
    assert len(_rows(processor, "FACT_HAS_PERIOD")) == 3
    assert len(_rows(processor, "FACT_HAS_ENTITY")) == 3
    assert len(_rows(processor, "FACT_HAS_UNIT")) == 2
    assert len(_rows(processor, "FACT_HAS_DIMENSION")) == 1
    assert len(_rows(processor, "DIMENSION_HAS_AXIS_ELEMENT")) == 1
    assert len(_rows(processor, "DIMENSION_HAS_MEMBER_ELEMENT")) == 1
    assert len(_rows(processor, "STRUCTURE_HAS_ASSOCIATION")) == 2
    assert len(_rows(processor, "ASSOCIATION_HAS_FROM_ELEMENT")) == 2
    assert len(_rows(processor, "ASSOCIATION_HAS_TO_ELEMENT")) == 2
    assert len(_rows(processor, "STRUCTURE_HAS_TAXONOMY")) == 1
    assert len(_rows(processor, "REPORT_USES_TAXONOMY")) == 1
    assert len(_rows(processor, "ENTITY_HAS_REPORT")) == 1
    assert len(_rows(processor, "ELEMENT_HAS_LABEL")) == 6
    assert len(_rows(processor, "TAXONOMY_HAS_LABEL")) == 6
    # Tables the filing does not fill are not written.
    assert not (processor.output_dir / "nodes" / "FactSet.parquet").exists()
    assert not (processor.output_dir / "nodes" / "Classification.parquet").exists()

  def test_columns_follow_xbrlkits_order(self, tmp_path, sec_filer, sec_report, model):
    processor = _processor(tmp_path, sec_filer, sec_report)
    _run(processor, model)
    for table in ("Entity", "Report", "Fact", "Element", "Period", "Association"):
      assert _columns(processor, table) == list(node_table(table).columns)
    for table in ("TAXONOMY_HAS_LABEL", "FACT_HAS_DIMENSION"):
      assert _columns(processor, table) == list(rel_table(table).columns)

  def test_ids_are_the_platforms(self, tmp_path, sec_filer, sec_report, model):
    processor = _processor(tmp_path, sec_filer, sec_report)
    _run(processor, model)

    report = _rows(processor, "Report")[0]
    assert report["identifier"] == graph_id("report", REPORT_URI)
    facts = {row["uri"]: row for row in _rows(processor, "Fact")}
    assets = facts[f"{REPORT_URI}#fact-a1"]
    assert assets["identifier"] == graph_id("fact", f"{REPORT_URI}#fact-a1")
    assert assets["numeric_value"] == 65728000000.0
    assert assets["fact_type"] == "Numeric" and assets["decimals"] == "-6"
    assert assets["has_dimensions"] is False and assets["dimension_count"] == 0
    elements = {row["qname"]: row for row in _rows(processor, "Element")}
    assert elements["us-gaap:Assets"]["identifier"] == graph_id(
      "element", f"{US_GAAP}#Assets"
    )
    assert elements["us-gaap:Assets"]["balance"] == "debit"

  def test_entity_and_report_rows_carry_the_filing(
    self, tmp_path, sec_filer, sec_report, model
  ):
    processor = _processor(tmp_path, sec_filer, sec_report)
    _run(processor, model)

    entity = processor.entity_data
    assert entity["cik"] == CIK
    assert entity["uri"] == f"http://www.sec.gov/CIK#{CIK}"
    assert entity["name"] == "NVIDIA CORP" and entity["ticker"] == "NVDA"
    assert entity["tax_id"] == "943177549"
    assert entity["is_parent"] is True and entity["status"] == "active"
    assert _rows(processor, "Entity")[0]["identifier"] == entity["identifier"]

    report = processor.report_data
    assert report["accession_number"] == "0001045810-24-000029"
    assert report["form"] == "10-K" and report["name"] == "10-K"
    assert report["filing_date"] == "2024-02-21"
    assert report["report_date"] == "2024-01-28"
    assert report["acceptance_date"] == "2024-02-21"
    assert report["is_inline_xbrl"] is True
    assert report["xbrl_processor_version"] == XBRL_GRAPH_PROCESSOR_VERSION
    assert report["fiscal_year_focus"] == 2024
    assert report["fiscal_period_focus"] == "FY"
    assert report["fiscal_year_end_month"] == 1
    assert report["processed"] is False and report["failed"] is False

  def test_text_blocks_stay_inline_without_externalization(
    self, tmp_path, sec_filer, sec_report, model
  ):
    processor = _processor(tmp_path, sec_filer, sec_report)
    _run(processor, model)
    policy = {r["uri"]: r for r in _rows(processor, "Fact")}[f"{REPORT_URI}#fact-a3"]
    assert policy["value"] == TEXT_BLOCK_HTML
    assert policy["value_type"] == "inline" and policy["content_type"] is None
    assert policy["fact_type"] == "Nonnumeric"

  def test_skip_textblock_facts(
    self, tmp_path, sec_filer, sec_report, model, monkeypatch
  ):
    monkeypatch.setattr(xbrl_graph, "XBRL_SKIP_TEXTBLOCK_FACTS", True)
    processor = _processor(tmp_path, sec_filer, sec_report)
    _run(processor, model)
    uris = {r["uri"] for r in _rows(processor, "Fact")}
    assert uris == {f"{REPORT_URI}#fact-a1", f"{REPORT_URI}#fact-a2"}
    assert len(_rows(processor, "REPORT_HAS_FACT")) == 2
    # The text block's element was reachable only through its fact.
    assert "nvda:RevenueRecognitionPolicyTextBlock" not in {
      r["qname"] for r in _rows(processor, "Element")
    }

  def test_base_only_schema_skips_undeclared_tables(
    self, tmp_path, sec_filer, sec_report, model
  ):
    processor = _processor(
      tmp_path, sec_filer, sec_report, schema_config={**SCHEMA_CONFIG, "extensions": []}
    )
    _run(processor, model)
    assert "Fact" not in processor.schema_to_dataframe_mapping
    assert len(_rows(processor, "Element")) == 6
    assert not (processor.output_dir / "nodes" / "Fact.parquet").exists()
    assert processor.report_data["accession_number"] == "0001045810-24-000029"

  def test_arelle_resources_close_even_when_the_parse_fails(
    self, tmp_path, sec_filer, sec_report
  ):
    processor = _processor(tmp_path, sec_filer, sec_report)
    arelle = _Arelle()
    with (
      patch(f"{MODULE}.ArelleClient", return_value=arelle.client),
      patch(f"{MODULE}.to_xbrl_model", side_effect=RuntimeError("bad DTS")),
      pytest.raises(RuntimeError, match="bad DTS"),
    ):
      processor.process()
    arelle.model_xbrl.close.assert_called_once()
    arelle.client.close.assert_called_once()

  def test_process_async_runs_process(self, tmp_path, sec_filer, sec_report, model):
    import asyncio

    processor = _processor(tmp_path, sec_filer, sec_report)
    arelle = _Arelle()
    with (
      patch(f"{MODULE}.ArelleClient", return_value=arelle.client),
      patch(f"{MODULE}.to_xbrl_model", return_value=model),
    ):
      asyncio.run(processor.process_async())
    assert len(_rows(processor, "Fact")) == 3


class _FakeExternalizer:
  """Externalizes HTML values; records what it was asked to upload."""

  enabled = True

  def __init__(self, fail=False):
    self.fail = fail
    self.queued: list[tuple[str, dict | None, dict | None]] = []
    self.batches = 0

  def should_externalize(self, value):
    return "<" in str(value) and ">" in str(value)

  def queue_value_for_s3(self, value, fact_id, entity_data, report_data):
    self.queued.append((fact_id, entity_data, report_data))
    if self.fail:
      return None
    url = f"https://cdn.example.com/{fact_id}.html"
    return {
      "url": url,
      "stored_value": url,
      "value_type": "external",
      "content_type": "text/html",
    }

  def process_batch_uploads(self):
    self.batches += 1


@pytest.mark.unit
class TestExternalization:
  def test_large_values_are_replaced_by_their_url(
    self, tmp_path, sec_filer, sec_report, model
  ):
    processor = _processor(tmp_path, sec_filer, sec_report)
    externalizer = _FakeExternalizer()
    processor.textblock_externalizer = externalizer
    _run(processor, model)

    facts = {r["uri"]: r for r in _rows(processor, "Fact")}
    policy = facts[f"{REPORT_URI}#fact-a3"]
    assert policy["value"] == f"https://cdn.example.com/{policy['identifier']}.html"
    assert policy["value_type"] == "external"
    assert policy["content_type"] == "text/html"
    assets = facts[f"{REPORT_URI}#fact-a1"]
    assert assets["value"] == "65728000000" and assets["value_type"] == "inline"
    assert externalizer.batches == 1

    # The upload key is built from the filing's coordinates on the projected rows.
    fact_id, entity_data, report_data = externalizer.queued[0]
    assert fact_id == policy["identifier"]
    assert entity_data["cik"] == CIK
    assert report_data["accession_number"] == "0001045810-24-000029"
    assert report_data["filing_date"] == "2024-02-21"

  def test_a_value_that_cannot_be_queued_stays_inline(
    self, tmp_path, sec_filer, sec_report, model
  ):
    processor = _processor(tmp_path, sec_filer, sec_report)
    processor.textblock_externalizer = _FakeExternalizer(fail=True)
    _run(processor, model)
    policy = {r["uri"]: r for r in _rows(processor, "Fact")}[f"{REPORT_URI}#fact-a3"]
    assert policy["value"] == TEXT_BLOCK_HTML
    assert policy["value_type"] == "inline" and policy["content_type"] is None

  def test_disabled_externalizer_is_not_consulted(
    self, tmp_path, sec_filer, sec_report, model
  ):
    processor = _processor(tmp_path, sec_filer, sec_report)
    externalizer = _FakeExternalizer()
    externalizer.enabled = False
    processor.textblock_externalizer = externalizer
    _run(processor, model)
    assert externalizer.queued == [] and externalizer.batches == 0


@pytest.mark.unit
class TestEnrichment:
  def test_enrichment_runs_on_the_projected_tables(
    self, tmp_path, sec_filer, sec_report, model, monkeypatch
  ):
    monkeypatch.setattr("robosystems.adapters.sec.config.XBRL_GRAPH_REFINEMENT", False)
    enricher = MagicMock(name="SemanticEnricher")
    enricher.embed_batch.side_effect = lambda texts: [[0.1, 0.2]] * len(texts)
    enricher.match_canonical.side_effect = lambda embedding, element: (
      ("Assets", 0.93) if element["qname"] == "us-gaap:Assets" else (None, None)
    )
    enricher.match_structure_canonical.return_value = ("balance_sheet", 0.8)

    processor = _processor(tmp_path, sec_filer, sec_report, enricher=enricher)
    processor.enable_semantic_enrichment = True
    _run(processor, model)

    elements = {r["qname"]: r for r in _rows(processor, "Element")}
    assert elements["us-gaap:Assets"]["canonical_concept"] == "Assets"
    assert elements["us-gaap:Assets"]["canonical_confidence"] == 0.93
    assert elements["us-gaap:Revenues"]["canonical_concept"] is None
    structure = _rows(processor, "Structure")[0]
    assert structure["name"] == "CONSOLIDATED BALANCE SHEETS"
    assert structure["type"] == "Statement"
    assert structure["canonical_type"] == "balance_sheet"
    assert enricher.embed_batch.call_count == 2


@pytest.mark.unit
class TestClassification:
  def test_classifier_receives_the_filing_and_its_output_is_written(
    self, tmp_path, sec_filer, sec_report, model, monkeypatch
  ):
    monkeypatch.setattr(
      "robosystems.adapters.sec.config.XBRL_ASSOCIATION_CLASSIFICATION", True
    )
    factset_id = "fs-1"
    result = MagicMock()
    result.classifications_df = pd.DataFrame(
      [
        {
          "identifier": "cls-1",
          "category": "concept_arrangement",
          "type": "RollUp",
          "source": "structural",
          "confidence": 1.0,
        }
      ]
    )
    result.assoc_classifications_df = pd.DataFrame([{"from": "assoc-1", "to": "cls-1"}])
    result.factsets_df = pd.DataFrame(
      [{"identifier": factset_id, "factset_type": "report", "provenance": "{}"}]
    )
    result.structure_factset_rels_df = pd.DataFrame(
      [{"from": "struct-1", "to": factset_id}]
    )
    result.factset_fact_rels_df = pd.DataFrame([{"from": factset_id, "to": "fact-1"}])
    result.report_factset_rels_df = pd.DataFrame(
      [{"from": "report-1", "to": factset_id}]
    )
    result.canonical_hints = {}

    processor = _processor(tmp_path, sec_filer, sec_report)
    with patch(
      "robosystems.adapters.sec.processors.classify.AssociationClassifier"
    ) as classifier_cls:
      classifier_cls.return_value.classify.return_value = result
      _run(processor, model)

    (output_dir, filing_meta), _ = classifier_cls.return_value.classify.call_args
    assert output_dir == processor.output_dir
    assert filing_meta.report_id == graph_id("report", REPORT_URI)
    assert filing_meta.accession == "0001045810-24-000029"
    assert filing_meta.filing_date == "2024-02-21"
    assert filing_meta.form == "10-K"
    assert filing_meta.filer_cik == CIK

    assert _rows(processor, "FactSet")[0]["identifier"] == factset_id
    assert _rows(processor, "Classification")[0]["type"] == "RollUp"
    assert len(_rows(processor, "REPORT_HAS_FACT_SET")) == 1

  def test_canonical_hints_upgrade_elements(
    self, tmp_path, sec_filer, sec_report, model, monkeypatch
  ):
    monkeypatch.setattr(
      "robosystems.adapters.sec.config.XBRL_ASSOCIATION_CLASSIFICATION", True
    )
    assets_id = graph_id("element", f"{US_GAAP}#Assets")
    result = MagicMock()
    for name in (
      "classifications_df",
      "assoc_classifications_df",
      "factsets_df",
      "structure_factset_rels_df",
      "factset_fact_rels_df",
      "report_factset_rels_df",
    ):
      setattr(result, name, pd.DataFrame())
    result.canonical_hints = {assets_id: ("Assets", 0.99)}

    processor = _processor(tmp_path, sec_filer, sec_report)
    with patch(
      "robosystems.adapters.sec.processors.classify.AssociationClassifier"
    ) as classifier_cls:
      classifier_cls.return_value.classify.return_value = result
      _run(processor, model)

    elements = {r["qname"]: r for r in _rows(processor, "Element")}
    assert elements["us-gaap:Assets"]["canonical_concept"] == "Assets"
    assert elements["us-gaap:Assets"]["canonical_confidence"] == 0.99

  def test_classifier_failure_is_non_critical(
    self, tmp_path, sec_filer, sec_report, model, monkeypatch
  ):
    monkeypatch.setattr(
      "robosystems.adapters.sec.config.XBRL_ASSOCIATION_CLASSIFICATION", True
    )
    processor = _processor(tmp_path, sec_filer, sec_report)
    with patch(
      "robosystems.adapters.sec.processors.classify.AssociationClassifier"
    ) as classifier_cls:
      classifier_cls.return_value.classify.side_effect = RuntimeError("no ladybug")
      _run(processor, model)
    assert len(_rows(processor, "Fact")) == 3
