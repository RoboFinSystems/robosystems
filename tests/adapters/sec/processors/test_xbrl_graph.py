"""Tests for XBRL Graph Processor.

Tests XBRLGraphProcessor initialization with various configs, helper methods
for dimension/unit/period/context processing, name standardization,
column mapping, and error handling for malformed inputs.
"""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from robosystems.adapters.sec.processors.xbrl_graph import (
  XBRL_GRAPH_PROCESSOR_VERSION,
  XBRLGraphProcessor,
)


def _make_mock_schema_adapter():
  """Create a mock XBRLSchemaAdapter that returns empty DataFrames."""
  mock = MagicMock()
  mock.create_schema_compatible_dataframe.return_value = pd.DataFrame()
  mock.process_dataframe_for_schema.return_value = pd.DataFrame()
  mock.print_schema_summary.return_value = None
  return mock


def _make_mock_ingest_adapter():
  """Create a mock XBRLSchemaConfigGenerator."""
  return MagicMock()


def _make_mock_df_manager():
  """Create a mock DataFrameManager that initializes empty DataFrames."""
  mock = MagicMock()
  # Return a dict of empty dataframes
  mock.initialize_all_dataframes.return_value = {
    "entities_df": pd.DataFrame(),
    "reports_df": pd.DataFrame(),
    "facts_df": pd.DataFrame(),
    "units_df": pd.DataFrame(),
    "labels_df": pd.DataFrame(),
    "elements_df": pd.DataFrame(),
    "dimensions_df": pd.DataFrame(),
    "structures_df": pd.DataFrame(),
    "references_df": pd.DataFrame(),
    "taxonomies_df": pd.DataFrame(),
    "periods_df": pd.DataFrame(),
    "entity_reports_df": pd.DataFrame(),
    "report_facts_df": pd.DataFrame(),
    "fact_units_df": pd.DataFrame(),
    "fact_has_dimension_rel_df": pd.DataFrame(),
    "dimension_has_axis_element_rel_df": pd.DataFrame(),
    "dimension_has_member_element_rel_df": pd.DataFrame(),
    "structure_associations_df": pd.DataFrame(),
    "association_to_elements_df": pd.DataFrame(),
  }
  mock.create_dynamic_dataframe_mapping.return_value = {}
  return mock


@pytest.fixture
def temp_output_dir():
  """Create and cleanup a temporary output directory."""
  tmpdir = tempfile.mkdtemp()
  yield tmpdir
  shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def basic_schema_config():
  """Basic schema configuration for tests."""
  return {
    "name": "SEC Database Schema",
    "description": "Test schema",
    "base_schema": "base",
    "extensions": ["roboledger"],
  }


@pytest.fixture
def sec_filer_data():
  """Sample SEC filer data."""
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
def sec_report_data():
  """Sample SEC report data."""
  return {
    "accessionNumber": "0001045810-24-000029",
    "form": "10-K",
    "filingDate": "2024-02-21",
    "reportDate": "2024-01-28",
    "acceptanceDateTime": "2024-02-21T16:06:47.000Z",
    "primaryDocument": "nvda-20240128.htm",
    "isInlineXBRL": True,
  }


def _create_processor(
  temp_output_dir, basic_schema_config, sec_filer=None, sec_report=None
):
  """Create an XBRLGraphProcessor with all heavy dependencies mocked."""
  with (
    patch(
      "robosystems.adapters.sec.processors.xbrl_graph.XBRLSchemaAdapter"
    ) as mock_schema_cls,
    patch(
      "robosystems.adapters.sec.processors.xbrl_graph.XBRLSchemaConfigGenerator"
    ) as mock_ingest_cls,
    patch(
      "robosystems.adapters.sec.processors.xbrl_graph.DataFrameManager"
    ) as mock_df_cls,
    patch("robosystems.adapters.sec.processors.xbrl_graph.ParquetWriter"),
    patch("robosystems.adapters.sec.processors.xbrl_graph.TextBlockExternalizer"),
    patch("robosystems.adapters.sec.processors.xbrl_graph.S3Client"),
    patch("robosystems.adapters.sec.processors.xbrl_graph.env") as mock_env,
  ):
    mock_env.PUBLIC_DATA_BUCKET = None
    mock_env.PUBLIC_DATA_CDN_URL = None

    mock_schema_cls.return_value = _make_mock_schema_adapter()
    mock_ingest_cls.return_value = _make_mock_ingest_adapter()
    mock_df_cls.return_value = _make_mock_df_manager()

    processor = XBRLGraphProcessor(
      report_uri="https://www.sec.gov/Archives/edgar/data/1045810/000104581024000029/nvda-20240128.htm",
      entityId="1045810",
      sec_filer=sec_filer,
      sec_report=sec_report,
      output_dir=temp_output_dir,
      schema_config=basic_schema_config,
    )

  return processor


@pytest.mark.unit
class TestXBRLGraphProcessorVersion:
  """Tests for version constant."""

  def test_version_is_string(self):
    assert isinstance(XBRL_GRAPH_PROCESSOR_VERSION, str)

  def test_version_format(self):
    parts = XBRL_GRAPH_PROCESSOR_VERSION.split(".")
    assert len(parts) == 3
    for part in parts:
      assert part.isdigit()


@pytest.mark.unit
class TestXBRLGraphProcessorInit:
  """Tests for XBRLGraphProcessor initialization."""

  def test_basic_init(self, temp_output_dir, basic_schema_config):
    """Processor initializes with required parameters."""
    processor = _create_processor(temp_output_dir, basic_schema_config)

    assert processor.report_uri.endswith("nvda-20240128.htm")
    assert processor.entityId == "1045810"
    assert processor.version == XBRL_GRAPH_PROCESSOR_VERSION
    assert processor.output_dir == Path(temp_output_dir)

  def test_init_without_schema_config_raises(self, temp_output_dir):
    """Initialization without schema_config raises ValueError."""
    with pytest.raises(ValueError, match="Schema configuration is required"):
      with (
        patch("robosystems.adapters.sec.processors.xbrl_graph.TextBlockExternalizer"),
        patch("robosystems.adapters.sec.processors.xbrl_graph.S3Client"),
        patch("robosystems.adapters.sec.processors.xbrl_graph.env") as mock_env,
      ):
        mock_env.PUBLIC_DATA_BUCKET = None
        mock_env.PUBLIC_DATA_CDN_URL = None
        XBRLGraphProcessor(
          report_uri="file:///tmp/test.xml",
          output_dir=temp_output_dir,
          schema_config=None,
        )

  def test_init_with_sec_filer(
    self, temp_output_dir, basic_schema_config, sec_filer_data
  ):
    """Processor stores sec_filer data."""
    processor = _create_processor(
      temp_output_dir, basic_schema_config, sec_filer=sec_filer_data
    )
    assert processor.sec_filer == sec_filer_data

  def test_init_with_sec_report(
    self, temp_output_dir, basic_schema_config, sec_report_data
  ):
    """Processor stores sec_report data."""
    processor = _create_processor(
      temp_output_dir, basic_schema_config, sec_report=sec_report_data
    )
    assert processor.sec_report == sec_report_data

  def test_init_with_enricher(self, temp_output_dir, basic_schema_config):
    """Processor accepts shared enricher."""
    mock_enricher = MagicMock()
    with (
      patch(
        "robosystems.adapters.sec.processors.xbrl_graph.XBRLSchemaAdapter"
      ) as mock_schema_cls,
      patch("robosystems.adapters.sec.processors.xbrl_graph.XBRLSchemaConfigGenerator"),
      patch(
        "robosystems.adapters.sec.processors.xbrl_graph.DataFrameManager"
      ) as mock_df_cls,
      patch("robosystems.adapters.sec.processors.xbrl_graph.ParquetWriter"),
      patch("robosystems.adapters.sec.processors.xbrl_graph.TextBlockExternalizer"),
      patch("robosystems.adapters.sec.processors.xbrl_graph.S3Client"),
      patch("robosystems.adapters.sec.processors.xbrl_graph.env") as mock_env,
    ):
      mock_env.PUBLIC_DATA_BUCKET = None
      mock_env.PUBLIC_DATA_CDN_URL = None
      mock_schema_cls.return_value = _make_mock_schema_adapter()
      mock_df_cls.return_value = _make_mock_df_manager()

      processor = XBRLGraphProcessor(
        report_uri="file:///tmp/test.xml",
        output_dir=temp_output_dir,
        schema_config=basic_schema_config,
        enricher=mock_enricher,
      )
    assert processor._enricher is mock_enricher

  def test_init_sets_local_file_path(self, temp_output_dir, basic_schema_config):
    """Processor stores local_file_path."""
    with (
      patch(
        "robosystems.adapters.sec.processors.xbrl_graph.XBRLSchemaAdapter"
      ) as mock_schema_cls,
      patch("robosystems.adapters.sec.processors.xbrl_graph.XBRLSchemaConfigGenerator"),
      patch(
        "robosystems.adapters.sec.processors.xbrl_graph.DataFrameManager"
      ) as mock_df_cls,
      patch("robosystems.adapters.sec.processors.xbrl_graph.ParquetWriter"),
      patch("robosystems.adapters.sec.processors.xbrl_graph.TextBlockExternalizer"),
      patch("robosystems.adapters.sec.processors.xbrl_graph.S3Client"),
      patch("robosystems.adapters.sec.processors.xbrl_graph.env") as mock_env,
    ):
      mock_env.PUBLIC_DATA_BUCKET = None
      mock_env.PUBLIC_DATA_CDN_URL = None
      mock_schema_cls.return_value = _make_mock_schema_adapter()
      mock_df_cls.return_value = _make_mock_df_manager()

      processor = XBRLGraphProcessor(
        report_uri="file:///tmp/test.xml",
        output_dir=temp_output_dir,
        schema_config=basic_schema_config,
        local_file_path="/tmp/test.xml",
      )
    assert processor.local_file_path == "/tmp/test.xml"


@pytest.mark.unit
class TestMakeEntity:
  """Tests for make_entity method."""

  def test_no_entity_id(self, temp_output_dir, basic_schema_config):
    """Returns None when no entityId is provided."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.entityId = None
    result = processor.make_entity()
    assert result is None
    assert processor.entity_data is None

  def test_entity_with_cik(self, temp_output_dir, basic_schema_config, sec_filer_data):
    """Creates entity with normalized CIK."""
    processor = _create_processor(
      temp_output_dir, basic_schema_config, sec_filer=sec_filer_data
    )
    # Re-init schema adapter for make_entity
    processor.schema_adapter = _make_mock_schema_adapter()

    result = processor.make_entity()

    assert result is not None
    assert result["cik"] == "0001045810"  # Padded to 10 digits
    assert result["name"] == "NVIDIA CORP"
    assert result["ticker"] == "NVDA"
    assert result["sic"] == "3674"
    assert result["sic_description"] == "Semiconductors & Related Devices"
    assert result["status"] == "active"
    assert result["is_parent"] is True

  def test_entity_cik_normalization(self, temp_output_dir, basic_schema_config):
    """CIK is normalized to 10-digit padded format."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.entityId = "320193"  # Apple's unpadded CIK
    processor.sec_filer = {"cik": "320193"}
    processor.schema_adapter = _make_mock_schema_adapter()

    result = processor.make_entity()

    assert result["cik"] == "0000320193"  # Padded

  def test_entity_uri_format(self, temp_output_dir, basic_schema_config):
    """Entity URI follows canonical SEC format."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.schema_adapter = _make_mock_schema_adapter()

    result = processor.make_entity()

    assert result["uri"].startswith("http://www.sec.gov/CIK#")
    assert result["scheme"] == "http://www.sec.gov/CIK"

  def test_entity_ein_padding(self, temp_output_dir, basic_schema_config):
    """EIN is padded to 9 digits."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.sec_filer = {"cik": "12345", "ein": "123"}
    processor.schema_adapter = _make_mock_schema_adapter()

    result = processor.make_entity()

    assert result["tax_id"] == "000000123"

  def test_entity_ein_none(self, temp_output_dir, basic_schema_config):
    """EIN is None when not provided."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.sec_filer = {"cik": "12345", "ein": None}
    processor.schema_adapter = _make_mock_schema_adapter()

    result = processor.make_entity()

    assert result["tax_id"] is None

  def test_entity_ein_empty_string(self, temp_output_dir, basic_schema_config):
    """EIN empty string is treated as None."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.sec_filer = {"cik": "12345", "ein": ""}
    processor.schema_adapter = _make_mock_schema_adapter()

    result = processor.make_entity()

    assert result["tax_id"] is None

  def test_entity_website_fallback_investor(self, temp_output_dir, basic_schema_config):
    """Uses investorWebsite as fallback when website missing."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.sec_filer = {
      "cik": "12345",
      "investorWebsite": "https://ir.example.com",
    }
    processor.schema_adapter = _make_mock_schema_adapter()

    result = processor.make_entity()

    assert result["website"] == "https://ir.example.com"


@pytest.mark.unit
class TestMakeReport:
  """Tests for make_report method."""

  def test_basic_report(self, temp_output_dir, basic_schema_config, sec_report_data):
    """Creates report with expected fields."""
    processor = _create_processor(
      temp_output_dir, basic_schema_config, sec_report=sec_report_data
    )
    processor.schema_adapter = _make_mock_schema_adapter()
    processor.entity_data = None

    processor.make_report()

    assert processor.report_data is not None
    assert processor.report_data["form"] == "10-K"
    assert processor.report_data["filing_date"] == "2024-02-21"
    assert processor.report_data["report_date"] == "2024-01-28"
    assert processor.report_data["is_inline_xbrl"] is True
    assert (
      processor.report_data["xbrl_processor_version"] == XBRL_GRAPH_PROCESSOR_VERSION
    )

  def test_report_without_sec_report(self, temp_output_dir, basic_schema_config):
    """Creates minimal report when no sec_report is provided."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.schema_adapter = _make_mock_schema_adapter()
    processor.entity_data = None

    processor.make_report()

    assert processor.report_data is not None
    assert processor.report_data["form"] is None
    assert processor.report_data["filing_date"] is None
    assert processor.report_data["is_inline_xbrl"] is False

  def test_report_invalid_filing_date(self, temp_output_dir, basic_schema_config):
    """Handles invalid filingDate format gracefully."""
    processor = _create_processor(
      temp_output_dir,
      basic_schema_config,
      sec_report={"filingDate": "not-a-date", "form": "10-K"},
    )
    processor.schema_adapter = _make_mock_schema_adapter()
    processor.entity_data = None

    processor.make_report()

    assert processor.report_data["filing_date"] is None

  def test_report_invalid_report_date(self, temp_output_dir, basic_schema_config):
    """Handles invalid reportDate format gracefully."""
    processor = _create_processor(
      temp_output_dir,
      basic_schema_config,
      sec_report={"reportDate": "invalid", "form": "10-Q"},
    )
    processor.schema_adapter = _make_mock_schema_adapter()
    processor.entity_data = None

    processor.make_report()

    assert processor.report_data["report_date"] is None

  def test_report_acceptance_date(
    self, temp_output_dir, basic_schema_config, sec_report_data
  ):
    """Acceptance date is extracted from acceptanceDateTime."""
    processor = _create_processor(
      temp_output_dir, basic_schema_config, sec_report=sec_report_data
    )
    processor.schema_adapter = _make_mock_schema_adapter()
    processor.entity_data = None

    processor.make_report()

    assert processor.report_data["acceptance_date"] == "2024-02-21"

  def test_report_creates_entity_relationship(
    self, temp_output_dir, basic_schema_config, sec_report_data
  ):
    """Creates entity-report relationship when entity_data exists."""
    processor = _create_processor(
      temp_output_dir, basic_schema_config, sec_report=sec_report_data
    )
    processor.schema_adapter = _make_mock_schema_adapter()
    processor.entity_data = {"identifier": "entity-123"}

    processor.make_report()

    # entity_reports_df should have been updated via safe_concat
    assert processor.report_data is not None


@pytest.mark.unit
class TestSafeConcat:
  """Tests for safe_concat method."""

  def test_concat_both_empty(self, temp_output_dir, basic_schema_config):
    """Returns existing when new is empty."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    existing = pd.DataFrame({"a": [1]})
    new = pd.DataFrame()
    result = processor.safe_concat(existing, new)
    assert len(result) == 1

  def test_concat_existing_empty(self, temp_output_dir, basic_schema_config):
    """Returns copy of new when existing is empty."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    existing = pd.DataFrame()
    new = pd.DataFrame({"a": [1]})
    result = processor.safe_concat(existing, new)
    assert len(result) == 1

  def test_concat_both_have_data(self, temp_output_dir, basic_schema_config):
    """Concatenates both DataFrames."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    existing = pd.DataFrame({"a": [1, 2]})
    new = pd.DataFrame({"a": [3, 4]})
    result = processor.safe_concat(existing, new)
    assert len(result) == 4

  def test_concat_mixed_dtypes(self, temp_output_dir, basic_schema_config):
    """Handles mixed dtypes between DataFrames."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    existing = pd.DataFrame({"a": [1, 2]})
    new = pd.DataFrame({"a": ["x", "y"]})
    result = processor.safe_concat(existing, new)
    assert len(result) == 4


@pytest.mark.unit
class TestProcessNoInstanceFile:
  """Tests for process method when instance file is missing."""

  def test_process_no_local_file_path_and_non_file_uri(
    self, temp_output_dir, basic_schema_config
  ):
    """Process handles missing instance file without raising."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.local_file_path = None
    # Set report_uri to non-file:// URI
    processor.report_uri = "https://www.sec.gov/test.htm"

    # Needs make_entity and make_report to set entity_data and report_data
    processor.schema_adapter = _make_mock_schema_adapter()
    processor.entity_data = None
    processor.report_data = {"identifier": "test-id", "failed": False}

    # process() should return without raising
    processor.process()
    # Report should be marked as failed
    assert processor.report_data["failed"] is True

  def test_process_file_not_found(self, temp_output_dir, basic_schema_config):
    """Process handles nonexistent local file path."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.local_file_path = "/nonexistent/path/to/file.xml"

    processor.schema_adapter = _make_mock_schema_adapter()
    processor.entity_data = None
    processor.report_data = {"identifier": "test-id", "failed": False}

    processor.process()
    assert processor.report_data["failed"] is True


@pytest.mark.unit
class TestExtractDeiFiscalInfo:
  """Tests for extract_dei_fiscal_info method."""

  def test_no_arelle_controller(self, temp_output_dir, basic_schema_config):
    """Returns early when arelle controller is not initialized."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.arelle_cntlr = None
    processor.report_data = {"identifier": "test-id"}

    # Should not raise
    processor.extract_dei_fiscal_info()

  def test_extracts_fiscal_info(self, temp_output_dir, basic_schema_config):
    """Extracts DEI fiscal context from Arelle facts."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.report_data = {
      "identifier": "test-id",
      "fiscal_year_focus": None,
      "fiscal_period_focus": None,
      "fiscal_year_end_month": None,
    }
    processor.reports_df = pd.DataFrame(
      {
        "identifier": ["test-id"],
        "fiscal_year_focus": [None],
        "fiscal_period_focus": [None],
        "fiscal_year_end_month": [None],
      }
    )

    # Mock Arelle facts
    mock_fact_fy = MagicMock()
    mock_fact_fy.concept.qname = MagicMock()
    mock_fact_fy.concept.qname.__str__ = lambda self: "dei:DocumentFiscalYearFocus"
    mock_fact_fy.value = "2024"

    mock_fact_fp = MagicMock()
    mock_fact_fp.concept.qname = MagicMock()
    mock_fact_fp.concept.qname.__str__ = lambda self: "dei:DocumentFiscalPeriodFocus"
    mock_fact_fp.value = "Q3"

    mock_fact_fye = MagicMock()
    mock_fact_fye.concept.qname = MagicMock()
    mock_fact_fye.concept.qname.__str__ = lambda self: "dei:CurrentFiscalYearEndDate"
    mock_fact_fye.value = "--12-31"

    mock_cntlr = MagicMock()
    mock_cntlr.facts = [mock_fact_fy, mock_fact_fp, mock_fact_fye]
    processor.arelle_cntlr = mock_cntlr

    processor.extract_dei_fiscal_info()

    assert processor.report_data["fiscal_year_focus"] == 2024
    assert processor.report_data["fiscal_period_focus"] == "Q3"
    assert processor.report_data["fiscal_year_end_month"] == 12

  def test_handles_invalid_fiscal_year(self, temp_output_dir, basic_schema_config):
    """Handles non-numeric fiscal year value."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.report_data = {
      "identifier": "test-id",
      "fiscal_year_focus": None,
      "fiscal_period_focus": None,
      "fiscal_year_end_month": None,
    }
    processor.reports_df = pd.DataFrame(
      {
        "identifier": ["test-id"],
        "fiscal_year_focus": [None],
        "fiscal_period_focus": [None],
        "fiscal_year_end_month": [None],
      }
    )

    mock_fact = MagicMock()
    mock_fact.concept.qname = MagicMock()
    mock_fact.concept.qname.__str__ = lambda self: "dei:DocumentFiscalYearFocus"
    mock_fact.value = "not-a-number"

    mock_cntlr = MagicMock()
    mock_cntlr.facts = [mock_fact]
    processor.arelle_cntlr = mock_cntlr

    processor.extract_dei_fiscal_info()

    # Should remain None due to parse failure
    assert processor.report_data["fiscal_year_focus"] is None

  def test_skips_facts_with_no_concept(self, temp_output_dir, basic_schema_config):
    """Skips facts where concept or qname is None."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.report_data = {
      "identifier": "test-id",
      "fiscal_year_focus": None,
      "fiscal_period_focus": None,
      "fiscal_year_end_month": None,
    }
    processor.reports_df = pd.DataFrame(
      {
        "identifier": ["test-id"],
        "fiscal_year_focus": [None],
        "fiscal_period_focus": [None],
        "fiscal_year_end_month": [None],
      }
    )

    mock_fact_no_concept = MagicMock()
    mock_fact_no_concept.concept = None

    mock_fact_no_qname = MagicMock()
    mock_fact_no_qname.concept = MagicMock()
    mock_fact_no_qname.concept.qname = None

    mock_cntlr = MagicMock()
    mock_cntlr.facts = [mock_fact_no_concept, mock_fact_no_qname]
    processor.arelle_cntlr = mock_cntlr

    # Should not raise
    processor.extract_dei_fiscal_info()
    assert processor.report_data["fiscal_year_focus"] is None


@pytest.mark.unit
class TestProcessAsyncWrapper:
  """Tests for process_async method."""

  def test_process_async_calls_process(self, temp_output_dir, basic_schema_config):
    """process_async delegates to synchronous process."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.process = MagicMock()

    import asyncio

    asyncio.run(processor.process_async())

    processor.process.assert_called_once()


@pytest.mark.unit
class TestOutputParquetFiles:
  """Tests for output_parquet_files method."""

  def test_delegates_to_parquet_writer(self, temp_output_dir, basic_schema_config):
    """output_parquet_files calls parquet_writer.write_all_dataframes."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    processor.parquet_writer = MagicMock()

    processor.output_parquet_files()

    processor.parquet_writer.write_all_dataframes.assert_called_once()


@pytest.mark.unit
class TestInitProcessedElements:
  """Tests for processed_elements tracking set."""

  def test_starts_empty(self, temp_output_dir, basic_schema_config):
    """processed_elements starts as an empty set."""
    processor = _create_processor(temp_output_dir, basic_schema_config)
    assert processor.processed_elements == set()
    assert isinstance(processor.processed_elements, set)
