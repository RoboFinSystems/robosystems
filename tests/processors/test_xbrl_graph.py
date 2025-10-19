"""Comprehensive tests for XBRL Graph Engine."""

import pytest
import re
import tempfile
import shutil
import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
import pandas as pd

from robosystems.processors.xbrl_graph import (
  XBRLGraphProcessor,
  XBRL_GRAPH_PROCESSOR_VERSION,
)
from robosystems.processors.xbrl import camel_to_snake, make_plural


def setup_mock_schema_processor(mock_schema_processor, node_names=None, rel_names=None):
  """Helper function to set up schema processor mocks consistently."""
  if node_names is None:
    node_names = ["Entity", "Report"]
  if rel_names is None:
    rel_names = ["ENTITY_HAS_REPORT"]

  mock_schema_instance = MagicMock()

  # Mock the schema builder and schema
  mock_schema_builder = MagicMock()
  mock_schema = MagicMock()

  # Create mock nodes
  mock_nodes = []
  for name in node_names:
    mock_node = MagicMock()
    mock_node.name = name
    mock_nodes.append(mock_node)

  # Create mock relationships
  mock_rels = []
  for name in rel_names:
    mock_rel = MagicMock()
    mock_rel.name = name
    mock_rels.append(mock_rel)

  mock_schema.nodes = mock_nodes
  mock_schema.relationships = mock_rels
  mock_schema_builder.schema = mock_schema
  mock_schema_instance.schema_builder = mock_schema_builder

  # Mock dataframe creation
  mock_schema_instance.create_schema_compatible_dataframe.return_value = pd.DataFrame()
  mock_schema_instance.populate_dataframe.return_value = pd.DataFrame()

  mock_schema_processor.return_value = mock_schema_instance
  return mock_schema_instance


class TestXBRLGraphProcessorInitialization:
  """Test XBRLGraphProcessor initialization."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.report_uri = "file:///tmp/test_report.xml"
    self.mock_schema_config = {
      "nodes": {
        "Entity": {"properties": [{"name": "identifier", "type": "STRING"}]},
        "Report": {"properties": [{"name": "identifier", "type": "STRING"}]},
        "Fact": {"properties": [{"name": "identifier", "type": "STRING"}]},
      },
      "relationships": {
        "ENTITY_HAS_REPORT": {"from": "Entity", "to": "Report"},
        "REPORT_HAS_FACT": {"from": "Report", "to": "Fact"},
      },
    }

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_initialization_with_schema_config(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test initialization with schema configuration."""
    # Mock the adapters
    mock_schema_instance = MagicMock()
    mock_ingest_instance = MagicMock()
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = mock_ingest_instance

    # Mock the schema adapter's DataFrame creation
    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )

    processor = XBRLGraphProcessor(
      report_uri=self.report_uri,
      entityId="test_kg1a2b3c",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    assert processor.report_uri == self.report_uri
    assert processor.entityId == "test_kg1a2b3c"
    assert processor.output_dir == Path(self.temp_dir)
    assert processor.version == XBRL_GRAPH_PROCESSOR_VERSION
    assert processor.schema_adapter == mock_schema_instance
    assert processor.ingest_adapter == mock_ingest_instance

  def test_initialization_without_schema_config(self):
    """Test initialization without schema configuration should raise error."""
    with pytest.raises(ValueError, match="Schema configuration is required"):
      XBRLGraphProcessor(
        report_uri=self.report_uri,
        entityId="test_kg1a2b3c",
        output_dir=self.temp_dir,
        schema_config=None,
      )

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_initialization_with_sec_data(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test initialization with SEC filer and report data."""
    mock_schema_instance = MagicMock()
    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = MagicMock()

    sec_filer = {
      "cik": "320193",
      "name": "Apple Inc.",
      "ticker": "AAPL",
      "sic": "3571",
      "sicDescription": "Electronic Computers",
    }

    sec_report = {
      "accessionNumber": "0000320193-23-000077",
      "form": "10-K",
      "filingDate": "2023-11-03",
      "isInlineXBRL": True,
    }

    processor = XBRLGraphProcessor(
      report_uri=self.report_uri,
      entityId="test_kg1a2b3c",
      sec_filer=sec_filer,
      sec_report=sec_report,
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    assert processor.sec_filer == sec_filer
    assert processor.sec_report == sec_report

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_initialization_with_feature_flags(
    self, mock_schema_ingestion_processor, mock_schema_processor, monkeypatch
  ):
    """Test initialization with feature flags enabled."""
    # Patch the environment variables
    monkeypatch.setattr("robosystems.config.env.XBRL_STANDARDIZED_FILENAMES", True)
    monkeypatch.setattr("robosystems.config.env.XBRL_TYPE_PREFIXES", True)
    monkeypatch.setattr("robosystems.config.env.XBRL_COLUMN_STANDARDIZATION", True)
    mock_schema_instance = MagicMock()
    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri=self.report_uri,
      entityId="test_kg1a2b3c",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    assert processor.enable_standardized_filenames is True
    assert processor.enable_type_prefixes is True
    assert processor.enable_column_standardization is True


class TestSafeConcatenation:
  """Test safe DataFrame concatenation functionality."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.mock_schema_config = {
      "nodes": {"Entity": {"properties": [{"name": "identifier", "type": "STRING"}]}},
      "relationships": {},
    }

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_safe_concat_empty_dataframes(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test safe concatenation with empty DataFrames."""
    mock_schema_instance = MagicMock()
    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    # Test empty + empty = empty
    existing_df = pd.DataFrame()
    new_df = pd.DataFrame()
    result = processor.safe_concat(existing_df, new_df)
    assert result.empty

    # Test empty + data = data
    data_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    result = processor.safe_concat(existing_df, data_df)
    assert len(result) == 2
    assert list(result.columns) == ["col1", "col2"]

    # Test data + empty = data
    result = processor.safe_concat(data_df, existing_df)
    assert len(result) == 2
    assert list(result.columns) == ["col1", "col2"]

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_safe_concat_dtype_compatibility(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test safe concatenation with different dtypes."""
    mock_schema_instance = MagicMock()
    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    # Create DataFrames with different dtypes for same column
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    df2 = pd.DataFrame({"col1": ["3", "4"], "col2": ["c", "d"]})  # col1 as string

    result = processor.safe_concat(df1, df2)

    assert len(result) == 4
    # Should handle dtype conversion gracefully
    assert "col1" in result.columns
    assert "col2" in result.columns


class TestEntityCreation:
  """Test entity data creation."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.mock_schema_config = {
      "nodes": {
        "Entity": {
          "properties": [
            {"name": "identifier", "type": "STRING"},
            {"name": "name", "type": "STRING"},
            {"name": "cik", "type": "STRING"},
          ]
        }
      },
      "relationships": {},
    }

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_make_entity_without_sec_data(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test entity creation without SEC filer data."""
    mock_schema_instance = MagicMock()

    # Mock the schema builder and schema
    mock_schema_builder = MagicMock()
    mock_schema = MagicMock()
    mock_node = MagicMock()
    mock_node.name = "Entity"
    mock_rel = MagicMock()
    mock_rel.name = "ENTITY_HAS_REPORT"

    mock_schema.nodes = [mock_node]
    mock_schema.relationships = [mock_rel]
    mock_schema_builder.schema = mock_schema
    mock_schema_instance.schema_builder = mock_schema_builder

    # Mock dataframe creation - this sets up the initial empty companies_df
    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )
    # Mock the process_dataframe_for_schema method used in make_entity
    mock_schema_instance.process_dataframe_for_schema.return_value = pd.DataFrame(
      [
        {
          "identifier": "test_kg1a2b3c",
          "name": None,
          "cik": None,
        }
      ]
    )
    mock_schema_instance.populate_dataframe.return_value = pd.DataFrame(
      [
        {
          "identifier": "test_kg1a2b3c",
          "name": None,
          "cik": None,
        }
      ]
    )
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test_kg1a2b3c",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    entity_data = processor.make_entity()
    assert entity_data is not None

    # The identifier should be a deterministic UUID7 generated from the entity URI
    # We check that it's a valid UUID format rather than exact match due to caching
    uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$"

    assert re.match(uuid_pattern, entity_data["identifier"], re.IGNORECASE)
    assert entity_data["name"] is None
    assert entity_data["cik"] == processor.entityId.zfill(10)
    assert len(processor.entities_df) == 1

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_make_entity_with_sec_data(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test entity creation with SEC filer data."""
    mock_schema_instance = MagicMock()

    # Mock the schema builder and schema
    mock_schema_builder = MagicMock()
    mock_schema = MagicMock()
    mock_node = MagicMock()
    mock_node.name = "Entity"
    mock_rel = MagicMock()
    mock_rel.name = "ENTITY_HAS_REPORT"

    mock_schema.nodes = [mock_node]
    mock_schema.relationships = [mock_rel]
    mock_schema_builder.schema = mock_schema
    mock_schema_instance.schema_builder = mock_schema_builder

    # Mock dataframe creation
    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )
    mock_schema_instance.populate_dataframe.return_value = pd.DataFrame(
      [
        {
          "identifier": "test_kg1a2b3c",
          "name": "Apple Inc.",
          "cik": "320193",
          "ticker": "AAPL",
        }
      ]
    )
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = MagicMock()

    sec_filer = {
      "cik": "320193",
      "name": "Apple Inc.",
      "ticker": "AAPL",
      "sic": "3571",
      "sicDescription": "Electronic Computers",
      "stateOfIncorporation": "CA",
      "fiscalYearEnd": "0930",
    }

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test_kg1a2b3c",
      sec_filer=sec_filer,
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    entity_data = processor.make_entity()
    assert entity_data is not None

    # The identifier should be a deterministic UUID7 generated from the entity URI
    # We check that it's a valid UUID format rather than exact match due to caching
    uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[0-9a-f]{4}-[0-9a-f]{12}$"

    assert re.match(uuid_pattern, entity_data["identifier"], re.IGNORECASE)
    assert entity_data["name"] == "Apple Inc."
    assert entity_data["cik"] == "320193"
    assert entity_data["ticker"] == "AAPL"
    assert entity_data["sic"] == "3571"
    assert entity_data["sic_description"] == "Electronic Computers"
    assert entity_data["industry"] == "Electronic Computers"
    assert entity_data["state_of_incorporation"] == "CA"
    assert entity_data["fiscal_year_end"] == "0930"

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_make_entity_without_entity_id(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test entity creation without entity ID."""
    mock_schema_instance = MagicMock()
    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId=None,
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    result = processor.make_entity()

    assert result is None
    assert processor.entity_data is None


class TestReportCreation:
  """Test report data creation."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.mock_schema_config = {
      "nodes": {
        "Report": {
          "properties": [
            {"name": "identifier", "type": "STRING"},
            {"name": "uri", "type": "STRING"},
            {"name": "filing_date", "type": "STRING"},
          ]
        }
      },
      "relationships": {},
    }

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_make_report_basic(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test basic report creation."""
    mock_schema_instance = MagicMock()

    # Mock the schema builder and schema
    mock_schema_builder = MagicMock()
    mock_schema = MagicMock()
    mock_node = MagicMock()
    mock_node.name = "Report"
    mock_rel = MagicMock()
    mock_rel.name = "ENTITY_HAS_REPORT"

    mock_schema.nodes = [mock_node]
    mock_schema.relationships = [mock_rel]
    mock_schema_builder.schema = mock_schema
    mock_schema_instance.schema_builder = mock_schema_builder

    # Mock dataframe creation
    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )
    # Mock the process_dataframe_for_schema method used in make_report
    mock_schema_instance.process_dataframe_for_schema.return_value = pd.DataFrame(
      [
        {
          "identifier": "test_id",
          "uri": "file:///test.xml",
        }
      ]
    )
    mock_schema_instance.populate_dataframe.return_value = pd.DataFrame(
      [
        {
          "identifier": "test_id",
          "uri": "file:///test.xml",
        }
      ]
    )
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test_entity",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    # Mock entity data for relationship creation
    processor.entity_data = {"identifier": "test_entity"}

    processor.make_report()

    assert processor.report_data is not None
    assert processor.report_data["uri"] == "file:///test.xml"
    assert (
      processor.report_data["xbrl_processor_version"] == XBRL_GRAPH_PROCESSOR_VERSION
    )
    assert processor.report_data["processed"] is False
    assert processor.report_data["failed"] is False
    assert len(processor.reports_df) == 1

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_make_report_with_sec_data(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test report creation with SEC report data."""
    mock_schema_instance = setup_mock_schema_processor(
      mock_schema_processor, ["Report"], ["ENTITY_HAS_REPORT"]
    )
    mock_schema_instance.populate_dataframe.return_value = pd.DataFrame(
      [
        {
          "identifier": "test_id",
          "uri": "file:///test.xml",
          "form": "10-K",
          "filing_date": "2023-11-03",
        }
      ]
    )
    mock_schema_ingestion_processor.return_value = MagicMock()

    sec_report = {
      "accessionNumber": "0000320193-23-000077",
      "form": "10-K",
      "filingDate": "2023-11-03",
      "reportDate": "2023-09-30",
      "acceptanceDateTime": "2023-11-03T16:30:42.000Z",
      "periodOfReport": "2023-09-30",
      "isInlineXBRL": True,
    }

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test_entity",
      sec_report=sec_report,
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    processor.make_report()
    assert processor.report_data is not None

    assert processor.report_data["accession_number"] == "0000320193-23-000077"
    assert processor.report_data["form"] == "10-K"
    assert processor.report_data["filing_date"] == "2023-11-03"
    assert processor.report_data["report_date"] == "2023-09-30"
    assert processor.report_data["acceptance_date"] == "2023-11-03"
    assert processor.report_data["period_end_date"] == "2023-09-30"
    assert processor.report_data["is_inline_xbrl"] is True

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_make_report_with_invalid_dates(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test report creation with invalid date formats."""
    mock_schema_instance = setup_mock_schema_processor(
      mock_schema_processor, ["Report"], ["ENTITY_HAS_REPORT"]
    )
    mock_schema_instance.populate_dataframe.return_value = pd.DataFrame(
      [
        {
          "identifier": "test_id",
          "filing_date": None,
        }
      ]
    )
    mock_schema_ingestion_processor.return_value = MagicMock()

    sec_report = {
      "filingDate": "invalid-date-format",
      "reportDate": "2023-13-45",  # Invalid date
      "acceptanceDateTime": "not-a-date",
    }

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test_entity",
      sec_report=sec_report,
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    processor.make_report()
    assert processor.report_data is not None

    # Should handle invalid dates gracefully
    assert processor.report_data["filing_date"] is None
    assert processor.report_data["report_date"] is None
    assert processor.report_data["acceptance_date"] is None


class TestS3Externalization:
  """Test S3 value externalization functionality."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.mock_schema_config = {
      "nodes": {
        "Fact": {
          "properties": [
            {"name": "identifier", "type": "STRING"},
            {"name": "value", "type": "STRING"},
            {"name": "value_type", "type": "STRING"},
          ]
        }
      },
      "relationships": {},
    }

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.processors.xbrl_graph.S3Client")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_should_externalize_value(
    self, mock_schema_ingestion_processor, mock_schema_processor, mock_s3_client
  ):
    """Test determining if values should be externalized to S3."""
    setup_mock_schema_processor(mock_schema_processor)
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    # Test HTML content detection
    html_value = "<div>Some HTML content</div>"
    assert processor.textblock_externalizer.should_externalize(html_value) is True

    # Test large value detection
    large_value = "x" * 10000  # Assuming threshold is less than 10000
    assert processor.textblock_externalizer.should_externalize(large_value) is True

    # Test small non-HTML value
    small_value = "Small plain text"
    assert processor.textblock_externalizer.should_externalize(small_value) is False

    # Test None value
    assert processor.textblock_externalizer.should_externalize(None) is False

  @patch("robosystems.processors.xbrl_graph.S3Client")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_generate_s3_key_with_metadata(
    self, mock_schema_ingestion_processor, mock_schema_processor, mock_s3_client
  ):
    """Test S3 key generation with report metadata."""
    setup_mock_schema_processor(mock_schema_processor)
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    entity_data = {"cik": "0000320193"}
    report_data = {
      "filing_date": "2023-11-03",
      "accession_number": "0000320193-23-000077",
    }

    s3_key = processor.textblock_externalizer._generate_s3_key(
      "fact123456", entity_data, report_data, "html"
    )
    assert s3_key == "2023/0000320193/0000320193-23-000077/fact_fact1234.html"

  @patch("robosystems.processors.xbrl_graph.S3Client")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_generate_s3_key_without_metadata(
    self, mock_schema_ingestion_processor, mock_schema_processor, mock_s3_client
  ):
    """Test S3 key generation without report metadata (fallback)."""
    setup_mock_schema_processor(mock_schema_processor)
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    # No entity_data or report_data
    current_year = datetime.datetime.now().strftime("%Y")
    s3_key = processor.textblock_externalizer._generate_s3_key(
      "fact123456", None, None, "txt"
    )
    assert s3_key == f"{current_year}/unknown/unknown/fact_fact1234.txt"


class TestParquetOutput:
  """Test parquet file output functionality."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.mock_schema_config = {
      "nodes": {
        "Entity": {"properties": [{"name": "identifier", "type": "STRING"}]},
        "Report": {"properties": [{"name": "identifier", "type": "STRING"}]},
        "Fact": {"properties": [{"name": "identifier", "type": "STRING"}]},
      },
      "relationships": {
        "ENTITY_HAS_REPORT": {"from": "Entity", "to": "Report"},
        "REPORT_HAS_FACT": {"from": "Report", "to": "Fact"},
      },
    }

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_output_parquet_files_creates_directories(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test that output_parquet_files creates nodes/ and relationships/ directories."""
    setup_mock_schema_processor(
      mock_schema_processor, ["Entity", "Report"], ["ENTITY_HAS_REPORT"]
    )
    mock_ingest_instance = MagicMock()
    mock_ingest_instance.get_all_relationship_tables.return_value = [
      "ENTITY_HAS_REPORT"
    ]
    mock_schema_ingestion_processor.return_value = mock_ingest_instance

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    # Add some test data
    processor.entities_df = pd.DataFrame([{"identifier": "entity1"}])
    processor.reports_df = pd.DataFrame([{"identifier": "report1"}])
    processor.entity_reports_df = pd.DataFrame([{"from": "entity1", "to": "report1"}])

    processor.output_parquet_files()

    # Check that directories were created
    nodes_dir = Path(self.temp_dir) / "nodes"
    relationships_dir = Path(self.temp_dir) / "relationships"
    assert nodes_dir.exists()
    assert relationships_dir.exists()

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_save_df_to_parquet_with_deduplication(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test that duplicate nodes are properly deduplicated."""
    setup_mock_schema_processor(mock_schema_processor, ["Entity"], [])
    mock_ingest_instance = MagicMock()
    mock_ingest_instance.get_all_relationship_tables.return_value = []
    mock_schema_ingestion_processor.return_value = mock_ingest_instance

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    # Create DataFrame with duplicates
    df_with_duplicates = pd.DataFrame(
      [
        {"identifier": "id1", "name": "Entity 1"},
        {"identifier": "id2", "name": "Entity 2"},
        {"identifier": "id1", "name": "Entity 1 Duplicate"},
      ]
    )

    # Create the nodes directory
    nodes_dir = Path(self.temp_dir) / "nodes"
    nodes_dir.mkdir(exist_ok=True)

    # Save using the ParquetWriter's write_dataframe_schema_driven method
    processor.parquet_writer.write_dataframe_schema_driven(
      df_with_duplicates, "Entity.parquet", "Entity"
    )

    # Load saved file and check deduplication
    saved_file = Path(self.temp_dir) / "nodes" / "Entity.parquet"
    if saved_file.exists():
      loaded_df = pd.read_parquet(saved_file)
      assert len(loaded_df) == 2  # Should only have 2 unique identifiers
      assert set(loaded_df["identifier"]) == {"id1", "id2"}


class TestDataFrameHelpers:
  """Test DataFrame helper methods."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.mock_schema_config = {
      "nodes": {"Entity": {"properties": [{"name": "identifier", "type": "STRING"}]}},
      "relationships": {},
    }

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_convert_schema_name_to_dataframe_attr(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test conversion of schema names to DataFrame attribute names."""
    setup_mock_schema_processor(mock_schema_processor)
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    # Test node conversions
    assert (
      processor.df_manager._convert_schema_name_to_dataframe_attr("Entity", True)
      == "entities_df"
    )
    assert (
      processor.df_manager._convert_schema_name_to_dataframe_attr("FactSet", True)
      == "fact_sets_df"
    )
    assert (
      processor.df_manager._convert_schema_name_to_dataframe_attr("FactDimension", True)
      == "fact_dimensions_df"
    )

    # Test relationship conversions
    assert (
      processor.df_manager._convert_schema_name_to_dataframe_attr(
        "ENTITY_HAS_REPORT", False
      )
      == "entity_reports_df"
    )
    assert (
      processor.df_manager._convert_schema_name_to_dataframe_attr(
        "FACT_HAS_ELEMENT", False
      )
      == "fact_elements_df"
    )
    # Special case for FACT_HAS_DIMENSION
    assert (
      processor.df_manager._convert_schema_name_to_dataframe_attr(
        "FACT_HAS_DIMENSION", False
      )
      == "fact_has_dimension_rel_df"
    )

  def test_camel_to_snake_conversion(self):
    """Test PascalCase to snake_case conversion."""
    assert camel_to_snake("Entity") == "entity"
    assert camel_to_snake("FactSet") == "fact_set"
    assert camel_to_snake("FactDimension") == "fact_dimension"
    assert camel_to_snake("TaxonomyLabel") == "taxonomy_label"

  def test_make_plural(self):
    """Test pluralization of words."""
    assert make_plural("entity") == "entities"
    assert make_plural("fact") == "facts"
    assert make_plural("taxonomy") == "taxonomies"
    assert make_plural("class") == "classes"
    assert make_plural("box") == "boxes"


class TestProcessMethod:
  """Test the main process method and error handling."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.mock_schema_config = {
      "nodes": {
        "Entity": {"properties": [{"name": "identifier", "type": "STRING"}]},
        "Report": {"properties": [{"name": "identifier", "type": "STRING"}]},
      },
      "relationships": {},
    }

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.processors.xbrl_graph.ArelleClient")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_process_with_missing_local_file(
    self, mock_schema_ingestion_processor, mock_schema_processor, mock_arelle_client
  ):
    """Test process method when local file is missing."""
    setup_mock_schema_processor(mock_schema_processor)
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="http://example.com/test.xml",  # Not a file:// URL
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
      local_file_path=None,  # No local file provided
    )

    processor.make_entity = MagicMock()
    processor.make_report = MagicMock()
    processor.report_data = {"identifier": "test_report"}
    processor.reports_df = pd.DataFrame(
      [{"identifier": "test_report", "failed": False}]
    )

    # Should handle missing file gracefully
    processor.process()

    # Check that report was marked as failed
    assert processor.report_data["failed"] is True

  @patch("robosystems.processors.xbrl_graph.ArelleClient")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  @patch("os.path.exists")
  def test_process_with_nonexistent_file(
    self,
    mock_exists,
    mock_schema_ingestion_processor,
    mock_schema_processor,
    mock_arelle_client,
  ):
    """Test process method when file doesn't exist."""
    setup_mock_schema_processor(mock_schema_processor)
    mock_schema_ingestion_processor.return_value = MagicMock()
    mock_exists.return_value = False  # File doesn't exist

    processor = XBRLGraphProcessor(
      report_uri="file:///nonexistent.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    processor.make_entity = MagicMock()
    processor.make_report = MagicMock()
    processor.report_data = {"identifier": "test_report"}
    processor.reports_df = pd.DataFrame(
      [{"identifier": "test_report", "failed": False}]
    )

    # Should handle nonexistent file gracefully
    processor.process()

    # Check that report was marked as failed
    assert processor.report_data["failed"] is True

  @patch("robosystems.processors.xbrl_graph.ArelleClient")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  @patch("os.path.exists")
  def test_process_async(
    self,
    mock_exists,
    mock_schema_ingestion_processor,
    mock_schema_processor,
    mock_arelle_client,
  ):
    """Test async process method."""
    import asyncio

    setup_mock_schema_processor(mock_schema_processor)
    mock_schema_ingestion_processor.return_value = MagicMock()
    mock_exists.return_value = True

    # Create a mock Arelle controller
    mock_controller = MagicMock()
    mock_arelle_client.return_value.controller.return_value = mock_controller

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
      local_file_path="/test.xml",
    )

    # Mock all the processing methods
    processor.make_entity = MagicMock()
    processor.make_report = MagicMock()
    processor.make_dts = MagicMock()
    processor.make_facts = MagicMock()
    processor.output_parquet_files = MagicMock()
    processor.report_data = {"identifier": "test_report"}

    # Run async process
    asyncio.run(processor.process_async())

    # Verify methods were called
    processor.make_entity.assert_called_once()
    processor.make_report.assert_called_once()
    processor.make_dts.assert_called_once()
    processor.make_facts.assert_called_once()
    processor.output_parquet_files.assert_called_once()


class TestSchemaCompleteness:
  """Test schema completeness functionality."""

  def setup_method(self):
    """Set up test fixtures."""
    self.temp_dir = tempfile.mkdtemp()
    self.mock_schema_config = {
      "nodes": {
        "Entity": {
          "properties": [
            {"name": "identifier", "type": "STRING"},
            {"name": "name", "type": "STRING"},
            {"name": "cik", "type": "STRING"},
            {"name": "created_at", "type": "TIMESTAMP"},
          ]
        }
      },
      "relationships": {
        "ENTITY_HAS_REPORT": {
          "from": "Entity",
          "to": "Report",
          "properties": [{"name": "relationship_type", "type": "STRING"}],
        }
      },
    }

  def teardown_method(self):
    """Clean up test fixtures."""
    shutil.rmtree(self.temp_dir, ignore_errors=True)

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_ensure_schema_completeness_adds_missing_columns(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test that missing schema columns are added with appropriate defaults."""
    mock_schema_instance = MagicMock()

    # Create proper mock properties with correct attributes
    mock_props = []
    for prop_name, prop_type in [
      ("identifier", "STRING"),
      ("name", "STRING"),
      ("cik", "STRING"),
      ("created_at", "TIMESTAMP"),
    ]:
      mock_prop = MagicMock()
      mock_prop.name = prop_name
      mock_prop.type = prop_type
      mock_props.append(mock_prop)

    # Mock get_schema_info to return node schema
    mock_schema_instance.get_schema_info.return_value = {
      "type": "node",
      "schema": {"properties": mock_props},
    }

    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    # Create DataFrame missing some columns
    df = pd.DataFrame([{"identifier": "id1"}])

    # Apply schema completeness
    complete_df = processor.df_manager.ensure_schema_completeness(df, "Entity")

    # Check that missing columns were added
    assert "name" in complete_df.columns
    assert "cik" in complete_df.columns
    assert "created_at" in complete_df.columns

    # Check default values
    assert complete_df["name"].iloc[0] == ""  # String default
    assert complete_df["cik"].iloc[0] == ""  # String default
    assert pd.isna(complete_df["created_at"].iloc[0])  # Timestamp can be None

  @patch("robosystems.processors.xbrl_graph.XBRLSchemaAdapter")
  @patch("robosystems.processors.xbrl_graph.XBRLSchemaConfigGenerator")
  def test_ensure_schema_completeness_for_relationships(
    self, mock_schema_ingestion_processor, mock_schema_processor
  ):
    """Test schema completeness for relationship tables."""
    mock_schema_instance = MagicMock()

    # Create proper mock property
    mock_prop = MagicMock()
    mock_prop.name = "relationship_type"
    mock_prop.type = "STRING"

    # Mock get_schema_info to return relationship schema
    mock_schema_instance.get_schema_info.return_value = {
      "type": "relationship",
      "schema": {"properties": [mock_prop]},
    }

    mock_schema_instance.create_schema_compatible_dataframe.return_value = (
      pd.DataFrame()
    )
    mock_schema_processor.return_value = mock_schema_instance
    mock_schema_ingestion_processor.return_value = MagicMock()

    processor = XBRLGraphProcessor(
      report_uri="file:///test.xml",
      entityId="test",
      output_dir=self.temp_dir,
      schema_config=self.mock_schema_config,
    )

    # Create DataFrame for relationship
    df = pd.DataFrame([{"from": "entity1", "to": "report1"}])

    # Apply schema completeness
    complete_df = processor.df_manager.ensure_schema_completeness(
      df, "ENTITY_HAS_REPORT"
    )

    # Check that foreign keys and properties are present
    assert "from" in complete_df.columns
    assert "to" in complete_df.columns
    assert "relationship_type" in complete_df.columns

    # Check column order (foreign keys should be first)
    assert list(complete_df.columns)[:2] == ["from", "to"]
