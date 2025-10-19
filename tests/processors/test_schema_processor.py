"""
Tests for XBRL Schema Adapter

Tests the DataFrame-to-Schema transformation adapter that handles schema validation,
column mapping, and DataFrame structure compatibility for XBRL processing.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch

from robosystems.processors.xbrl.schema_adapter import XBRLSchemaAdapter
from robosystems.schemas.models import Node, Relationship, Property, Schema


@pytest.fixture
def mock_schema_builder():
  """Create a mock schema builder with test schema data."""
  with patch(
    "robosystems.processors.xbrl.schema_adapter.KuzuSchemaBuilder"
  ) as MockBuilder:
    builder_instance = MockBuilder.return_value

    # Create mock compiled schema with nodes and relationships
    compiled_schema = Schema(
      name="TestSchema",
      nodes=[
        Node(
          name="Entity",
          properties=[
            Property(name="cik", type="STRING", is_primary_key=True),
            Property(name="ticker", type="STRING", is_primary_key=False),
            Property(name="name", type="STRING", is_primary_key=False),
            Property(name="sic", type="INT", is_primary_key=False),
          ],
        ),
        Node(
          name="Report",
          properties=[
            Property(name="report_id", type="STRING", is_primary_key=True),
            Property(name="form_type", type="STRING", is_primary_key=False),
            Property(name="filing_date", type="DATE", is_primary_key=False),
            Property(name="period_end", type="DATE", is_primary_key=False),
          ],
        ),
        Node(
          name="Fact",
          properties=[
            Property(name="fact_id", type="STRING", is_primary_key=True),
            Property(name="value", type="DOUBLE", is_primary_key=False),
            Property(name="is_numeric", type="BOOLEAN", is_primary_key=False),
          ],
        ),
      ],
      relationships=[
        Relationship(
          name="ENTITY_HAS_REPORT",
          from_node="Entity",
          to_node="Report",
          properties=[
            Property(name="relationship_date", type="DATE"),
            Property(name="is_amendment", type="BOOLEAN"),
          ],
        ),
        Relationship(
          name="REPORT_HAS_FACT",
          from_node="Report",
          to_node="Fact",
          properties=[
            Property(name="fact_order", type="INT"),
            Property(name="confidence_score", type="DOUBLE"),
          ],
        ),
        Relationship(
          name="FACT_HAS_DIMENSION",
          from_node="Fact",
          to_node="Dimension",
          properties=[],
        ),
      ],
    )

    builder_instance.schema = compiled_schema
    builder_instance.load_schemas = Mock()

    yield MockBuilder


@pytest.fixture
def basic_schema_config():
  """Basic schema configuration for testing."""
  return {
    "name": "Test Schema",
    "description": "Test schema configuration",
    "version": "1.0.0",
    "base_schema": "base",
    "extensions": ["test_ext"],
  }


@pytest.fixture
def processor_with_mock_schema(mock_schema_builder, basic_schema_config):
  """Create a processor with mocked schema builder."""
  return XBRLSchemaAdapter(basic_schema_config)


class TestXBRLSchemaAdapter:
  """Test suite for XBRLSchemaAdapter."""

  def test_initialization(self, processor_with_mock_schema, basic_schema_config):
    """Test processor initialization."""
    processor = processor_with_mock_schema

    assert processor.schema_config == basic_schema_config
    assert processor.schema_builder is not None
    assert processor.compiled_schema is not None
    assert len(processor.node_schemas) > 0
    assert len(processor.relationship_schemas) > 0

  def test_extract_schema_definitions(self, processor_with_mock_schema):
    """Test schema definition extraction."""
    processor = processor_with_mock_schema

    # Check node schemas
    assert "Entity" in processor.node_schemas
    assert processor.node_schemas["Entity"]["primary_keys"] == ["cik"]
    assert processor.node_schemas["Entity"]["table_type"] == "node"
    assert len(processor.node_schemas["Entity"]["properties"]) == 4

    assert "Report" in processor.node_schemas
    assert processor.node_schemas["Report"]["primary_keys"] == ["report_id"]

    # Check relationship schemas
    assert "ENTITY_HAS_REPORT" in processor.relationship_schemas
    assert processor.relationship_schemas["ENTITY_HAS_REPORT"]["from_node"] == "Entity"
    assert processor.relationship_schemas["ENTITY_HAS_REPORT"]["to_node"] == "Report"
    assert (
      processor.relationship_schemas["ENTITY_HAS_REPORT"]["table_type"]
      == "relationship"
    )
    assert len(processor.relationship_schemas["ENTITY_HAS_REPORT"]["properties"]) == 2

  def test_create_schema_compatible_dataframe_for_node(
    self, processor_with_mock_schema
  ):
    """Test creating schema-compatible DataFrame for nodes."""
    processor = processor_with_mock_schema

    df = processor.create_schema_compatible_dataframe("Entity")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["cik", "ticker", "name", "sic"]
    assert len(df) == 0  # Should be empty

  def test_create_schema_compatible_dataframe_for_relationship(
    self, processor_with_mock_schema
  ):
    """Test creating schema-compatible DataFrame for relationships."""
    processor = processor_with_mock_schema

    df = processor.create_schema_compatible_dataframe("ENTITY_HAS_REPORT")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["from", "to", "relationship_date", "is_amendment"]
    assert len(df) == 0  # Should be empty

  def test_create_schema_compatible_dataframe_with_xbrl_mapping(
    self, processor_with_mock_schema
  ):
    """Test creating DataFrame with XBRL table name mapping."""
    processor = processor_with_mock_schema

    # Test with XBRL table name that maps to schema name
    df = processor.create_schema_compatible_dataframe("EntityReports")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["from", "to", "relationship_date", "is_amendment"]

  def test_create_schema_compatible_dataframe_unknown_table(
    self, processor_with_mock_schema
  ):
    """Test creating DataFrame for unknown table."""
    processor = processor_with_mock_schema

    df = processor.create_schema_compatible_dataframe("UnknownTable")

    assert isinstance(df, pd.DataFrame)
    assert len(df.columns) == 0  # Should be empty DataFrame

  def test_process_dataframe_for_schema_with_complete_data(
    self, processor_with_mock_schema
  ):
    """Test processing complete data dictionary."""
    processor = processor_with_mock_schema

    data_dict = {
      "cik": "0001234567",
      "ticker": "TSLA",
      "name": "Tesla Inc",
      "sic": 3711,
    }

    df = processor.process_dataframe_for_schema("Entity", data_dict)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["cik"] == "0001234567"
    assert df.iloc[0]["ticker"] == "TSLA"
    assert df.iloc[0]["name"] == "Tesla Inc"
    assert df.iloc[0]["sic"] == 3711

  def test_process_dataframe_for_schema_with_missing_columns(
    self, processor_with_mock_schema
  ):
    """Test processing data with missing columns."""
    processor = processor_with_mock_schema

    data_dict = {
      "cik": "0001234567",
      "name": "Tesla Inc",
      # Missing: ticker, sic
    }

    df = processor.process_dataframe_for_schema("Entity", data_dict)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["cik"] == "0001234567"
    assert df.iloc[0]["name"] == "Tesla Inc"
    assert df.iloc[0]["ticker"] == ""  # Default for STRING
    assert df.iloc[0]["sic"] == 0  # Default for INT

  def test_process_dataframe_for_relationship(self, processor_with_mock_schema):
    """Test processing relationship data."""
    processor = processor_with_mock_schema

    data_dict = {
      "from": "entity_123",
      "to": "report_456",
      "relationship_date": "2024-01-01",
      "is_amendment": True,
    }

    df = processor.process_dataframe_for_schema("ENTITY_HAS_REPORT", data_dict)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["from"] == "entity_123"
    assert df.iloc[0]["to"] == "report_456"
    assert df.iloc[0]["relationship_date"] == "2024-01-01"
    assert df.iloc[0]["is_amendment"] == True  # noqa: E712 - numpy boolean comparison

  def test_process_dataframe_for_unknown_schema(self, processor_with_mock_schema):
    """Test processing data for unknown schema."""
    processor = processor_with_mock_schema

    data_dict = {"unknown_field": "value"}

    df = processor.process_dataframe_for_schema("UnknownTable", data_dict)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["unknown_field"] == "value"

  def test_validate_dataframe_schema_valid(self, processor_with_mock_schema):
    """Test validating a valid DataFrame."""
    processor = processor_with_mock_schema

    df = pd.DataFrame(
      {
        "cik": ["123"],
        "ticker": ["TSLA"],
        "name": ["Tesla"],
        "sic": [3711],
      }
    )

    result = processor.validate_dataframe_schema("Entity", df)

    assert result["valid"] is True
    assert "message" in result

  def test_validate_dataframe_schema_column_count_mismatch(
    self, processor_with_mock_schema
  ):
    """Test validating DataFrame with wrong column count."""
    processor = processor_with_mock_schema

    df = pd.DataFrame(
      {
        "cik": ["123"],
        "ticker": ["TSLA"],
        # Missing: name, sic
      }
    )

    result = processor.validate_dataframe_schema("Entity", df)

    assert result["valid"] is False
    assert "Column count mismatch" in result["error"]
    assert result["expected_columns"] == ["cik", "ticker", "name", "sic"]
    assert result["actual_columns"] == ["cik", "ticker"]

  def test_validate_dataframe_schema_column_name_mismatch(
    self, processor_with_mock_schema
  ):
    """Test validating DataFrame with wrong column names."""
    processor = processor_with_mock_schema

    df = pd.DataFrame(
      {
        "cik": ["123"],
        "symbol": ["TSLA"],  # Wrong name
        "name": ["Tesla"],
        "industry": [3711],  # Wrong name
      }
    )

    result = processor.validate_dataframe_schema("Entity", df)

    assert result["valid"] is False
    assert "Column names do not match" in result["error"]
    assert "ticker" in result["missing_columns"]
    assert "sic" in result["missing_columns"]
    assert "symbol" in result["extra_columns"]
    assert "industry" in result["extra_columns"]

  def test_validate_dataframe_schema_unknown_table(self, processor_with_mock_schema):
    """Test validating DataFrame for unknown table."""
    processor = processor_with_mock_schema

    df = pd.DataFrame({"col1": [1], "col2": [2]})

    result = processor.validate_dataframe_schema("UnknownTable", df)

    assert result["valid"] is False
    assert "Schema not found" in result["error"]

  def test_get_schema_info_for_node(self, processor_with_mock_schema):
    """Test getting schema info for a node."""
    processor = processor_with_mock_schema

    info = processor.get_schema_info("Entity")

    assert info["type"] == "node"
    assert info["column_count"] == 4
    assert info["original_name"] == "Entity"
    assert info["schema_name"] == "Entity"
    assert info["schema"]["primary_keys"] == ["cik"]

  def test_get_schema_info_for_relationship(self, processor_with_mock_schema):
    """Test getting schema info for a relationship."""
    processor = processor_with_mock_schema

    info = processor.get_schema_info("ENTITY_HAS_REPORT")

    assert info["type"] == "relationship"
    assert info["column_count"] == 2
    assert info["original_name"] == "ENTITY_HAS_REPORT"
    assert info["schema_name"] == "ENTITY_HAS_REPORT"
    assert info["schema"]["from_node"] == "Entity"
    assert info["schema"]["to_node"] == "Report"

  def test_get_schema_info_with_xbrl_mapping(self, processor_with_mock_schema):
    """Test getting schema info using XBRL table name."""
    processor = processor_with_mock_schema

    info = processor.get_schema_info("EntityReports")

    assert info["type"] == "relationship"
    assert info["original_name"] == "EntityReports"
    assert info["schema_name"] == "ENTITY_HAS_REPORT"

  def test_get_schema_info_unknown_table(self, processor_with_mock_schema):
    """Test getting schema info for unknown table."""
    processor = processor_with_mock_schema

    info = processor.get_schema_info("UnknownTable")

    assert info["type"] == "unknown"
    assert info["schema"] is None
    assert info["column_count"] == 0

  def test_get_available_schemas(self, processor_with_mock_schema):
    """Test getting list of available schemas."""
    processor = processor_with_mock_schema

    schemas = processor.get_available_schemas()

    assert "Entity" in schemas
    assert "Report" in schemas
    assert "Fact" in schemas
    assert "ENTITY_HAS_REPORT" in schemas
    assert "REPORT_HAS_FACT" in schemas
    assert "FACT_HAS_DIMENSION" in schemas

  @patch("robosystems.processors.xbrl.schema_adapter.logger")
  def test_print_schema_summary(self, mock_logger, processor_with_mock_schema):
    """Test printing schema summary."""
    processor = processor_with_mock_schema

    processor.print_schema_summary()

    # Verify logger was called
    assert mock_logger.debug.called
    calls = mock_logger.debug.call_args_list

    # Check for key messages
    debug_messages = [str(call[0][0]) for call in calls]
    assert any("XBRL SCHEMA ADAPTER SUMMARY" in msg for msg in debug_messages)
    assert any("Node Schemas:" in msg for msg in debug_messages)
    assert any("Relationship Schemas:" in msg for msg in debug_messages)
    assert any("XBRL Table Mappings:" in msg for msg in debug_messages)

  def test_resolve_schema_name(self, processor_with_mock_schema):
    """Test schema name resolution."""
    processor = processor_with_mock_schema

    # Test direct name (no mapping needed)
    assert processor._resolve_schema_name("Entity") == "Entity"
    assert processor._resolve_schema_name("ENTITY_HAS_REPORT") == "ENTITY_HAS_REPORT"

    # Test XBRL mapping
    assert processor._resolve_schema_name("EntityReports") == "ENTITY_HAS_REPORT"
    assert processor._resolve_schema_name("ReportFacts") == "REPORT_HAS_FACT"
    assert processor._resolve_schema_name("FactDimensions") == "FACT_HAS_DIMENSION"

    # Test unknown name
    assert processor._resolve_schema_name("UnknownTable") == "UnknownTable"

  def test_build_column_list_for_node(self, processor_with_mock_schema):
    """Test building column list for nodes."""
    processor = processor_with_mock_schema

    schema_info = processor.node_schemas["Entity"]
    columns = processor._build_column_list(schema_info)

    assert columns == ["cik", "ticker", "name", "sic"]

  def test_build_column_list_for_relationship(self, processor_with_mock_schema):
    """Test building column list for relationships."""
    processor = processor_with_mock_schema

    schema_info = processor.relationship_schemas["ENTITY_HAS_REPORT"]
    columns = processor._build_column_list(schema_info)

    assert columns == ["from", "to", "relationship_date", "is_amendment"]
    assert columns[0] == "from"  # Foreign keys come first
    assert columns[1] == "to"

  def test_get_default_value_for_type(self, processor_with_mock_schema):
    """Test default value generation for different data types."""
    processor = processor_with_mock_schema

    # String types
    assert processor._get_default_value_for_type("STRING") == ""
    assert processor._get_default_value_for_type("VARCHAR") == ""
    assert processor._get_default_value_for_type("TEXT") == ""
    assert processor._get_default_value_for_type("string") == ""  # Case insensitive

    # Integer types
    assert processor._get_default_value_for_type("INT") == 0
    assert processor._get_default_value_for_type("INT64") == 0
    assert processor._get_default_value_for_type("INTEGER") == 0
    assert processor._get_default_value_for_type("INT32") == 0

    # Float types
    assert processor._get_default_value_for_type("DOUBLE") == 0.0
    assert processor._get_default_value_for_type("FLOAT") == 0.0
    assert processor._get_default_value_for_type("DECIMAL") == 0.0

    # Boolean types
    assert processor._get_default_value_for_type("BOOLEAN") is False
    assert processor._get_default_value_for_type("BOOL") is False

    # Date types
    assert processor._get_default_value_for_type("DATE") is None
    assert processor._get_default_value_for_type("TIMESTAMP") is None

    # Unknown type
    assert processor._get_default_value_for_type("UNKNOWN_TYPE") is None

  def test_process_data_with_schema_for_node(self, processor_with_mock_schema):
    """Test processing data with schema for nodes."""
    processor = processor_with_mock_schema

    schema_info = processor.node_schemas["Entity"]
    data_dict = {
      "cik": "123",
      "name": "Test Corp",
      # Missing: ticker, sic
    }

    processed = processor._process_data_with_schema(data_dict, schema_info)

    assert processed["cik"] == "123"
    assert processed["name"] == "Test Corp"
    assert processed["ticker"] == ""  # Default for STRING
    assert processed["sic"] == 0  # Default for INT

  def test_process_data_with_schema_for_relationship(self, processor_with_mock_schema):
    """Test processing data with schema for relationships."""
    processor = processor_with_mock_schema

    schema_info = processor.relationship_schemas["ENTITY_HAS_REPORT"]
    data_dict = {
      "from": "entity_1",
      "to": "report_1",
      "is_amendment": True,
      # Missing: relationship_date
    }

    processed = processor._process_data_with_schema(data_dict, schema_info)

    assert processed["from"] == "entity_1"
    assert processed["to"] == "report_1"
    assert processed["is_amendment"] == True  # noqa: E712 - numpy boolean
    assert processed["relationship_date"] is None  # Default for DATE


class TestXBRLTableMapping:
  """Test XBRL table name mappings."""

  def test_all_xbrl_mappings_present(self, processor_with_mock_schema):
    """Test that all expected XBRL mappings are present."""
    processor = processor_with_mock_schema

    expected_mappings = {
      "EntityReports": "ENTITY_HAS_REPORT",
      "ReportFacts": "REPORT_HAS_FACT",
      "ReportFactSets": "REPORT_HAS_FACT_SET",
      "ReportTaxonomies": "REPORT_USES_TAXONOMY",
      "FactUnits": "FACT_HAS_UNIT",
      "FactDimensions": "FACT_HAS_DIMENSION",
      "FactEntities": "FACT_HAS_ENTITY",
      "FactElements": "FACT_HAS_ELEMENT",
      "FactPeriods": "FACT_HAS_PERIOD",
      "FactSetFacts": "FACT_SET_CONTAINS_FACT",
      "ElementLabels": "ELEMENT_HAS_LABEL",
      "ElementReferences": "ELEMENT_HAS_REFERENCE",
      "StructureTaxonomies": "STRUCTURE_HAS_TAXONOMY",
      "TaxonomyLabels": "TAXONOMY_HAS_LABEL",
      "TaxonomyReferences": "TAXONOMY_HAS_REFERENCE",
      "StructureAssociations": "STRUCTURE_HAS_ASSOCIATION",
      "AssociationFromElements": "ASSOCIATION_HAS_FROM_ELEMENT",
      "AssociationToElements": "ASSOCIATION_HAS_TO_ELEMENT",
      "FactDimensionElements": "FACT_DIMENSION_REFERENCES_ELEMENT",
    }

    for xbrl_name, schema_name in expected_mappings.items():
      assert processor.XBRL_TABLE_MAPPING.get(xbrl_name) == schema_name


class TestEdgeCases:
  """Test edge cases and error handling."""

  @patch("robosystems.processors.xbrl.schema_adapter.KuzuSchemaBuilder")
  def test_empty_schema(self, mock_builder, basic_schema_config):
    """Test processor with empty schema."""
    builder_instance = mock_builder.return_value
    compiled_schema = Schema(
      name="EmptySchema",
      nodes=[],
      relationships=[],
    )
    builder_instance.schema = compiled_schema

    processor = XBRLSchemaAdapter(basic_schema_config)

    assert len(processor.node_schemas) == 0
    assert len(processor.relationship_schemas) == 0
    assert processor.get_available_schemas() == []

  @patch("robosystems.processors.xbrl.schema_adapter.KuzuSchemaBuilder")
  def test_node_with_no_properties(self, mock_builder, basic_schema_config):
    """Test node with no properties."""
    builder_instance = mock_builder.return_value
    compiled_schema = Schema(
      name="TestSchema",
      nodes=[
        Node(name="EmptyNode", properties=[]),
      ],
      relationships=[],
    )
    builder_instance.schema = compiled_schema

    processor = XBRLSchemaAdapter(basic_schema_config)

    df = processor.create_schema_compatible_dataframe("EmptyNode")
    assert list(df.columns) == []

  @patch("robosystems.processors.xbrl.schema_adapter.KuzuSchemaBuilder")
  def test_relationship_with_no_properties(self, mock_builder, basic_schema_config):
    """Test relationship with no properties."""
    builder_instance = mock_builder.return_value
    compiled_schema = Schema(
      name="TestSchema",
      nodes=[
        Node(name="NodeA", properties=[]),
        Node(name="NodeB", properties=[]),
      ],
      relationships=[
        Relationship(
          name="SIMPLE_REL",
          from_node="NodeA",
          to_node="NodeB",
          properties=[],
        ),
      ],
    )
    builder_instance.schema = compiled_schema

    processor = XBRLSchemaAdapter(basic_schema_config)

    df = processor.create_schema_compatible_dataframe("SIMPLE_REL")
    assert list(df.columns) == ["from", "to"]  # Only foreign keys

  @patch("robosystems.processors.xbrl.schema_adapter.KuzuSchemaBuilder")
  def test_multiple_primary_keys(self, mock_builder, basic_schema_config):
    """Test node with multiple primary keys."""
    builder_instance = mock_builder.return_value
    compiled_schema = Schema(
      name="TestSchema",
      nodes=[
        Node(
          name="CompositeKeyNode",
          properties=[
            Property(name="key1", type="STRING", is_primary_key=True),
            Property(name="key2", type="INT", is_primary_key=True),
            Property(name="data", type="STRING", is_primary_key=False),
          ],
        ),
      ],
      relationships=[],
    )
    builder_instance.schema = compiled_schema

    processor = XBRLSchemaAdapter(basic_schema_config)

    assert processor.node_schemas["CompositeKeyNode"]["primary_keys"] == [
      "key1",
      "key2",
    ]

  def test_dataframe_with_extra_columns_ignored(self, processor_with_mock_schema):
    """Test that extra columns in data dict are ignored."""
    processor = processor_with_mock_schema

    data_dict = {
      "cik": "123",
      "ticker": "TSLA",
      "name": "Tesla",
      "sic": 3711,
      "extra_column_1": "ignored",
      "extra_column_2": "also ignored",
    }

    df = processor.process_dataframe_for_schema("Entity", data_dict)

    assert len(df.columns) == 4  # Only schema columns
    assert "extra_column_1" not in df.columns
    assert "extra_column_2" not in df.columns
