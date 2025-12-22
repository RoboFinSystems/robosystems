"""
Tests for XBRL Schema Config Generator

Tests the schema-driven ingestion configuration generation and file pattern matching
for XBRL processing.
"""

from unittest.mock import patch

import pytest

from robosystems.adapters.sec.processors.schema import (
  XBRLSchemaConfigGenerator,
  create_custom_ingestion_processor,
  create_roboledger_ingestion_processor,
)
from robosystems.schemas.models import Node, Property, Relationship, Schema


@pytest.fixture
def mock_schema_manager():
  """Create a mock schema manager with test schema data."""
  with patch("robosystems.adapters.sec.processors.schema.SchemaManager") as MockManager:
    manager_instance = MockManager.return_value

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
          ],
        ),
        Node(
          name="Report",
          properties=[
            Property(name="report_id", type="STRING", is_primary_key=True),
            Property(name="form_type", type="STRING", is_primary_key=False),
            Property(name="filing_date", type="DATE", is_primary_key=False),
          ],
        ),
        Node(
          name="Fact",
          properties=[
            Property(name="fact_id", type="STRING", is_primary_key=True),
            Property(name="value", type="DOUBLE", is_primary_key=False),
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
          ],
        ),
        Relationship(
          name="REPORT_HAS_FACT",
          from_node="Report",
          to_node="Fact",
          properties=[
            Property(name="fact_order", type="INT"),
            Property(name="is_primary", type="BOOLEAN"),
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

    manager_instance.load_and_compile_schema.return_value = compiled_schema

    yield MockManager


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
def processor_with_mock_schema(mock_schema_manager, basic_schema_config):
  """Create a processor with mocked schema manager."""
  return XBRLSchemaConfigGenerator(basic_schema_config)


class TestXBRLSchemaConfigGenerator:
  """Test suite for XBRLSchemaConfigGenerator."""

  def test_initialization(self, processor_with_mock_schema, basic_schema_config):
    """Test processor initialization."""
    processor = processor_with_mock_schema

    assert processor.schema_config == basic_schema_config
    assert processor.config.name == "Test Schema"
    assert processor.config.base_schema == "base"
    assert processor.config.extensions == ["test_ext"]
    assert processor.ingest_config is not None
    assert processor.compiled_schema is not None

  def test_create_schema_configuration(self, processor_with_mock_schema):
    """Test schema configuration creation."""
    processor = processor_with_mock_schema

    config = processor._create_schema_configuration(
      {
        "name": "Custom Schema",
        "description": "Custom description",
        "version": "2.0.0",
        "base_schema": "custom_base",
        "extensions": ["ext1", "ext2"],
      }
    )

    assert config.name == "Custom Schema"
    assert config.description == "Custom description"
    assert config.version == "2.0.0"
    assert config.base_schema == "custom_base"
    assert config.extensions == ["ext1", "ext2"]

  def test_create_schema_configuration_defaults(self, processor_with_mock_schema):
    """Test schema configuration creation with defaults."""
    processor = processor_with_mock_schema

    config = processor._create_schema_configuration({})

    assert config.name == "Dynamic Ingestion Schema"
    assert config.description == "Schema-driven ingestion configuration"
    assert config.version == "1.0.0"
    assert config.base_schema == "base"
    assert config.extensions == []

  def test_generate_ingest_config(self, processor_with_mock_schema):
    """Test ingestion configuration generation."""
    processor = processor_with_mock_schema
    ingest_config = processor.ingest_config

    # Check node tables
    assert "Entity" in ingest_config.node_tables
    assert "Report" in ingest_config.node_tables
    assert "Fact" in ingest_config.node_tables

    # Check relationship tables
    assert "ENTITY_HAS_REPORT" in ingest_config.relationship_tables
    assert "REPORT_HAS_FACT" in ingest_config.relationship_tables
    assert "FACT_HAS_DIMENSION" in ingest_config.relationship_tables

    # Check file pattern mapping
    assert len(ingest_config.file_pattern_mapping) > 0

    # Check table name mapping
    assert "entity" in ingest_config.table_name_mapping
    assert ingest_config.table_name_mapping["entity"] == "Entity"

  def test_create_node_table_info(self, processor_with_mock_schema):
    """Test node table info creation."""
    processor = processor_with_mock_schema

    entity_info = processor.ingest_config.node_tables["Entity"]

    assert entity_info.name == "Entity"
    assert entity_info.is_relationship is False
    assert entity_info.primary_keys == ["cik"]
    assert entity_info.columns == ["cik", "ticker", "name"]
    assert entity_info.from_node is None
    assert entity_info.to_node is None
    assert len(entity_info.file_patterns) > 0

  def test_create_relationship_table_info(self, processor_with_mock_schema):
    """Test relationship table info creation."""
    processor = processor_with_mock_schema

    rel_info = processor.ingest_config.relationship_tables["ENTITY_HAS_REPORT"]

    assert rel_info.name == "ENTITY_HAS_REPORT"
    assert rel_info.is_relationship is True
    assert rel_info.primary_keys == []
    assert rel_info.columns == ["from", "to", "relationship_date"]
    assert rel_info.from_node == "Entity"
    assert rel_info.to_node == "Report"
    assert rel_info.properties == ["relationship_date"]
    assert len(rel_info.file_patterns) > 0

  def test_generate_file_patterns_for_node(self, processor_with_mock_schema):
    """Test file pattern generation for nodes."""
    processor = processor_with_mock_schema

    patterns = processor._generate_file_patterns("Entity", is_relationship=False)

    assert "entity" in patterns
    assert "node_entity" in patterns
    assert "entity_" in patterns
    assert "entity" in patterns  # Without underscores

  def test_generate_file_patterns_for_relationship(self, processor_with_mock_schema):
    """Test file pattern generation for relationships."""
    processor = processor_with_mock_schema

    patterns = processor._generate_file_patterns(
      "ENTITY_HAS_REPORT", is_relationship=True
    )

    assert "entity_has_report" in patterns
    assert "rel_entity_has_report" in patterns
    assert "entity_has_report_" in patterns

  def test_pascal_to_snake_case(self, processor_with_mock_schema):
    """Test PascalCase to snake_case conversion."""
    processor = processor_with_mock_schema

    assert processor._pascal_to_snake("Entity") == "entity"
    assert processor._pascal_to_snake("ReportFact") == "report_fact"
    assert (
      processor._pascal_to_snake("FactDimensionElement") == "fact_dimension_element"
    )
    assert processor._pascal_to_snake("XMLData") == "xml_data"
    assert processor._pascal_to_snake("HTMLParser") == "html_parser"
    assert processor._pascal_to_snake("simple") == "simple"

  def test_is_relationship_file(self, processor_with_mock_schema):
    """Test relationship file detection."""
    processor = processor_with_mock_schema

    # Test with relationship files
    assert processor.is_relationship_file("/tmp/xyz/ENTITY_HAS_REPORT/file.parquet")
    assert processor.is_relationship_file(
      "s3://bucket/relationships/REPORT_HAS_FACT/data.parquet"
    )

    # Test with node files
    assert not processor.is_relationship_file("/tmp/xyz/Entity/file.parquet")
    assert not processor.is_relationship_file("s3://bucket/nodes/Report/data.parquet")

    # Test with unknown files
    assert not processor.is_relationship_file("unknown_file.parquet")

  def test_get_table_name_from_file_with_path(self, processor_with_mock_schema):
    """Test table name extraction from file path."""
    processor = processor_with_mock_schema

    # Test node paths
    assert (
      processor.get_table_name_from_file("/tmp/xyz/Entity/file.parquet") == "Entity"
    )
    assert (
      processor.get_table_name_from_file("s3://bucket/nodes/Report/data.parquet")
      == "Report"
    )
    assert processor.get_table_name_from_file("/data/Fact/20241231.parquet") == "Fact"

    # Test relationship paths
    assert (
      processor.get_table_name_from_file("/tmp/ENTITY_HAS_REPORT/file.parquet")
      == "ENTITY_HAS_REPORT"
    )
    assert (
      processor.get_table_name_from_file("s3://bucket/REPORT_HAS_FACT/data.parquet")
      == "REPORT_HAS_FACT"
    )

    # Test Windows-style paths
    assert (
      processor.get_table_name_from_file("C:\\data\\Entity\\file.parquet") == "Entity"
    )

    # Test unknown paths
    assert processor.get_table_name_from_file("unknown/path/file.parquet") is None
    assert processor.get_table_name_from_file("file.parquet") is None

  def test_get_table_info(self, processor_with_mock_schema):
    """Test table info retrieval."""
    processor = processor_with_mock_schema

    # Test node table
    entity_info = processor.get_table_info("Entity")
    assert entity_info is not None
    assert entity_info.name == "Entity"
    assert entity_info.is_relationship is False

    # Test relationship table
    rel_info = processor.get_table_info("ENTITY_HAS_REPORT")
    assert rel_info is not None
    assert rel_info.name == "ENTITY_HAS_REPORT"
    assert rel_info.is_relationship is True

    # Test unknown table
    unknown_info = processor.get_table_info("UnknownTable")
    assert unknown_info is None

  def test_get_relationship_info(self, processor_with_mock_schema):
    """Test relationship info extraction."""
    processor = processor_with_mock_schema

    # Test valid relationship
    rel_info = processor.get_relationship_info("ENTITY_HAS_REPORT")
    assert rel_info is not None
    assert rel_info == ("ENTITY_HAS_REPORT", "Entity", "Report")

    # Test node table (should return None)
    node_info = processor.get_relationship_info("Entity")
    assert node_info is None

    # Test unknown table
    unknown_info = processor.get_relationship_info("UnknownTable")
    assert unknown_info is None

  def test_get_relationship_info_with_missing_nodes(
    self, mock_schema_manager, basic_schema_config
  ):
    """Test relationship info when from_node or to_node is missing."""
    # Modify the mock to have a relationship with None nodes
    compiled_schema = (
      mock_schema_manager.return_value.load_and_compile_schema.return_value
    )
    compiled_schema.relationships.append(
      Relationship(
        name="BROKEN_RELATIONSHIP",
        from_node=None,
        to_node=None,
        properties=[],
      )
    )

    processor = XBRLSchemaConfigGenerator(basic_schema_config)

    rel_info = processor.get_relationship_info("BROKEN_RELATIONSHIP")
    assert rel_info is None

  def test_get_all_node_tables(self, processor_with_mock_schema):
    """Test retrieving all node table names."""
    processor = processor_with_mock_schema

    node_tables = processor.get_all_node_tables()
    assert "Entity" in node_tables
    assert "Report" in node_tables
    assert "Fact" in node_tables
    assert len(node_tables) == 3

  def test_get_all_relationship_tables(self, processor_with_mock_schema):
    """Test retrieving all relationship table names."""
    processor = processor_with_mock_schema

    rel_tables = processor.get_all_relationship_tables()
    assert "ENTITY_HAS_REPORT" in rel_tables
    assert "REPORT_HAS_FACT" in rel_tables
    assert "FACT_HAS_DIMENSION" in rel_tables
    assert len(rel_tables) == 3

  def test_get_schema_statistics(self, processor_with_mock_schema):
    """Test schema statistics generation."""
    processor = processor_with_mock_schema

    stats = processor.get_schema_statistics()

    assert stats["schema_name"] == "Test Schema"
    assert stats["base_schema"] == "base"
    assert stats["extensions"] == ["test_ext"]
    assert stats["total_nodes"] == 3
    assert stats["total_relationships"] == 3
    assert stats["total_file_patterns"] > 0
    assert len(stats["node_tables"]) == 3
    assert len(stats["relationship_tables"]) == 3

  @patch("robosystems.adapters.sec.processors.schema.logger")
  def test_print_configuration_summary(self, mock_logger, processor_with_mock_schema):
    """Test configuration summary printing."""
    processor = processor_with_mock_schema

    processor.print_configuration_summary()

    # Verify logger was called with expected messages
    assert mock_logger.debug.called
    calls = mock_logger.debug.call_args_list

    # Check for key messages in the debug output
    debug_messages = [str(call[0][0]) for call in calls]
    assert any("Schema-Driven Ingestion Configuration" in msg for msg in debug_messages)
    assert any("Test Schema" in msg for msg in debug_messages)
    assert any("Node Tables:" in msg for msg in debug_messages)
    assert any("Relationship Tables:" in msg for msg in debug_messages)


class TestFactoryFunctions:
  """Test factory functions for creating processors."""

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_create_roboledger_ingestion_processor(self, mock_manager):
    """Test RoboLedger processor creation."""
    compiled_schema = Schema(
      name="RoboLedger",
      nodes=[],
      relationships=[],
    )
    mock_manager.return_value.load_and_compile_schema.return_value = compiled_schema

    processor = create_roboledger_ingestion_processor()

    assert processor is not None
    assert processor.config.name == "RoboLedger Ingestion Schema"
    assert processor.config.base_schema == "base"
    assert processor.config.extensions == ["roboledger"]

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_create_custom_ingestion_processor(self, mock_manager):
    """Test custom processor creation."""
    compiled_schema = Schema(
      name="Custom",
      nodes=[],
      relationships=[],
    )
    mock_manager.return_value.load_and_compile_schema.return_value = compiled_schema

    processor = create_custom_ingestion_processor(["ext1", "ext2"])

    assert processor is not None
    assert processor.config.name == "Custom Ingestion Schema (ext1, ext2)"
    assert processor.config.base_schema == "base"
    assert processor.config.extensions == ["ext1", "ext2"]


class TestEdgeCases:
  """Test edge cases and error handling."""

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_empty_schema(self, mock_manager, basic_schema_config):
    """Test processor with empty schema."""
    compiled_schema = Schema(
      name="EmptySchema",
      nodes=[],
      relationships=[],
    )
    mock_manager.return_value.load_and_compile_schema.return_value = compiled_schema

    processor = XBRLSchemaConfigGenerator(basic_schema_config)

    assert len(processor.ingest_config.node_tables) == 0
    assert len(processor.ingest_config.relationship_tables) == 0
    assert len(processor.ingest_config.file_pattern_mapping) == 0

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_relationship_with_many_properties(self, mock_manager, basic_schema_config):
    """Test relationship with many properties."""
    compiled_schema = Schema(
      name="TestSchema",
      nodes=[
        Node(name="NodeA", properties=[]),
        Node(name="NodeB", properties=[]),
      ],
      relationships=[
        Relationship(
          name="COMPLEX_RELATIONSHIP",
          from_node="NodeA",
          to_node="NodeB",
          properties=[Property(name=f"prop_{i}", type="STRING") for i in range(10)],
        ),
      ],
    )
    mock_manager.return_value.load_and_compile_schema.return_value = compiled_schema

    processor = XBRLSchemaConfigGenerator(basic_schema_config)

    rel_info = processor.ingest_config.relationship_tables["COMPLEX_RELATIONSHIP"]
    assert len(rel_info.properties) == 10
    assert len(rel_info.columns) == 12  # from, to, + 10 properties

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_special_characters_in_names(self, mock_manager, basic_schema_config):
    """Test handling of special characters in table names."""
    compiled_schema = Schema(
      name="TestSchema",
      nodes=[
        Node(
          name="Node123ABC",
          properties=[
            Property(name="id", type="STRING", is_primary_key=True),
          ],
        ),
      ],
      relationships=[],
    )
    mock_manager.return_value.load_and_compile_schema.return_value = compiled_schema

    processor = XBRLSchemaConfigGenerator(basic_schema_config)

    patterns = processor._generate_file_patterns("Node123ABC", is_relationship=False)
    assert "node123_abc" in patterns
    assert "node_node123_abc" in patterns
