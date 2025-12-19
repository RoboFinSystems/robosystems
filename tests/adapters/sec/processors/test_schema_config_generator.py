from unittest.mock import MagicMock, patch

import pytest

from robosystems.adapters.sec.processors.schema import (
  XBRLSchemaConfigGenerator,
  create_custom_ingestion_processor,
  create_roboledger_ingestion_processor,
)


@pytest.fixture
def mock_schema_manager():
  manager = MagicMock()

  node_entity = MagicMock()
  node_entity.name = "Entity"
  prop1 = MagicMock()
  prop1.name = "identifier"
  prop1.is_primary_key = True
  prop2 = MagicMock()
  prop2.name = "name"
  prop2.is_primary_key = False
  node_entity.properties = [prop1, prop2]

  node_report = MagicMock()
  node_report.name = "Report"
  prop3 = MagicMock()
  prop3.name = "identifier"
  prop3.is_primary_key = True
  prop4 = MagicMock()
  prop4.name = "filing_date"
  prop4.is_primary_key = False
  node_report.properties = [prop3, prop4]

  rel_entity_report = MagicMock()
  rel_entity_report.name = "ENTITY_HAS_REPORT"
  rel_entity_report.from_node = "Entity"
  rel_entity_report.to_node = "Report"
  prop5 = MagicMock()
  prop5.name = "relationship_id"
  rel_entity_report.properties = [prop5]

  compiled_schema = MagicMock()
  compiled_schema.nodes = [node_entity, node_report]
  compiled_schema.relationships = [rel_entity_report]

  manager.load_and_compile_schema.return_value = compiled_schema
  return manager


@pytest.fixture
def schema_config():
  return {
    "name": "Test Schema",
    "description": "Test Description",
    "version": "1.0.0",
    "base_schema": "base",
    "extensions": ["test_ext"],
  }


class TestXBRLSchemaConfigGeneratorInitialization:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_initialization(self, mock_manager_class, mock_schema_manager, schema_config):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator(schema_config)

    assert generator.schema_config == schema_config
    assert generator.schema_manager == mock_schema_manager
    assert (
      generator.compiled_schema
      == mock_schema_manager.load_and_compile_schema.return_value
    )
    assert generator.ingest_config is not None

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_schema_configuration_creation(
    self, mock_manager_class, mock_schema_manager, schema_config
  ):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator(schema_config)

    assert generator.config.name == "Test Schema"
    assert generator.config.description == "Test Description"
    assert generator.config.base_schema == "base"
    assert generator.config.extensions == ["test_ext"]

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_default_configuration(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    assert generator.config.name == "Dynamic Ingestion Schema"
    assert generator.config.base_schema == "base"
    assert generator.config.extensions == []


class TestPascalToSnake:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_simple_pascal_case(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    assert generator._pascal_to_snake("Entity") == "entity"
    assert generator._pascal_to_snake("EntityReport") == "entity_report"

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_multiple_capitals(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    assert generator._pascal_to_snake("HTTPSConnection") == "https_connection"
    assert generator._pascal_to_snake("XMLParser") == "xml_parser"

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_with_numbers(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    assert generator._pascal_to_snake("Element2023") == "element2023"


class TestGenerateFilePatterns:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_generate_node_patterns(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    patterns = generator._generate_file_patterns("Entity", is_relationship=False)

    assert "entity" in patterns
    assert "node_entity" in patterns
    assert "entity_" in patterns

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_generate_relationship_patterns(
    self, mock_manager_class, mock_schema_manager
  ):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    patterns = generator._generate_file_patterns(
      "ENTITY_HAS_REPORT", is_relationship=True
    )

    assert "entity_has_report" in patterns
    assert "rel_entity_has_report" in patterns
    assert "entity_has_report_" in patterns


class TestCreateNodeTableInfo:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_create_node_table_info(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    node = MagicMock()
    node.name = "TestNode"
    prop1 = MagicMock()
    prop1.name = "id"
    prop1.is_primary_key = True
    prop2 = MagicMock()
    prop2.name = "value"
    prop2.is_primary_key = False
    node.properties = [prop1, prop2]

    table_info = generator._create_node_table_info(node)

    assert table_info.name == "TestNode"
    assert table_info.is_relationship is False
    assert table_info.primary_keys == ["id"]
    assert table_info.columns == ["id", "value"]
    assert len(table_info.file_patterns) > 0


class TestCreateRelationshipTableInfo:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_create_relationship_table_info(
    self, mock_manager_class, mock_schema_manager
  ):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    relationship = MagicMock()
    relationship.name = "TEST_HAS_RELATION"
    relationship.from_node = "TestNode"
    relationship.to_node = "OtherNode"
    prop1 = MagicMock()
    prop1.name = "weight"
    relationship.properties = [prop1]

    table_info = generator._create_relationship_table_info(relationship)

    assert table_info.name == "TEST_HAS_RELATION"
    assert table_info.is_relationship is True
    assert table_info.from_node == "TestNode"
    assert table_info.to_node == "OtherNode"
    assert table_info.columns == ["from", "to", "weight"]
    assert table_info.properties == ["weight"]

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_create_relationship_no_properties(
    self, mock_manager_class, mock_schema_manager
  ):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    relationship = MagicMock()
    relationship.name = "SIMPLE_REL"
    relationship.from_node = "Node1"
    relationship.to_node = "Node2"
    relationship.properties = None

    table_info = generator._create_relationship_table_info(relationship)

    assert table_info.columns == ["from", "to"]
    assert table_info.properties == []


class TestIngestConfigGeneration:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_generate_ingest_config(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    assert "Entity" in generator.ingest_config.node_tables
    assert "Report" in generator.ingest_config.node_tables
    assert "ENTITY_HAS_REPORT" in generator.ingest_config.relationship_tables
    assert len(generator.ingest_config.file_pattern_mapping) > 0

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_table_name_mapping(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    assert "entity" in generator.ingest_config.table_name_mapping
    assert "report" in generator.ingest_config.table_name_mapping
    assert generator.ingest_config.table_name_mapping["entity"] == "Entity"


class TestIsRelationshipFile:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_is_relationship_file_true(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.is_relationship_file(
      "relationships/ENTITY_HAS_REPORT/file.parquet"
    )

    assert result is True

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_is_relationship_file_false(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.is_relationship_file("nodes/Entity/file.parquet")

    assert result is False

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_is_relationship_file_unknown(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.is_relationship_file("unknown/path/file.parquet")

    assert result is False


class TestGetTableNameFromFile:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_table_name_from_path_node(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_table_name_from_file("nodes/Entity/file.parquet")

    assert result == "Entity"

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_table_name_from_path_relationship(
    self, mock_manager_class, mock_schema_manager
  ):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_table_name_from_file(
      "relationships/ENTITY_HAS_REPORT/file.parquet"
    )

    assert result == "ENTITY_HAS_REPORT"

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_table_name_from_windows_path(
    self, mock_manager_class, mock_schema_manager
  ):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_table_name_from_file("nodes\\Entity\\file.parquet")

    assert result == "Entity"

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_table_name_unknown_path(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_table_name_from_file("unknown/Unknown/file.parquet")

    assert result is None

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_table_name_no_path(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_table_name_from_file("file.parquet")

    assert result is None


class TestGetTableInfo:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_node_table_info(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_table_info("Entity")

    assert result is not None
    assert result.name == "Entity"
    assert result.is_relationship is False

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_relationship_table_info(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_table_info("ENTITY_HAS_REPORT")

    assert result is not None
    assert result.name == "ENTITY_HAS_REPORT"
    assert result.is_relationship is True

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_unknown_table_info(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_table_info("UnknownTable")

    assert result is None


class TestGetRelationshipInfo:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_relationship_info(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_relationship_info("ENTITY_HAS_REPORT")

    assert result is not None
    assert result == ("ENTITY_HAS_REPORT", "Entity", "Report")

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_relationship_info_not_found(
    self, mock_manager_class, mock_schema_manager
  ):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_relationship_info("UnknownRelationship")

    assert result is None


class TestGetAllTables:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_all_node_tables(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_all_node_tables()

    assert "Entity" in result
    assert "Report" in result
    assert len(result) == 2

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_all_relationship_tables(self, mock_manager_class, mock_schema_manager):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator({})

    result = generator.get_all_relationship_tables()

    assert "ENTITY_HAS_REPORT" in result
    assert len(result) == 1


class TestGetSchemaStatistics:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_get_schema_statistics(
    self, mock_manager_class, mock_schema_manager, schema_config
  ):
    mock_manager_class.return_value = mock_schema_manager

    generator = XBRLSchemaConfigGenerator(schema_config)

    stats = generator.get_schema_statistics()

    assert stats["schema_name"] == "Test Schema"
    assert stats["base_schema"] == "base"
    assert stats["extensions"] == ["test_ext"]
    assert stats["total_nodes"] == 2
    assert stats["total_relationships"] == 1
    assert "Entity" in stats["node_tables"]
    assert "ENTITY_HAS_REPORT" in stats["relationship_tables"]


class TestFactoryFunctions:
  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_create_roboledger_ingestion_processor(
    self, mock_manager_class, mock_schema_manager
  ):
    mock_manager_class.return_value = mock_schema_manager

    generator = create_roboledger_ingestion_processor()

    assert generator.config.name == "RoboLedger Ingestion Schema"
    assert generator.config.base_schema == "base"
    assert "roboledger" in generator.config.extensions

  @patch("robosystems.adapters.sec.processors.schema.SchemaManager")
  def test_create_custom_ingestion_processor(
    self, mock_manager_class, mock_schema_manager
  ):
    mock_manager_class.return_value = mock_schema_manager

    generator = create_custom_ingestion_processor(["custom1", "custom2"])

    assert "custom1, custom2" in generator.config.name
    assert generator.config.base_schema == "base"
    assert "custom1" in generator.config.extensions
    assert "custom2" in generator.config.extensions
