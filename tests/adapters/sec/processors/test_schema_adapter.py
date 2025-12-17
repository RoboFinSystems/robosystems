import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from robosystems.adapters.sec.processors.schema import XBRLSchemaAdapter


@pytest.fixture
def mock_schema_config():
  return {
    "xbrl_sec": {
      "nodes": ["Entity", "Report", "Fact"],
      "relationships": ["ENTITY_HAS_REPORT", "REPORT_HAS_FACT"],
    }
  }


@pytest.fixture
def mock_schema_builder():
  builder = MagicMock()

  node_entity = MagicMock()
  node_entity.name = "Entity"
  prop1 = MagicMock()
  prop1.name = "identifier"
  prop1.type = "STRING"
  prop1.is_primary_key = True
  prop2 = MagicMock()
  prop2.name = "name"
  prop2.type = "STRING"
  prop2.is_primary_key = False
  node_entity.properties = [prop1, prop2]

  node_report = MagicMock()
  node_report.name = "Report"
  prop3 = MagicMock()
  prop3.name = "identifier"
  prop3.type = "STRING"
  prop3.is_primary_key = True
  prop4 = MagicMock()
  prop4.name = "filing_date"
  prop4.type = "DATE"
  prop4.is_primary_key = False
  node_report.properties = [prop3, prop4]

  rel_entity_report = MagicMock()
  rel_entity_report.name = "ENTITY_HAS_REPORT"
  rel_entity_report.from_node = "Entity"
  rel_entity_report.to_node = "Report"
  prop5 = MagicMock()
  prop5.name = "relationship_id"
  prop5.type = "STRING"
  rel_entity_report.properties = [prop5]

  schema = MagicMock()
  schema.nodes = [node_entity, node_report]
  schema.relationships = [rel_entity_report]

  builder.schema = schema
  return builder


class TestXBRLSchemaAdapterInitialization:
  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_initialization(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    assert adapter.schema_config == mock_schema_config
    assert adapter.schema_builder == mock_schema_builder
    assert adapter.compiled_schema == mock_schema_builder.schema
    mock_builder_class.assert_called_once_with(mock_schema_config)
    mock_schema_builder.load_schemas.assert_called_once()

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_schema_extraction_nodes(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    assert "Entity" in adapter.node_schemas
    assert "Report" in adapter.node_schemas
    assert adapter.node_schemas["Entity"]["table_type"] == "node"
    assert adapter.node_schemas["Entity"]["primary_keys"] == ["identifier"]
    assert len(adapter.node_schemas["Entity"]["properties"]) == 2

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_schema_extraction_relationships(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    assert "ENTITY_HAS_REPORT" in adapter.relationship_schemas
    assert (
      adapter.relationship_schemas["ENTITY_HAS_REPORT"]["table_type"] == "relationship"
    )
    assert adapter.relationship_schemas["ENTITY_HAS_REPORT"]["from_node"] == "Entity"
    assert adapter.relationship_schemas["ENTITY_HAS_REPORT"]["to_node"] == "Report"
    assert len(adapter.relationship_schemas["ENTITY_HAS_REPORT"]["properties"]) == 1

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_schema_extraction_empty_schema(self, mock_builder_class, mock_schema_config):
    empty_builder = MagicMock()
    empty_builder.schema = None
    mock_builder_class.return_value = empty_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    assert adapter.node_schemas == {}
    assert adapter.relationship_schemas == {}


class TestResolveSchemaName:
  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_resolve_mapped_table_name(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter._resolve_schema_name("EntityReports")

    assert result == "ENTITY_HAS_REPORT"

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_resolve_unmapped_table_name(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter._resolve_schema_name("Entity")

    assert result == "Entity"


class TestGetSchemaInfo:
  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_get_node_schema_info(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter._get_schema_info("Entity")

    assert result is not None
    assert result["table_type"] == "node"
    assert "properties" in result

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_get_relationship_schema_info(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter._get_schema_info("ENTITY_HAS_REPORT")

    assert result is not None
    assert result["table_type"] == "relationship"
    assert result["from_node"] == "Entity"
    assert result["to_node"] == "Report"

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_get_unknown_schema_info(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter._get_schema_info("UnknownSchema")

    assert result is None


class TestBuildColumnList:
  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_build_node_column_list(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)
    schema_info = adapter._get_schema_info("Entity")

    result = adapter._build_column_list(schema_info)

    assert result == ["identifier", "name"]

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_build_relationship_column_list(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)
    schema_info = adapter._get_schema_info("ENTITY_HAS_REPORT")

    result = adapter._build_column_list(schema_info)

    assert result == ["from", "to", "relationship_id"]


class TestGetDefaultValueForType:
  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_string_type_default(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    assert adapter._get_default_value_for_type("STRING") == ""
    assert adapter._get_default_value_for_type("VARCHAR") == ""
    assert adapter._get_default_value_for_type("TEXT") == ""

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_int_type_default(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    assert adapter._get_default_value_for_type("INT") == 0
    assert adapter._get_default_value_for_type("INT64") == 0
    assert adapter._get_default_value_for_type("INTEGER") == 0
    assert adapter._get_default_value_for_type("INT32") == 0

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_float_type_default(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    assert adapter._get_default_value_for_type("DOUBLE") == 0.0
    assert adapter._get_default_value_for_type("FLOAT") == 0.0
    assert adapter._get_default_value_for_type("DECIMAL") == 0.0

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_boolean_type_default(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    assert adapter._get_default_value_for_type("BOOLEAN") is False
    assert adapter._get_default_value_for_type("BOOL") is False

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_date_type_default(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    assert adapter._get_default_value_for_type("DATE") is None
    assert adapter._get_default_value_for_type("TIMESTAMP") is None

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_unknown_type_default(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    assert adapter._get_default_value_for_type("UNKNOWN") is None


class TestCreateSchemaCompatibleDataFrame:
  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_create_node_dataframe(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter.create_schema_compatible_dataframe("Entity")

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["identifier", "name"]
    assert len(result) == 0

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_create_relationship_dataframe(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter.create_schema_compatible_dataframe("ENTITY_HAS_REPORT")

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["from", "to", "relationship_id"]
    assert len(result) == 0

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_create_dataframe_with_mapping(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter.create_schema_compatible_dataframe("EntityReports")

    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["from", "to", "relationship_id"]

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_create_dataframe_unknown_schema(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter.create_schema_compatible_dataframe("UnknownTable")

    assert isinstance(result, pd.DataFrame)
    assert len(result.columns) == 0


class TestProcessDataFrameForSchema:
  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_process_complete_data(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    data_dict = {"identifier": "entity123", "name": "Test Company"}

    result = adapter.process_dataframe_for_schema("Entity", data_dict)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result["identifier"].iloc[0] == "entity123"
    assert result["name"].iloc[0] == "Test Company"

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_process_partial_data_with_defaults(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    data_dict = {"identifier": "entity123"}

    result = adapter.process_dataframe_for_schema("Entity", data_dict)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result["identifier"].iloc[0] == "entity123"
    assert result["name"].iloc[0] == ""

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_process_relationship_data(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    data_dict = {"from": "entity123", "to": "report456", "relationship_id": "rel789"}

    result = adapter.process_dataframe_for_schema("ENTITY_HAS_REPORT", data_dict)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result["from"].iloc[0] == "entity123"
    assert result["to"].iloc[0] == "report456"
    assert result["relationship_id"].iloc[0] == "rel789"

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_process_unknown_schema(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    data_dict = {"field1": "value1", "field2": "value2"}

    result = adapter.process_dataframe_for_schema("UnknownTable", data_dict)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1


class TestValidateDataFrameSchema:
  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_validate_correct_dataframe(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    df = pd.DataFrame({"identifier": ["entity123"], "name": ["Test Company"]})

    result = adapter.validate_dataframe_schema("Entity", df)

    assert result["valid"] is True
    assert "message" in result

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_validate_missing_columns(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    df = pd.DataFrame({"identifier": ["entity123"]})

    result = adapter.validate_dataframe_schema("Entity", df)

    assert result["valid"] is False
    assert "Column count mismatch" in result["error"]
    assert "missing_columns" in result
    assert "name" in result["missing_columns"]

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_validate_extra_columns(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    df = pd.DataFrame(
      {
        "identifier": ["entity123"],
        "name": ["Test Company"],
        "extra_field": ["extra_value"],
      }
    )

    result = adapter.validate_dataframe_schema("Entity", df)

    assert result["valid"] is False
    assert "Column count mismatch" in result["error"]
    assert "extra_columns" in result
    assert "extra_field" in result["extra_columns"]

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_validate_wrong_column_names(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    df = pd.DataFrame({"identifier": ["entity123"], "title": ["Test Company"]})

    result = adapter.validate_dataframe_schema("Entity", df)

    assert result["valid"] is False
    assert "Column names do not match schema" in result["error"]
    assert "missing_columns" in result
    assert "extra_columns" in result

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_validate_unknown_schema(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    df = pd.DataFrame({"field1": ["value1"]})

    result = adapter.validate_dataframe_schema("UnknownTable", df)

    assert result["valid"] is False
    assert "Schema not found" in result["error"]


class TestGetSchemaInfoPublic:
  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_get_schema_info_node(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter.get_schema_info("Entity")

    assert result["type"] == "node"
    assert result["column_count"] == 2
    assert result["schema_name"] == "Entity"
    assert result["original_name"] == "Entity"

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_get_schema_info_with_mapping(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter.get_schema_info("EntityReports")

    assert result["type"] == "relationship"
    assert result["schema_name"] == "ENTITY_HAS_REPORT"
    assert result["original_name"] == "EntityReports"

  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_get_schema_info_unknown(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter.get_schema_info("UnknownTable")

    assert result["type"] == "unknown"
    assert result["schema"] is None
    assert result["column_count"] == 0


class TestGetAvailableSchemas:
  @patch("robosystems.adapters.sec.processors.schema.LadybugSchemaBuilder")
  def test_get_available_schemas(
    self, mock_builder_class, mock_schema_config, mock_schema_builder
  ):
    mock_builder_class.return_value = mock_schema_builder

    adapter = XBRLSchemaAdapter(mock_schema_config)

    result = adapter.get_available_schemas()

    assert "Entity" in result
    assert "Report" in result
    assert "ENTITY_HAS_REPORT" in result
    assert len(result) == 3


class TestXBRLTableMapping:
  def test_xbrl_table_mapping_exists(self):
    assert "EntityReports" in XBRLSchemaAdapter.XBRL_TABLE_MAPPING
    assert "ReportFacts" in XBRLSchemaAdapter.XBRL_TABLE_MAPPING
    assert "FactElements" in XBRLSchemaAdapter.XBRL_TABLE_MAPPING

  def test_xbrl_table_mapping_values(self):
    assert XBRLSchemaAdapter.XBRL_TABLE_MAPPING["EntityReports"] == "ENTITY_HAS_REPORT"
    assert XBRLSchemaAdapter.XBRL_TABLE_MAPPING["ReportFacts"] == "REPORT_HAS_FACT"
    assert XBRLSchemaAdapter.XBRL_TABLE_MAPPING["FactElements"] == "FACT_HAS_ELEMENT"
