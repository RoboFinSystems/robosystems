import pandas as pd
import pytest
from unittest.mock import MagicMock

from robosystems.processors.xbrl.dataframe_manager import DataFrameManager


class TestDataFrameManagerInitialization:
  def test_initialization_with_adapters(self):
    schema_adapter = MagicMock()
    ingest_adapter = MagicMock()

    manager = DataFrameManager(schema_adapter, ingest_adapter)

    assert manager.schema_adapter == schema_adapter
    assert manager.ingest_adapter == ingest_adapter
    assert manager.enable_column_standardization is False
    assert manager.dataframes == {}
    assert manager.schema_to_dataframe_mapping == {}

  def test_initialization_with_column_standardization(self):
    schema_adapter = MagicMock()
    ingest_adapter = MagicMock()

    manager = DataFrameManager(
      schema_adapter, ingest_adapter, enable_column_standardization=True
    )

    assert manager.enable_column_standardization is True


class TestConvertSchemaNameToDataFrameAttr:
  def test_node_simple_name(self):
    manager = DataFrameManager(MagicMock(), MagicMock())

    result = manager._convert_schema_name_to_dataframe_attr("Entity", is_node=True)

    assert result == "entities_df"

  def test_node_multiple_words(self):
    manager = DataFrameManager(MagicMock(), MagicMock())

    result = manager._convert_schema_name_to_dataframe_attr(
      "FactDimension", is_node=True
    )

    assert result == "fact_dimensions_df"

  def test_relationship_fact_has_dimension(self):
    manager = DataFrameManager(MagicMock(), MagicMock())

    result = manager._convert_schema_name_to_dataframe_attr(
      "FACT_HAS_DIMENSION", is_node=False
    )

    assert result == "fact_has_dimension_rel_df"

  def test_relationship_entity_has_report(self):
    manager = DataFrameManager(MagicMock(), MagicMock())

    result = manager._convert_schema_name_to_dataframe_attr(
      "ENTITY_HAS_REPORT", is_node=False
    )

    assert result == "entity_reports_df"

  def test_relationship_report_has_fact(self):
    manager = DataFrameManager(MagicMock(), MagicMock())

    result = manager._convert_schema_name_to_dataframe_attr(
      "REPORT_HAS_FACT", is_node=False
    )

    assert result == "report_facts_df"

  def test_relationship_without_has(self):
    manager = DataFrameManager(MagicMock(), MagicMock())

    result = manager._convert_schema_name_to_dataframe_attr(
      "SOME_OTHER_REL", is_node=False
    )

    assert result == "some_other_rel_df"


class TestGetSetDataFrame:
  def test_get_dataframe_exists(self):
    manager = DataFrameManager(MagicMock(), MagicMock())
    df = pd.DataFrame({"a": [1, 2, 3]})
    manager.dataframes["test_df"] = df

    result = manager.get_dataframe("test_df")

    pd.testing.assert_frame_equal(result, df)

  def test_get_dataframe_not_exists(self):
    manager = DataFrameManager(MagicMock(), MagicMock())

    result = manager.get_dataframe("nonexistent_df")

    assert result is None

  def test_set_dataframe(self):
    manager = DataFrameManager(MagicMock(), MagicMock())
    df = pd.DataFrame({"a": [1, 2, 3]})

    manager.set_dataframe("test_df", df)

    assert "test_df" in manager.dataframes
    pd.testing.assert_frame_equal(manager.dataframes["test_df"], df)


class TestInitializeAllDataFrames:
  def test_initialize_without_schema_adapter(self):
    manager = DataFrameManager(None, MagicMock())

    with pytest.raises(ValueError) as exc_info:
      manager.initialize_all_dataframes()

    assert "Schema configuration is required" in str(exc_info.value)

  def test_initialize_with_empty_schema(self):
    schema_adapter = MagicMock()
    schema_builder = MagicMock()
    schema = MagicMock()
    schema.nodes = []
    schema.relationships = []
    schema_builder.schema = schema
    schema_adapter.schema_builder = schema_builder

    manager = DataFrameManager(schema_adapter, MagicMock())

    result = manager.initialize_all_dataframes()

    assert result == {}

  def test_initialize_with_node_types(self):
    schema_adapter = MagicMock()
    schema_builder = MagicMock()
    schema = MagicMock()

    node1 = MagicMock()
    node1.name = "Entity"
    node2 = MagicMock()
    node2.name = "Report"

    schema.nodes = [node1, node2]
    schema.relationships = []
    schema_builder.schema = schema
    schema_adapter.schema_builder = schema_builder

    df1 = pd.DataFrame({"id": [], "name": []})
    df2 = pd.DataFrame({"id": [], "filing_date": []})
    schema_adapter.create_schema_compatible_dataframe.side_effect = [df1, df2]

    manager = DataFrameManager(schema_adapter, MagicMock())

    result = manager.initialize_all_dataframes()

    assert "entities_df" in result
    assert "reports_df" in result
    assert len(result) >= 2

  def test_initialize_with_relationships(self):
    schema_adapter = MagicMock()
    schema_builder = MagicMock()
    schema = MagicMock()

    node1 = MagicMock()
    node1.name = "Entity"

    rel1 = MagicMock()
    rel1.name = "ENTITY_HAS_REPORT"

    schema.nodes = [node1]
    schema.relationships = [rel1]
    schema_builder.schema = schema
    schema_adapter.schema_builder = schema_builder

    df_node = pd.DataFrame({"id": []})
    df_rel = pd.DataFrame({"from": [], "to": []})
    schema_adapter.create_schema_compatible_dataframe.side_effect = [df_node, df_rel]

    manager = DataFrameManager(schema_adapter, MagicMock())

    result = manager.initialize_all_dataframes()

    assert "entities_df" in result
    assert "entity_reports_df" in result

  def test_initialize_node_creation_error(self):
    schema_adapter = MagicMock()
    schema_builder = MagicMock()
    schema = MagicMock()

    node1 = MagicMock()
    node1.name = "Entity"

    schema.nodes = [node1]
    schema.relationships = []
    schema_builder.schema = schema
    schema_adapter.schema_builder = schema_builder

    schema_adapter.create_schema_compatible_dataframe.side_effect = RuntimeError(
      "Schema error"
    )

    manager = DataFrameManager(schema_adapter, MagicMock())

    with pytest.raises(ValueError) as exc_info:
      manager.initialize_all_dataframes()

    assert "Failed to initialize DataFrame" in str(exc_info.value)


class TestCreateDynamicDataFrameMapping:
  def test_create_mapping_without_schema_adapter(self):
    manager = DataFrameManager(None, MagicMock())

    result = manager.create_dynamic_dataframe_mapping()

    assert result == {}

  def test_create_mapping_with_nodes(self):
    schema_adapter = MagicMock()
    schema_builder = MagicMock()
    schema = MagicMock()

    node1 = MagicMock()
    node1.name = "Entity"

    schema.nodes = [node1]
    schema.relationships = []
    schema_builder.schema = schema
    schema_adapter.schema_builder = schema_builder

    manager = DataFrameManager(schema_adapter, MagicMock())
    manager.dataframes["entities_df"] = pd.DataFrame()

    result = manager.create_dynamic_dataframe_mapping()

    assert "Entity" in result
    assert result["Entity"] == "entities_df"

  def test_create_mapping_with_relationships(self):
    schema_adapter = MagicMock()
    schema_builder = MagicMock()
    schema = MagicMock()

    rel1 = MagicMock()
    rel1.name = "ENTITY_HAS_REPORT"

    schema.nodes = []
    schema.relationships = [rel1]
    schema_builder.schema = schema
    schema_adapter.schema_builder = schema_builder

    manager = DataFrameManager(schema_adapter, MagicMock())
    manager.dataframes["entity_reports_df"] = pd.DataFrame()

    result = manager.create_dynamic_dataframe_mapping()

    assert "ENTITY_HAS_REPORT" in result
    assert result["ENTITY_HAS_REPORT"] == "entity_reports_df"

  def test_create_mapping_with_additional_relationships(self):
    schema_adapter = MagicMock()
    schema_builder = MagicMock()
    schema = MagicMock()

    schema.nodes = []
    schema.relationships = []
    schema_builder.schema = schema
    schema_adapter.schema_builder = schema_builder

    manager = DataFrameManager(schema_adapter, MagicMock())
    manager.dataframes["fact_dimension_axis_element_rel_df"] = pd.DataFrame()
    manager.dataframes["fact_set_contains_facts_df"] = pd.DataFrame()

    result = manager.create_dynamic_dataframe_mapping()

    assert "FACT_DIMENSION_AXIS_ELEMENT" in result
    assert "FACT_SET_CONTAINS_FACT" in result

  def test_create_mapping_no_schema(self):
    schema_adapter = MagicMock()
    schema_builder = MagicMock()
    schema_builder.schema = None
    schema_adapter.schema_builder = schema_builder

    manager = DataFrameManager(schema_adapter, MagicMock())

    result = manager.create_dynamic_dataframe_mapping()

    assert result == {}


class TestStandardizeDataFrameColumns:
  def test_standardize_disabled(self):
    manager = DataFrameManager(
      MagicMock(), MagicMock(), enable_column_standardization=False
    )
    df = pd.DataFrame({"old_name": [1, 2, 3]})

    result = manager.standardize_dataframe_columns(df, "TestTable")

    assert list(result.columns) == ["old_name"]

  def test_standardize_empty_dataframe(self):
    manager = DataFrameManager(
      MagicMock(), MagicMock(), enable_column_standardization=True
    )
    df = pd.DataFrame()

    result = manager.standardize_dataframe_columns(df, "TestTable")

    assert result.empty

  def test_standardize_no_mapping(self):
    manager = DataFrameManager(
      MagicMock(), MagicMock(), enable_column_standardization=True
    )
    df = pd.DataFrame({"col1": [1, 2, 3]})

    result = manager.standardize_dataframe_columns(df, "UnknownTable")

    assert list(result.columns) == ["col1"]


class TestEnsureSchemaCompleteness:
  def test_ensure_completeness_without_adapter(self):
    manager = DataFrameManager(None, MagicMock())
    df = pd.DataFrame({"a": [1, 2, 3]})

    result = manager.ensure_schema_completeness(df, "TestTable")

    pd.testing.assert_frame_equal(result, df)

  def test_ensure_completeness_unknown_table(self):
    schema_adapter = MagicMock()
    schema_adapter.get_schema_info.return_value = {"type": "unknown"}

    manager = DataFrameManager(schema_adapter, MagicMock())
    df = pd.DataFrame({"a": [1, 2, 3]})

    result = manager.ensure_schema_completeness(df, "UnknownTable")

    pd.testing.assert_frame_equal(result, df)

  def test_ensure_completeness_adds_missing_columns(self):
    schema_adapter = MagicMock()

    prop1 = MagicMock()
    prop1.name = "id"
    prop1.type = "STRING"

    prop2 = MagicMock()
    prop2.name = "name"
    prop2.type = "STRING"

    schema_adapter.get_schema_info.return_value = {
      "type": "node",
      "schema": {"properties": [prop1, prop2]},
    }

    manager = DataFrameManager(schema_adapter, MagicMock())
    df = pd.DataFrame({"id": ["1", "2"]})

    result = manager.ensure_schema_completeness(df, "Entity")

    assert "name" in result.columns
    assert list(result["name"]) == ["", ""]

  def test_ensure_completeness_relationship_adds_from_to(self):
    schema_adapter = MagicMock()

    prop1 = MagicMock()
    prop1.name = "label"
    prop1.type = "STRING"

    schema_adapter.get_schema_info.return_value = {
      "type": "relationship",
      "schema": {"properties": [prop1]},
    }

    manager = DataFrameManager(schema_adapter, MagicMock())
    df = pd.DataFrame({"label": ["rel1"]})

    result = manager.ensure_schema_completeness(df, "ENTITY_HAS_REPORT")

    assert "from" in result.columns
    assert "to" in result.columns
    assert "label" in result.columns

  def test_ensure_completeness_int_column(self):
    schema_adapter = MagicMock()

    prop1 = MagicMock()
    prop1.name = "count"
    prop1.type = "INT64"

    schema_adapter.get_schema_info.return_value = {
      "type": "node",
      "schema": {"properties": [prop1]},
    }

    manager = DataFrameManager(schema_adapter, MagicMock())
    df = pd.DataFrame({"id": [1, 2]})

    result = manager.ensure_schema_completeness(df, "Stats")

    assert "count" in result.columns
    assert list(result["count"]) == [0, 0]

  def test_ensure_completeness_float_column(self):
    schema_adapter = MagicMock()

    prop1 = MagicMock()
    prop1.name = "value"
    prop1.type = "DOUBLE"

    schema_adapter.get_schema_info.return_value = {
      "type": "node",
      "schema": {"properties": [prop1]},
    }

    manager = DataFrameManager(schema_adapter, MagicMock())
    df = pd.DataFrame({"id": [1, 2]})

    result = manager.ensure_schema_completeness(df, "Metrics")

    assert "value" in result.columns
    assert list(result["value"]) == [0.0, 0.0]

  def test_ensure_completeness_boolean_column(self):
    schema_adapter = MagicMock()

    prop1 = MagicMock()
    prop1.name = "is_active"
    prop1.type = "BOOLEAN"

    schema_adapter.get_schema_info.return_value = {
      "type": "node",
      "schema": {"properties": [prop1]},
    }

    manager = DataFrameManager(schema_adapter, MagicMock())
    df = pd.DataFrame({"id": [1, 2]})

    result = manager.ensure_schema_completeness(df, "Flags")

    assert "is_active" in result.columns
    assert list(result["is_active"]) == [False, False]

  def test_ensure_completeness_preserves_existing_columns(self):
    schema_adapter = MagicMock()

    prop1 = MagicMock()
    prop1.name = "id"
    prop1.type = "STRING"

    schema_adapter.get_schema_info.return_value = {
      "type": "node",
      "schema": {"properties": [prop1]},
    }

    manager = DataFrameManager(schema_adapter, MagicMock())
    df = pd.DataFrame({"id": ["1", "2"], "extra": ["a", "b"]})

    result = manager.ensure_schema_completeness(df, "Entity")

    assert "id" in result.columns
    assert "extra" in result.columns
    assert list(result["id"]) == ["1", "2"]
    assert list(result["extra"]) == ["a", "b"]

  def test_ensure_completeness_error_handling(self):
    schema_adapter = MagicMock()
    schema_adapter.get_schema_info.side_effect = RuntimeError("Schema error")

    manager = DataFrameManager(schema_adapter, MagicMock())
    df = pd.DataFrame({"a": [1, 2, 3]})

    result = manager.ensure_schema_completeness(df, "TestTable")

    pd.testing.assert_frame_equal(result, df)
