import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path

from robosystems.adapters.sec.processors.parquet import ParquetWriter


@pytest.fixture(autouse=True)
def mock_path_mkdir():
  with patch("pathlib.Path.mkdir"):
    yield


@pytest.fixture
def mock_dependencies():
  schema_adapter = MagicMock()
  ingest_adapter = MagicMock()
  df_manager = MagicMock()
  return schema_adapter, ingest_adapter, df_manager


class TestParquetWriterInitialization:
  def test_initialization_basic(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    output_dir = Path("/test/output")

    writer = ParquetWriter(output_dir, schema_adapter, ingest_adapter, df_manager)

    assert writer.output_dir == output_dir
    assert writer.schema_adapter == schema_adapter
    assert writer.ingest_adapter == ingest_adapter
    assert writer.df_manager == df_manager
    assert writer.enable_standardized_filenames is False
    assert writer.enable_type_prefixes is False
    assert writer.enable_column_standardization is False
    assert writer.sec_filer is None
    assert writer.sec_report is None

  def test_initialization_with_all_options(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    output_dir = Path("/test/output")
    sec_filer = MagicMock()
    sec_report = MagicMock()

    writer = ParquetWriter(
      output_dir,
      schema_adapter,
      ingest_adapter,
      df_manager,
      enable_standardized_filenames=True,
      enable_type_prefixes=True,
      enable_column_standardization=True,
      sec_filer=sec_filer,
      sec_report=sec_report,
    )

    assert writer.enable_standardized_filenames is True
    assert writer.enable_type_prefixes is True
    assert writer.enable_column_standardization is True
    assert writer.sec_filer == sec_filer
    assert writer.sec_report == sec_report


class TestGenerateStandardizedFilename:
  def test_basic_filename(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    result = writer.generate_standardized_filename("Entity")

    assert result == "Entity.parquet"

  def test_filename_with_type_prefix_node(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(
      Path("/test"),
      schema_adapter,
      ingest_adapter,
      df_manager,
      enable_type_prefixes=True,
    )

    result = writer.generate_standardized_filename("Entity", is_relationship=False)

    assert result == "node__Entity.parquet"

  def test_filename_with_type_prefix_relationship(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(
      Path("/test"),
      schema_adapter,
      ingest_adapter,
      df_manager,
      enable_type_prefixes=True,
    )

    result = writer.generate_standardized_filename(
      "ENTITY_HAS_REPORT", is_relationship=True
    )

    assert result == "rel__ENTITY_HAS_REPORT.parquet"

  def test_filename_with_filing_metadata(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    sec_filer = MagicMock()
    sec_filer.cik = "0000320193"
    sec_report = MagicMock()
    sec_report.filing_date = "2023-09-30"

    writer = ParquetWriter(
      Path("/test"),
      schema_adapter,
      ingest_adapter,
      df_manager,
      enable_standardized_filenames=True,
      sec_filer=sec_filer,
      sec_report=sec_report,
    )

    result = writer.generate_standardized_filename("Entity")

    assert result == "Entity_20230930_0000320193.parquet"

  def test_filename_with_type_prefix_and_metadata(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    sec_filer = MagicMock()
    sec_filer.cik = "0000320193"
    sec_report = MagicMock()
    sec_report.filing_date = "2023-09-30"

    writer = ParquetWriter(
      Path("/test"),
      schema_adapter,
      ingest_adapter,
      df_manager,
      enable_standardized_filenames=True,
      enable_type_prefixes=True,
      sec_filer=sec_filer,
      sec_report=sec_report,
    )

    result = writer.generate_standardized_filename("Entity", is_relationship=False)

    assert result == "node__Entity_20230930_0000320193.parquet"

  def test_filename_missing_filing_date(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    sec_filer = MagicMock()
    sec_filer.cik = "0000320193"
    sec_report = MagicMock()
    sec_report.filing_date = ""

    writer = ParquetWriter(
      Path("/test"),
      schema_adapter,
      ingest_adapter,
      df_manager,
      enable_standardized_filenames=True,
      sec_filer=sec_filer,
      sec_report=sec_report,
    )

    result = writer.generate_standardized_filename("Entity")

    assert result == "Entity.parquet"

  def test_filename_missing_cik(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    sec_filer = MagicMock()
    sec_filer.cik = "unknown"
    sec_report = MagicMock()
    sec_report.filing_date = "2023-09-30"

    writer = ParquetWriter(
      Path("/test"),
      schema_adapter,
      ingest_adapter,
      df_manager,
      enable_standardized_filenames=True,
      sec_filer=sec_filer,
      sec_report=sec_report,
    )

    result = writer.generate_standardized_filename("Entity")

    assert result == "Entity.parquet"


class TestIsRelationshipFilename:
  def test_node_filename(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    assert writer._is_relationship_filename("Entity") is False
    assert writer._is_relationship_filename("Fact") is False
    assert writer._is_relationship_filename("Report") is False

  def test_relationship_filename(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    assert writer._is_relationship_filename("entity_reports") is True
    assert writer._is_relationship_filename("report_facts") is True
    assert writer._is_relationship_filename("fact_has_dimension_rel") is True
    assert writer._is_relationship_filename("element_labels") is True


class TestFixColumnTypesBySchema:
  def test_entity_column_types(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame(
      {
        "identifier": ["1", "2"],
        "name": ["Company A", "Company B"],
        "ein": [123456789, 987654321],
        "ticker": ["AAPL", "GOOGL"],
      }
    )

    result = writer._fix_column_types_by_schema(df, "Entity")

    assert result["name"].dtype == "object"
    assert result["ein"].dtype == "object"
    assert result["ticker"].dtype == "object"
    assert result["ein"].iloc[0] == "123456789"

  def test_entity_ein_padding(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame({"identifier": ["1"], "ein": [123456]})

    result = writer._fix_column_types_by_schema(df, "Entity")

    assert result["ein"].iloc[0] == "000123456"

  def test_unit_column_types(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame(
      {
        "identifier": ["1", "2"],
        "uri": ["http://example.com/usd", "http://example.com/shares"],
        "measure": ["USD", "shares"],
      }
    )

    result = writer._fix_column_types_by_schema(df, "Unit")

    assert result["uri"].dtype == "object"
    assert result["measure"].dtype == "object"

  def test_report_column_types(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame(
      {
        "identifier": ["1"],
        "name": ["Annual Report"],
        "accession_number": ["0000320193-23-000077"],
        "form": ["10-K"],
      }
    )

    result = writer._fix_column_types_by_schema(df, "Report")

    assert result["name"].dtype == "object"
    assert result["accession_number"].dtype == "object"
    assert result["form"].dtype == "object"

  def test_association_weight_type(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame({"identifier": ["1", "2"], "weight": ["1.5", "2.3"]})

    result = writer._fix_column_types_by_schema(df, "Association")

    assert result["weight"].dtype == "float64"

  def test_unknown_schema_no_changes(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame({"identifier": ["1", "2"], "value": [100, 200]})

    result = writer._fix_column_types_by_schema(df, "UnknownTable")

    assert result["value"].dtype == "int64"


class TestFixColumnTypesByFilename:
  def test_entity_filename(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame(
      {
        "identifier": ["1"],
        "name": ["Company A"],
        "ein": [123456],
        "ticker": ["AAPL"],
      }
    )

    result = writer._fix_column_types_by_filename(df, "Entity.parquet")

    assert result["name"].dtype == "object"
    assert result["ein"].dtype == "object"
    assert result["ein"].iloc[0] == "000123456"

  def test_unit_filename(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame({"identifier": ["1"], "uri": ["http://example.com/usd"]})

    result = writer._fix_column_types_by_filename(df, "Unit.parquet")

    assert result["uri"].dtype == "object"

  def test_fact_filename(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame(
      {
        "identifier": ["1"],
        "uri": ["fact://test"],
        "value": ["1000"],
        "fact_type": ["numeric"],
      }
    )

    result = writer._fix_column_types_by_filename(df, "Fact.parquet")

    assert result["uri"].dtype == "object"
    assert result["value"].dtype == "object"
    assert result["fact_type"].dtype == "object"


class TestWriteDataFrameSchemaDriven:
  def test_write_empty_dataframe(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame()

    with patch.object(pd.DataFrame, "to_parquet") as mock_to_parquet:
      writer.write_dataframe_schema_driven(df, "Entity.parquet", "Entity")

      mock_to_parquet.assert_not_called()

  def test_write_node_dataframe(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    ingest_adapter.get_all_relationship_tables.return_value = []
    df_manager.ensure_schema_completeness.return_value = pd.DataFrame(
      {"identifier": ["1", "2"], "name": ["A", "B"]}
    )

    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame({"identifier": ["1", "2"], "name": ["A", "B"]})

    with patch.object(pd.DataFrame, "to_parquet") as mock_to_parquet:
      writer.write_dataframe_schema_driven(df, "Entity.parquet", "Entity")

      mock_to_parquet.assert_called_once()
      args, kwargs = mock_to_parquet.call_args
      assert "nodes" in str(args[0])
      assert "Entity.parquet" in str(args[0])

  def test_write_relationship_dataframe(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    ingest_adapter.get_all_relationship_tables.return_value = ["ENTITY_HAS_REPORT"]
    df_manager.ensure_schema_completeness.return_value = pd.DataFrame(
      {"from": ["1", "2"], "to": ["3", "4"]}
    )

    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame({"from": ["1", "2"], "to": ["3", "4"]})

    with patch.object(pd.DataFrame, "to_parquet") as mock_to_parquet:
      writer.write_dataframe_schema_driven(
        df, "ENTITY_HAS_REPORT.parquet", "ENTITY_HAS_REPORT"
      )

      mock_to_parquet.assert_called_once()
      args, kwargs = mock_to_parquet.call_args
      assert "relationships" in str(args[0])

  def test_deduplication_by_identifier(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    ingest_adapter.get_all_relationship_tables.return_value = []

    df_manager.ensure_schema_completeness.side_effect = lambda df, schema: df

    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame(
      {"identifier": ["1", "2", "1"], "name": ["A", "B", "A_duplicate"]}
    )

    saved_dfs = []

    def capture_to_parquet(self, *args, **kwargs):
      saved_dfs.append(self.copy())

    with patch.object(pd.DataFrame, "to_parquet", capture_to_parquet):
      writer.write_dataframe_schema_driven(df, "Entity.parquet", "Entity")

      assert len(saved_dfs) == 1
      saved_df = saved_dfs[0]
      assert len(saved_df) == 2
      assert list(saved_df["identifier"]) == ["1", "2"]

  def test_no_deduplication_for_relationships(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    ingest_adapter.get_all_relationship_tables.return_value = ["ENTITY_HAS_REPORT"]

    df_manager.ensure_schema_completeness.side_effect = lambda df, schema: df

    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame(
      {"identifier": ["1", "2", "1"], "from": ["A", "B", "C"], "to": ["D", "E", "F"]}
    )

    saved_dfs = []

    def capture_to_parquet(self, *args, **kwargs):
      saved_dfs.append(self.copy())

    with patch.object(pd.DataFrame, "to_parquet", capture_to_parquet):
      writer.write_dataframe_schema_driven(
        df, "ENTITY_HAS_REPORT.parquet", "ENTITY_HAS_REPORT"
      )

      assert len(saved_dfs) == 1
      saved_df = saved_dfs[0]
      assert len(saved_df) == 3

  def test_column_standardization_enabled(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    ingest_adapter.get_all_relationship_tables.return_value = []
    df_manager.ensure_schema_completeness.side_effect = lambda df, schema: df
    df_manager.standardize_dataframe_columns.return_value = pd.DataFrame(
      {"identifier": ["1"], "standardized_name": ["A"]}
    )

    writer = ParquetWriter(
      Path("/test"),
      schema_adapter,
      ingest_adapter,
      df_manager,
      enable_column_standardization=True,
    )

    df = pd.DataFrame({"identifier": ["1"], "name": ["A"]})

    with patch.object(pd.DataFrame, "to_parquet"):
      writer.write_dataframe_schema_driven(df, "Entity.parquet", "Entity")

      df_manager.standardize_dataframe_columns.assert_called_once()


class TestWriteDataFrame:
  def test_write_with_subdirectory(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    ingest_adapter.get_all_relationship_tables.return_value = []
    df_manager.ensure_schema_completeness.side_effect = lambda df, schema: df

    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame({"identifier": ["1"], "name": ["A"]})

    with patch.object(pd.DataFrame, "to_parquet") as mock_to_parquet:
      writer.write_dataframe(df, "nodes/Entity.parquet")

      mock_to_parquet.assert_called_once()
      args = mock_to_parquet.call_args[0]
      assert "nodes" in str(args[0])

  def test_write_without_subdirectory(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    ingest_adapter.get_all_relationship_tables.return_value = []
    df_manager.ensure_schema_completeness.side_effect = lambda df, schema: df

    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame({"identifier": ["1"], "name": ["A"]})

    with patch.object(pd.DataFrame, "to_parquet") as mock_to_parquet:
      writer.write_dataframe(df, "Entity.parquet")

      mock_to_parquet.assert_called_once()

  def test_table_name_conversion(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    ingest_adapter.get_all_relationship_tables.return_value = []
    df_manager.ensure_schema_completeness.side_effect = lambda df, schema: df

    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    df = pd.DataFrame({"identifier": ["1"], "from": ["A"], "to": ["B"]})

    with patch.object(pd.DataFrame, "to_parquet"):
      writer.write_dataframe(df, "fact_has_dimension.parquet")

      df_manager.ensure_schema_completeness.assert_called_once()
      call_args = df_manager.ensure_schema_completeness.call_args[0]
      assert call_args[1] == "FactDimensionsRel"


class TestWriteAllDataFrames:
  def test_write_all_with_schema_adapter(self, mock_dependencies):
    schema_adapter, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), schema_adapter, ingest_adapter, df_manager)

    processor = MagicMock()
    processor.entities_df = pd.DataFrame({"identifier": ["1"], "name": ["A"]})

    mapping = {"Entity": "entities_df"}

    ingest_adapter.get_all_relationship_tables.return_value = []
    df_manager.ensure_schema_completeness.side_effect = lambda df, schema: df

    with patch.object(pd.DataFrame, "to_parquet"):
      writer.write_all_dataframes(mapping, processor)

  def test_write_all_without_schema_adapter(self, mock_dependencies):
    _, ingest_adapter, df_manager = mock_dependencies
    writer = ParquetWriter(Path("/test"), None, ingest_adapter, df_manager)

    processor = MagicMock()
    processor.entities_df = pd.DataFrame({"identifier": ["1"], "name": ["A"]})
    processor.reports_df = pd.DataFrame({"identifier": ["2"], "name": ["Report"]})
    processor.facts_df = pd.DataFrame()
    processor.units_df = pd.DataFrame()
    processor.fact_dimensions_df = pd.DataFrame()
    processor.elements_df = pd.DataFrame()
    processor.labels_df = pd.DataFrame()
    processor.references_df = pd.DataFrame()
    processor.structures_df = pd.DataFrame()
    processor.associations_df = pd.DataFrame()
    processor.periods_df = pd.DataFrame()
    processor.taxonomies_df = pd.DataFrame()
    processor.fact_sets_df = pd.DataFrame()
    processor.taxonomy_labels_df = pd.DataFrame()
    processor.taxonomy_references_df = pd.DataFrame()
    processor.entity_reports_df = pd.DataFrame()
    processor.report_facts_df = pd.DataFrame()
    processor.report_fact_sets_df = pd.DataFrame()
    processor.report_uses_taxonomy_df = pd.DataFrame()
    processor.fact_units_df = pd.DataFrame()
    processor.fact_has_dimension_rel_df = pd.DataFrame()
    processor.fact_entities_df = pd.DataFrame()
    processor.fact_elements_df = pd.DataFrame()
    processor.fact_periods_df = pd.DataFrame()
    processor.fact_set_contains_facts_df = pd.DataFrame()
    processor.element_labels_df = pd.DataFrame()
    processor.element_references_df = pd.DataFrame()
    processor.structure_taxonomies_df = pd.DataFrame()
    processor.taxonomy_labels_df = pd.DataFrame()
    processor.taxonomy_references_df = pd.DataFrame()
    processor.structure_associations_df = pd.DataFrame()
    processor.association_from_elements_df = pd.DataFrame()
    processor.association_to_elements_df = pd.DataFrame()
    processor.fact_dimension_axis_element_rel_df = pd.DataFrame()
    processor.fact_dimension_member_element_rel_df = pd.DataFrame()

    mapping = {}

    ingest_adapter.get_all_relationship_tables.return_value = []
    df_manager.ensure_schema_completeness.side_effect = lambda df, schema: df

    with patch.object(writer, "write_dataframe") as mock_write:
      writer.write_all_dataframes(mapping, processor)

      assert mock_write.call_count >= 2
