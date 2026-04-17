"""
Extended unit tests for LadybugDB ingestion operations.

Covers functions and paths not exercised by test_ingest.py:
- _categorize_files_schema_driven: node vs relationship classification
- _parse_filename_schema_driven: filename-to-table mapping edge cases
- _ingest_node_schema_driven / _ingest_relationship_schema_driven: error handling
- _create_table_from_schema: Arrow type mapping, column intersection, SQL generation
- _create_relationship_table_from_schema: relationship DDL, missing from/to
- _is_valid_identifier: SQL injection prevention
- _sanitize_parameter_name: parameter name sanitization
- _map_arrow_to_lbug_type: Arrow to LadybugDB type mapping
- _is_global_relationship_schema_driven / _is_global_entity_schema_driven: global detection
- _copy_node_data_schema_driven / _copy_relationship_data_schema_driven: COPY operations
"""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.graph.engine.ingest import (
  _categorize_files_schema_driven,
  _copy_node_data_schema_driven,
  _copy_relationship_data_schema_driven,
  _create_relationship_table_from_schema,
  _create_table_from_schema,
  _ingest_node_schema_driven,
  _ingest_relationship_schema_driven,
  _is_global_entity_schema_driven,
  _is_global_relationship_schema_driven,
  _is_valid_identifier,
  _map_arrow_to_lbug_type,
  _parse_filename_schema_driven,
  _sanitize_parameter_name,
)

# The ingest module imports pyarrow.parquet lazily inside functions as `pq`.
# We must patch `pyarrow.parquet.ParquetFile` at the actual location.
PQ_PARQUET_FILE = "pyarrow.parquet.ParquetFile"


def _make_arrow_schema(fields):
  """Create a mock Arrow schema with named/typed fields.

  Args:
      fields: List of (name, arrow_type_str) tuples.
  """
  mock_fields = []
  for name, arrow_type in fields:
    field = MagicMock()
    field.name = name
    field.type = arrow_type
    mock_fields.append(field)
  schema = MagicMock()
  schema.__iter__ = lambda s: iter(mock_fields)
  schema.__len__ = lambda s: len(mock_fields)
  schema.__getitem__ = lambda s, i: mock_fields[i]
  return schema


def _setup_parquet_mock(mock_pq_cls, fields):
  """Wire up a mock ParquetFile class with the given fields."""
  pq_instance = MagicMock()
  mock_pq_cls.return_value = pq_instance
  pq_instance.schema_arrow = _make_arrow_schema(fields)
  pq_instance.metadata.num_rows = 100
  return pq_instance


# ---------------------------------------------------------------------------
# _categorize_files_schema_driven
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCategorizeFilesSchemaDrivern:
  """Test schema-driven file categorization into nodes and relationships."""

  def test_empty_file_list(self):
    """Empty input returns two empty lists."""
    adapter = MagicMock()
    nodes, rels = _categorize_files_schema_driven([], adapter)
    assert nodes == []
    assert rels == []

  def test_all_nodes(self):
    """All files classified as nodes when adapter says none are relationships."""
    adapter = MagicMock()
    adapter.is_relationship_file.return_value = False
    files = ["/data/Entity.parquet", "/data/Unit.parquet"]

    nodes, rels = _categorize_files_schema_driven(files, adapter)

    assert nodes == files
    assert rels == []
    assert adapter.is_relationship_file.call_count == 2

  def test_all_relationships(self):
    """All files classified as relationships."""
    adapter = MagicMock()
    adapter.is_relationship_file.return_value = True
    files = ["/data/EMPLOYS.parquet", "/data/HAS_REPORT.parquet"]

    nodes, rels = _categorize_files_schema_driven(files, adapter)

    assert nodes == []
    assert rels == files

  def test_mixed_classification(self):
    """Files correctly split between nodes and relationships."""
    adapter = MagicMock()
    adapter.is_relationship_file.side_effect = [False, True, False, True]
    files = ["/a.parquet", "/b.parquet", "/c.parquet", "/d.parquet"]

    nodes, rels = _categorize_files_schema_driven(files, adapter)

    assert nodes == ["/a.parquet", "/c.parquet"]
    assert rels == ["/b.parquet", "/d.parquet"]

  def test_full_path_passed_to_adapter(self):
    """Full path (not just filename) is passed to the adapter."""
    adapter = MagicMock()
    adapter.is_relationship_file.return_value = False
    full_path = "/tmp/processed/nodes/Entity/data_0.parquet"

    _categorize_files_schema_driven([full_path], adapter)

    adapter.is_relationship_file.assert_called_once_with(full_path)


# ---------------------------------------------------------------------------
# _parse_filename_schema_driven
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestParseFilenameSchemaDrivern:
  """Test filename parsing via schema adapter."""

  def test_successful_parse(self):
    """Returns dict with table_name, is_relationship, table_info."""
    adapter = MagicMock()
    adapter.get_table_name_from_file.return_value = "Entity"
    mock_info = MagicMock()
    mock_info.is_relationship = False
    adapter.get_table_info.return_value = mock_info

    result = _parse_filename_schema_driven("/tmp/Entity.parquet", adapter)

    assert result is not None
    assert result["table_name"] == "Entity"
    assert result["is_relationship"] is False
    assert result["table_info"] is mock_info
    assert result["file_path"] == "/tmp/Entity.parquet"

  def test_no_table_name_returns_none(self):
    """Returns None when adapter cannot map filename to table."""
    adapter = MagicMock()
    adapter.get_table_name_from_file.return_value = None

    result = _parse_filename_schema_driven("/tmp/unknown.parquet", adapter)

    assert result is None

  def test_no_table_info_returns_none(self):
    """Returns None when adapter has no info for the mapped table name."""
    adapter = MagicMock()
    adapter.get_table_name_from_file.return_value = "Orphan"
    adapter.get_table_info.return_value = None

    result = _parse_filename_schema_driven("/tmp/Orphan.parquet", adapter)

    assert result is None

  def test_relationship_file_detected(self):
    """Correctly identifies relationship files via table_info."""
    adapter = MagicMock()
    adapter.get_table_name_from_file.return_value = "HAS_REPORT"
    mock_info = MagicMock()
    mock_info.is_relationship = True
    adapter.get_table_info.return_value = mock_info

    result = _parse_filename_schema_driven("/data/HAS_REPORT.parquet", adapter)

    assert result is not None
    assert result["is_relationship"] is True


# ---------------------------------------------------------------------------
# _ingest_node_schema_driven / _ingest_relationship_schema_driven
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestIngestNodeSchemaDrivern:
  """Test node ingestion error handling paths."""

  @patch("robosystems.operations.graph.engine.ingest._copy_node_data_schema_driven")
  @patch("robosystems.operations.graph.engine.ingest._create_table_from_schema")
  def test_success_path(self, mock_create, mock_copy):
    """Successful node ingestion returns True."""
    mock_create.return_value = True
    mock_copy.return_value = True
    engine = MagicMock()
    adapter = MagicMock()
    info = MagicMock()
    table_info = {"table_name": "Entity", "table_info": info}

    result = _ingest_node_schema_driven(engine, "/f.parquet", table_info, adapter)

    assert result is True
    mock_create.assert_called_once_with(engine, "Entity", info, "/f.parquet")
    mock_copy.assert_called_once()

  @patch("robosystems.operations.graph.engine.ingest._create_table_from_schema")
  def test_table_creation_fails(self, mock_create):
    """Returns False when table creation fails."""
    mock_create.return_value = False
    engine = MagicMock()
    adapter = MagicMock()
    table_info = {"table_name": "BadTable", "table_info": MagicMock()}

    result = _ingest_node_schema_driven(engine, "/f.parquet", table_info, adapter)

    assert result is False

  def test_exception_in_ingestion(self):
    """Returns False on exception."""
    engine = MagicMock()
    adapter = MagicMock()
    # table_info missing "table_info" key triggers KeyError
    table_info = {"table_name": "Entity"}

    result = _ingest_node_schema_driven(engine, "/f.parquet", table_info, adapter)

    assert result is False


@pytest.mark.unit
class TestIngestRelationshipSchemaDrivern:
  """Test relationship ingestion error handling paths."""

  @patch(
    "robosystems.operations.graph.engine.ingest._copy_relationship_data_schema_driven"
  )
  @patch(
    "robosystems.operations.graph.engine.ingest._create_relationship_table_from_schema"
  )
  def test_success_path(self, mock_create, mock_copy):
    """Successful relationship ingestion returns True."""
    mock_create.return_value = True
    mock_copy.return_value = True
    engine = MagicMock()
    adapter = MagicMock()
    info = MagicMock()
    table_info = {"table_name": "EMPLOYS", "table_info": info}

    result = _ingest_relationship_schema_driven(
      engine, "/f.parquet", table_info, adapter
    )

    assert result is True

  @patch(
    "robosystems.operations.graph.engine.ingest._create_relationship_table_from_schema"
  )
  def test_table_creation_fails(self, mock_create):
    """Returns False when relationship table creation fails."""
    mock_create.return_value = False
    engine = MagicMock()
    adapter = MagicMock()
    table_info = {"table_name": "BAD_REL", "table_info": MagicMock()}

    result = _ingest_relationship_schema_driven(
      engine, "/f.parquet", table_info, adapter
    )

    assert result is False

  def test_exception_in_ingestion(self):
    """Returns False on exception."""
    engine = MagicMock()
    adapter = MagicMock()
    table_info = {"table_name": "EMPLOYS"}  # missing "table_info"

    result = _ingest_relationship_schema_driven(
      engine, "/f.parquet", table_info, adapter
    )

    assert result is False


# ---------------------------------------------------------------------------
# _create_table_from_schema
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCreateTableFromSchema:
  """Test node table creation with parquet schema validation."""

  @patch(PQ_PARQUET_FILE)
  def test_successful_creation(self, mock_pq_cls):
    """Table is created with correct SQL when schema and parquet overlap."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.primary_keys = ["id"]
    ingest_info.columns = ["id", "name"]
    ingest_info.is_relationship = False

    _setup_parquet_mock(
      mock_pq_cls, [("id", "string"), ("name", "string"), ("extra_col", "int64")]
    )

    result = _create_table_from_schema(engine, "Entity", ingest_info, "/f.parquet")

    assert result is True
    engine.execute_query.assert_called_once()
    sql = engine.execute_query.call_args[0][0]
    assert "CREATE NODE TABLE IF NOT EXISTS Entity" in sql
    assert "PRIMARY KEY (id)" in sql

  @patch(PQ_PARQUET_FILE)
  def test_no_matching_columns(self, mock_pq_cls):
    """Returns False when schema and parquet have no column overlap."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.primary_keys = ["id"]
    ingest_info.columns = ["id", "name"]
    ingest_info.is_relationship = False

    _setup_parquet_mock(mock_pq_cls, [("foo", "string"), ("bar", "int64")])

    result = _create_table_from_schema(engine, "Entity", ingest_info, "/f.parquet")

    assert result is False

  @patch(PQ_PARQUET_FILE)
  def test_relationship_info_as_node_table_rejected(self, mock_pq_cls):
    """Returns False when relationship info has no primary keys and is_relationship."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.primary_keys = []
    ingest_info.columns = ["from", "to"]
    ingest_info.is_relationship = True

    _setup_parquet_mock(mock_pq_cls, [("from", "string"), ("to", "string")])

    result = _create_table_from_schema(engine, "BAD_REL", ingest_info, "/f.parquet")

    assert result is False

  @patch(PQ_PARQUET_FILE)
  def test_fallback_primary_key_when_none_defined(self, mock_pq_cls):
    """Falls back to first column as PK when no PKs and not a relationship."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.primary_keys = []
    ingest_info.columns = ["first_col", "second_col"]
    ingest_info.is_relationship = False

    _setup_parquet_mock(mock_pq_cls, [("first_col", "string"), ("second_col", "int64")])

    result = _create_table_from_schema(engine, "NoKeys", ingest_info, "/f.parquet")

    assert result is True
    sql = engine.execute_query.call_args[0][0]
    assert "PRIMARY KEY (first_col)" in sql

  @patch(PQ_PARQUET_FILE)
  def test_reserved_word_quoting(self, mock_pq_cls):
    """SQL reserved words in column names are backtick-quoted."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.primary_keys = ["id"]
    ingest_info.columns = ["id", "order", "group"]
    ingest_info.is_relationship = False

    _setup_parquet_mock(
      mock_pq_cls, [("id", "string"), ("order", "int64"), ("group", "string")]
    )

    result = _create_table_from_schema(
      engine, "WithReserved", ingest_info, "/f.parquet"
    )

    assert result is True
    sql = engine.execute_query.call_args[0][0]
    assert "`order`" in sql
    assert "`group`" in sql

  @patch(PQ_PARQUET_FILE)
  def test_already_exists_error_returns_true(self, mock_pq_cls):
    """Returns True when table already exists (idempotent)."""
    engine = MagicMock()
    engine.execute_query.side_effect = RuntimeError("Table already exists")
    ingest_info = MagicMock()
    ingest_info.primary_keys = ["id"]
    ingest_info.columns = ["id"]
    ingest_info.is_relationship = False

    _setup_parquet_mock(mock_pq_cls, [("id", "string")])

    result = _create_table_from_schema(engine, "Existing", ingest_info, "/f.parquet")

    assert result is True

  @patch(PQ_PARQUET_FILE)
  def test_non_exists_error_returns_false(self, mock_pq_cls):
    """Returns False on other errors."""
    engine = MagicMock()
    engine.execute_query.side_effect = RuntimeError("Connection lost")
    ingest_info = MagicMock()
    ingest_info.primary_keys = ["id"]
    ingest_info.columns = ["id"]
    ingest_info.is_relationship = False

    _setup_parquet_mock(mock_pq_cls, [("id", "string")])

    result = _create_table_from_schema(engine, "Broken", ingest_info, "/f.parquet")

    assert result is False

  @patch(PQ_PARQUET_FILE)
  def test_reserved_word_primary_key_quoting(self, mock_pq_cls):
    """Primary key that is a reserved word gets backtick-quoted."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.primary_keys = ["order"]
    ingest_info.columns = ["order", "value"]
    ingest_info.is_relationship = False

    _setup_parquet_mock(mock_pq_cls, [("order", "int64"), ("value", "string")])

    result = _create_table_from_schema(engine, "OrderTable", ingest_info, "/f.parquet")

    assert result is True
    sql = engine.execute_query.call_args[0][0]
    assert "PRIMARY KEY (`order`)" in sql


# ---------------------------------------------------------------------------
# _create_relationship_table_from_schema
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCreateRelationshipTableFromSchema:
  """Test relationship table DDL generation."""

  @patch(PQ_PARQUET_FILE)
  def test_basic_relationship_creation(self, mock_pq_cls):
    """Creates REL TABLE with FROM/TO."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.from_node = "Entity"
    ingest_info.to_node = "Report"
    ingest_info.properties = None

    _setup_parquet_mock(mock_pq_cls, [("from", "string"), ("to", "string")])

    result = _create_relationship_table_from_schema(
      engine, "HAS_REPORT", ingest_info, "/f.parquet"
    )

    assert result is True
    sql = engine.execute_query.call_args[0][0]
    assert "CREATE REL TABLE IF NOT EXISTS HAS_REPORT" in sql
    assert "FROM Entity TO Report" in sql

  @patch(PQ_PARQUET_FILE)
  def test_relationship_with_properties(self, mock_pq_cls):
    """Properties are included in the DDL."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.from_node = "Entity"
    ingest_info.to_node = "Report"
    ingest_info.properties = ["weight", "date"]

    _setup_parquet_mock(
      mock_pq_cls,
      [("from", "string"), ("to", "string"), ("weight", "double"), ("date", "date32")],
    )

    result = _create_relationship_table_from_schema(
      engine, "WEIGHTED", ingest_info, "/f.parquet"
    )

    assert result is True
    sql = engine.execute_query.call_args[0][0]
    assert "weight DOUBLE" in sql
    assert "date DATE" in sql

  @patch(PQ_PARQUET_FILE)
  def test_missing_from_node(self, mock_pq_cls):
    """Returns False when from_node is missing."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.from_node = None
    ingest_info.to_node = "Report"

    _setup_parquet_mock(mock_pq_cls, [("from", "string"), ("to", "string")])

    result = _create_relationship_table_from_schema(
      engine, "BAD", ingest_info, "/f.parquet"
    )

    assert result is False

  @patch(PQ_PARQUET_FILE)
  def test_missing_to_node(self, mock_pq_cls):
    """Returns False when to_node is missing."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.from_node = "Entity"
    ingest_info.to_node = None

    _setup_parquet_mock(mock_pq_cls, [("from", "string"), ("to", "string")])

    result = _create_relationship_table_from_schema(
      engine, "BAD", ingest_info, "/f.parquet"
    )

    assert result is False

  @patch(PQ_PARQUET_FILE)
  def test_missing_from_to_columns(self, mock_pq_cls):
    """Returns False when parquet is missing 'from'/'to' columns."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.from_node = "Entity"
    ingest_info.to_node = "Report"

    _setup_parquet_mock(mock_pq_cls, [("source", "string"), ("target", "string")])

    result = _create_relationship_table_from_schema(
      engine, "MISSING_FT", ingest_info, "/f.parquet"
    )

    assert result is False

  @patch(PQ_PARQUET_FILE)
  def test_already_exists_returns_true(self, mock_pq_cls):
    """Returns True when relationship table already exists."""
    engine = MagicMock()
    engine.execute_query.side_effect = RuntimeError("already exists")
    ingest_info = MagicMock()
    ingest_info.from_node = "A"
    ingest_info.to_node = "B"
    ingest_info.properties = None

    _setup_parquet_mock(mock_pq_cls, [("from", "string"), ("to", "string")])

    result = _create_relationship_table_from_schema(
      engine, "EXISTS", ingest_info, "/f.parquet"
    )

    assert result is True

  @patch(PQ_PARQUET_FILE)
  def test_other_error_returns_false(self, mock_pq_cls):
    """Returns False on non-exists errors."""
    engine = MagicMock()
    engine.execute_query.side_effect = RuntimeError("connection lost")
    ingest_info = MagicMock()
    ingest_info.from_node = "A"
    ingest_info.to_node = "B"
    ingest_info.properties = None

    _setup_parquet_mock(mock_pq_cls, [("from", "string"), ("to", "string")])

    result = _create_relationship_table_from_schema(
      engine, "BROKEN", ingest_info, "/f.parquet"
    )

    assert result is False

  @patch(PQ_PARQUET_FILE)
  def test_reserved_word_property_quoting(self, mock_pq_cls):
    """Reserved words in property names get backtick-quoted."""
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.from_node = "A"
    ingest_info.to_node = "B"
    ingest_info.properties = ["order"]

    _setup_parquet_mock(
      mock_pq_cls, [("from", "string"), ("to", "string"), ("order", "int64")]
    )

    result = _create_relationship_table_from_schema(
      engine, "WITH_ORDER", ingest_info, "/f.parquet"
    )

    assert result is True
    sql = engine.execute_query.call_args[0][0]
    assert "`order`" in sql


# ---------------------------------------------------------------------------
# _is_valid_identifier
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestIsValidIdentifier:
  """Test SQL identifier validation for injection prevention."""

  def test_valid_simple_names(self):
    assert _is_valid_identifier("Entity") is True
    assert _is_valid_identifier("my_table") is True
    assert _is_valid_identifier("_private") is True
    assert _is_valid_identifier("Table123") is True

  def test_empty_string(self):
    assert _is_valid_identifier("") is False

  def test_none_value(self):
    assert _is_valid_identifier(None) is False

  def test_numeric_start(self):
    assert _is_valid_identifier("123abc") is False

  def test_special_characters_rejected(self):
    assert _is_valid_identifier("table name") is False
    assert _is_valid_identifier("table-name") is False
    assert _is_valid_identifier("table.name") is False
    assert _is_valid_identifier("table;drop") is False
    assert _is_valid_identifier("name@domain") is False

  def test_sql_injection_patterns(self):
    """SQL injection patterns are rejected."""
    assert _is_valid_identifier("'; DROP TABLE --") is False
    assert _is_valid_identifier("table; DELETE FROM") is False
    assert _is_valid_identifier("UNION SELECT *") is False

  def test_non_string_type(self):
    """Non-string values return False."""
    assert _is_valid_identifier(42) is False
    assert _is_valid_identifier(["list"]) is False


# ---------------------------------------------------------------------------
# _sanitize_parameter_name
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSanitizeParameterName:
  """Test parameter name sanitization."""

  def test_already_valid(self):
    assert _sanitize_parameter_name("valid_name") == "valid_name"

  def test_spaces_replaced(self):
    assert _sanitize_parameter_name("my name") == "my_name"

  def test_dashes_replaced(self):
    assert _sanitize_parameter_name("my-name") == "my_name"

  def test_dots_replaced(self):
    assert _sanitize_parameter_name("my.name") == "my_name"

  def test_leading_digit_prefixed(self):
    assert _sanitize_parameter_name("123abc") == "_123abc"

  def test_multiple_special_chars(self):
    assert _sanitize_parameter_name("a-b.c d@e") == "a_b_c_d_e"

  def test_empty_string_returns_param(self):
    assert _sanitize_parameter_name("") == "param"

  def test_all_special_chars(self):
    """All-special-char input still produces a usable name."""
    result = _sanitize_parameter_name("@#$")
    assert result.isidentifier() or result == "param"


# ---------------------------------------------------------------------------
# _map_arrow_to_lbug_type
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMapArrowToLbugType:
  """Test Arrow to LadybugDB type mapping."""

  def test_string_types(self):
    assert _map_arrow_to_lbug_type("string") == "STRING"
    assert _map_arrow_to_lbug_type("utf8") == "STRING"
    assert _map_arrow_to_lbug_type("large_string") == "STRING"

  def test_integer_types(self):
    assert _map_arrow_to_lbug_type("int64") == "INT64"
    assert _map_arrow_to_lbug_type("int32") == "INT32"
    assert _map_arrow_to_lbug_type("int16") == "INT32"
    assert _map_arrow_to_lbug_type("int8") == "INT32"

  def test_float_types(self):
    assert _map_arrow_to_lbug_type("double") == "DOUBLE"
    assert _map_arrow_to_lbug_type("float64") == "DOUBLE"
    assert _map_arrow_to_lbug_type("float") == "FLOAT"
    assert _map_arrow_to_lbug_type("float32") == "FLOAT"

  def test_boolean_type(self):
    assert _map_arrow_to_lbug_type("bool") == "BOOLEAN"

  def test_timestamp_types(self):
    assert _map_arrow_to_lbug_type("timestamp[ms]") == "TIMESTAMP"
    assert _map_arrow_to_lbug_type("timestamp[us, tz=UTC]") == "TIMESTAMP"
    assert _map_arrow_to_lbug_type("datetime64") == "TIMESTAMP"

  def test_date_type(self):
    assert _map_arrow_to_lbug_type("date32[day]") == "DATE"
    assert _map_arrow_to_lbug_type("date64") == "DATE"

  def test_decimal_maps_to_double(self):
    assert _map_arrow_to_lbug_type("decimal128(10,2)") == "DOUBLE"

  def test_unknown_type_defaults_to_string(self):
    assert _map_arrow_to_lbug_type("binary") == "STRING"
    assert _map_arrow_to_lbug_type("fixed_size_binary[16]") == "STRING"

  def test_case_insensitive(self):
    """Type matching is case-insensitive."""
    assert _map_arrow_to_lbug_type("STRING") == "STRING"
    assert _map_arrow_to_lbug_type("INT64") == "INT64"
    assert _map_arrow_to_lbug_type("Double") == "DOUBLE"
    assert _map_arrow_to_lbug_type("BOOL") == "BOOLEAN"


# ---------------------------------------------------------------------------
# _is_global_entity_schema_driven / _is_global_relationship_schema_driven
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGlobalDetection:
  """Test global entity and relationship detection from base schema."""

  def test_global_entities(self):
    """Entities in BASE_NODES are detected as global."""
    assert _is_global_entity_schema_driven("Entity") is True
    assert _is_global_entity_schema_driven("Period") is True
    assert _is_global_entity_schema_driven("Unit") is True
    assert _is_global_entity_schema_driven("Element") is True

  def test_non_global_entities(self):
    """Entities not in BASE_NODES are not global."""
    assert _is_global_entity_schema_driven("CustomNode") is False
    assert _is_global_entity_schema_driven("Person") is False
    assert _is_global_entity_schema_driven("entity_facts") is False

  def test_global_relationships(self):
    """Relationships in BASE_RELATIONSHIPS are detected as global."""
    assert _is_global_relationship_schema_driven("ENTITY_EVOLVED_FROM") is True
    assert _is_global_relationship_schema_driven("ENTITY_OWNS_ENTITY") is True
    assert _is_global_relationship_schema_driven("ELEMENT_HAS_LABEL") is True

  def test_non_global_relationships(self):
    """Relationships not in BASE_RELATIONSHIPS are not global."""
    assert _is_global_relationship_schema_driven("EMPLOYS") is False
    assert _is_global_relationship_schema_driven("custom_rel") is False

  def test_global_entity_empty_string(self):
    """Empty string is not a global entity."""
    assert _is_global_entity_schema_driven("") is False

  def test_global_relationship_empty_string(self):
    """Empty string is not a global relationship."""
    assert _is_global_relationship_schema_driven("") is False


# ---------------------------------------------------------------------------
# _copy_node_data_schema_driven
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCopyNodeDataSchemaDrivern:
  """Test COPY FROM operations for node data."""

  @patch("robosystems.operations.graph.engine.ingest._is_global_entity_schema_driven")
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_global_entity_uses_ignore_errors(self, mock_pq_cls, mock_valid, mock_global):
    """Global entities use COPY with IGNORE_ERRORS=true."""
    mock_valid.return_value = True
    mock_global.return_value = True
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.columns = ["id", "name"]
    _setup_parquet_mock(mock_pq_cls, [("id", "string")])

    result = _copy_node_data_schema_driven(engine, "/f.parquet", "Entity", ingest_info)

    assert result is True
    copy_sql = engine.execute_query.call_args[0][0]
    assert "IGNORE_ERRORS=true" in copy_sql

  @patch("robosystems.operations.graph.engine.ingest._is_global_entity_schema_driven")
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_non_global_entity_standard_copy(self, mock_pq_cls, mock_valid, mock_global):
    """Non-global entities use standard COPY without IGNORE_ERRORS."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock()
    ingest_info = MagicMock()
    ingest_info.columns = ["id"]
    _setup_parquet_mock(mock_pq_cls, [("id", "string")])

    result = _copy_node_data_schema_driven(
      engine, "/f.parquet", "CustomNode", ingest_info
    )

    assert result is True
    copy_sql = engine.execute_query.call_args[0][0]
    assert "IGNORE_ERRORS" not in copy_sql

  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  def test_invalid_table_name_rejected(self, mock_valid):
    """Returns False for invalid table names (SQL injection prevention)."""
    mock_valid.return_value = False
    engine = MagicMock()
    ingest_info = MagicMock()

    result = _copy_node_data_schema_driven(
      engine, "/f.parquet", "'; DROP TABLE --", ingest_info
    )

    assert result is False

  @patch("robosystems.operations.graph.engine.ingest._is_global_entity_schema_driven")
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_copy_failure_returns_false(self, mock_pq_cls, mock_valid, mock_global):
    """Returns False when COPY FROM fails."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock()
    engine.execute_query.side_effect = RuntimeError("COPY failed")
    ingest_info = MagicMock()
    ingest_info.columns = ["id"]
    _setup_parquet_mock(mock_pq_cls, [("id", "string")])

    result = _copy_node_data_schema_driven(engine, "/f.parquet", "Broken", ingest_info)

    assert result is False

  @patch("robosystems.operations.graph.engine.ingest._is_global_entity_schema_driven")
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_timeout_set_and_reset(self, mock_pq_cls, mock_valid, mock_global):
    """Engine query timeout is set before COPY and reset after."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock()
    engine.set_query_timeout = MagicMock()
    ingest_info = MagicMock()
    ingest_info.columns = ["id"]
    _setup_parquet_mock(mock_pq_cls, [("id", "string")])

    _copy_node_data_schema_driven(engine, "/f.parquet", "Table", ingest_info)

    # Should be called with 30-minute timeout then reset to 2 minutes
    calls = engine.set_query_timeout.call_args_list
    assert len(calls) == 2
    assert calls[0][0][0] == 1800000  # 30 minutes
    assert calls[1][0][0] == 120000  # 2 minutes

  @patch("robosystems.operations.graph.engine.ingest._is_global_entity_schema_driven")
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_timeout_reset_on_failure(self, mock_pq_cls, mock_valid, mock_global):
    """Timeout is reset even when COPY fails."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock()
    engine.set_query_timeout = MagicMock()
    engine.execute_query.side_effect = RuntimeError("COPY failed")
    ingest_info = MagicMock()
    ingest_info.columns = ["id"]
    _setup_parquet_mock(mock_pq_cls, [("id", "string")])

    _copy_node_data_schema_driven(engine, "/f.parquet", "Broken", ingest_info)

    # Timeout should still be reset in the finally block
    calls = engine.set_query_timeout.call_args_list
    assert len(calls) == 2
    assert calls[1][0][0] == 120000

  @patch("robosystems.operations.graph.engine.ingest._is_global_entity_schema_driven")
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_column_mismatch_error_logging(self, mock_pq_cls, mock_valid, mock_global):
    """Column mismatch errors log parquet columns for debugging."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock()
    engine.execute_query.side_effect = RuntimeError("column not found: missing_col")
    ingest_info = MagicMock()
    ingest_info.columns = ["id", "missing_col"]

    pq_instance = _setup_parquet_mock(mock_pq_cls, [("id", "string")])
    # The second call to ParquetFile (inside error handler) needs to work too
    field_mock = MagicMock()
    field_mock.name = "id"
    pq_instance.schema_arrow = [field_mock]

    result = _copy_node_data_schema_driven(
      engine, "/f.parquet", "MissingCol", ingest_info
    )

    assert result is False

  @patch("robosystems.operations.graph.engine.ingest._is_global_entity_schema_driven")
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_no_timeout_support(self, mock_pq_cls, mock_valid, mock_global):
    """Engine without set_query_timeout still works."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock(spec=[])  # No attributes
    engine.execute_query = MagicMock()
    ingest_info = MagicMock()
    ingest_info.columns = ["id"]
    _setup_parquet_mock(mock_pq_cls, [("id", "string")])

    result = _copy_node_data_schema_driven(
      engine, "/f.parquet", "NoTimeout", ingest_info
    )

    assert result is True


# ---------------------------------------------------------------------------
# _copy_relationship_data_schema_driven
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCopyRelationshipDataSchemaDrivern:
  """Test COPY FROM operations for relationship data."""

  @patch(
    "robosystems.operations.graph.engine.ingest._is_global_relationship_schema_driven"
  )
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_global_relationship_uses_ignore_errors(
    self, mock_pq_cls, mock_valid, mock_global
  ):
    """Global relationships use COPY with IGNORE_ERRORS=true."""
    mock_valid.return_value = True
    mock_global.return_value = True
    engine = MagicMock()
    ingest_info = MagicMock()
    adapter = MagicMock()
    _setup_parquet_mock(mock_pq_cls, [("from", "string"), ("to", "string")])

    result = _copy_relationship_data_schema_driven(
      engine, "/f.parquet", "ENTITY_EVOLVED_FROM", ingest_info, adapter
    )

    assert result is True
    copy_sql = engine.execute_query.call_args[0][0]
    assert "IGNORE_ERRORS=true" in copy_sql

  @patch(
    "robosystems.operations.graph.engine.ingest._is_global_relationship_schema_driven"
  )
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_non_global_relationship_standard_copy(
    self, mock_pq_cls, mock_valid, mock_global
  ):
    """Non-global relationships use standard COPY."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock()
    ingest_info = MagicMock()
    adapter = MagicMock()
    _setup_parquet_mock(mock_pq_cls, [("from", "string"), ("to", "string")])

    result = _copy_relationship_data_schema_driven(
      engine, "/f.parquet", "custom_rel", ingest_info, adapter
    )

    assert result is True
    copy_sql = engine.execute_query.call_args[0][0]
    assert "IGNORE_ERRORS" not in copy_sql

  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  def test_invalid_table_name_rejected(self, mock_valid):
    """Invalid table names are rejected."""
    mock_valid.return_value = False
    engine = MagicMock()
    ingest_info = MagicMock()
    adapter = MagicMock()

    result = _copy_relationship_data_schema_driven(
      engine, "/f.parquet", "bad;name", ingest_info, adapter
    )

    assert result is False

  @patch(
    "robosystems.operations.graph.engine.ingest._is_global_relationship_schema_driven"
  )
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_copy_failure_returns_false(self, mock_pq_cls, mock_valid, mock_global):
    """Returns False when COPY FROM fails."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock()
    engine.execute_query.side_effect = RuntimeError("foreign key violation")
    ingest_info = MagicMock()
    ingest_info.from_node = "A"
    ingest_info.to_node = "B"
    ingest_info.properties = None
    adapter = MagicMock()

    pq_instance = _setup_parquet_mock(
      mock_pq_cls, [("from", "string"), ("to", "string")]
    )
    field_mock = MagicMock()
    field_mock.name = "from"
    pq_instance.schema_arrow = [field_mock]

    result = _copy_relationship_data_schema_driven(
      engine, "/f.parquet", "Broken", ingest_info, adapter
    )

    assert result is False

  @patch(
    "robosystems.operations.graph.engine.ingest._is_global_relationship_schema_driven"
  )
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_timeout_set_and_reset(self, mock_pq_cls, mock_valid, mock_global):
    """Engine query timeout is set before COPY and reset after."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock()
    engine.set_query_timeout = MagicMock()
    ingest_info = MagicMock()
    adapter = MagicMock()
    _setup_parquet_mock(mock_pq_cls, [("from", "string"), ("to", "string")])

    _copy_relationship_data_schema_driven(
      engine, "/f.parquet", "Rel", ingest_info, adapter
    )

    calls = engine.set_query_timeout.call_args_list
    assert len(calls) == 2
    assert calls[0][0][0] == 1800000
    assert calls[1][0][0] == 120000

  @patch(
    "robosystems.operations.graph.engine.ingest._is_global_relationship_schema_driven"
  )
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_timeout_reset_on_failure(self, mock_pq_cls, mock_valid, mock_global):
    """Timeout is reset even when COPY fails."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock()
    engine.set_query_timeout = MagicMock()
    engine.execute_query.side_effect = RuntimeError("COPY failed")
    ingest_info = MagicMock()
    ingest_info.from_node = "A"
    ingest_info.to_node = "B"
    ingest_info.properties = None
    adapter = MagicMock()

    pq_instance = _setup_parquet_mock(
      mock_pq_cls, [("from", "string"), ("to", "string")]
    )
    field_mock = MagicMock()
    field_mock.name = "from"
    pq_instance.schema_arrow = [field_mock]

    _copy_relationship_data_schema_driven(
      engine, "/f.parquet", "Broken", ingest_info, adapter
    )

    calls = engine.set_query_timeout.call_args_list
    assert len(calls) == 2
    assert calls[1][0][0] == 120000

  @patch(
    "robosystems.operations.graph.engine.ingest._is_global_relationship_schema_driven"
  )
  @patch("robosystems.operations.graph.engine.ingest._is_valid_identifier")
  @patch(PQ_PARQUET_FILE)
  def test_no_timeout_support(self, mock_pq_cls, mock_valid, mock_global):
    """Engine without set_query_timeout still works."""
    mock_valid.return_value = True
    mock_global.return_value = False
    engine = MagicMock(spec=[])
    engine.execute_query = MagicMock()
    ingest_info = MagicMock()
    adapter = MagicMock()
    _setup_parquet_mock(mock_pq_cls, [("from", "string"), ("to", "string")])

    result = _copy_relationship_data_schema_driven(
      engine, "/f.parquet", "NoTimeout", ingest_info, adapter
    )

    assert result is True
