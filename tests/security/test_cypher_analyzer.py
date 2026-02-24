"""Cypher security analyzer tests."""

import pytest

from robosystems.security.cypher_analyzer import (
  CypherOperationType,
  CypherSecurityAnalyzer,
  analyze_cypher_query,
  has_system_calls,
  is_admin_operation,
  is_bulk_operation,
  is_schema_ddl,
  is_write_operation,
)


@pytest.fixture
def analyzer():
  return CypherSecurityAnalyzer()


class TestAnalyzeQuery:
  """Tests for CypherSecurityAnalyzer.analyze_query."""

  def test_read_only_query(self, analyzer):
    result = analyzer.analyze_query("MATCH (n:Entity) RETURN n LIMIT 10")
    assert result == CypherOperationType.READ

  def test_write_query_create(self, analyzer):
    result = analyzer.analyze_query("CREATE (n:Entity {name: 'test'})")
    assert result in (CypherOperationType.WRITE, CypherOperationType.MIXED)

  def test_write_query_merge(self, analyzer):
    result = analyzer.analyze_query("MERGE (n:Entity {name: 'test'})")
    assert result in (CypherOperationType.WRITE, CypherOperationType.MIXED)

  def test_write_query_delete(self, analyzer):
    result = analyzer.analyze_query("MATCH (n) DELETE n")
    assert result in (CypherOperationType.WRITE, CypherOperationType.MIXED)

  def test_write_query_set(self, analyzer):
    result = analyzer.analyze_query("MATCH (n:Entity) SET n.name = 'updated'")
    assert result == CypherOperationType.MIXED

  def test_mixed_query(self, analyzer):
    result = analyzer.analyze_query(
      "MATCH (n:Entity) WHERE n.name = 'test' SET n.active = true RETURN n"
    )
    assert result == CypherOperationType.MIXED

  def test_empty_query_raises(self, analyzer):
    with pytest.raises(ValueError, match="non-empty string"):
      analyzer.analyze_query("")

  def test_none_query_raises(self, analyzer):
    with pytest.raises(ValueError, match="non-empty string"):
      analyzer.analyze_query(None)

  def test_detach_delete(self, analyzer):
    result = analyzer.analyze_query("MATCH (n) DETACH DELETE n")
    assert result in (CypherOperationType.WRITE, CypherOperationType.MIXED)


class TestIsWriteOperation:
  """Tests for is_write_operation convenience function."""

  def test_read_query(self):
    assert is_write_operation("MATCH (n) RETURN n") is False

  def test_write_query(self):
    assert is_write_operation("CREATE (n:Test)") is True

  def test_invalid_query_defaults_to_write(self):
    # Empty string should default to True for security
    assert is_write_operation("") is True


class TestIsSchemaDD:
  """Tests for schema DDL detection."""

  def test_create_node_table(self, analyzer):
    assert analyzer.is_schema_ddl(
      "CREATE NODE TABLE Person(name STRING, PRIMARY KEY(name))"
    )

  def test_create_rel_table(self, analyzer):
    assert analyzer.is_schema_ddl("CREATE REL TABLE KNOWS(FROM Person TO Person)")

  def test_drop_node_table(self, analyzer):
    assert analyzer.is_schema_ddl("DROP NODE TABLE Person")

  def test_alter_table(self, analyzer):
    assert analyzer.is_schema_ddl("ALTER TABLE Person ADD COLUMN age INT64")

  def test_rename_table(self, analyzer):
    assert analyzer.is_schema_ddl("RENAME TABLE Person TO Employee")

  def test_non_ddl_query(self, analyzer):
    assert analyzer.is_schema_ddl("MATCH (n:Person) RETURN n") is False

  def test_module_level_function(self):
    assert is_schema_ddl("CREATE NODE TABLE Test(id STRING, PRIMARY KEY(id))")


class TestIsBulkOperation:
  """Tests for bulk operation detection."""

  def test_copy_operation(self, analyzer):
    assert analyzer.is_bulk_operation("COPY Person FROM 'file.csv'")

  def test_load_operation(self, analyzer):
    assert analyzer.is_bulk_operation("LOAD FROM 'data.parquet'")

  def test_import_operation(self, analyzer):
    assert analyzer.is_bulk_operation("IMPORT DATABASE 'backup.db'")

  def test_non_bulk_query(self, analyzer):
    assert analyzer.is_bulk_operation("MATCH (n) RETURN n") is False

  def test_module_level_function(self):
    assert is_bulk_operation("COPY Entity FROM 'file.csv'")


class TestIsAdminOperation:
  """Tests for admin operation detection."""

  def test_export_operation(self, analyzer):
    assert analyzer.is_admin_operation("EXPORT DATABASE 'backup.db'")

  def test_attach_operation(self, analyzer):
    assert analyzer.is_admin_operation("ATTACH 'other.db' AS other")

  def test_use_operation(self, analyzer):
    assert analyzer.is_admin_operation("USE other_db")

  def test_non_admin_query(self, analyzer):
    assert analyzer.is_admin_operation("MATCH (n) RETURN n") is False

  def test_module_level_function(self):
    assert is_admin_operation("EXPORT DATABASE 'test'")


class TestHasSystemCalls:
  """Tests for system call detection."""

  def test_show_tables(self, analyzer):
    assert analyzer.has_system_calls("CALL show_tables() RETURN *")

  def test_table_info(self, analyzer):
    assert analyzer.has_system_calls("CALL table_info('Entity') RETURN *")

  def test_current_setting(self, analyzer):
    assert analyzer.has_system_calls("CALL current_setting('threads') RETURN *")

  def test_db_version(self, analyzer):
    assert analyzer.has_system_calls("CALL db_version() RETURN *")

  def test_non_system_call(self, analyzer):
    assert analyzer.has_system_calls("MATCH (n) RETURN n") is False

  def test_module_level_function(self):
    assert has_system_calls("CALL show_tables() RETURN *")


class TestQuerySecurityValidation:
  """Tests for query security validation."""

  def test_excessively_long_query(self, analyzer):
    long_query = "MATCH (n) RETURN n " + " " * 200000
    with pytest.raises(ValueError, match="maximum allowed length"):
      analyzer.analyze_query(long_query)

  def test_unbalanced_comments(self, analyzer):
    with pytest.raises(ValueError, match="Unbalanced comment"):
      analyzer.analyze_query("/* unclosed comment MATCH (n) RETURN n")

  def test_injection_create_user(self, analyzer):
    with pytest.raises(ValueError, match="dangerous"):
      analyzer.analyze_query("MATCH (n) RETURN n; CREATE USER admin")

  def test_injection_drop_database(self, analyzer):
    with pytest.raises(ValueError, match="dangerous"):
      analyzer.analyze_query("MATCH (n) RETURN n; DROP DATABASE test")


class TestCleanQuery:
  """Tests for query cleaning (comment/string removal)."""

  def test_removes_line_comments(self, analyzer):
    cleaned = analyzer._clean_query("MATCH (n) // this is a comment\nRETURN n")
    assert "//" not in cleaned

  def test_removes_block_comments(self, analyzer):
    cleaned = analyzer._clean_query("MATCH (n) /* comment */ RETURN n")
    assert "/*" not in cleaned
    assert "*/" not in cleaned

  def test_removes_string_literals(self, analyzer):
    cleaned = analyzer._clean_query("MATCH (n) WHERE n.name = 'CREATE' RETURN n")
    assert "STRING_LITERAL" in cleaned

  def test_write_in_string_not_detected(self, analyzer):
    # CREATE inside a string should NOT be detected as a write operation
    result = analyzer.analyze_query(
      "MATCH (n) WHERE n.name = 'CREATE something' RETURN n"
    )
    assert result == CypherOperationType.READ

  def test_write_in_comment_not_detected(self, analyzer):
    result = analyzer.analyze_query("MATCH (n) /* CREATE (m:Node) */ RETURN n")
    assert result == CypherOperationType.READ


class TestGetWriteOperationDetails:
  """Tests for get_write_operation_details."""

  def test_read_query_details(self, analyzer):
    details = analyzer.get_write_operation_details("MATCH (n:Entity) RETURN n")
    assert details["operation_type"] == "read"
    assert details["is_write_operation"] is False
    assert details["analysis_successful"] is True

  def test_write_query_details(self, analyzer):
    details = analyzer.get_write_operation_details("CREATE (n:Entity {name: 'test'})")
    assert details["is_write_operation"] is True
    assert "CREATE" in details["write_keywords_found"]

  def test_invalid_query_details(self, analyzer):
    details = analyzer.get_write_operation_details("")
    assert details["analysis_successful"] is False
    assert details["is_write_operation"] is True  # Safe default

  def test_module_level_function(self):
    details = analyze_cypher_query("MATCH (n) RETURN n")
    assert details["operation_type"] == "read"


class TestKeywordContextValidation:
  """Tests for _validate_keyword_context."""

  def test_keyword_not_part_of_identifier(self, analyzer):
    # "SET" should not match inside "DATASET"
    result = analyzer.analyze_query("MATCH (n:DATASET) RETURN n")
    assert result == CypherOperationType.READ

  def test_keyword_not_part_of_prefix(self, analyzer):
    # "CREATE" should not match inside "RECREATE"
    result = analyzer.analyze_query("MATCH (n) WHERE n.name = 'RECREATED' RETURN n")
    assert result == CypherOperationType.READ
