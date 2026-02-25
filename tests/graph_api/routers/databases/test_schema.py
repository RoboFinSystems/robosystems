"""Tests for database schema router endpoints."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.routers.databases import schema as schema_module
from robosystems.graph_api.routers.databases.schema import (
  escape_identifier,
  validate_ddl_statement,
)

MODULE = "robosystems.graph_api.routers.databases.schema"


class FakeConnection:
  """Fake database connection for testing."""

  def __init__(self, fail_on_statement=None, fail_on_rollback=False):
    self.executed = []
    self._fail_on_statement = fail_on_statement
    self._fail_on_rollback = fail_on_rollback

  def execute(self, statement):
    self.executed.append(statement)
    if self._fail_on_statement and self._fail_on_statement in statement:
      raise RuntimeError(f"Execution failed for: {statement}")
    if self._fail_on_rollback and statement == "ROLLBACK":
      raise RuntimeError("Rollback failed")


class FakeConnectionManager:
  """Context manager that returns a FakeConnection."""

  def __init__(self, conn):
    self.conn = conn

  def __enter__(self):
    return self.conn

  def __exit__(self, exc_type, exc_val, exc_tb):
    return False


class FakeDBManager:
  """Fake database manager for testing."""

  def __init__(self, databases=None, connection=None):
    self._databases = databases or []
    self._connection = connection

  def list_databases(self):
    return list(self._databases)

  def get_connection(self, graph_id, read_only=False):
    return FakeConnectionManager(self._connection)


class FakeLadybugService:
  """Fake LadybugDB service for testing."""

  def __init__(
    self, read_only=False, databases=None, connection=None, query_result=None
  ):
    self.read_only = read_only
    self.db_manager = FakeDBManager(databases, connection)
    self._query_result = query_result

  def execute_query(self, request):
    if self._query_result is not None:
      return self._query_result
    return SimpleNamespace(data=[])


@pytest.fixture
def schema_client():
  """Create TestClient with schema router."""

  def _factory(ladybug_service=None):
    app = FastAPI()
    app.include_router(schema_module.router)
    svc = ladybug_service or FakeLadybugService()
    app.dependency_overrides[get_ladybug_service] = lambda: svc
    return TestClient(app)

  return _factory


class TestInstallSchema:
  """Test cases for POST /databases/{graph_id}/schema endpoint."""

  @pytest.mark.unit
  def test_install_schema_success(self, schema_client):
    """Test successful schema installation."""
    conn = FakeConnection()
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
      connection=conn,
    )
    client = schema_client(svc)

    response = client.post(
      "/databases/testgraph/schema",
      json={
        "type": "custom",
        "ddl": "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
      },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["statements_executed"] == 1
    assert "successfully" in data["message"]

  @pytest.mark.unit
  def test_install_schema_multiple_statements(self, schema_client):
    """Test schema installation with multiple DDL statements."""
    conn = FakeConnection()
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
      connection=conn,
    )
    client = schema_client(svc)

    ddl = (
      "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id));"
      "CREATE NODE TABLE Company (id INT64, name STRING, PRIMARY KEY (id));"
      "CREATE REL TABLE WORKS_AT (FROM Person TO Company)"
    )

    response = client.post(
      "/databases/testgraph/schema",
      json={"type": "ddl", "ddl": ddl},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True
    assert data["statements_executed"] == 3

  @pytest.mark.unit
  def test_install_schema_read_only_rejected(self, schema_client):
    """Test that schema installation is rejected on read-only nodes."""
    svc = FakeLadybugService(read_only=True)
    client = schema_client(svc)

    response = client.post(
      "/databases/testgraph/schema",
      json={
        "type": "custom",
        "ddl": "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))",
      },
    )

    assert response.status_code == 403
    assert "read-only" in response.json()["detail"]

  @pytest.mark.unit
  def test_install_schema_database_not_found(self, schema_client):
    """Test that schema installation fails when database not found."""
    svc = FakeLadybugService(
      read_only=False,
      databases=["other_db"],
    )
    client = schema_client(svc)

    response = client.post(
      "/databases/nonexistent/schema",
      json={
        "type": "custom",
        "ddl": "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))",
      },
    )

    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

  @pytest.mark.unit
  def test_install_schema_invalid_type(self, schema_client):
    """Test that invalid schema type returns error."""
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
    )
    client = schema_client(svc)

    response = client.post(
      "/databases/testgraph/schema",
      json={
        "type": "invalid_type",
        "ddl": "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))",
      },
    )

    assert response.status_code == 400
    assert "must be 'custom' or 'ddl'" in response.json()["detail"]

  @pytest.mark.unit
  def test_install_schema_missing_ddl(self, schema_client):
    """Test that missing DDL returns error."""
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
    )
    client = schema_client(svc)

    response = client.post(
      "/databases/testgraph/schema",
      json={"type": "custom"},
    )

    assert response.status_code == 400
    assert "DDL commands are required" in response.json()["detail"]

  @pytest.mark.unit
  def test_install_schema_dangerous_ddl_blocked(self, schema_client):
    """Test that dangerous DDL statements are blocked.

    Note: The HTTPException raised for dangerous DDL is caught by the outer
    try/except, so it returns 200 with success=False rather than a 400 status.
    """
    conn = FakeConnection()
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
      connection=conn,
    )
    client = schema_client(svc)

    response = client.post(
      "/databases/testgraph/schema",
      json={
        "type": "ddl",
        "ddl": "DROP TABLE Person",
      },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert data["statements_executed"] == 0
    assert (
      "forbidden operations" in data["message"] or "failed" in data["message"].lower()
    )

  @pytest.mark.unit
  def test_install_schema_statement_execution_failure(self, schema_client):
    """Test that a failed DDL statement returns appropriate response."""
    conn = FakeConnection(fail_on_statement="CREATE NODE TABLE Broken")
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
      connection=conn,
    )
    client = schema_client(svc)

    response = client.post(
      "/databases/testgraph/schema",
      json={
        "type": "custom",
        "ddl": "CREATE NODE TABLE Broken (id INT64, PRIMARY KEY (id))",
      },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert data["statements_executed"] == 0  # Rolled back
    assert "Failed to execute" in data["message"]

  @pytest.mark.unit
  def test_install_schema_with_metadata(self, schema_client):
    """Test schema installation with metadata logging."""
    conn = FakeConnection()
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
      connection=conn,
    )
    client = schema_client(svc)

    response = client.post(
      "/databases/testgraph/schema",
      json={
        "type": "custom",
        "ddl": "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))",
        "metadata": {"schema_name": "test", "version": "1.0"},
      },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True

  @pytest.mark.unit
  def test_install_schema_transaction_commit(self, schema_client):
    """Test that successful schema installation uses proper transaction."""
    conn = FakeConnection()
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
      connection=conn,
    )
    client = schema_client(svc)

    response = client.post(
      "/databases/testgraph/schema",
      json={
        "type": "custom",
        "ddl": "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))",
      },
    )

    assert response.status_code == 200
    # Verify transaction lifecycle
    assert "BEGIN TRANSACTION" in conn.executed
    assert "COMMIT" in conn.executed

  @pytest.mark.unit
  def test_install_schema_transaction_rollback_on_failure(self, schema_client):
    """Test that failed schema installation rolls back transaction."""
    conn = FakeConnection(fail_on_statement="CREATE NODE TABLE Broken")
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
      connection=conn,
    )
    client = schema_client(svc)

    response = client.post(
      "/databases/testgraph/schema",
      json={
        "type": "custom",
        "ddl": "CREATE NODE TABLE Broken (id INT64, PRIMARY KEY (id))",
      },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    # Verify rollback was attempted
    assert "ROLLBACK" in conn.executed
    assert "COMMIT" not in conn.executed

  @pytest.mark.unit
  def test_install_schema_mixed_valid_and_dangerous(self, schema_client):
    """Test that mixing valid and dangerous DDL blocks all execution.

    All statements are validated before any execution. When a dangerous
    statement is found, the HTTPException is caught by the outer try/except
    and returned as success=False.
    """
    conn = FakeConnection()
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
      connection=conn,
    )
    client = schema_client(svc)

    ddl = "CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id));DROP TABLE Person"

    response = client.post(
      "/databases/testgraph/schema",
      json={"type": "ddl", "ddl": ddl},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is False
    assert data["statements_executed"] == 0

  @pytest.mark.unit
  def test_install_schema_backtick_quoted_identifiers(self, schema_client):
    """Test schema installation with backtick-quoted identifiers."""
    conn = FakeConnection()
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
      connection=conn,
    )
    client = schema_client(svc)

    response = client.post(
      "/databases/testgraph/schema",
      json={
        "type": "custom",
        "ddl": "CREATE NODE TABLE IF NOT EXISTS `Decision`(`identifier` STRING, PRIMARY KEY(`identifier`))",
      },
    )

    assert response.status_code == 200
    data = response.json()
    assert data["success"] is True


class TestGetSchema:
  """Test cases for GET /databases/{graph_id}/schema endpoint."""

  @pytest.mark.unit
  def test_get_schema_success(self, schema_client):
    """Test successful schema retrieval."""
    # Create mock result with table data
    show_tables_result = SimpleNamespace(
      data=[
        {"id": 0, "name": "Person", "type": "NODE", "comment": "People"},
        {"id": 1, "name": "FOLLOWS", "type": "REL", "comment": "Follow relations"},
      ]
    )

    # TABLE_INFO will be called for each table
    table_info_result = SimpleNamespace(data=[])

    call_count = [0]

    def mock_execute_query(request):
      if "SHOW_TABLES" in request.cypher:
        return show_tables_result
      call_count[0] += 1
      return table_info_result

    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
    )
    svc.execute_query = mock_execute_query
    client = schema_client(svc)

    response = client.get("/databases/testgraph/schema")

    assert response.status_code == 200
    data = response.json()
    assert data["database"] == "testgraph"
    assert len(data["tables"]) == 2
    assert "Person" in data["node_tables"]
    assert "FOLLOWS" in data["rel_tables"]

  @pytest.mark.unit
  def test_get_schema_database_not_found(self, schema_client):
    """Test schema retrieval for non-existent database."""
    svc = FakeLadybugService(
      read_only=False,
      databases=["other_db"],
    )
    client = schema_client(svc)

    response = client.get("/databases/nonexistent/schema")

    assert response.status_code == 404
    assert "not found" in response.json()["detail"]

  @pytest.mark.unit
  def test_get_schema_empty_database(self, schema_client):
    """Test schema retrieval for database with no tables."""
    empty_result = SimpleNamespace(data=[])

    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
      query_result=empty_result,
    )
    client = schema_client(svc)

    response = client.get("/databases/testgraph/schema")

    assert response.status_code == 200
    data = response.json()
    assert data["database"] == "testgraph"
    assert data["tables"] == []
    assert data["node_tables"] == []
    assert data["rel_tables"] == []

  @pytest.mark.unit
  def test_get_schema_with_table_properties(self, schema_client):
    """Test schema retrieval includes table properties."""
    show_tables_result = SimpleNamespace(
      data=[
        {"id": 0, "name": "Person", "type": "NODE", "comment": ""},
      ]
    )

    # TABLE_INFO returns properties as dict with list values
    table_info_result = SimpleNamespace(
      data=[
        {"col": [0, "id", "INT64", None, True]},
        {"col": [1, "name", "STRING", None, False]},
      ]
    )

    def mock_execute_query(request):
      if "SHOW_TABLES" in request.cypher:
        return show_tables_result
      return table_info_result

    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
    )
    svc.execute_query = mock_execute_query
    client = schema_client(svc)

    response = client.get("/databases/testgraph/schema")

    assert response.status_code == 200
    data = response.json()
    person_table = data["tables"][0]
    assert person_table["name"] == "Person"
    assert len(person_table["properties"]) == 2
    assert person_table["properties"][0]["name"] == "id"
    assert person_table["properties"][0]["type"] == "INT64"
    assert person_table["properties"][1]["name"] == "name"
    assert person_table["properties"][1]["type"] == "STRING"

  @pytest.mark.unit
  def test_get_schema_with_list_properties(self, schema_client):
    """Test schema retrieval when TABLE_INFO returns list rows."""
    show_tables_result = SimpleNamespace(
      data=[
        {"id": 0, "name": "Company", "type": "NODE", "comment": ""},
      ]
    )

    # TABLE_INFO returns properties as plain lists
    table_info_result = SimpleNamespace(
      data=[
        [0, "id", "INT64", None, True],
        [1, "revenue", "DOUBLE", None, False],
      ]
    )

    def mock_execute_query(request):
      if "SHOW_TABLES" in request.cypher:
        return show_tables_result
      return table_info_result

    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
    )
    svc.execute_query = mock_execute_query
    client = schema_client(svc)

    response = client.get("/databases/testgraph/schema")

    assert response.status_code == 200
    data = response.json()
    company_table = data["tables"][0]
    assert len(company_table["properties"]) == 2
    assert company_table["properties"][0]["name"] == "id"
    assert company_table["properties"][1]["name"] == "revenue"
    assert company_table["properties"][1]["type"] == "DOUBLE"

  @pytest.mark.unit
  def test_get_schema_table_info_failure_graceful(self, schema_client):
    """Test that TABLE_INFO failure for a table is handled gracefully."""
    show_tables_result = SimpleNamespace(
      data=[
        {"id": 0, "name": "GoodTable", "type": "NODE", "comment": ""},
        {"id": 1, "name": "BadTable", "type": "NODE", "comment": ""},
      ]
    )

    call_count = [0]

    def mock_execute_query(request):
      if "SHOW_TABLES" in request.cypher:
        return show_tables_result
      call_count[0] += 1
      if call_count[0] == 2:  # Second TABLE_INFO call
        raise RuntimeError("Cannot get info for BadTable")
      return SimpleNamespace(data=[])

    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
    )
    svc.execute_query = mock_execute_query
    client = schema_client(svc)

    response = client.get("/databases/testgraph/schema")

    assert response.status_code == 200
    data = response.json()
    # Both tables should be present; BadTable has empty properties
    assert len(data["tables"]) == 2
    bad_table = data["tables"][1]
    assert bad_table["name"] == "BadTable"
    assert bad_table["properties"] == []

  @pytest.mark.unit
  def test_get_schema_warming_up(self, schema_client):
    """Test schema retrieval during replica warmup returns 503."""
    svc = FakeLadybugService(
      read_only=False,
      databases=[],  # Database not yet available
    )
    client = schema_client(svc)

    with patch("robosystems.graph_api.routers.health.is_warming_up", return_value=True):
      response = client.get("/databases/testgraph/schema")

    assert response.status_code == 503
    data = response.json()
    assert "warming" in str(data["detail"])

  @pytest.mark.unit
  def test_get_schema_not_found_not_warming(self, schema_client):
    """Test schema retrieval returns 404 when not warming up."""
    svc = FakeLadybugService(
      read_only=False,
      databases=["other_db"],
    )
    client = schema_client(svc)

    with patch(
      "robosystems.graph_api.routers.health.is_warming_up", return_value=False
    ):
      response = client.get("/databases/testgraph/schema")

    assert response.status_code == 404

  @pytest.mark.unit
  def test_get_schema_query_exception(self, schema_client):
    """Test that query exception returns 500."""

    def failing_execute_query(request):
      raise RuntimeError("LadybugDB internal error")

    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
    )
    svc.execute_query = failing_execute_query
    client = schema_client(svc)

    response = client.get("/databases/testgraph/schema")

    assert response.status_code == 500
    assert "Failed to retrieve schema" in response.json()["detail"]

  @pytest.mark.unit
  def test_get_schema_path_traversal_blocked(self, schema_client):
    """Test that path traversal in graph_id is blocked.

    FastAPI may reject the route before our validation (404) or
    our validation catches it (400). Both are acceptable.
    """
    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
    )
    client = schema_client(svc)

    response = client.get("/databases/..%2Fetc%2Fpasswd/schema")
    # Either 400 (our validation) or 404 (FastAPI routing) is acceptable
    assert response.status_code in (400, 404)

  @pytest.mark.unit
  def test_get_schema_categorizes_table_types(self, schema_client):
    """Test that tables are correctly categorized into node and rel tables."""
    show_tables_result = SimpleNamespace(
      data=[
        {"id": 0, "name": "Person", "type": "NODE", "comment": ""},
        {"id": 1, "name": "Company", "type": "NODE", "comment": ""},
        {"id": 2, "name": "WORKS_AT", "type": "REL", "comment": ""},
        {"id": 3, "name": "FOLLOWS", "type": "REL", "comment": ""},
      ]
    )

    def mock_execute_query(request):
      if "SHOW_TABLES" in request.cypher:
        return show_tables_result
      return SimpleNamespace(data=[])

    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
    )
    svc.execute_query = mock_execute_query
    client = schema_client(svc)

    response = client.get("/databases/testgraph/schema")

    assert response.status_code == 200
    data = response.json()
    assert data["node_tables"] == ["Person", "Company"]
    assert data["rel_tables"] == ["WORKS_AT", "FOLLOWS"]

  @pytest.mark.unit
  def test_get_schema_invalid_table_name_skipped(self, schema_client):
    """Test that invalid table names are handled gracefully during TABLE_INFO."""
    show_tables_result = SimpleNamespace(
      data=[
        {"id": 0, "name": "valid_table", "type": "NODE", "comment": ""},
        {"id": 1, "name": "123invalid", "type": "NODE", "comment": ""},
      ]
    )

    def mock_execute_query(request):
      if "SHOW_TABLES" in request.cypher:
        return show_tables_result
      return SimpleNamespace(data=[])

    svc = FakeLadybugService(
      read_only=False,
      databases=["testgraph"],
    )
    svc.execute_query = mock_execute_query
    client = schema_client(svc)

    response = client.get("/databases/testgraph/schema")

    assert response.status_code == 200
    data = response.json()
    # Both tables should be present
    assert len(data["tables"]) == 2
    # The invalid-named table should have empty properties due to ValueError
    invalid_table = data["tables"][1]
    assert invalid_table["properties"] == []


class TestValidateDDLStatement:
  """Tests for the validate_ddl_statement helper function."""

  @pytest.mark.unit
  def test_create_node_table_allowed(self):
    """Test that CREATE NODE TABLE is allowed."""
    assert (
      validate_ddl_statement("CREATE NODE TABLE Person (id INT64, PRIMARY KEY (id))")
      is True
    )

  @pytest.mark.unit
  def test_create_rel_table_allowed(self):
    """Test that CREATE REL TABLE is allowed."""
    assert (
      validate_ddl_statement("CREATE REL TABLE FOLLOWS (FROM Person TO Person)") is True
    )

  @pytest.mark.unit
  def test_create_index_allowed(self):
    """Test that CREATE INDEX is allowed."""
    assert validate_ddl_statement("CREATE INDEX idx_name ON Person(name)") is True

  @pytest.mark.unit
  def test_comment_on_table_allowed(self):
    """Test that COMMENT ON TABLE is allowed."""
    assert validate_ddl_statement("COMMENT ON TABLE Person IS 'User profiles'") is True

  @pytest.mark.unit
  def test_if_not_exists_allowed(self):
    """Test that IF NOT EXISTS clause is allowed."""
    assert (
      validate_ddl_statement(
        "CREATE NODE TABLE IF NOT EXISTS Person (id INT64, PRIMARY KEY (id))"
      )
      is True
    )

  @pytest.mark.unit
  def test_drop_table_blocked(self):
    """Test that DROP TABLE is blocked."""
    assert validate_ddl_statement("DROP TABLE Person") is False

  @pytest.mark.unit
  def test_alter_table_blocked(self):
    """Test that ALTER TABLE is blocked."""
    assert validate_ddl_statement("ALTER TABLE Person ADD COLUMN age INT") is False

  @pytest.mark.unit
  def test_truncate_blocked(self):
    """Test that TRUNCATE is blocked."""
    assert validate_ddl_statement("TRUNCATE TABLE Person") is False

  @pytest.mark.unit
  def test_delete_from_blocked(self):
    """Test that DELETE FROM is blocked."""
    assert validate_ddl_statement("DELETE FROM Person WHERE id = 1") is False

  @pytest.mark.unit
  def test_update_blocked(self):
    """Test that UPDATE is blocked."""
    assert validate_ddl_statement("UPDATE Person SET name = 'hacked'") is False

  @pytest.mark.unit
  def test_grant_blocked(self):
    """Test that GRANT is blocked."""
    assert validate_ddl_statement("GRANT ALL ON Person TO public") is False

  @pytest.mark.unit
  def test_load_csv_blocked(self):
    """Test that LOAD CSV is blocked."""
    assert validate_ddl_statement("LOAD CSV FROM '/etc/passwd' AS line") is False

  @pytest.mark.unit
  def test_copy_from_blocked(self):
    """Test that COPY FROM is blocked."""
    assert validate_ddl_statement("COPY Person FROM '/tmp/data.csv'") is False

  @pytest.mark.unit
  def test_system_call_blocked(self):
    """Test that system calls are blocked."""
    assert validate_ddl_statement("CALL dbms.listUsers()") is False

  @pytest.mark.unit
  def test_unrecognized_statement_blocked(self):
    """Test that unrecognized statements are blocked."""
    assert validate_ddl_statement("SELECT * FROM Person") is False
    assert validate_ddl_statement("MATCH (n) RETURN n") is False

  @pytest.mark.unit
  def test_case_insensitive(self):
    """Test validation is case-insensitive."""
    assert validate_ddl_statement("create node table Test (id INT64)") is True
    assert validate_ddl_statement("drop table Test") is False

  @pytest.mark.unit
  def test_whitespace_handling(self):
    """Test validation handles leading/trailing whitespace."""
    assert validate_ddl_statement("  CREATE NODE TABLE Test (id INT64)  ") is True
    assert validate_ddl_statement("  DROP TABLE Test  ") is False


class TestEscapeIdentifier:
  """Tests for the escape_identifier helper function."""

  @pytest.mark.unit
  def test_valid_simple_identifier(self):
    """Test valid simple identifiers."""
    assert escape_identifier("Person") == "Person"
    assert escape_identifier("my_table") == "my_table"
    assert escape_identifier("Table123") == "Table123"
    assert escape_identifier("_private") == "_private"

  @pytest.mark.unit
  def test_strips_quotes(self):
    """Test that surrounding quotes are stripped."""
    assert escape_identifier("'Person'") == "Person"
    assert escape_identifier('"Person"') == "Person"

  @pytest.mark.unit
  def test_strips_whitespace(self):
    """Test that surrounding whitespace is stripped."""
    assert escape_identifier("  Person  ") == "Person"

  @pytest.mark.unit
  def test_rejects_starting_with_number(self):
    """Test that identifiers starting with numbers are rejected."""
    with pytest.raises(ValueError):
      escape_identifier("123Table")

  @pytest.mark.unit
  def test_rejects_special_characters(self):
    """Test that special characters are rejected."""
    invalid = ["table-name", "table name", "table.name", "table;name", "table'name"]
    for ident in invalid:
      with pytest.raises(ValueError):
        escape_identifier(ident)

  @pytest.mark.unit
  def test_rejects_empty_string(self):
    """Test that empty string is rejected."""
    with pytest.raises(ValueError):
      escape_identifier("")

  @pytest.mark.unit
  def test_rejects_sql_injection(self):
    """Test that SQL injection attempts are rejected."""
    with pytest.raises(ValueError):
      escape_identifier("Person'; DROP TABLE User; --")

    with pytest.raises(ValueError):
      escape_identifier("Person UNION SELECT * FROM passwords")
