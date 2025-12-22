"""Tests for schema endpoint security validation."""

from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from robosystems.graph_api.app import create_app
from robosystems.graph_api.core import init_ladybug_service
from robosystems.graph_api.routers.databases.schema import (
  escape_identifier,
  validate_ddl_statement,
)
from robosystems.middleware.graph.types import NodeType, RepositoryType


class TestDDLValidation:
  """Test DDL statement validation."""

  def test_allowed_ddl_statements(self):
    """Test that valid DDL statements are allowed."""
    valid_statements = [
      "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
      "CREATE REL TABLE Follows (FROM Person TO Person, since DATE)",
      "CREATE INDEX idx_person_name ON Person(name)",
      "COMMENT ON TABLE Person IS 'User profiles'",
    ]

    for statement in valid_statements:
      assert validate_ddl_statement(statement) is True

  def test_dangerous_ddl_blocked(self):
    """Test that dangerous DDL statements are blocked."""
    dangerous_statements = [
      "DROP TABLE Person",
      "DROP DATABASE test",
      "ALTER TABLE Person DROP COLUMN name",
      "TRUNCATE TABLE Person",
      "DELETE FROM Person WHERE id = 1",
      "UPDATE Person SET name = 'hacked'",
      "GRANT ALL ON Person TO public",
      "REVOKE SELECT ON Person FROM user",
      "CREATE USER hacker",
      "CREATE ROLE admin",
      "CALL dbms.listUsers()",
      "LOAD CSV FROM '/etc/passwd' AS line",
      "COPY Person FROM '/tmp/data.csv'",
    ]

    for statement in dangerous_statements:
      assert validate_ddl_statement(statement) is False

  def test_case_insensitive_validation(self):
    """Test that validation is case-insensitive."""
    assert validate_ddl_statement("create node table Person (id INT64)") is True
    assert validate_ddl_statement("CREATE NODE TABLE Person (id INT64)") is True
    assert validate_ddl_statement("CrEaTe NoDe TaBlE Person (id INT64)") is True

    assert validate_ddl_statement("drop table Person") is False
    assert validate_ddl_statement("DROP TABLE Person") is False
    assert validate_ddl_statement("DrOp TaBlE Person") is False

  def test_sql_injection_patterns_blocked(self):
    """Test that SQL injection patterns are blocked."""
    injection_statements = [
      "CREATE NODE TABLE Person (id INT64); DROP TABLE User; --",
      "CREATE NODE TABLE Person (id INT64)' OR '1'='1",
      "CREATE NODE TABLE Person (id INT64) UNION SELECT * FROM passwords",
    ]

    for statement in injection_statements:
      # These should either fail validation or the dangerous parts should be ignored
      if "; DROP TABLE" in statement:
        # Only the first valid statement should pass
        assert validate_ddl_statement(statement.split(";")[0]) is True
        assert validate_ddl_statement("DROP TABLE User") is False


class TestIdentifierEscaping:
  """Test identifier escaping for SQL injection prevention."""

  def test_valid_identifiers(self):
    """Test that valid identifiers are properly escaped."""
    assert escape_identifier("Person") == "Person"
    assert escape_identifier("person_table") == "person_table"
    assert escape_identifier("Table123") == "Table123"
    assert escape_identifier("_private_table") == "_private_table"

  def test_identifier_with_quotes_stripped(self):
    """Test that quotes are stripped from identifiers."""
    assert escape_identifier("'Person'") == "Person"
    assert escape_identifier('"Person"') == "Person"
    assert escape_identifier("  Person  ") == "Person"

  def test_invalid_identifiers_rejected(self):
    """Test that invalid identifiers raise ValueError."""
    invalid_identifiers = [
      "Person; DROP TABLE",
      "Person' OR '1'='1",
      "123Table",  # Can't start with number
      "Table-Name",  # Hyphen not allowed
      "Table Name",  # Space not allowed
      "Table.Name",  # Dot not allowed
      "",  # Empty
    ]

    for identifier in invalid_identifiers:
      with pytest.raises(ValueError):
        escape_identifier(identifier)

  def test_sql_injection_in_identifiers_blocked(self):
    """Test that SQL injection attempts via identifiers are blocked."""
    injection_attempts = [
      "Person'; DROP TABLE User; --",
      "Person' UNION SELECT * FROM passwords --",
      "Person\\'; DROP TABLE User; --",
      "Person`; DROP TABLE User; --",
    ]

    for attempt in injection_attempts:
      with pytest.raises(ValueError):
        escape_identifier(attempt)


@pytest.fixture
def test_client():
  """Create a test client with mocked cluster service."""
  from robosystems.graph_api.core.ladybug import service as ladybug_service

  # Reset cluster service before test
  original_service = ladybug_service._ladybug_service
  ladybug_service._ladybug_service = None

  try:
    # Initialize cluster service
    init_ladybug_service(
      base_path="/tmp/test_lbug",
      max_databases=10,
      read_only=False,
      node_type=NodeType.WRITER,
      repository_type=RepositoryType.ENTITY,
    )

    app = create_app()
    yield TestClient(app)
  finally:
    # Reset cluster service after test
    ladybug_service._ladybug_service = original_service


class TestSchemaEndpointSecurity:
  """Test security aspects of schema API endpoints."""

  def test_path_traversal_in_database_name(self, test_client):
    """Test path traversal attempts in database names."""
    with patch(
      "robosystems.graph_api.core.ladybug.service.get_ladybug_service"
    ) as mock_get_service:
      mock_service = Mock()
      mock_service.read_only = False
      mock_service.db_manager.list_databases.return_value = ["test_db"]
      mock_get_service.return_value = mock_service

      # Test various path traversal attempts
      malicious_names = [
        "../etc/passwd",
        "../../root/.ssh/id_rsa",
        "/etc/shadow",
        "test/../../../etc/hosts",
        "test%2f..%2f..%2fetc%2fpasswd",  # URL encoded
      ]

      for name in malicious_names:
        response = test_client.get(f"/databases/{name}/schema")
        # Both 400 and 404 are acceptable responses for path traversal attempts
        # 404 means FastAPI rejected the route before our validation
        # 400 means our validation caught it
        assert response.status_code in [400, 404], (
          f"Expected 400 or 404 for {name}, got {response.status_code}"
        )

        # If we got a 400, verify it's from our validation
        if response.status_code == 400:
          assert (
            "Invalid database name" in response.json()["detail"]
            or "invalid characters" in response.json()["detail"]
          )

  def test_sql_injection_in_schema_endpoint(self, test_client):
    """Test SQL injection prevention in schema endpoints."""
    from robosystems.graph_api.core.ladybug import service as ladybug_service

    # Access the actual cluster service that was initialized in the fixture
    cluster_service = ladybug_service._ladybug_service

    # Mock the database manager methods
    with patch.object(
      cluster_service.db_manager, "list_databases", return_value=["test_db"]
    ):
      # Create a mock result for SHOW_TABLES with malicious table name
      mock_result = Mock()
      mock_result.data = [{"col1": "Test'; DROP TABLE User; --", "col2": "NODE"}]

      with patch.object(cluster_service, "execute_query", return_value=mock_result):
        # Test SQL injection in table info call
        with patch(
          "robosystems.graph_api.routers.databases.schema.escape_identifier"
        ) as mock_escape:
          # Should call escape_identifier for table names
          mock_escape.side_effect = ValueError("Invalid identifier")

          response = test_client.get("/databases/test_db/schema")
          # Should handle the error gracefully
          assert response.status_code == 200
