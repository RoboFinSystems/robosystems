"""Schema builder tests."""

from unittest.mock import MagicMock

import pytest

from robosystems.schemas.builder import (
  LadybugSchemaBuilder,
  create_schema_from_config,
)
from robosystems.schemas.models import Schema


@pytest.fixture
def base_config():
  return {
    "name": "Test Schema",
    "description": "A test schema",
    "version": "1.0.0",
    "base_schema": "base",
    "extensions": [],
  }


@pytest.fixture
def ledger_config():
  return {
    "name": "Ledger Schema",
    "description": "With roboledger",
    "version": "1.0.0",
    "base_schema": "base",
    "extensions": ["roboledger"],
  }


class TestLadybugSchemaBuilderInit:
  """Tests for LadybugSchemaBuilder initialization."""

  def test_stores_config(self, base_config):
    builder = LadybugSchemaBuilder(base_config)
    assert builder.config == base_config
    assert builder.schema is None


class TestLoadSchemas:
  """Tests for load_schemas method."""

  def test_load_base_schema(self, base_config):
    builder = LadybugSchemaBuilder(base_config)
    result = builder.load_schemas()
    assert result is builder  # Fluent API
    assert builder.schema is not None
    assert len(builder.schema.nodes) > 0

  def test_load_with_extension(self, ledger_config):
    builder = LadybugSchemaBuilder(ledger_config)
    builder.load_schemas()
    assert builder.schema is not None
    assert len(builder.schema.nodes) > 0
    assert len(builder.schema.relationships) > 0

  def test_fluent_api(self, base_config):
    schema = LadybugSchemaBuilder(base_config).load_schemas().get_schema()
    assert isinstance(schema, Schema)


class TestGetSchema:
  """Tests for get_schema method."""

  def test_raises_if_not_loaded(self, base_config):
    builder = LadybugSchemaBuilder(base_config)
    with pytest.raises(ValueError):
      builder.get_schema()

  def test_returns_schema(self, base_config):
    builder = LadybugSchemaBuilder(base_config)
    builder.load_schemas()
    schema = builder.get_schema()
    assert isinstance(schema, Schema)


class TestGenerateCypher:
  """Tests for generate_cypher method."""

  def test_raises_if_not_loaded(self, base_config):
    builder = LadybugSchemaBuilder(base_config)
    with pytest.raises(ValueError):
      builder.generate_cypher()

  def test_generates_ddl(self, base_config):
    builder = LadybugSchemaBuilder(base_config)
    builder.load_schemas()
    cypher = builder.generate_cypher()
    assert isinstance(cypher, str)
    assert "CREATE" in cypher
    assert len(cypher) > 0


class TestApplyToConnection:
  """Tests for apply_to_connection method."""

  def test_executes_statements(self, base_config):
    builder = LadybugSchemaBuilder(base_config)
    builder.load_schemas()

    mock_conn = MagicMock()
    builder.apply_to_connection(mock_conn)

    # Should have called execute at least once
    assert mock_conn.execute.call_count > 0

  def test_strips_comments(self, base_config):
    builder = LadybugSchemaBuilder(base_config)
    builder.load_schemas()

    mock_conn = MagicMock()
    builder.apply_to_connection(mock_conn)

    # Verify no comments in executed statements
    for call in mock_conn.execute.call_args_list:
      stmt = call[0][0]
      assert "/*" not in stmt
      assert "--" not in stmt


class TestCreateSchemaFromConfig:
  """Tests for create_schema_from_config convenience function."""

  def test_returns_schema(self, base_config):
    schema = create_schema_from_config(base_config)
    assert isinstance(schema, Schema)
    assert len(schema.nodes) > 0

  def test_with_extensions(self, ledger_config):
    schema = create_schema_from_config(ledger_config)
    assert isinstance(schema, Schema)
    assert len(schema.nodes) > 0
