"""Tests for Kuzu database schema setup operations."""

import pytest
from unittest.mock import MagicMock, patch

from robosystems.operations.kuzu.schema_setup import (
  KuzuSchemaManager,
  ensure_schema,
)


class TestKuzuSchemaManager:
  """Test cases for KuzuSchemaManager."""

  def test_init(self):
    """Test schema manager initialization."""
    mock_engine = MagicMock()
    manager = KuzuSchemaManager(mock_engine)
    assert manager.engine == mock_engine

  def test_schema_exists_true(self):
    """Test checking when schema exists."""
    mock_engine = MagicMock()
    mock_engine.execute_query.return_value = [
      {"name": "Entity", "type": "NODE"},
      {"name": "Fact", "type": "NODE"},
    ]

    manager = KuzuSchemaManager(mock_engine)
    exists = manager.schema_exists()

    assert exists is True
    mock_engine.execute_query.assert_called_once_with("CALL show_tables() RETURN *")

  def test_schema_exists_false(self):
    """Test checking when schema doesn't exist."""
    mock_engine = MagicMock()
    mock_engine.execute_query.return_value = []

    manager = KuzuSchemaManager(mock_engine)
    exists = manager.schema_exists()

    assert exists is False

  def test_schema_exists_error(self):
    """Test schema exists check with error."""
    mock_engine = MagicMock()
    mock_engine.execute_query.side_effect = Exception("Database error")

    manager = KuzuSchemaManager(mock_engine)
    exists = manager.schema_exists()

    # Returns False when error occurs
    assert exists is False

  def test_get_existing_tables(self):
    """Test getting existing tables from database."""
    mock_engine = MagicMock()
    mock_engine.execute_query.return_value = [
      {"name": "Entity", "type": "NODE"},
      {"name": "Fact", "type": "NODE"},
      {"name": "HAS_FACT", "type": "REL"},
      {"name": "IN_CONTEXT", "type": "REL"},
    ]

    manager = KuzuSchemaManager(mock_engine)
    existing = manager.get_existing_tables()

    assert "Entity" in existing["nodes"]
    assert "Fact" in existing["nodes"]
    assert "HAS_FACT" in existing["relationships"]
    assert "IN_CONTEXT" in existing["relationships"]
    assert len(existing["nodes"]) == 2
    assert len(existing["relationships"]) == 2

  def test_get_existing_tables_error(self):
    """Test getting tables when error occurs."""
    mock_engine = MagicMock()
    mock_engine.execute_query.side_effect = Exception("Database error")

    manager = KuzuSchemaManager(mock_engine)
    existing = manager.get_existing_tables()

    # Returns empty sets on error
    assert len(existing["nodes"]) == 0
    assert len(existing["relationships"]) == 0

  @patch(
    "robosystems.operations.kuzu.schema_setup.create_roboledger_ingestion_processor"
  )
  def test_initialize_schema_already_exists(self, mock_create_processor):
    """Test schema initialization when it already exists."""
    mock_engine = MagicMock()
    mock_engine.execute_query.return_value = [{"name": "Entity", "type": "NODE"}]

    manager = KuzuSchemaManager(mock_engine)
    result = manager.initialize_schema()

    assert result is False
    mock_create_processor.assert_not_called()

  @patch(
    "robosystems.operations.kuzu.schema_setup.create_roboledger_ingestion_processor"
  )
  def test_initialize_schema_first_time(self, mock_create_processor):
    """Test schema initialization for the first time."""
    # Setup mocks
    mock_engine = MagicMock()
    mock_engine.execute_query.side_effect = [
      [],  # schema_exists check
      [],  # get_existing_tables
    ]

    # Mock schema processor
    mock_processor = MagicMock()
    mock_node_info = MagicMock()
    mock_node_info.columns = ["identifier", "name", "created_at"]
    mock_node_info.primary_keys = ["identifier"]

    mock_rel_info = MagicMock()
    mock_rel_info.from_node = "Entity"
    mock_rel_info.to_node = "Fact"
    mock_rel_info.properties = ["weight", "created_at"]

    mock_processor.ingest_config.node_tables = {"Entity": mock_node_info}
    mock_processor.ingest_config.relationship_tables = {"HAS_FACT": mock_rel_info}
    mock_create_processor.return_value = mock_processor

    manager = KuzuSchemaManager(mock_engine)
    result = manager.initialize_schema()

    assert result is True
    mock_create_processor.assert_called_once()

    # Verify CREATE statements were executed
    calls = mock_engine.execute_query.call_args_list
    assert any("CREATE NODE TABLE" in str(call) for call in calls)

  def test_create_node_table(self):
    """Test creating a node table."""
    mock_engine = MagicMock()
    manager = KuzuSchemaManager(mock_engine)

    mock_table_info = MagicMock()
    mock_table_info.columns = [
      "identifier",
      "name",
      "amount",
      "created_at",
      "processed",
    ]
    mock_table_info.primary_keys = ["identifier"]

    result = manager._create_node_table("TestNode", mock_table_info)

    assert result is True
    mock_engine.execute_query.assert_called_once()

    # Check the CREATE statement contains expected elements
    create_stmt = mock_engine.execute_query.call_args[0][0]
    assert "CREATE NODE TABLE TestNode" in create_stmt
    assert "identifier STRING PRIMARY KEY" in create_stmt
    assert "name STRING" in create_stmt
    assert "amount DOUBLE" in create_stmt
    assert "created_at TIMESTAMP" in create_stmt
    assert "processed BOOLEAN" in create_stmt

  def test_create_node_table_error(self):
    """Test node table creation with error."""
    mock_engine = MagicMock()
    mock_engine.execute_query.side_effect = Exception("Create failed")

    manager = KuzuSchemaManager(mock_engine)

    mock_table_info = MagicMock()
    mock_table_info.columns = ["id"]
    mock_table_info.primary_keys = ["id"]

    result = manager._create_node_table("TestNode", mock_table_info)

    assert result is False

  def test_create_relationship_table(self):
    """Test creating a relationship table."""
    mock_engine = MagicMock()
    manager = KuzuSchemaManager(mock_engine)

    mock_table_info = MagicMock()
    mock_table_info.from_node = "Entity"
    mock_table_info.to_node = "Fact"
    mock_table_info.properties = ["weight", "confidence", "created_at"]

    result = manager._create_relationship_table("TEST_REL", mock_table_info)

    assert result is True
    mock_engine.execute_query.assert_called_once()

    # Check the CREATE statement
    create_stmt = mock_engine.execute_query.call_args[0][0]
    assert "CREATE REL TABLE TEST_REL" in create_stmt
    assert "FROM Entity TO Fact" in create_stmt
    assert "weight DOUBLE" in create_stmt
    assert "confidence DOUBLE" in create_stmt
    assert "created_at TIMESTAMP" in create_stmt

  def test_create_relationship_table_no_properties(self):
    """Test creating a relationship table without properties."""
    mock_engine = MagicMock()
    manager = KuzuSchemaManager(mock_engine)

    mock_table_info = MagicMock()
    mock_table_info.from_node = "Entity"
    mock_table_info.to_node = "Fact"
    mock_table_info.properties = None

    result = manager._create_relationship_table("SIMPLE_REL", mock_table_info)

    assert result is True

    # Check the CREATE statement has no properties
    create_stmt = mock_engine.execute_query.call_args[0][0]
    assert "CREATE REL TABLE SIMPLE_REL(FROM Entity TO Fact)" in create_stmt


class TestKuzuSchemaManagerAdditional:
  """Additional test cases for KuzuSchemaManager to improve coverage."""

  @patch("robosystems.operations.kuzu.schema_setup.XBRLSchemaConfigGenerator")
  @patch("robosystems.operations.kuzu.schema_setup.logger")
  def test_initialize_schema_with_custom_config(
    self, mock_logger, mock_processor_class
  ):
    """Test schema initialization with custom schema configuration."""
    # Setup mocks
    mock_engine = MagicMock()
    mock_engine.get_existing_tables.return_value = {
      "nodes": set(),
      "relationships": set(),
    }
    mock_engine.execute_query.return_value = []

    # Create mock processor
    mock_processor = MagicMock()
    mock_processor.ingest_config.node_tables = {
      "CustomNode": MagicMock(columns={"id": "INT64", "name": "STRING"})
    }
    mock_processor.ingest_config.relationship_tables = {}
    mock_processor_class.return_value = mock_processor

    # Create custom schema config
    custom_schema = "custom_schema_config"

    # Create schema manager
    manager = KuzuSchemaManager(mock_engine)

    # Initialize with custom schema
    manager.initialize_schema(schema_config=custom_schema)

    # Verify XBRLSchemaConfigGenerator was called with custom schema (line 104 coverage)
    mock_processor_class.assert_called_once_with(custom_schema)
    assert mock_engine.execute_query.called

  def test_initialize_schema_skip_existing_tables(self):
    """Test that existing tables are skipped during initialization."""
    # Setup mocks
    mock_engine = MagicMock()

    # Create schema processor mock
    with patch(
      "robosystems.operations.kuzu.schema_setup.create_roboledger_ingestion_processor"
    ) as mock_processor_creator:
      mock_processor = MagicMock()
      mock_processor.ingest_config.node_tables = {
        "Entity": MagicMock(columns={"id": "STRING"}),  # Already exists
        "NewNode": MagicMock(columns={"id": "STRING"}),  # New node
      }
      mock_processor.ingest_config.relationship_tables = {
        "HAS_SUBMISSION": MagicMock(
          source="Entity", target="Submission"
        ),  # Already exists
        "NEW_REL": MagicMock(source="Entity", target="NewNode"),  # New relationship
      }
      mock_processor_creator.return_value = mock_processor

      # Create schema manager
      manager = KuzuSchemaManager(mock_engine)

      # Mock get_existing_tables to return existing tables
      with patch.object(manager, "get_existing_tables") as mock_get_existing:
        mock_get_existing.return_value = {
          "nodes": {"Entity", "Submission"},
          "relationships": {"HAS_SUBMISSION"},
        }

        # Mock _create_node_table and _create_relationship_table to track calls
        with patch.object(
          manager, "_create_node_table", return_value=True
        ) as mock_create_node:
          with patch.object(
            manager, "_create_relationship_table", return_value=True
          ) as mock_create_rel:
            # Initialize schema
            manager.initialize_schema()

            # Verify only new tables were created (existing ones skipped)
            mock_create_node.assert_called_once_with(
              "NewNode", mock_processor.ingest_config.node_tables["NewNode"]
            )
            mock_create_rel.assert_called_once_with(
              "NEW_REL", mock_processor.ingest_config.relationship_tables["NEW_REL"]
            )


class TestEnsureSchema:
  """Test cases for ensure_schema function."""

  @patch("robosystems.middleware.graph.engine.Engine")
  @patch("robosystems.operations.kuzu.path_utils.ensure_kuzu_directory")
  def test_ensure_schema_success(self, mock_ensure_dir, mock_engine_class):
    """Test successful schema creation."""
    # Setup mocks
    mock_engine = MagicMock()
    mock_engine.execute_query.return_value = []  # No existing schema
    mock_engine_class.return_value = mock_engine

    # Run ensure_schema
    with patch(
      "robosystems.operations.kuzu.schema_setup.KuzuSchemaManager"
    ) as mock_manager_class:
      mock_manager = MagicMock()
      mock_manager.initialize_schema.return_value = True
      mock_manager_class.return_value = mock_manager

      result = ensure_schema("/tmp/test.kuzu")

    assert result is True
    mock_ensure_dir.assert_called_once()
    mock_manager.initialize_schema.assert_called_once()

  @patch("robosystems.operations.kuzu.schema_setup.KuzuSchemaManager")
  @patch("robosystems.middleware.graph.engine.Engine")
  @patch("robosystems.operations.kuzu.path_utils.ensure_kuzu_directory")
  def test_ensure_schema_error(
    self, mock_ensure_dir, mock_engine_class, mock_manager_class
  ):
    """Test ensure_schema with error."""
    # Make the schema manager raise an error
    mock_manager = MagicMock()
    mock_manager.initialize_schema.side_effect = Exception(
      "Schema initialization failed"
    )
    mock_manager_class.return_value = mock_manager

    with pytest.raises(Exception, match="Schema initialization failed"):
      ensure_schema("/tmp/test.kuzu")
