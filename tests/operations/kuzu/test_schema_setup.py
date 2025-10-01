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

  @patch("robosystems.utils.uuid.generate_deterministic_uuid7")
  def test_create_graph_metadata_for_sec(self, mock_generate_uuid):
    """Test creating GraphMetadata node for SEC repository."""
    mock_engine = MagicMock()
    mock_engine.database.database_path = "/data/kuzu/sec.kuzu"
    mock_engine.execute_query.side_effect = [
      [],  # GraphMetadata doesn't exist yet
      [{"id": "sec"}],  # Creation result
    ]

    mock_generate_uuid.return_value = "test-uuid-123"

    manager = KuzuSchemaManager(mock_engine)
    manager._create_graph_metadata_if_needed()

    # Verify GraphMetadata was created
    calls = mock_engine.execute_query.call_args_list
    assert len(calls) == 2

    # Check the CREATE query
    create_call = calls[1]
    assert "CREATE (m:GraphMetadata" in str(create_call)

    # Check parameters
    params = create_call[0][1]
    assert params["graph_id"] == "sec"
    assert params["name"] == "SEC EDGAR Repository"
    assert params["tier"] == "shared"

  def test_create_graph_metadata_skips_non_repository(self):
    """Test that GraphMetadata is skipped for non-repository databases."""
    mock_engine = MagicMock()
    mock_engine.database.database_path = "/data/kuzu/kg123456.kuzu"

    manager = KuzuSchemaManager(mock_engine)
    manager._create_graph_metadata_if_needed()

    # Should not execute any queries for non-repository databases
    mock_engine.execute_query.assert_not_called()


class TestKuzuSchemaManagerAdditional:
  """Additional test cases for KuzuSchemaManager to improve coverage."""

  @patch("robosystems.operations.kuzu.schema_setup.SchemaIngestionProcessor")
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

    # Verify SchemaIngestionProcessor was called with custom schema (line 104 coverage)
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

  @patch("robosystems.operations.kuzu.schema_setup.logger")
  def test_create_graph_metadata_already_exists(self, mock_logger):
    """Test GraphMetadata creation when it already exists."""
    # Setup mocks
    mock_engine = MagicMock()
    # Set database path to simulate SEC database
    mock_engine.database.database_path = "/path/to/sec.kuzu"

    # Simulate GraphMetadata already exists
    mock_engine.execute_query.side_effect = [
      [{"count": 1}],  # GraphMetadata exists (line 233)
    ]

    # Create schema manager for SEC database
    manager = KuzuSchemaManager(mock_engine)

    # Try to create metadata
    manager._create_graph_metadata_if_needed()

    # Verify skip logging was called (lines 234-235 coverage)
    mock_logger.info.assert_any_call("GraphMetadata already exists, skipping creation")

  @patch("robosystems.operations.kuzu.schema_setup.logger")
  def test_create_graph_metadata_error_handling(self, mock_logger):
    """Test error handling in GraphMetadata creation."""
    # Setup mocks
    mock_engine = MagicMock()
    # Set database path to simulate SEC database
    mock_engine.database.database_path = "/path/to/sec.kuzu"

    # First query succeeds (check existence), second fails (creation)
    mock_engine.execute_query.side_effect = [
      [],  # GraphMetadata doesn't exist
      Exception("Database error"),  # Creation fails
    ]

    # Create schema manager for SEC database
    manager = KuzuSchemaManager(mock_engine)

    # Try to create metadata - should not raise
    manager._create_graph_metadata_if_needed()

    # Verify error logging was called (lines 349-350 coverage)
    mock_logger.error.assert_called()
    error_msg = mock_logger.error.call_args[0][0]
    assert "Failed to create GraphMetadata node" in error_msg

  @patch("robosystems.operations.kuzu.schema_setup.logger")
  def test_create_graph_metadata_query_exception(self, mock_logger):
    """Test GraphMetadata creation when initial query throws exception."""
    # Setup mocks
    mock_engine = MagicMock()
    # Set database path to simulate SEC database
    mock_engine.database.database_path = "/path/to/sec.kuzu"

    # First query throws exception (table doesn't exist)
    # Second query succeeds (creation)
    mock_engine.execute_query.side_effect = [
      Exception("Table GraphMetadata does not exist"),  # Lines 236-238
      None,  # Creation succeeds
    ]

    # Create schema manager for SEC database
    manager = KuzuSchemaManager(mock_engine)

    # Try to create metadata
    manager._create_graph_metadata_if_needed()

    # Verify creation was attempted after exception
    assert mock_engine.execute_query.call_count == 2

  @patch("robosystems.operations.kuzu.schema_setup.logger")
  def test_initialize_schema_creates_metadata_on_success(self, mock_logger):
    """Test that GraphMetadata is created after successful schema initialization."""
    # Setup mocks
    mock_engine = MagicMock()
    # Set database path to simulate SEC database
    mock_engine.database.database_path = "/path/to/sec.kuzu"
    mock_engine.get_existing_tables.return_value = {
      "nodes": set(),
      "relationships": set(),
    }
    mock_engine.execute_query.return_value = []

    # Create schema processor mock
    with patch(
      "robosystems.operations.kuzu.schema_setup.create_roboledger_ingestion_processor"
    ) as mock_processor_creator:
      mock_processor = MagicMock()
      mock_processor.ingest_config.node_tables = {
        "NewNode": MagicMock(columns={"id": "INT64"})
      }
      mock_processor.ingest_config.relationship_tables = {}
      mock_processor_creator.return_value = mock_processor

      # Create schema manager for SEC database
      manager = KuzuSchemaManager(mock_engine)

      # Initialize schema
      with patch.object(
        manager, "_create_graph_metadata_if_needed"
      ) as mock_create_metadata:
        manager.initialize_schema()

        # Verify metadata creation was called (line 153 coverage)
        mock_create_metadata.assert_called_once()


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
