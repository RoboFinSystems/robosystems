"""Unit tests for graph database operation utilities."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.graph_api.client.exceptions import GraphClientError


@pytest.mark.unit
class TestIsMultitenantMode:
  """Tests for is_multitenant_mode."""

  def test_always_returns_true(self):
    """Test that multi-tenant mode is always True for graph databases."""
    from robosystems.middleware.graph.utils.database import is_multitenant_mode

    assert is_multitenant_mode() is True


@pytest.mark.unit
class TestGetDatabaseName:
  """Tests for get_database_name."""

  def test_with_valid_graph_id(self):
    """Test database name resolution with a valid graph ID."""
    from robosystems.middleware.graph.utils.database import get_database_name

    assert get_database_name("kg1a2b3c") == "kg1a2b3c"

  def test_with_none_returns_default(self):
    """Test that None graph_id returns 'default'."""
    from robosystems.middleware.graph.utils.database import get_database_name

    assert get_database_name(None) == "default"

  def test_without_argument_returns_default(self):
    """Test that no argument returns 'default'."""
    from robosystems.middleware.graph.utils.database import get_database_name

    assert get_database_name() == "default"

  def test_with_default_string_returns_default(self):
    """Test that 'default' string returns 'default'."""
    from robosystems.middleware.graph.utils.database import get_database_name

    assert get_database_name("default") == "default"

  def test_shared_repository_routes_to_repo_name(self):
    """Test that shared repository IDs route to their database name."""
    from robosystems.middleware.graph.utils.database import get_database_name

    assert get_database_name("sec") == "sec"

  def test_invalid_graph_id_raises_value_error(self):
    """Test that invalid graph IDs raise ValueError via validation."""
    from robosystems.middleware.graph.utils.database import get_database_name

    with pytest.raises(ValueError, match="contains invalid characters"):
      get_database_name("invalid.graph@id")

  def test_graph_id_with_underscore(self):
    """Test that graph IDs with underscores work."""
    from robosystems.middleware.graph.utils.database import get_database_name

    assert get_database_name("kg1a2b3c_subgraph") == "kg1a2b3c_subgraph"

  def test_graph_id_with_hyphen(self):
    """Test that graph IDs with hyphens work."""
    from robosystems.middleware.graph.utils.database import get_database_name

    assert get_database_name("test-graph") == "test-graph"


@pytest.mark.unit
class TestGetRepositoryDatabaseName:
  """Tests for get_repository_database_name."""

  def test_valid_repository_returns_name(self):
    """Test that a valid repository returns its ID as database name."""
    from robosystems.middleware.graph.utils.database import (
      get_repository_database_name,
    )

    assert get_repository_database_name("sec") == "sec"

  def test_unknown_repository_raises_value_error(self):
    """Test that an unknown repository raises ValueError."""
    from robosystems.middleware.graph.utils.database import (
      get_repository_database_name,
    )

    with pytest.raises(ValueError, match="Unknown shared repository"):
      get_repository_database_name("unknown_repo")

  def test_user_graph_id_raises_value_error(self):
    """Test that a user graph ID raises ValueError."""
    from robosystems.middleware.graph.utils.database import (
      get_repository_database_name,
    )

    with pytest.raises(ValueError, match="Unknown shared repository"):
      get_repository_database_name("kg01234567890abcdef")


@pytest.mark.unit
class TestListSharedRepositories:
  """Tests for list_shared_repositories."""

  def test_returns_list(self):
    """Test that it returns a list."""
    from robosystems.middleware.graph.utils.database import list_shared_repositories

    result = list_shared_repositories()
    assert isinstance(result, list)

  def test_contains_sec(self):
    """Test that the list contains 'sec' as a registered repository."""
    from robosystems.middleware.graph.utils.database import list_shared_repositories

    result = list_shared_repositories()
    assert "sec" in result

  def test_returns_strings(self):
    """Test that all items in the list are strings."""
    from robosystems.middleware.graph.utils.database import list_shared_repositories

    result = list_shared_repositories()
    for repo_id in result:
      assert isinstance(repo_id, str)


@pytest.mark.unit
class TestLogDatabaseOperation:
  """Tests for log_database_operation."""

  def test_logs_with_graph_id(self):
    """Test logging with graph_id present."""
    from robosystems.middleware.graph.utils.database import log_database_operation

    # Should not raise
    log_database_operation("CREATE", "test_db", "graph123")

  def test_logs_without_graph_id(self):
    """Test logging without graph_id."""
    from robosystems.middleware.graph.utils.database import log_database_operation

    # Should not raise
    log_database_operation("QUERY", "test_db")

  def test_logs_with_none_graph_id(self):
    """Test logging with None graph_id."""
    from robosystems.middleware.graph.utils.database import log_database_operation

    # Should not raise
    log_database_operation("DELETE", "test_db", None)

  @patch("robosystems.middleware.graph.utils.database.logger")
  def test_info_log_with_graph_id(self, mock_logger):
    """Test that logger.info is called with graph_id details."""
    from robosystems.middleware.graph.utils.database import log_database_operation

    log_database_operation("CREATE", "test_db", "graph123")
    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args[0][0]
    assert "CREATE" in call_args
    assert "test_db" in call_args
    assert "graph123" in call_args

  @patch("robosystems.middleware.graph.utils.database.logger")
  def test_info_log_without_graph_id(self, mock_logger):
    """Test that logger.info is called without graph_id details."""
    from robosystems.middleware.graph.utils.database import log_database_operation

    log_database_operation("QUERY", "test_db")
    mock_logger.info.assert_called_once()
    call_args = mock_logger.info.call_args[0][0]
    assert "QUERY" in call_args
    assert "test_db" in call_args


@pytest.mark.unit
class TestGetDatabasePathForGraph:
  """Tests for get_database_path_for_graph."""

  @patch("robosystems.operations.lbug.path_utils.env")
  def test_returns_string_path(self, mock_env):
    """Test that a string path is returned."""
    mock_env.LBUG_DATABASE_PATH = "/tmp/lbug-dbs"
    from robosystems.middleware.graph.utils.database import (
      get_database_path_for_graph,
    )

    result = get_database_path_for_graph("test_graph")
    assert isinstance(result, str)

  @patch("robosystems.operations.lbug.path_utils.env")
  def test_path_contains_graph_id(self, mock_env):
    """Test that the path contains the graph ID."""
    mock_env.LBUG_DATABASE_PATH = "/tmp/lbug-dbs"
    from robosystems.middleware.graph.utils.database import (
      get_database_path_for_graph,
    )

    result = get_database_path_for_graph("my_graph")
    assert "my_graph" in result

  @patch("robosystems.operations.lbug.path_utils.env")
  def test_path_has_lbug_extension(self, mock_env):
    """Test that the path has .lbug extension."""
    mock_env.LBUG_DATABASE_PATH = "/tmp/lbug-dbs"
    from robosystems.middleware.graph.utils.database import (
      get_database_path_for_graph,
    )

    result = get_database_path_for_graph("test_graph")
    assert result.endswith(".lbug")

  @patch("robosystems.operations.lbug.path_utils.env")
  def test_path_uses_configured_base(self, mock_env):
    """Test that the path uses the configured LBUG_DATABASE_PATH."""
    mock_env.LBUG_DATABASE_PATH = "/custom/path"
    from robosystems.middleware.graph.utils.database import (
      get_database_path_for_graph,
    )

    result = get_database_path_for_graph("test_graph")
    assert result.startswith("/custom/path")


@pytest.mark.unit
class TestGetMaxDatabasesPerNode:
  """Tests for get_max_databases_per_node."""

  def test_default_value(self):
    """Test that default value is returned when no tier is configured."""
    from robosystems.middleware.graph.utils.database import (
      get_max_databases_per_node,
    )

    # Without CLUSTER_TIER set, should return default of 10
    result = get_max_databases_per_node()
    assert result == 10

  @patch("robosystems.middleware.graph.utils.database.env")
  def test_returns_tier_specific_value(self, mock_env):
    """Test that tier-specific value is returned when configured."""
    mock_env.CLUSTER_TIER = "ladybug-large"
    with patch(
      "robosystems.config.graph_tier.GraphTierConfig.get_instance_config",
      return_value={"databases_per_instance": 25},
    ):
      from robosystems.middleware.graph.utils.database import (
        get_max_databases_per_node,
      )

      result = get_max_databases_per_node()
      assert result == 25

  @patch("robosystems.middleware.graph.utils.database.env")
  def test_fallback_on_config_error(self, mock_env):
    """Test fallback to default on configuration errors."""
    mock_env.CLUSTER_TIER = "invalid-tier"
    with patch(
      "robosystems.config.graph_tier.GraphTierConfig.get_instance_config",
      side_effect=Exception("Config error"),
    ):
      from robosystems.middleware.graph.utils.database import (
        get_max_databases_per_node,
      )

      result = get_max_databases_per_node()
      assert result == 10

  @patch("robosystems.middleware.graph.utils.database.env")
  def test_fallback_when_databases_per_instance_not_in_config(self, mock_env):
    """Test fallback when databases_per_instance key is missing from config."""
    mock_env.CLUSTER_TIER = "ladybug-standard"
    with patch(
      "robosystems.config.graph_tier.GraphTierConfig.get_instance_config",
      return_value={},
    ):
      from robosystems.middleware.graph.utils.database import (
        get_max_databases_per_node,
      )

      result = get_max_databases_per_node()
      assert result == 10

  @patch("robosystems.middleware.graph.utils.database.env")
  def test_returns_default_when_cluster_tier_is_none(self, mock_env):
    """Test returns default when CLUSTER_TIER is None."""
    mock_env.CLUSTER_TIER = None
    from robosystems.middleware.graph.utils.database import (
      get_max_databases_per_node,
    )

    result = get_max_databases_per_node()
    assert result == 10


@pytest.mark.unit
class TestEnsureDatabaseWithSchema:
  """Tests for ensure_database_with_schema (async)."""

  @pytest.mark.asyncio
  async def test_existing_healthy_database_returns_exists(self):
    """Test that an existing healthy database returns exists status."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      return_value={"is_healthy": True, "name": "test_db"}
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      result = await ensure_database_with_schema(
        graph_url="http://localhost:8001",
        db_name="test_db",
        schema_name="entity",
      )

    assert result["created"] is False
    assert result["status"] == "exists"
    assert result["database"]["is_healthy"] is True

  @pytest.mark.asyncio
  async def test_creates_database_when_not_found_graph_client_error(self):
    """Test database creation when GraphClientError with 'not found' is raised."""
    mock_client = AsyncMock()
    # First call: get_database_info raises not found
    # Second call (after create): get_database_info returns healthy
    mock_client.get_database_info = AsyncMock(
      side_effect=[
        GraphClientError("Database not found"),
        {"is_healthy": True, "name": "new_db"},
      ]
    )
    mock_client.create_database = AsyncMock(return_value={"status": "created"})
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      result = await ensure_database_with_schema(
        graph_url="http://localhost:8001",
        db_name="new_db",
        schema_name="entity",
      )

    assert result["created"] is True
    assert result["status"] == "created"
    mock_client.create_database.assert_called_once()

  @pytest.mark.asyncio
  async def test_creates_database_when_not_found_http_404(self):
    """Test database creation when HTTP 404 is raised."""
    from httpx import HTTPStatusError, Request, Response

    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 404
    mock_response.text = "Not Found"
    mock_request = MagicMock(spec=Request)

    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=[
        HTTPStatusError("Not Found", request=mock_request, response=mock_response),
        {"is_healthy": True, "name": "new_db"},
      ]
    )
    mock_client.create_database = AsyncMock(return_value={"status": "created"})
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      result = await ensure_database_with_schema(
        graph_url="http://localhost:8001",
        db_name="new_db",
        schema_name="entity",
      )

    assert result["created"] is True
    assert result["status"] == "created"

  @pytest.mark.asyncio
  async def test_raises_runtime_error_on_non_404_http_error(self):
    """Test that non-404 HTTP errors raise RuntimeError."""
    from httpx import HTTPStatusError, Request, Response

    mock_response = MagicMock(spec=Response)
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_request = MagicMock(spec=Request)

    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=HTTPStatusError(
        "Server Error", request=mock_request, response=mock_response
      )
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      with pytest.raises(RuntimeError, match="Failed to check database existence"):
        await ensure_database_with_schema(
          graph_url="http://localhost:8001",
          db_name="test_db",
          schema_name="entity",
        )

  @pytest.mark.asyncio
  async def test_raises_runtime_error_on_non_not_found_graph_client_error(self):
    """Test that non-'not found' GraphClientError raises RuntimeError."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=GraphClientError("Connection refused")
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      with pytest.raises(RuntimeError, match="Failed to check database existence"):
        await ensure_database_with_schema(
          graph_url="http://localhost:8001",
          db_name="test_db",
          schema_name="entity",
        )

  @pytest.mark.asyncio
  async def test_reraises_unexpected_errors(self):
    """Test that unexpected exceptions are re-raised."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=TypeError("unexpected type error")
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      with pytest.raises(TypeError, match="unexpected type error"):
        await ensure_database_with_schema(
          graph_url="http://localhost:8001",
          db_name="test_db",
          schema_name="entity",
        )

  @pytest.mark.asyncio
  async def test_sets_authorization_header_with_api_key(self):
    """Test that API key is set as Bearer token in headers."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      return_value={"is_healthy": True, "name": "test_db"}
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ) as mock_graph_client_cls:
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      await ensure_database_with_schema(
        graph_url="http://localhost:8001",
        db_name="test_db",
        schema_name="entity",
        api_key="test-api-key",
      )

      mock_graph_client_cls.assert_called_once_with(
        base_url="http://localhost:8001",
        headers={"Authorization": "Bearer test-api-key"},
      )

  @pytest.mark.asyncio
  async def test_no_auth_header_without_api_key(self):
    """Test that no auth header is set when api_key is None."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      return_value={"is_healthy": True, "name": "test_db"}
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ) as mock_graph_client_cls:
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      await ensure_database_with_schema(
        graph_url="http://localhost:8001",
        db_name="test_db",
        schema_name="entity",
      )

      mock_graph_client_cls.assert_called_once_with(
        base_url="http://localhost:8001",
        headers={},
      )

  @pytest.mark.asyncio
  async def test_schema_type_entity_for_entity_schemas(self):
    """Test that entity schema names produce 'entity' schema_type."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=[
        GraphClientError("Database not found"),
        {"is_healthy": True, "name": "test_db"},
      ]
    )
    mock_client.create_database = AsyncMock(return_value={"status": "created"})
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      await ensure_database_with_schema(
        graph_url="http://localhost:8001",
        db_name="test_db",
        schema_name="entity",
      )

    mock_client.create_database.assert_called_once_with(
      graph_id="test_db",
      schema_type="entity",
      repository_name=None,
    )

  @pytest.mark.asyncio
  async def test_schema_type_entity_financials(self):
    """Test that entity_financials schema produces 'entity' schema_type."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=[
        GraphClientError("Database not found"),
        {"is_healthy": True, "name": "test_db"},
      ]
    )
    mock_client.create_database = AsyncMock(return_value={"status": "created"})
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      await ensure_database_with_schema(
        graph_url="http://localhost:8001",
        db_name="test_db",
        schema_name="entity_financials",
      )

    mock_client.create_database.assert_called_once_with(
      graph_id="test_db",
      schema_type="entity",
      repository_name=None,
    )

  @pytest.mark.asyncio
  async def test_schema_type_shared_for_sec_schemas(self):
    """Test that SEC schema names produce 'shared' schema_type."""
    for schema_name in ["sec", "sec_xbrl", "sec_filings"]:
      mock_client = AsyncMock()
      mock_client.get_database_info = AsyncMock(
        side_effect=[
          GraphClientError("Database not found"),
          {"is_healthy": True, "name": "test_db"},
        ]
      )
      mock_client.create_database = AsyncMock(return_value={"status": "created"})
      mock_client.__aenter__ = AsyncMock(return_value=mock_client)
      mock_client.__aexit__ = AsyncMock(return_value=False)

      with patch(
        "robosystems.middleware.graph.utils.database.GraphClient",
        return_value=mock_client,
      ):
        from robosystems.middleware.graph.utils.database import (
          ensure_database_with_schema,
        )

        await ensure_database_with_schema(
          graph_url="http://localhost:8001",
          db_name="test_db",
          schema_name=schema_name,
        )

      mock_client.create_database.assert_called_once_with(
        graph_id="test_db",
        schema_type="shared",
        repository_name="sec",
      )

  @pytest.mark.asyncio
  async def test_schema_type_shared_for_industry_and_economic(self):
    """Test that industry/economic schema names produce 'shared' schema_type."""
    for schema_name in ["industry", "economic"]:
      mock_client = AsyncMock()
      mock_client.get_database_info = AsyncMock(
        side_effect=[
          GraphClientError("Database not found"),
          {"is_healthy": True, "name": "test_db"},
        ]
      )
      mock_client.create_database = AsyncMock(return_value={"status": "created"})
      mock_client.__aenter__ = AsyncMock(return_value=mock_client)
      mock_client.__aexit__ = AsyncMock(return_value=False)

      with patch(
        "robosystems.middleware.graph.utils.database.GraphClient",
        return_value=mock_client,
      ):
        from robosystems.middleware.graph.utils.database import (
          ensure_database_with_schema,
        )

        await ensure_database_with_schema(
          graph_url="http://localhost:8001",
          db_name="test_db",
          schema_name=schema_name,
        )

      mock_client.create_database.assert_called_once_with(
        graph_id="test_db",
        schema_type="shared",
        repository_name=schema_name,
      )

  @pytest.mark.asyncio
  async def test_schema_type_shared_for_shared_repo_db_name(self):
    """Test that a shared repository db_name produces 'shared' schema_type."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=[
        GraphClientError("Database not found"),
        {"is_healthy": True, "name": "sec"},
      ]
    )
    mock_client.create_database = AsyncMock(return_value={"status": "created"})
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      await ensure_database_with_schema(
        graph_url="http://localhost:8001",
        db_name="sec",
        schema_name="custom",
      )

    mock_client.create_database.assert_called_once_with(
      graph_id="sec",
      schema_type="shared",
      repository_name="sec",
    )

  @pytest.mark.asyncio
  async def test_schema_type_custom_for_unknown_schema(self):
    """Test that unknown schema names produce 'custom' schema_type."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=[
        GraphClientError("Database not found"),
        {"is_healthy": True, "name": "test_db"},
      ]
    )
    mock_client.create_database = AsyncMock(return_value={"status": "created"})
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      await ensure_database_with_schema(
        graph_url="http://localhost:8001",
        db_name="test_db",
        schema_name="my_custom_schema",
      )

    mock_client.create_database.assert_called_once_with(
      graph_id="test_db",
      schema_type="custom",
      repository_name=None,
    )

  @pytest.mark.asyncio
  @pytest.mark.timeout(10)
  async def test_timeout_on_database_not_becoming_healthy(self):
    """Test that RuntimeError is raised when database never becomes healthy."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=[
        GraphClientError("Database not found"),
        # All subsequent health checks return unhealthy
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
        {"is_healthy": False},
      ]
    )
    mock_client.create_database = AsyncMock(return_value={"status": "created"})
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with (
      patch(
        "robosystems.middleware.graph.utils.database.GraphClient",
        return_value=mock_client,
      ),
      patch(
        "robosystems.middleware.graph.utils.database.asyncio.sleep",
        new_callable=AsyncMock,
      ),
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      with pytest.raises(RuntimeError, match="Database creation failed"):
        await ensure_database_with_schema(
          graph_url="http://localhost:8001",
          db_name="test_db",
          schema_name="entity",
        )

  @pytest.mark.asyncio
  async def test_creation_failure_raises_runtime_error(self):
    """Test that a creation failure raises RuntimeError."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      side_effect=GraphClientError("Database not found")
    )
    mock_client.create_database = AsyncMock(side_effect=Exception("Creation failed"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema,
      )

      with pytest.raises(RuntimeError, match="Database creation failed"):
        await ensure_database_with_schema(
          graph_url="http://localhost:8001",
          db_name="test_db",
          schema_name="entity",
        )


@pytest.mark.unit
class TestEnsureDatabaseWithSchemaSync:
  """Tests for ensure_database_with_schema_sync."""

  def test_sync_wrapper_returns_result(self):
    """Test that the sync wrapper returns the result from the async function."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      return_value={"is_healthy": True, "name": "test_db"}
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema_sync,
      )

      result = ensure_database_with_schema_sync(
        graph_url="http://localhost:8001",
        db_name="test_db",
        schema_name="entity",
      )

    assert result["created"] is False
    assert result["status"] == "exists"

  def test_sync_wrapper_passes_api_key(self):
    """Test that the sync wrapper forwards the api_key parameter."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      return_value={"is_healthy": True, "name": "test_db"}
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ) as mock_graph_client_cls:
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema_sync,
      )

      ensure_database_with_schema_sync(
        graph_url="http://localhost:8001",
        db_name="test_db",
        schema_name="entity",
        api_key="test-key",
      )

      mock_graph_client_cls.assert_called_once_with(
        base_url="http://localhost:8001",
        headers={"Authorization": "Bearer test-key"},
      )

  def test_sync_wrapper_ignores_redis_client(self):
    """Test that the sync wrapper ignores the redis_client parameter."""
    mock_client = AsyncMock()
    mock_client.get_database_info = AsyncMock(
      return_value={"is_healthy": True, "name": "test_db"}
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    mock_redis = MagicMock()

    with patch(
      "robosystems.middleware.graph.utils.database.GraphClient",
      return_value=mock_client,
    ):
      from robosystems.middleware.graph.utils.database import (
        ensure_database_with_schema_sync,
      )

      # Should not raise even with redis_client passed
      result = ensure_database_with_schema_sync(
        graph_url="http://localhost:8001",
        db_name="test_db",
        schema_name="entity",
        redis_client=mock_redis,
      )

    assert result["status"] == "exists"
