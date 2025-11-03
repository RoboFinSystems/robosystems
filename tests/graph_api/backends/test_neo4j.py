import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import json

from robosystems.graph_api.backends.neo4j import Neo4jBackend


class TestNeo4jBackendInitialization:
  @patch("robosystems.graph_api.backends.neo4j.env")
  def test_backend_initialization_community(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=False)

    assert backend.enterprise is False
    assert backend.bolt_url == "bolt://localhost:7687"
    assert backend.driver is None

  @patch("robosystems.graph_api.backends.neo4j.env")
  def test_backend_initialization_enterprise(self, mock_env):
    mock_env.NEO4J_URI = "neo4j://cluster:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=True)

    assert backend.enterprise is True
    assert backend.bolt_url == "neo4j://cluster:7687"

  @patch("robosystems.graph_api.backends.neo4j.env")
  def test_get_database_name_community(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=False)

    assert backend._get_database_name("test_graph") == "neo4j"

  @patch("robosystems.graph_api.backends.neo4j.env")
  def test_get_database_name_enterprise(self, mock_env):
    mock_env.NEO4J_URI = "neo4j://cluster:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=True)

    assert backend._get_database_name("test_graph") == "kg_test_graph_main"


class TestNeo4jBackendConnection:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.AsyncGraphDatabase")
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_connect_with_env_password(self, mock_env, mock_graph_db):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"
    mock_env.NEO4J_PASSWORD = "password123"
    mock_env.NEO4J_MAX_CONNECTION_LIFETIME = 3600
    mock_env.NEO4J_MAX_CONNECTION_POOL_SIZE = 50
    mock_env.NEO4J_CONNECTION_ACQUISITION_TIMEOUT = 60

    mock_driver = MagicMock()
    mock_graph_db.driver.return_value = mock_driver

    backend = Neo4jBackend()
    await backend._connect()

    assert backend._password == "password123"
    assert backend.driver == mock_driver
    mock_graph_db.driver.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.boto3")
  @patch("robosystems.graph_api.backends.neo4j.AsyncGraphDatabase")
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_connect_with_secrets_manager(
    self, mock_env, mock_graph_db, mock_boto3
  ):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"
    mock_env.NEO4J_PASSWORD = ""
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "prod"
    mock_env.NEO4J_MAX_CONNECTION_LIFETIME = 3600
    mock_env.NEO4J_MAX_CONNECTION_POOL_SIZE = 50
    mock_env.NEO4J_CONNECTION_ACQUISITION_TIMEOUT = 60

    mock_secrets_client = MagicMock()
    mock_boto3.client.return_value = mock_secrets_client
    mock_secrets_client.get_secret_value.return_value = {
      "SecretString": json.dumps({"password": "secret_password"})
    }

    mock_driver = MagicMock()
    mock_graph_db.driver.return_value = mock_driver

    backend = Neo4jBackend()
    await backend._connect()

    assert backend._password == "secret_password"
    mock_secrets_client.get_secret_value.assert_called_once_with(
      SecretId="robosystems/prod/neo4j"
    )


class TestNeo4jBackendQueryExecution:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_execute_query_success(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend()

    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_result = AsyncMock()

    mock_result.data.return_value = [
      {"id": 1, "name": "Alice"},
      {"id": 2, "name": "Bob"},
    ]

    mock_session.run.return_value = mock_result
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    result = await backend.execute_query("test_graph", "MATCH (n) RETURN n")

    assert len(result) == 2
    assert result[0] == {"id": 1, "name": "Alice"}
    mock_session.run.assert_called_once_with("MATCH (n) RETURN n", {})

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_execute_query_with_parameters(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend()

    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_result = AsyncMock()

    mock_result.data.return_value = [{"id": 1}]

    mock_session.run.return_value = mock_result
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    params = {"id": 1}
    result = await backend.execute_query(
      "test_graph", "MATCH (n {id: $id}) RETURN n", params
    )

    assert len(result) == 1
    mock_session.run.assert_called_once_with("MATCH (n {id: $id}) RETURN n", params)

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_execute_write_success(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend()

    mock_driver = MagicMock()
    mock_session = AsyncMock()

    mock_session.execute_write.return_value = [{"count": 1}]
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    result = await backend.execute_write("test_graph", "CREATE (n:Node)")

    assert result == [{"count": 1}]
    mock_session.execute_write.assert_called_once()


class TestNeo4jBackendDatabaseManagement:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_create_database_community_error(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=False)
    backend.driver = AsyncMock()

    with pytest.raises(ValueError) as exc_info:
      await backend.create_database("test_db")

    assert "Enterprise" in str(exc_info.value)

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_create_database_enterprise(self, mock_env):
    mock_env.NEO4J_URI = "neo4j://cluster:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=True)

    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    result = await backend.create_database("test_db")

    assert result is True
    mock_session.run.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_delete_database_community_error(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=False)
    backend.driver = AsyncMock()

    with pytest.raises(ValueError) as exc_info:
      await backend.delete_database("test_db")

    assert "Community" in str(exc_info.value)

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_delete_database_default_error(self, mock_env):
    mock_env.NEO4J_URI = "neo4j://cluster:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=True)
    backend.driver = AsyncMock()

    with pytest.raises(ValueError) as exc_info:
      await backend.delete_database("neo4j")

    assert "default" in str(exc_info.value)

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_delete_database_enterprise(self, mock_env):
    mock_env.NEO4J_URI = "neo4j://cluster:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=True)

    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    result = await backend.delete_database("test_db")

    assert result is True
    mock_session.run.assert_called_once()

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_list_databases_community(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=False)
    backend.driver = AsyncMock()

    result = await backend.list_databases()

    assert result == ["neo4j"]

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_list_databases_enterprise(self, mock_env):
    mock_env.NEO4J_URI = "neo4j://cluster:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=True)

    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_result = AsyncMock()

    mock_result.data.return_value = [
      {"name": "neo4j"},
      {"name": "kg_test_main"},
    ]

    mock_session.run.return_value = mock_result
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    result = await backend.list_databases()

    assert result == ["neo4j", "kg_test_main"]


class TestNeo4jBackendDatabaseInfo:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_get_database_info_success(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=True)

    mock_driver = MagicMock()
    mock_session = AsyncMock()

    mock_node_result = AsyncMock()
    mock_node_data = {"count": 100}
    mock_node_result.single.return_value = mock_node_data

    mock_rel_result = AsyncMock()
    mock_rel_data = {"count": 50}
    mock_rel_result.single.return_value = mock_rel_data

    mock_size_result = AsyncMock()
    mock_size_data = {"sizeOnDisk": 1024}
    mock_size_result.single.return_value = mock_size_data

    mock_session.run.side_effect = [
      mock_node_result,
      mock_rel_result,
      mock_size_result,
    ]

    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    result = await backend.get_database_info("test_db")

    assert result.name == "test_db"
    assert result.node_count == 100
    assert result.relationship_count == 50
    assert result.size_bytes == 1024

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_get_database_info_error(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend()

    mock_driver = MagicMock()
    mock_session = AsyncMock()

    mock_session.run.side_effect = RuntimeError("Query error")
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    result = await backend.get_database_info("test_db")

    assert result.name == "test_db"
    assert result.node_count == 0
    assert result.relationship_count == 0


class TestNeo4jBackendClusterTopology:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_get_cluster_topology_community(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=False)
    backend.driver = AsyncMock()

    result = await backend.get_cluster_topology()

    assert result.mode == "single"
    assert result.leader == {"url": "bolt://localhost:7687"}

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_get_cluster_topology_enterprise(self, mock_env):
    mock_env.NEO4J_URI = "neo4j://cluster:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend(enterprise=True)

    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_result = AsyncMock()

    mock_result.data.return_value = [
      {
        "id": "server1",
        "address": "bolt://server1:7687",
        "role": "LEADER",
        "database": "neo4j",
      },
      {
        "id": "server2",
        "address": "bolt://server2:7687",
        "role": "FOLLOWER",
        "database": "neo4j",
      },
    ]

    mock_session.run.return_value = mock_result
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    result = await backend.get_cluster_topology()

    assert result.mode == "cluster"
    assert result.leader is not None
    assert result.leader["role"] == "LEADER"
    assert len(result.followers) == 1

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_health_check_success(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend()

    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    result = await backend.health_check()

    assert result is True

  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_health_check_failure(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend()

    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_session.run.side_effect = RuntimeError("Connection error")
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend.driver = mock_driver

    result = await backend.health_check()

    assert result is False


class TestNeo4jBackendClose:
  @pytest.mark.asyncio
  @patch("robosystems.graph_api.backends.neo4j.env")
  async def test_close(self, mock_env):
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"

    backend = Neo4jBackend()

    mock_driver = MagicMock()
    mock_driver.close = AsyncMock()
    backend.driver = mock_driver

    await backend.close()

    mock_driver.close.assert_called_once()
    assert backend.driver is None
