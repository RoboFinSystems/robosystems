"""Comprehensive unit tests for Neo4jBackend - targeting missed coverage lines."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.graph_api.backends.base import S3IngestionError
from robosystems.graph_api.backends.neo4j import Neo4jBackend


def _make_backend(enterprise=False, driver=None):
  """Helper to create a Neo4jBackend with mocked env."""
  with patch("robosystems.graph_api.backends.neo4j.env") as mock_env:
    mock_env.NEO4J_URI = "bolt://localhost:7687"
    mock_env.NEO4J_USERNAME = "neo4j"
    mock_env.NEO4J_PASSWORD = "password"
    mock_env.AWS_REGION = "us-east-1"
    mock_env.ENVIRONMENT = "prod"
    backend = Neo4jBackend(enterprise=enterprise)
  if driver is not None:
    backend.driver = driver
  return backend


@pytest.mark.unit
class TestNeo4jBackendEnsureConnected:
  """Tests for _ensure_connected lazy initialization."""

  @pytest.mark.asyncio
  async def test_ensure_connected_skips_if_driver_exists(self):
    """Should not call _connect if driver already exists."""
    mock_driver = MagicMock()
    backend = _make_backend(driver=mock_driver)

    with patch.object(backend, "_connect", new_callable=AsyncMock) as mock_connect:
      await backend._ensure_connected()
      mock_connect.assert_not_called()

  @pytest.mark.asyncio
  async def test_ensure_connected_calls_connect_if_no_driver(self):
    """Should call _connect if driver is None."""
    backend = _make_backend()
    assert backend.driver is None

    with patch.object(backend, "_connect", new_callable=AsyncMock) as mock_connect:
      await backend._ensure_connected()
      mock_connect.assert_called_once()


@pytest.mark.unit
class TestNeo4jBackendConnect:
  """Tests for _connect with password sourcing."""

  @pytest.mark.asyncio
  async def test_connect_with_none_password_uses_secrets_manager(self):
    """Should use Secrets Manager when NEO4J_PASSWORD is None."""
    mock_secrets = MagicMock()
    mock_secrets.get_secret_value.return_value = {
      "SecretString": json.dumps({"password": "sm_password"})
    }

    with (
      patch("robosystems.graph_api.backends.neo4j.env") as mock_env,
      patch("robosystems.graph_api.backends.neo4j.boto3") as mock_boto3,
      patch("robosystems.graph_api.backends.neo4j.AsyncGraphDatabase") as mock_agdb,
      patch("robosystems.graph_api.backends.neo4j.NEO4J_MAX_CONNECTION_LIFETIME", 3600),
      patch("robosystems.graph_api.backends.neo4j.NEO4J_MAX_CONNECTION_POOL_SIZE", 50),
      patch(
        "robosystems.graph_api.backends.neo4j.NEO4J_CONNECTION_ACQUISITION_TIMEOUT",
        60,
      ),
    ):
      mock_env.NEO4J_URI = "bolt://localhost:7687"
      mock_env.NEO4J_USERNAME = "neo4j"
      mock_env.NEO4J_PASSWORD = None
      mock_env.AWS_REGION = "us-east-1"
      mock_env.ENVIRONMENT = "staging"
      mock_boto3.client.return_value = mock_secrets
      mock_agdb.driver.return_value = MagicMock()

      backend = Neo4jBackend()
      await backend._connect()

    assert backend._password == "sm_password"
    mock_secrets.get_secret_value.assert_called_once_with(
      SecretId="robosystems/staging/neo4j"
    )


@pytest.mark.unit
class TestNeo4jBackendGetDatabaseName:
  """Tests for _get_database_name routing logic."""

  def test_explicit_database_parameter(self):
    """Should return explicit database parameter when provided."""
    backend = _make_backend(enterprise=True)
    assert backend._get_database_name("graph1", database="custom_db") == "custom_db"

  def test_enterprise_auto_naming(self):
    """Should generate kg_graphid_main for enterprise mode."""
    backend = _make_backend(enterprise=True)
    assert backend._get_database_name("abc123") == "kg_abc123_main"

  def test_community_always_neo4j(self):
    """Should always return 'neo4j' for community edition."""
    backend = _make_backend(enterprise=False)
    assert backend._get_database_name("abc123") == "neo4j"


@pytest.mark.unit
class TestNeo4jBackendExecuteWriteTransaction:
  """Tests for execute_write transaction function."""

  @pytest.mark.asyncio
  async def test_execute_write_uses_write_transaction(self):
    """Should use session.execute_write for write operations."""
    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_session.execute_write.return_value = [{"count": 42}]
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend = _make_backend(enterprise=False, driver=mock_driver)

    result = await backend.execute_write("g1", "CREATE (n:Node)", {"key": "val"})

    assert result == [{"count": 42}]
    mock_session.execute_write.assert_called_once()

  @pytest.mark.asyncio
  async def test_execute_write_with_custom_database(self):
    """Should use custom database name in session."""
    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_session.execute_write.return_value = []
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend = _make_backend(enterprise=True, driver=mock_driver)

    await backend.execute_write("g1", "CREATE (n)", database="custom_db")

    mock_driver.session.assert_called_once_with(database="custom_db")


@pytest.mark.unit
class TestNeo4jBackendGetDatabaseInfo:
  """Tests for get_database_info edge cases."""

  @pytest.mark.asyncio
  async def test_get_database_info_community_no_size(self):
    """Community edition should not query sizeOnDisk."""
    mock_driver = MagicMock()
    mock_session = AsyncMock()

    mock_node_result = AsyncMock()
    mock_node_result.single.return_value = {"count": 10}

    mock_rel_result = AsyncMock()
    mock_rel_result.single.return_value = {"count": 5}

    mock_session.run.side_effect = [mock_node_result, mock_rel_result]
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend = _make_backend(enterprise=False, driver=mock_driver)

    result = await backend.get_database_info("test_db")

    assert result.node_count == 10
    assert result.relationship_count == 5
    assert result.size_bytes == 0

  @pytest.mark.asyncio
  async def test_get_database_info_null_single_results(self):
    """Should handle None from single() gracefully."""
    mock_driver = MagicMock()
    mock_session = AsyncMock()

    mock_node_result = AsyncMock()
    mock_node_result.single.return_value = None

    mock_rel_result = AsyncMock()
    mock_rel_result.single.return_value = None

    mock_session.run.side_effect = [mock_node_result, mock_rel_result]
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend = _make_backend(enterprise=False, driver=mock_driver)

    result = await backend.get_database_info("test_db")

    assert result.node_count == 0
    assert result.relationship_count == 0


@pytest.mark.unit
class TestNeo4jBackendClusterTopology:
  """Tests for get_cluster_topology edge cases."""

  @pytest.mark.asyncio
  async def test_cluster_topology_exception_fallback(self):
    """Should fall back to single mode on error."""
    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_session.run.side_effect = RuntimeError("Cluster not available")
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend = _make_backend(enterprise=True, driver=mock_driver)

    result = await backend.get_cluster_topology()

    assert result.mode == "single"
    assert result.leader == {"url": "bolt://localhost:7687"}

  @pytest.mark.asyncio
  async def test_cluster_topology_no_leader(self):
    """Should handle topology with no leader."""
    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_result = AsyncMock()
    mock_result.data.return_value = [
      {
        "id": "s1",
        "address": "bolt://s1:7687",
        "role": "FOLLOWER",
        "database": "neo4j",
      },
    ]
    mock_session.run.return_value = mock_result
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend = _make_backend(enterprise=True, driver=mock_driver)

    result = await backend.get_cluster_topology()

    assert result.mode == "cluster"
    assert result.leader is None
    assert len(result.followers) == 1


@pytest.mark.unit
class TestNeo4jBackendIngestFromS3:
  """Tests for ingest_from_s3 S3 discovery and batch loading."""

  @pytest.mark.asyncio
  async def test_ingest_no_files_found(self):
    """Should return zero records when no parquet files found."""
    mock_driver = MagicMock()
    backend = _make_backend(enterprise=False, driver=mock_driver)

    mock_s3_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": []}]
    mock_s3_client.get_paginator.return_value = mock_paginator

    mock_boto3 = MagicMock()
    mock_boto3.client.return_value = mock_s3_client

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
      result = await backend.ingest_from_s3(
        graph_id="g1",
        table_name="T",
        s3_pattern="s3://bucket/path/*.parquet",
        s3_credentials={
          "aws_access_key_id": "fake",
          "aws_secret_access_key": "fake",
          "region": "us-east-1",
        },
      )

    assert result["records_loaded"] == 0
    assert result["files_processed"] == 0

  @pytest.mark.asyncio
  async def test_ingest_s3_list_error(self):
    """Should raise S3IngestionError when S3 listing fails."""
    from botocore.exceptions import ClientError

    mock_driver = MagicMock()
    backend = _make_backend(enterprise=False, driver=mock_driver)

    mock_s3_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError(
      {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
      "ListObjectsV2",
    )
    mock_s3_client.get_paginator.return_value = mock_paginator

    mock_boto3 = MagicMock()
    mock_boto3.client.return_value = mock_s3_client

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
      with pytest.raises(S3IngestionError):
        await backend.ingest_from_s3(
          graph_id="g1",
          table_name="T",
          s3_pattern="s3://bucket/path/*.parquet",
          s3_credentials={
            "aws_access_key_id": "fake",
            "aws_secret_access_key": "fake",
            "region": "us-east-1",
          },
        )

  @pytest.mark.asyncio
  async def test_ingest_s3_credentials_passed_to_client(self):
    """Should pass explicit credentials to S3 client."""
    mock_driver = MagicMock()
    backend = _make_backend(enterprise=False, driver=mock_driver)

    mock_s3_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": []}]
    mock_s3_client.get_paginator.return_value = mock_paginator

    s3_creds = {
      "aws_access_key_id": "AK",
      "aws_secret_access_key": "SK",
      "region": "eu-west-1",
      "endpoint_url": "http://localhost:4566",
    }

    mock_boto3 = MagicMock()
    mock_boto3.client.return_value = mock_s3_client

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
      await backend.ingest_from_s3(
        graph_id="g1",
        table_name="T",
        s3_pattern="s3://bucket/path/*.parquet",
        s3_credentials=s3_creds,
      )

    mock_boto3.client.assert_called_once_with(
      "s3",
      aws_access_key_id="AK",
      aws_secret_access_key="SK",
      region_name="eu-west-1",
      endpoint_url="http://localhost:4566",
    )

  @pytest.mark.asyncio
  async def test_ingest_file_loading_error_with_ignore_errors(self):
    """Should skip files that fail to load when ignore_errors is True."""
    mock_driver = MagicMock()
    mock_session = AsyncMock()
    mock_driver.session.return_value.__aenter__.return_value = mock_session
    backend = _make_backend(enterprise=False, driver=mock_driver)

    mock_s3_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
      {
        "Contents": [
          {"Key": "path/file.parquet"},
        ]
      }
    ]
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_s3_client.get_object.side_effect = Exception("Download failed")

    mock_boto3 = MagicMock()
    mock_boto3.client.return_value = mock_s3_client

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
      result = await backend.ingest_from_s3(
        graph_id="g1",
        table_name="T",
        s3_pattern="s3://bucket/path/*.parquet",
        s3_credentials={
          "aws_access_key_id": "fake",
          "aws_secret_access_key": "fake",
          "region": "us-east-1",
        },
        ignore_errors=True,
      )

    assert result["records_loaded"] == 0
    assert result["files_processed"] == 1

  @pytest.mark.asyncio
  async def test_ingest_file_loading_error_without_ignore_errors(self):
    """Should raise when file fails to load and ignore_errors is False."""
    mock_driver = MagicMock()
    backend = _make_backend(enterprise=False, driver=mock_driver)

    mock_s3_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
      {
        "Contents": [
          {"Key": "path/file.parquet"},
        ]
      }
    ]
    mock_s3_client.get_paginator.return_value = mock_paginator
    mock_s3_client.get_object.side_effect = Exception("Download failed")

    mock_boto3 = MagicMock()
    mock_boto3.client.return_value = mock_s3_client

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
      with pytest.raises(Exception, match="Download failed"):
        await backend.ingest_from_s3(
          graph_id="g1",
          table_name="T",
          s3_pattern="s3://bucket/path/*.parquet",
          s3_credentials={
            "aws_access_key_id": "fake",
            "aws_secret_access_key": "fake",
            "region": "us-east-1",
          },
          ignore_errors=False,
        )

  @pytest.mark.asyncio
  async def test_ingest_s3_pattern_stripping(self):
    """Should strip s3:// prefix from pattern."""
    mock_driver = MagicMock()
    backend = _make_backend(enterprise=False, driver=mock_driver)

    mock_s3_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": []}]
    mock_s3_client.get_paginator.return_value = mock_paginator

    mock_boto3 = MagicMock()
    mock_boto3.client.return_value = mock_s3_client

    with patch.dict("sys.modules", {"boto3": mock_boto3}):
      await backend.ingest_from_s3(
        graph_id="g1",
        table_name="T",
        s3_pattern="s3://my-bucket/data/*.parquet",
        s3_credentials={
          "aws_access_key_id": "fake",
          "aws_secret_access_key": "fake",
          "region": "us-east-1",
        },
      )

    # Should have called paginate with bucket="my-bucket"
    mock_paginator.paginate.assert_called_once_with(Bucket="my-bucket", Prefix="data")


@pytest.mark.unit
class TestNeo4jBackendClose:
  """Tests for close method."""

  @pytest.mark.asyncio
  async def test_close_with_no_driver(self):
    """Should handle close gracefully when driver is None."""
    backend = _make_backend()
    assert backend.driver is None

    await backend.close()

    assert backend.driver is None
