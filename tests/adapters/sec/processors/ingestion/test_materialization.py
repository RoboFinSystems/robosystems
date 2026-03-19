"""Tests for LadybugDB materialization operations.

Tests LadybugMaterializer initialization, materialize_from_duckdb,
copy_incremental_to_ladybug, update_entities_from_s3, and helper functions.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.adapters.sec.processors.ingestion.materialization import (
  _is_empty_value,
  _to_native,
)


@pytest.mark.unit
class TestIsEmptyValue:
  """Tests for _is_empty_value helper."""

  def test_none_is_empty(self):
    assert _is_empty_value(None) is True

  def test_empty_string_is_empty(self):
    assert _is_empty_value("") is True

  def test_whitespace_string_is_empty(self):
    assert _is_empty_value("   ") is True

  def test_nonempty_string_not_empty(self):
    assert _is_empty_value("hello") is False

  def test_zero_not_empty(self):
    assert _is_empty_value(0) is False

  def test_false_not_empty(self):
    assert _is_empty_value(False) is False

  def test_nan_is_empty(self):
    assert _is_empty_value(float("nan")) is True

  def test_float_not_empty(self):
    assert _is_empty_value(3.14) is False


@pytest.mark.unit
class TestToNative:
  """Tests for _to_native helper."""

  def test_none_returns_none(self):
    assert _to_native(None) is None

  def test_empty_string_returns_none(self):
    assert _to_native("") is None

  def test_whitespace_string_returns_none(self):
    assert _to_native("   ") is None

  def test_normal_string_stripped(self):
    assert _to_native("  hello  ") == "hello"

  def test_int_passes_through(self):
    assert _to_native(42) == 42

  def test_bool_passes_through(self):
    assert _to_native(True) is True

  def test_float_passes_through(self):
    assert _to_native(3.14) == 3.14

  def test_nan_returns_none(self):
    assert _to_native(float("nan")) is None

  def test_numpy_int(self):
    """numpy int64 with .item() method converts to native Python int."""
    mock_np_int = MagicMock()
    mock_np_int.item.return_value = 42
    # _is_empty_value returns False for non-None MagicMock
    assert _to_native(mock_np_int) == 42

  def test_arbitrary_object_to_string(self):
    """Arbitrary objects are converted to string."""
    result = _to_native(["list", "value"])
    assert result == "['list', 'value']"


@pytest.mark.unit
class TestLadybugMaterializerInit:
  """Tests for LadybugMaterializer initialization."""

  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  def test_default_init(self, mock_env, mock_s3_client):
    """Default initialization sets expected attributes."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    assert mat.graph_id == "sec"
    assert mat.source_prefix == "sec/processed"
    assert mat.bucket == "test-bucket"

  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  def test_custom_init(self, mock_env, mock_s3_client):
    """Custom initialization overrides defaults."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer(graph_id="custom", source_prefix="custom/prefix")
    assert mat.graph_id == "custom"
    assert mat.source_prefix == "custom/prefix"


@pytest.mark.unit
class TestMaterializeFromDuckDBErrors:
  """Tests for materialize_from_duckdb error handling."""

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_graph_client_init_failure(
    self, mock_env, mock_s3_client, mock_context, mock_get_client
  ):
    """Returns error when graph client fails to initialize."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_env.ENVIRONMENT = "dev"

    mock_context.get_all_table_names_for_context.return_value = {"Entity": "nodes"}
    mock_context.SEC_REPOSITORY = "sec"

    mock_get_client.side_effect = RuntimeError("Connection refused")

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.materialize_from_duckdb()

    assert result.status == "error"
    assert "Graph client initialization failed" in result.error

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_empty_table_names(self, mock_env, mock_s3_client, mock_context):
    """Returns no_data when no tables are found in schema."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_env.ENVIRONMENT = "dev"

    mock_context.get_all_table_names_for_context.return_value = {}
    mock_context.SEC_REPOSITORY = "sec"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.materialize_from_duckdb()

    assert result.status == "no_data"

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_unexpected_exception(
    self, mock_env, mock_s3_client, mock_context, mock_get_client
  ):
    """Unexpected exceptions are caught and returned as error."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_env.ENVIRONMENT = "dev"

    mock_context.get_all_table_names_for_context.side_effect = RuntimeError("Boom")
    mock_context.SEC_REPOSITORY = "sec"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.materialize_from_duckdb()

    assert result.status == "error"
    assert "Boom" in result.error


@pytest.mark.unit
class TestMaterializeFromDuckDBSuccess:
  """Tests for successful materialize_from_duckdb."""

  @pytest.mark.asyncio
  @patch(
    "robosystems.operations.graph.shared_repository_service.ensure_shared_repository_exists",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_specific_table_names(
    self,
    mock_env,
    mock_s3_client,
    mock_context,
    mock_get_client,
    mock_ensure_repo,
  ):
    """Can materialize specific table names instead of all from schema."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_env.ENVIRONMENT = "dev"

    mock_ensure_repo.return_value = {"status": "exists"}

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client
    mock_client.materialize_table.return_value = {
      "rows_ingested": 100,
      "execution_time_ms": 500,
      "status": "success",
    }

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.materialize_from_duckdb(table_names=["Entity"])

    assert result.status == "success"
    assert result.total_rows_ingested == 100
    mock_client.materialize_table.assert_called_once()


@pytest.mark.unit
class TestCopyIncrementalToLadybug:
  """Tests for copy_incremental_to_ladybug."""

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.s3_get_table_patterns"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_successful_copy(
    self, mock_env, mock_s3_client, mock_context, mock_get_client, mock_patterns
  ):
    """Successful incremental copy returns correct row counts."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    mock_context.get_all_table_names_for_context.return_value = {
      "Entity": "nodes",
    }
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client
    mock_client.copy_from_s3.return_value = {
      "status": "completed",
      "records_loaded": 200,
      "duration_seconds": 5.0,
    }

    mock_patterns.return_value = [
      "s3://test-bucket/sec/processed/filed=2024-Q1/nodes/Entity/*.parquet"
    ]

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.copy_incremental_to_ladybug(year=2024, quarter=1)

    assert result.status == "success"
    assert result.total_rows == 200
    assert "Entity" in result.table_names

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.s3_get_table_patterns"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_no_files_for_table_skipped(
    self, mock_env, mock_s3_client, mock_context, mock_get_client, mock_patterns
  ):
    """Tables with no S3 files are skipped gracefully."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    mock_context.get_all_table_names_for_context.return_value = {
      "Entity": "nodes",
    }
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client

    mock_patterns.return_value = []

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.copy_incremental_to_ladybug(year=2024, quarter=1)

    assert result.status == "success"
    assert "Entity" in result.table_names
    mock_client.copy_from_s3.assert_not_called()

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.s3_get_table_patterns"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_copy_failure_tracked(
    self, mock_env, mock_s3_client, mock_context, mock_get_client, mock_patterns
  ):
    """Failed copies are tracked in result."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    mock_context.get_all_table_names_for_context.return_value = {
      "Entity": "nodes",
    }
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client
    mock_client.copy_from_s3.return_value = {
      "status": "failed",
      "error": "Table not found in LadybugDB",
    }

    mock_patterns.return_value = [
      "s3://test-bucket/sec/processed/filed=2024-Q1/nodes/Entity/*.parquet"
    ]

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.copy_incremental_to_ladybug(year=2024, quarter=1)

    assert result.status == "partial"
    assert len(result.failed_tables) == 1
    assert result.failed_tables[0]["table"] == "Entity"

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.s3_get_table_patterns"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.RoboLedgerContext"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_copy_exception_no_files_found(
    self, mock_env, mock_s3_client, mock_context, mock_get_client, mock_patterns
  ):
    """Exception with 'No files found' is treated as skip, not failure."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    mock_context.get_all_table_names_for_context.return_value = {
      "Entity": "nodes",
    }
    mock_context.SEC_REPOSITORY = "sec"

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client
    mock_client.copy_from_s3.side_effect = RuntimeError("No files found for pattern")

    mock_patterns.return_value = [
      "s3://test-bucket/sec/processed/filed=2024-Q1/nodes/Entity/*.parquet"
    ]

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.copy_incremental_to_ladybug(year=2024, quarter=1)

    assert result.status == "success"
    assert "Entity" in result.table_names

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_graph_client_failure(self, mock_env, mock_s3_client, mock_get_client):
    """Returns error when graph client fails."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_get_client.side_effect = RuntimeError("Connection refused")

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.copy_incremental_to_ladybug(year=2024, quarter=1)

    assert result.status == "error"
    assert "Connection refused" in result.error


@pytest.mark.unit
class TestUpdateEntitiesFromS3:
  """Tests for update_entities_from_s3."""

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.s3_table_data_exists"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_no_entity_data_returns_no_changes(
    self, mock_env, mock_s3_client, mock_data_exists
  ):
    """Returns no_changes when no Entity file exists for the quarter."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_data_exists.return_value = False

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.update_entities_from_s3(year=2024, quarter=1)

    assert result.status == "no_changes"
    assert result.entities_checked == 0

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.s3_table_data_exists"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_no_changes_needed(
    self, mock_env, mock_s3_client_cls, mock_data_exists, mock_get_client
  ):
    """Returns no_changes when all entities match existing data."""
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_data_exists.return_value = True

    # Create entity parquet bytes
    entity_table = pa.table(
      {
        "identifier": ["ent1"],
        "name": ["Test Corp"],
        "legal_name": ["Test Corp"],
        "ticker": ["TEST"],
        "exchange": ["NYSE"],
        "category": ["Large"],
        "fiscal_year_end": ["1231"],
        "phone": ["555-1234"],
        "website": ["https://test.com"],
        "status": ["active"],
        "sic": ["3674"],
        "sic_description": ["Semiconductors"],
      }
    )
    buf = io.BytesIO()
    pq.write_table(entity_table, buf)
    parquet_bytes = buf.getvalue()

    # Mock S3Client instance
    mock_s3_instance = MagicMock()
    mock_s3_client_cls.return_value = mock_s3_instance
    mock_s3_instance.object_exists.return_value = True
    mock_s3_instance.s3_client.get_object.return_value = {
      "Body": MagicMock(read=MagicMock(return_value=parquet_bytes))
    }

    # Graph client returns matching entity (no changes)
    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client
    mock_client.query.return_value = {
      "data": [
        {
          "identifier": "ent1",
          "name": "Test Corp",
          "legal_name": "Test Corp",
          "ticker": "TEST",
          "exchange": "NYSE",
          "category": "Large",
          "fiscal_year_end": "1231",
          "phone": "555-1234",
          "website": "https://test.com",
          "status": "active",
          "sic": "3674",
          "sic_description": "Semiconductors",
        }
      ]
    }

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.update_entities_from_s3(year=2024, quarter=1)

    assert result.status == "no_changes"
    assert result.entities_checked == 1
    assert result.entities_unchanged == 1

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.s3_table_data_exists"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_entity_with_changes_updated(
    self, mock_env, mock_s3_client_cls, mock_data_exists, mock_get_client
  ):
    """Entities with changed fields are updated via MERGE."""
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_data_exists.return_value = True

    entity_table = pa.table(
      {
        "identifier": ["ent1"],
        "name": ["New Name Corp"],  # Changed
        "legal_name": ["New Name Corp"],
        "ticker": ["TEST"],
        "exchange": ["NYSE"],
        "category": ["Large"],
        "fiscal_year_end": ["1231"],
        "phone": ["555-1234"],
        "website": ["https://test.com"],
        "status": ["active"],
        "sic": ["3674"],
        "sic_description": ["Semiconductors"],
      }
    )
    buf = io.BytesIO()
    pq.write_table(entity_table, buf)
    parquet_bytes = buf.getvalue()

    mock_s3_instance = MagicMock()
    mock_s3_client_cls.return_value = mock_s3_instance
    mock_s3_instance.object_exists.return_value = True
    mock_s3_instance.s3_client.get_object.return_value = {
      "Body": MagicMock(read=MagicMock(return_value=parquet_bytes))
    }

    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client
    mock_client.query.return_value = {
      "data": [
        {
          "identifier": "ent1",
          "name": "Old Name Corp",  # Old value
          "legal_name": "Old Name Corp",
          "ticker": "TEST",
          "exchange": "NYSE",
          "category": "Large",
          "fiscal_year_end": "1231",
          "phone": "555-1234",
          "website": "https://test.com",
          "status": "active",
          "sic": "3674",
          "sic_description": "Semiconductors",
        }
      ]
    }

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.update_entities_from_s3(year=2024, quarter=1)

    assert result.status == "success"
    assert result.entities_updated == 1
    assert result.entities_checked == 1
    # Verify MERGE query was executed
    # The second call is the MERGE query (first was the SELECT)
    assert mock_client.query.call_count == 2

  @pytest.mark.asyncio
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.get_graph_client"
  )
  @patch(
    "robosystems.adapters.sec.processors.ingestion.materialization.s3_table_data_exists"
  )
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_new_entity_not_updated(
    self, mock_env, mock_s3_client_cls, mock_data_exists, mock_get_client
  ):
    """Entities not in graph (new) are skipped (COPY handles them)."""
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"
    mock_data_exists.return_value = True

    entity_table = pa.table(
      {
        "identifier": ["ent_new"],
        "name": ["Brand New Corp"],
        "legal_name": [None],
        "ticker": [None],
        "exchange": [None],
        "category": [None],
        "fiscal_year_end": [None],
        "phone": [None],
        "website": [None],
        "status": ["active"],
        "sic": [None],
        "sic_description": [None],
      }
    )
    buf = io.BytesIO()
    pq.write_table(entity_table, buf)
    parquet_bytes = buf.getvalue()

    mock_s3_instance = MagicMock()
    mock_s3_client_cls.return_value = mock_s3_instance
    mock_s3_instance.object_exists.return_value = True
    mock_s3_instance.s3_client.get_object.return_value = {
      "Body": MagicMock(read=MagicMock(return_value=parquet_bytes))
    }

    # No existing entities in graph
    mock_client = AsyncMock()
    mock_get_client.return_value = mock_client
    mock_client.query.return_value = {"data": []}

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()
    result = await mat.update_entities_from_s3(year=2024, quarter=1)

    assert result.status == "no_changes"
    assert result.entities_checked == 1
    assert result.entities_updated == 0

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_exception_returns_error(self, mock_env, mock_s3_client_cls):
    """Unexpected exceptions return error result."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    # Patch s3_table_data_exists to raise
    with patch(
      "robosystems.adapters.sec.processors.ingestion.materialization.s3_table_data_exists"
    ) as mock_exists:
      mock_exists.side_effect = RuntimeError("Unexpected error")

      mat = LadybugMaterializer()
      result = await mat.update_entities_from_s3(year=2024, quarter=1)

      assert result.status == "error"
      assert "Unexpected error" in result.error


@pytest.mark.unit
class TestTriggerIngestion:
  """Tests for _trigger_ingestion batched materialization."""

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_single_pass_for_small_table(self, mock_env, mock_s3_client):
    """Small tables use single-pass materialization."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()

    mock_client = AsyncMock()
    mock_client.materialize_table.return_value = {
      "rows_ingested": 500,
      "execution_time_ms": 100,
      "status": "success",
    }

    result = await mat._trigger_ingestion(
      table_names=["Entity"],
      graph_client=mock_client,
      batch_materialization=True,
      batch_size=10_000_000,
    )

    assert result["total_rows_ingested"] == 500
    # Small table should not trigger row count query
    mock_client.query_table.assert_not_called()

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_batched_for_large_table(self, mock_env, mock_s3_client):
    """Large tables (Fact) use batched materialization when row count exceeds threshold."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()

    mock_client = AsyncMock()
    mock_client.query_table.return_value = {
      "rows": [[50_000_000]]  # 50M rows
    }
    mock_client.materialize_table.return_value = {
      "rows_ingested": 16_666_666,
      "execution_time_ms": 30000,
    }

    await mat._trigger_ingestion(
      table_names=["Fact"],
      graph_client=mock_client,
      batch_materialization=True,
      batch_size=10_000_000,
    )

    # 50M / 20M = 3 batches
    assert mock_client.materialize_table.call_count == 3
    # Check that batch_num and num_batches were passed
    first_call = mock_client.materialize_table.call_args_list[0]
    assert first_call.kwargs["batch_num"] == 0
    assert first_call.kwargs["num_batches"] == 3

  @pytest.mark.asyncio
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.S3Client")
  @patch("robosystems.adapters.sec.processors.ingestion.materialization.env")
  async def test_materialization_error_tracked(self, mock_env, mock_s3_client):
    """Materialization errors are captured in results."""
    mock_env.SHARED_PROCESSED_BUCKET = "test-bucket"

    from robosystems.adapters.sec.processors.ingestion.materialization import (
      LadybugMaterializer,
    )

    mat = LadybugMaterializer()

    mock_client = AsyncMock()
    mock_client.materialize_table.side_effect = RuntimeError("OOM")

    result = await mat._trigger_ingestion(
      table_names=["Entity"],
      graph_client=mock_client,
    )

    assert len(result["tables"]) == 1
    assert result["tables"][0]["status"] == "error"
    assert "OOM" in result["tables"][0]["error"]
