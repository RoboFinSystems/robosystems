"""
Unit tests for the connection sync endpoint.

Tests the sync_connection endpoint covering happy path, not found,
timeout, provider disabled, and service error scenarios.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.connections.sync import sync_connection

SYNC_MODULE = "robosystems.routers.graphs.connections.sync"

GRAPH_ID = "kg01234567890abcdef"
USER_ID = "usr_test123"
CONNECTION_ID = "conn_test456"


def _make_mock_user(user_id: str = USER_ID):
  user = MagicMock()
  user.id = user_id
  user.email = "test@example.com"
  return user


def _make_mock_cache():
  """Return an AsyncMock IdempotencyCache that always misses (no replay)."""
  cache = AsyncMock()
  cache.get = AsyncMock(return_value=None)
  cache.put = AsyncMock(return_value=None)
  return cache


def _make_connection_dict(
  connection_id: str = CONNECTION_ID,
  provider: str = "quickbooks",
  entity_id: str = GRAPH_ID,
  status: str = "connected",
) -> dict:
  return {
    "connection_id": connection_id,
    "provider": provider,
    "entity_id": entity_id,
    "status": status,
    "created_at": datetime.now(UTC),
    "updated_at": datetime.now(UTC),
    "metadata": {
      "last_sync": None,
      "realm_id": "123456",
    },
  }


def _make_sync_request(
  full_rebuild: bool = False,
  since_date=None,
  sync_options: dict | None = None,
):
  from robosystems.models.api.graphs.connections import SyncConnectionRequest

  return SyncConnectionRequest(
    full_rebuild=full_rebuild,
    since_date=since_date,
    sync_options=sync_options,
  )


def _make_robustness_components():
  circuit_breaker = MagicMock()
  circuit_breaker.check_circuit = MagicMock()
  circuit_breaker.record_success = MagicMock()
  circuit_breaker.record_failure = MagicMock()

  timeout_coordinator = MagicMock()
  timeout_coordinator.calculate_timeout = MagicMock(return_value=60.0)

  operation_logger = MagicMock()
  operation_logger.log_external_service_call = MagicMock()

  return {
    "circuit_breaker": circuit_breaker,
    "timeout_coordinator": timeout_coordinator,
    "operation_logger": operation_logger,
    "operation_start_time": 0.0,
  }


# ---------------------------------------------------------------------------
# sync_connection
# ---------------------------------------------------------------------------


class TestSyncConnection:
  """Tests for the sync_connection endpoint."""

  @pytest.fixture(autouse=True)
  def _bypass_sync_lock(self):
    """Phase 3 B7 — the sync endpoint acquires a `DistributedLock`
    against the real Valkey LOCKS database. Unit tests run sequentially
    against shared Valkey state, so the first test's lock for
    `conn_test456` would 409 every subsequent test for the same
    connection_id (TTL is 30 min). Patch the lock to always acquire.

    Tests that specifically cover the 409 contention path mock this
    differently per-test.
    """
    mock_lock_instance = MagicMock()
    mock_lock_result = MagicMock()
    mock_lock_result.acquired = True
    mock_lock_instance.acquire.return_value = mock_lock_result

    mock_lock_class = MagicMock(return_value=mock_lock_instance)
    mock_create_redis = MagicMock(return_value=MagicMock())

    with (
      patch(
        "robosystems.middleware.auth.distributed_lock.DistributedLock",
        mock_lock_class,
      ),
      patch(
        "robosystems.config.valkey_registry.create_redis_client",
        mock_create_redis,
      ),
    ):
      yield

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_success_quickbooks(self):
    """Happy path: QuickBooks sync returns OperationEnvelope with task_id in result."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_success"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.sync_connection = AsyncMock(return_value="task_qb_001")

      result = await sync_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
        idempotency_key=None,
        cache=_make_mock_cache(),
      )

    assert result.result["task_id"] == "task_qb_001"
    assert result.status == "pending"
    assert result.result["connection_id"] == CONNECTION_ID
    assert "QUICKBOOKS" in result.result["message"]

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_success_sec(self):
    """Happy path: SEC sync returns OperationEnvelope with task_id in result."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    connection_dict = _make_connection_dict(provider="sec")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_success"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.sync_connection = AsyncMock(return_value="task_sec_001")

      result = await sync_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
        idempotency_key=None,
        cache=_make_mock_cache(),
      )

    assert result.result["task_id"] == "task_sec_001"
    assert "SEC" in result.result["message"]

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_not_found_raises_404(self):
    """Connection not found → 404 Not Found."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_failure"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=None,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await sync_connection(
          graph_id=GRAPH_ID,
          connection_id="nonexistent",
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
          idempotency_key=None,
          cache=_make_mock_cache(),
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_provider_disabled_raises_403(self):
    """Provider disabled via ValueError → 403 Forbidden."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_failure"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(
        side_effect=ValueError("Provider is disabled")
      )

      with pytest.raises(HTTPException) as exc_info:
        await sync_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
          idempotency_key=None,
          cache=_make_mock_cache(),
        )

    assert exc_info.value.status_code == 403

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_timeout_raises_504(self):
    """Timeout during sync operation → 504 Gateway Timeout."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_failure"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
      patch(f"{SYNC_MODULE}.asyncio.wait_for", side_effect=TimeoutError()),
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())

      with pytest.raises(HTTPException) as exc_info:
        await sync_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
          idempotency_key=None,
          cache=_make_mock_cache(),
        )

    assert exc_info.value.status_code == 504

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_service_error_raises_500(self):
    """Unexpected exception during sync → 500 Internal Server Error."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_failure"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.sync_connection = AsyncMock(
        side_effect=RuntimeError("Queue unavailable")
      )

      with pytest.raises(HTTPException) as exc_info:
        await sync_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
          idempotency_key=None,
          cache=_make_mock_cache(),
        )

    assert exc_info.value.status_code == 500

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_http_exception_propagates(self):
    """HTTPException raised internally is re-raised as-is."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    original_exc = HTTPException(status_code=402, detail="Payment required")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_failure"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        side_effect=original_exc,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await sync_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
          idempotency_key=None,
          cache=_make_mock_cache(),
        )

    assert exc_info.value.status_code == 402

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_passes_sync_options_to_registry(self):
    """Top-level fields are merged into sync_options before being passed to the provider registry."""
    from datetime import date

    mock_user = _make_mock_user()
    mock_db = MagicMock()
    sync_options = {"lookback_days": 90, "form_types": ["10-K"]}
    request = _make_sync_request(
      full_rebuild=True,
      since_date=date(2024, 1, 1),
      sync_options=sync_options,
    )
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_success"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      sync_mock = AsyncMock(return_value="task_123")
      mock_registry.sync_connection = sync_mock

      await sync_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
        idempotency_key=None,
        cache=_make_mock_cache(),
      )

    sync_mock.assert_awaited_once_with(
      "quickbooks",
      connection_dict,
      {
        "lookback_days": 90,
        "form_types": ["10-K"],
        "full_rebuild": True,
        "since_date": "2024-01-01",
      },
      GRAPH_ID,
    )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_passes_none_when_no_options(self):
    """With no top-level fields and no sync_options, the registry receives None."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_success"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      sync_mock = AsyncMock(return_value="task_456")
      mock_registry.sync_connection = sync_mock

      await sync_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
        idempotency_key=None,
        cache=_make_mock_cache(),
      )

    sync_mock.assert_awaited_once_with("quickbooks", connection_dict, None, GRAPH_ID)

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_circuit_breaker_checked(self):
    """Circuit breaker check_circuit is invoked before the operation."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_success"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.sync_connection = AsyncMock(return_value="task_001")

      await sync_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
        idempotency_key=None,
        cache=_make_mock_cache(),
      )

    components["circuit_breaker"].check_circuit.assert_called_once_with(
      GRAPH_ID, "connection_sync"
    )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_record_failure_on_provider_disabled(self):
    """record_operation_failure is called when provider is disabled."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_failure") as mock_record_failure,
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(
        side_effect=ValueError("Provider disabled")
      )

      with pytest.raises(HTTPException):
        await sync_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
          idempotency_key=None,
          cache=_make_mock_cache(),
        )

    mock_record_failure.assert_called_once()
    call_kwargs = mock_record_failure.call_args.kwargs
    assert call_kwargs.get("error_type") == "provider_disabled"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_record_success_on_completion(self):
    """record_operation_success is called on successful sync."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_success") as mock_record_success,
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.sync_connection = AsyncMock(return_value="task_success")

      await sync_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
        idempotency_key=None,
        cache=_make_mock_cache(),
      )

    mock_record_success.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_message_contains_uppercase_provider(self):
    """Return message includes uppercase provider name."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_success"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.sync_connection = AsyncMock(return_value="task_quickbooks")

      result = await sync_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
        idempotency_key=None,
        cache=_make_mock_cache(),
      )

    assert "QUICKBOOKS" in result.result["message"]

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sync_connection_get_provider_called_with_provider_name(self):
    """provider_registry.get_provider is called with the lowercased provider."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_sync_request()
    # Use mixed-case provider in stored connection to test lowercasing
    connection_dict = _make_connection_dict(provider="QuickBooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{SYNC_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{SYNC_MODULE}.record_operation_start"),
      patch(f"{SYNC_MODULE}.record_operation_success"),
      patch(
        f"{SYNC_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(f"{SYNC_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.sync_connection = AsyncMock(return_value="task_001")

      await sync_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
        idempotency_key=None,
        cache=_make_mock_cache(),
      )

    mock_registry.get_provider.assert_called_once_with("quickbooks")
