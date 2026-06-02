"""
Unit tests for connection management endpoints.

Tests CRUD operations: create_connection, list_connections,
get_connection, and delete_connection.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.connections.management import (
  create_connection,
  delete_connection,
  get_connection,
  list_connections,
  set_connection_write_policy,
)

MANAGEMENT_MODULE = "robosystems.routers.graphs.connections.management"

GRAPH_ID = "kg01234567890abcdef"
USER_ID = "usr_test123"
CONNECTION_ID = "conn_test456"


def _make_mock_user(user_id: str = USER_ID):
  user = MagicMock()
  user.id = user_id
  user.email = "test@example.com"
  return user


def _make_connection_dict(
  connection_id: str = CONNECTION_ID,
  provider: str = "quickbooks",
  entity_id: str = GRAPH_ID,
  status: str = "connected",
  last_sync: str | None = None,
) -> dict:
  return {
    "connection_id": connection_id,
    "provider": provider,
    "entity_id": entity_id,
    "status": status,
    "created_at": datetime.now(UTC),
    "updated_at": datetime.now(UTC),
    "metadata": {
      "last_sync": last_sync,
      "realm_id": "123456",
    },
  }


def _make_create_request(
  provider: str = "quickbooks",
  entity_id: str = GRAPH_ID,
):
  from robosystems.models.api.graphs.connections import (
    CreateConnectionRequest,
    QuickBooksConnectionConfig,
    SECConnectionConfig,
  )

  if provider == "quickbooks":
    return CreateConnectionRequest(
      provider="quickbooks",
      entity_id=entity_id,
      quickbooks_config=QuickBooksConnectionConfig(),
    )
  elif provider == "sec":
    return CreateConnectionRequest(
      provider="sec",
      entity_id=entity_id,
      sec_config=SECConnectionConfig(cik="0001234567"),
    )
  else:
    raise ValueError(f"Unsupported provider in helper: {provider}")


def _make_robustness_components():
  circuit_breaker = MagicMock()
  circuit_breaker.check_circuit = MagicMock()
  circuit_breaker.record_success = MagicMock()
  circuit_breaker.record_failure = MagicMock()

  timeout_coordinator = MagicMock()
  timeout_coordinator.calculate_timeout = MagicMock(return_value=30.0)

  operation_logger = MagicMock()
  operation_logger.log_external_service_call = MagicMock()

  return {
    "circuit_breaker": circuit_breaker,
    "timeout_coordinator": timeout_coordinator,
    "operation_logger": operation_logger,
    "operation_start_time": 0.0,
  }


# ---------------------------------------------------------------------------
# create_connection
# ---------------------------------------------------------------------------


class TestCreateConnection:
  """Tests for the create_connection endpoint."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_connection_success_quickbooks(self):
    """Happy path: creates a QuickBooks connection and returns ConnectionResponse."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_create_request(provider="quickbooks")
    connection_dict = _make_connection_dict(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{MANAGEMENT_MODULE}.record_operation_start"),
      patch(f"{MANAGEMENT_MODULE}.record_operation_success"),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.create_connection = AsyncMock(return_value=CONNECTION_ID)

      result = await create_connection(
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result.connection_id == CONNECTION_ID
    assert result.provider == "quickbooks"
    assert result.status == "connected"
    mock_registry.get_provider.assert_called_once_with("quickbooks")
    mock_registry.create_connection.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_connection_success_sec(self):
    """Happy path: creates an SEC connection."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_create_request(provider="sec")
    connection_dict = _make_connection_dict(provider="sec")
    components = _make_robustness_components()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{MANAGEMENT_MODULE}.record_operation_start"),
      patch(f"{MANAGEMENT_MODULE}.record_operation_success"),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.create_connection = AsyncMock(return_value=CONNECTION_ID)

      result = await create_connection(
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result.connection_id == CONNECTION_ID
    assert result.provider == "sec"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_connection_provider_disabled_raises_403(self):
    """Provider disabled via ValueError maps to 403 Forbidden."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_create_request(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{MANAGEMENT_MODULE}.record_operation_start"),
      patch(f"{MANAGEMENT_MODULE}.record_operation_failure"),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(
        side_effect=ValueError("QuickBooks provider is disabled")
      )

      with pytest.raises(HTTPException) as exc_info:
        await create_connection(
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 403

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_connection_timeout_raises_504(self):
    """Timeout during provider create_connection raises 504 Gateway Timeout."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_create_request(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{MANAGEMENT_MODULE}.record_operation_start"),
      patch(f"{MANAGEMENT_MODULE}.record_operation_failure"),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
      patch(f"{MANAGEMENT_MODULE}.asyncio.wait_for", side_effect=TimeoutError()),
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())

      with pytest.raises(HTTPException) as exc_info:
        await create_connection(
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 504

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_connection_service_error_raises_500(self):
    """Unexpected exception during connection creation raises 500."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_create_request(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{MANAGEMENT_MODULE}.record_operation_start"),
      patch(f"{MANAGEMENT_MODULE}.record_operation_failure"),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.create_connection = AsyncMock(
        side_effect=RuntimeError("Database connection lost")
      )

      with pytest.raises(HTTPException) as exc_info:
        await create_connection(
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 500

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_connection_get_connection_returns_none_raises_500(self):
    """If the newly created connection cannot be retrieved, raises 500."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_create_request(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{MANAGEMENT_MODULE}.record_operation_start"),
      patch(f"{MANAGEMENT_MODULE}.record_operation_failure"),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=None,
      ),
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.create_connection = AsyncMock(return_value=CONNECTION_ID)

      with pytest.raises(HTTPException) as exc_info:
        await create_connection(
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 500

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_connection_http_exception_propagates(self):
    """An HTTPException raised internally is re-raised as-is."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_create_request(provider="quickbooks")
    components = _make_robustness_components()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{MANAGEMENT_MODULE}.record_operation_start"),
      patch(f"{MANAGEMENT_MODULE}.record_operation_failure"),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      original_exc = HTTPException(status_code=409, detail="Already exists")
      mock_registry.create_connection = AsyncMock(side_effect=original_exc)

      with pytest.raises(HTTPException) as exc_info:
        await create_connection(
          graph_id=GRAPH_ID,
          request=request,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 409

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_connection_metadata_last_sync_included(self):
    """ConnectionResponse includes last_sync from metadata when present."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    request = _make_create_request(provider="quickbooks")
    last_sync_ts = "2026-01-01T12:00:00Z"
    connection_dict = _make_connection_dict(last_sync=last_sync_ts)
    components = _make_robustness_components()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.create_robustness_components",
        return_value=components,
      ),
      patch(f"{MANAGEMENT_MODULE}.record_operation_start"),
      patch(f"{MANAGEMENT_MODULE}.record_operation_success"),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.create_connection = AsyncMock(return_value=CONNECTION_ID)

      result = await create_connection(
        graph_id=GRAPH_ID,
        request=request,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result.last_sync == last_sync_ts


# ---------------------------------------------------------------------------
# list_connections
# ---------------------------------------------------------------------------


class TestListConnections:
  """Tests for the list_connections endpoint."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_list_connections_returns_all(self):
    """Happy path: returns all connections for the graph."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    connections = [
      _make_connection_dict(connection_id="conn_1", provider="quickbooks"),
      _make_connection_dict(connection_id="conn_2", provider="sec"),
    ]

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.list_connections",
      new_callable=AsyncMock,
      return_value=connections,
    ):
      result = await list_connections(
        graph_id=GRAPH_ID,
        entity_id=None,
        provider=None,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert len(result) == 2
    assert result[0].connection_id == "conn_1"
    assert result[1].connection_id == "conn_2"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_list_connections_returns_empty_list(self):
    """Returns an empty list when no connections exist."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.list_connections",
      new_callable=AsyncMock,
      return_value=[],
    ):
      result = await list_connections(
        graph_id=GRAPH_ID,
        entity_id=None,
        provider=None,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result == []

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_list_connections_filters_by_entity_id(self):
    """Passes entity_id filter to the service."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    connections = [
      _make_connection_dict(entity_id="entity_specific"),
    ]

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.list_connections",
      new_callable=AsyncMock,
      return_value=connections,
    ) as mock_list:
      await list_connections(
        graph_id=GRAPH_ID,
        entity_id="entity_specific",
        provider=None,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    mock_list.assert_called_once_with(
      entity_id="entity_specific",
      provider=None,
      user_id=USER_ID,
      graph_id=GRAPH_ID,
    )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_list_connections_filters_by_provider(self):
    """Passes provider filter to the service."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.list_connections",
      new_callable=AsyncMock,
      return_value=[],
    ) as mock_list:
      await list_connections(
        graph_id=GRAPH_ID,
        entity_id=None,
        provider="quickbooks",
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    mock_list.assert_called_once_with(
      entity_id=None,
      provider="quickbooks",
      user_id=USER_ID,
      graph_id=GRAPH_ID,
    )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_list_connections_empty_string_entity_id_treated_as_none(self):
    """Empty string entity_id is treated as None (or None) via `or None` guard."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.list_connections",
      new_callable=AsyncMock,
      return_value=[],
    ) as mock_list:
      await list_connections(
        graph_id=GRAPH_ID,
        entity_id="",
        provider=None,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    # The router uses `entity_id or None` — empty string → None
    call_kwargs = mock_list.call_args.kwargs
    assert call_kwargs["entity_id"] is None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_list_connections_service_error_raises_500(self):
    """Unexpected exception from service raises 500."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.list_connections",
      new_callable=AsyncMock,
      side_effect=RuntimeError("Unexpected DB error"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await list_connections(
          graph_id=GRAPH_ID,
          entity_id=None,
          provider=None,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 500

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_list_connections_provider_lowercase(self):
    """Provider field in response is lowercased regardless of storage case."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    conn = _make_connection_dict(provider="QuickBooks")

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.list_connections",
      new_callable=AsyncMock,
      return_value=[conn],
    ):
      result = await list_connections(
        graph_id=GRAPH_ID,
        entity_id=None,
        provider=None,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result[0].provider == "quickbooks"


# ---------------------------------------------------------------------------
# get_connection
# ---------------------------------------------------------------------------


class TestGetConnection:
  """Tests for the get_connection endpoint."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_get_connection_success(self):
    """Happy path: returns connection details."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    connection_dict = _make_connection_dict()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=connection_dict,
    ):
      result = await get_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result.connection_id == CONNECTION_ID
    assert result.status == "connected"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_get_connection_not_found_raises_404(self):
    """Service returns None → 404 Not Found."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=None,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await get_connection(
          graph_id=GRAPH_ID,
          connection_id="nonexistent",
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_get_connection_service_error_raises_500(self):
    """Unexpected exception from the service raises 500."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      side_effect=RuntimeError("Unexpected error"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await get_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 500

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_get_connection_http_exception_propagates(self):
    """An HTTPException raised internally is re-raised as-is."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    original_exc = HTTPException(status_code=403, detail="Forbidden")

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      side_effect=original_exc,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await get_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 403

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_get_connection_updated_at_optional(self):
    """Connection without updated_at is handled gracefully."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    conn = _make_connection_dict()
    del conn["updated_at"]  # Remove the key entirely

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=conn,
    ):
      result = await get_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result.updated_at is None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_get_connection_passes_user_id(self):
    """Service is called with the authenticated user's ID."""
    mock_user = _make_mock_user(user_id="usr_specific")
    mock_db = MagicMock()
    connection_dict = _make_connection_dict()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=connection_dict,
    ) as mock_get:
      await get_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    mock_get.assert_called_once_with(CONNECTION_ID, "usr_specific")


# ---------------------------------------------------------------------------
# delete_connection
# ---------------------------------------------------------------------------


class TestDeleteConnection:
  """Tests for the delete_connection endpoint."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_delete_connection_success(self):
    """Happy path: deletes connection and returns SuccessResponse."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    connection_dict = _make_connection_dict()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.delete_connection",
        new_callable=AsyncMock,
        return_value=True,
      ),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.cleanup_connection = AsyncMock()

      result = await delete_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result.success is True
    assert CONNECTION_ID in result.message

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_delete_connection_not_found_raises_404(self):
    """Connection not found on initial get → 404 Not Found."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      return_value=None,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await delete_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_delete_connection_delete_returns_false_raises_500(self):
    """Service.delete_connection returns False → 500 (connection existed but delete failed)."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    connection_dict = _make_connection_dict()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.delete_connection",
        new_callable=AsyncMock,
        return_value=False,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await delete_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 500

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_delete_connection_provider_disabled_still_deletes(self):
    """Provider disabled during cleanup skips cleanup but still deletes."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    connection_dict = _make_connection_dict()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.delete_connection",
        new_callable=AsyncMock,
        return_value=True,
      ) as mock_delete,
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(
        side_effect=ValueError("Provider is disabled")
      )

      result = await delete_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result.success is True
    mock_delete.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_delete_connection_service_error_raises_500(self):
    """Unexpected exception from service raises 500."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    connection_dict = _make_connection_dict()

    with (
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.delete_connection",
        new_callable=AsyncMock,
        side_effect=RuntimeError("DB connection lost"),
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await delete_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 500

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_delete_connection_cleanup_called_with_provider(self):
    """Cleanup is invoked on the correct provider after deletion."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    connection_dict = _make_connection_dict(provider="sec")

    with (
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.delete_connection",
        new_callable=AsyncMock,
        return_value=True,
      ),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.cleanup_connection = AsyncMock()

      await delete_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    mock_registry.cleanup_connection.assert_awaited_once_with(
      "sec", connection_dict, GRAPH_ID
    )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_delete_connection_response_contains_connection_id_and_provider(self):
    """SuccessResponse data includes connection_id and provider fields."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    connection_dict = _make_connection_dict(provider="quickbooks")

    with (
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
        new_callable=AsyncMock,
        return_value=connection_dict,
      ),
      patch(
        f"{MANAGEMENT_MODULE}.ConnectionService.delete_connection",
        new_callable=AsyncMock,
        return_value=True,
      ),
      patch(f"{MANAGEMENT_MODULE}.provider_registry") as mock_registry,
    ):
      mock_registry.get_provider = MagicMock(return_value=MagicMock())
      mock_registry.cleanup_connection = AsyncMock()

      result = await delete_connection(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result.data is not None
    assert result.data["connection_id"] == CONNECTION_ID
    assert result.data["provider"] == "quickbooks"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_delete_connection_http_exception_propagates(self):
    """HTTPException raised internally is re-raised unchanged."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    original_exc = HTTPException(status_code=403, detail="Forbidden")

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.get_connection",
      new_callable=AsyncMock,
      side_effect=original_exc,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await delete_connection(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 403


class TestSetConnectionWritePolicy:
  """Tests for the set_connection_write_policy endpoint."""

  def _request(self, write_policy: str = "qb_authoritative"):
    from robosystems.models.api.graphs.connections import SetWritePolicyRequest

    return SetWritePolicyRequest(write_policy=write_policy)

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_success_surfaces_write_policy(self):
    """Happy path: service returns the updated connection; write_policy
    is surfaced in the response."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()
    connection_dict = _make_connection_dict()
    connection_dict["write_policy"] = "qb_authoritative"

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.set_write_policy",
      new_callable=AsyncMock,
      return_value=connection_dict,
    ) as mock_set:
      result = await set_connection_write_policy(
        graph_id=GRAPH_ID,
        connection_id=CONNECTION_ID,
        request=self._request("qb_authoritative"),
        current_user=mock_user,
        db=mock_db,
        _rate_limit=None,
      )

    assert result.connection_id == CONNECTION_ID
    assert result.write_policy == "qb_authoritative"
    # graph_id is passed for scoping (IDOR guard).
    assert mock_set.call_args.kwargs["graph_id"] == GRAPH_ID
    assert mock_set.call_args.kwargs["write_policy"] == "qb_authoritative"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_not_found_raises_404(self):
    """Service returns None (missing / wrong graph / wrong user) → 404."""
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.set_write_policy",
      new_callable=AsyncMock,
      return_value=None,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await set_connection_write_policy(
          graph_id=GRAPH_ID,
          connection_id="nonexistent",
          request=self._request("native"),
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_service_error_raises_500(self):
    mock_user = _make_mock_user()
    mock_db = MagicMock()

    with patch(
      f"{MANAGEMENT_MODULE}.ConnectionService.set_write_policy",
      new_callable=AsyncMock,
      side_effect=RuntimeError("boom"),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await set_connection_write_policy(
          graph_id=GRAPH_ID,
          connection_id=CONNECTION_ID,
          request=self._request(),
          current_user=mock_user,
          db=mock_db,
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 500
