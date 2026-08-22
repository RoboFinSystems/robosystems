# pyright: reportGeneralTypeIssues=false, reportArgumentType=false, reportOperatorIssue=false, reportOptionalMemberAccess=false
"""Tests for QuickBooks provider operations."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

MODULE = "robosystems.operations.providers.quickbooks_provider"


@pytest.mark.unit
class TestQuickBooksOAuthProvider:
  @patch(f"{MODULE}.env")
  def test_sandbox_base_url(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_env.INTUIT_CLIENT_ID = "client_id"
    mock_env.INTUIT_CLIENT_SECRET = "client_secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    assert provider._base_url == "https://sandbox-quickbooks.api.intuit.com"

  @patch(f"{MODULE}.env")
  def test_production_base_url(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "production"
    mock_env.INTUIT_CLIENT_ID = "client_id"
    mock_env.INTUIT_CLIENT_SECRET = "client_secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    assert provider._base_url == "https://quickbooks.api.intuit.com"

  @patch(f"{MODULE}.env")
  def test_properties(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_env.INTUIT_CLIENT_ID = "my_client_id"
    mock_env.INTUIT_CLIENT_SECRET = "my_secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    assert provider.name == "quickbooks"
    assert provider.client_id == "my_client_id"
    assert provider.client_secret == "my_secret"
    assert provider.authorize_url == "https://appcenter.intuit.com/connect/oauth2"
    assert (
      provider.token_url == "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
    )
    assert provider.scopes == ["com.intuit.quickbooks.accounting"]

  @patch(f"{MODULE}.env")
  def test_additional_auth_params(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_env.INTUIT_CLIENT_ID = "id"
    mock_env.INTUIT_CLIENT_SECRET = "secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    assert provider.get_additional_auth_params() == {"access_type": "offline"}

  @patch(f"{MODULE}.env")
  def test_extract_provider_data(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_env.INTUIT_CLIENT_ID = "id"
    mock_env.INTUIT_CLIENT_SECRET = "secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    result = provider.extract_provider_data({"realmId": "12345", "other": "data"})

    assert result == {"realm_id": "12345"}

  @patch(f"{MODULE}.env")
  def test_extract_provider_data_missing_realm(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_env.INTUIT_CLIENT_ID = "id"
    mock_env.INTUIT_CLIENT_SECRET = "secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    result = provider.extract_provider_data({})

    assert result == {"realm_id": ""}


@pytest.mark.unit
class TestGetEntityInfo:
  @pytest.mark.asyncio
  @patch(f"{MODULE}.env")
  async def test_success(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_env.INTUIT_CLIENT_ID = "id"
    mock_env.INTUIT_CLIENT_SECRET = "secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
      "CompanyInfo": {"CompanyName": "Acme Corp", "Id": "123"}
    }

    with patch("httpx.AsyncClient") as MockClient:
      mock_client = AsyncMock()
      mock_client.get.return_value = mock_response
      MockClient.return_value.__aenter__.return_value = mock_client

      result = await provider.get_entity_info("access_tok", "realm123")

    assert result["CompanyName"] == "Acme Corp"

  @pytest.mark.asyncio
  @patch(f"{MODULE}.env")
  async def test_failure_returns_empty(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_env.INTUIT_CLIENT_ID = "id"
    mock_env.INTUIT_CLIENT_SECRET = "secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    mock_response = MagicMock()
    mock_response.status_code = 401

    with patch("httpx.AsyncClient") as MockClient:
      mock_client = AsyncMock()
      mock_client.get.return_value = mock_response
      MockClient.return_value.__aenter__.return_value = mock_client

      result = await provider.get_entity_info("bad_token", "realm123")

    assert result == {}


@pytest.mark.unit
class TestValidateConnection:
  @pytest.mark.asyncio
  @patch(f"{MODULE}.env")
  async def test_valid(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_env.INTUIT_CLIENT_ID = "id"
    mock_env.INTUIT_CLIENT_SECRET = "secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    with patch.object(provider, "get_entity_info", new_callable=AsyncMock) as mock_info:
      mock_info.return_value = {"CompanyName": "Corp"}

      result = await provider.validate_connection("tok", "realm")

    assert result is True

  @pytest.mark.asyncio
  @patch(f"{MODULE}.env")
  async def test_invalid(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_env.INTUIT_CLIENT_ID = "id"
    mock_env.INTUIT_CLIENT_SECRET = "secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    with patch.object(provider, "get_entity_info", new_callable=AsyncMock) as mock_info:
      mock_info.return_value = {}

      result = await provider.validate_connection("tok", "realm")

    assert result is False

  @pytest.mark.asyncio
  @patch(f"{MODULE}.env")
  async def test_exception_returns_false(self, mock_env):
    mock_env.INTUIT_ENVIRONMENT = "sandbox"
    mock_env.INTUIT_CLIENT_ID = "id"
    mock_env.INTUIT_CLIENT_SECRET = "secret"

    from robosystems.operations.providers.quickbooks_provider import (
      QuickBooksOAuthProvider,
    )

    provider = QuickBooksOAuthProvider()

    with patch.object(provider, "get_entity_info", new_callable=AsyncMock) as mock_info:
      mock_info.side_effect = Exception("network error")

      result = await provider.validate_connection("tok", "realm")

    assert result is False


@pytest.mark.unit
class TestCreateQuickbooksConnection:
  @pytest.mark.asyncio
  async def test_creates_pending_connection(self):
    from robosystems.operations.providers.quickbooks_provider import (
      create_quickbooks_connection,
    )

    mock_config = MagicMock()
    mock_config.realm_id = "realm999"
    mock_db = MagicMock()

    with patch(f"{MODULE}.ConnectionService") as MockSvc:
      MockSvc.create_connection = AsyncMock(return_value={"connection_id": "conn_new"})

      result = await create_quickbooks_connection(
        entity_id="ent_1",
        config=mock_config,
        user_id="usr_1",
        graph_id="kg_test",
        db=mock_db,
      )

    assert result == "conn_new"
    call_kwargs = MockSvc.create_connection.call_args.kwargs
    assert call_kwargs["provider"] == "quickbooks"
    assert call_kwargs["metadata"]["status"] == "pending_oauth"
    assert call_kwargs["metadata"]["realm_id"] == "realm999"

  @pytest.mark.asyncio
  async def test_creates_without_realm_id(self):
    from robosystems.operations.providers.quickbooks_provider import (
      create_quickbooks_connection,
    )

    mock_config = MagicMock()
    mock_config.realm_id = None
    mock_db = MagicMock()

    with patch(f"{MODULE}.ConnectionService") as MockSvc:
      MockSvc.create_connection = AsyncMock(return_value={"connection_id": "conn_new"})

      await create_quickbooks_connection(
        entity_id="ent_1",
        config=mock_config,
        user_id="usr_1",
        graph_id="kg_test",
        db=mock_db,
      )

    call_kwargs = MockSvc.create_connection.call_args.kwargs
    assert call_kwargs["metadata"]["realm_id"] is None


@pytest.mark.unit
class TestSyncQuickbooksConnection:
  @pytest.mark.asyncio
  async def test_submits_dagster_job(self):
    from robosystems.operations.providers.quickbooks_provider import (
      sync_quickbooks_connection,
    )

    connection = {
      "connection_id": "conn_1",
      "user_id": "usr_1",
      "metadata": {"realm_id": "realm123"},
    }

    with patch(
      "robosystems.middleware.sse.dagster_monitor.submit_dagster_job_sync",
      return_value="run_abc123",
    ) as mock_submit:
      result = await sync_quickbooks_connection(
        connection=connection,
        sync_options={"full_rebuild": True, "lookback_days": 90},
        graph_id="kg_test",
      )

    assert result.dispatched is True
    assert result.task_id == "run_abc123"
    call_kwargs = mock_submit.call_args
    assert call_kwargs.kwargs["job_name"] == "qb_sync"
    run_config = call_kwargs.kwargs["run_config"]
    assert run_config["ops"]["qb_extract"]["config"]["graph_id"] == "kg_test"
    assert run_config["ops"]["qb_extract"]["config"]["full_rebuild"] is True
    assert run_config["ops"]["qb_extract"]["config"]["lookback_days"] == 90
    tags = call_kwargs.kwargs["tags"]
    assert tags["pipeline"] == "quickbooks"

  @pytest.mark.asyncio
  async def test_defaults_sync_options(self):
    from robosystems.operations.providers.quickbooks_provider import (
      sync_quickbooks_connection,
    )

    connection = {
      "connection_id": "conn_1",
      "user_id": "usr_1",
      "metadata": {"realm_id": "realm123"},
    }

    with patch(
      "robosystems.middleware.sse.dagster_monitor.submit_dagster_job_sync",
      return_value="run_xyz",
    ) as mock_submit:
      await sync_quickbooks_connection(
        connection=connection,
        sync_options=None,
        graph_id="kg_test",
      )

    run_config = mock_submit.call_args.kwargs["run_config"]
    assert run_config["ops"]["qb_extract"]["config"]["full_rebuild"] is False
    assert run_config["ops"]["qb_extract"]["config"]["lookback_days"] == 60
    assert run_config["ops"]["qb_extract"]["config"]["since_date"] == ""

  @pytest.mark.asyncio
  async def test_passes_since_date_through(self):
    from robosystems.operations.providers.quickbooks_provider import (
      sync_quickbooks_connection,
    )

    connection = {
      "connection_id": "conn_1",
      "user_id": "usr_1",
      "metadata": {"realm_id": "realm123"},
    }

    with patch(
      "robosystems.middleware.sse.dagster_monitor.submit_dagster_job_sync",
      return_value="run_def",
    ) as mock_submit:
      await sync_quickbooks_connection(
        connection=connection,
        sync_options={"since_date": "2024-06-01"},
        graph_id="kg_test",
      )

    run_config = mock_submit.call_args.kwargs["run_config"]
    assert run_config["ops"]["qb_extract"]["config"]["since_date"] == "2024-06-01"
    assert run_config["ops"]["qb_extract"]["config"]["full_rebuild"] is False

  @pytest.mark.asyncio
  async def test_missing_realm_id_raises(self):
    from robosystems.operations.providers.quickbooks_provider import (
      sync_quickbooks_connection,
    )

    connection = {
      "connection_id": "conn_1",
      "user_id": "usr_1",
      "metadata": {},
    }

    with pytest.raises(ValueError, match="realm_id"):
      await sync_quickbooks_connection(
        connection=connection,
        sync_options=None,
        graph_id="kg_test",
      )

  @pytest.mark.asyncio
  async def test_none_metadata_raises(self):
    from robosystems.operations.providers.quickbooks_provider import (
      sync_quickbooks_connection,
    )

    connection = {
      "connection_id": "conn_1",
      "user_id": "usr_1",
      "metadata": None,
    }

    with pytest.raises(ValueError, match="realm_id"):
      await sync_quickbooks_connection(
        connection=connection,
        sync_options=None,
        graph_id="kg_test",
      )


@pytest.mark.unit
class TestCleanupQuickbooksConnection:
  @pytest.mark.asyncio
  async def test_logs_cleanup(self):
    from robosystems.operations.providers.quickbooks_provider import (
      cleanup_quickbooks_connection,
    )

    connection = {"entity_id": "ent_1", "connection_id": "conn_1"}

    # Should not raise
    await cleanup_quickbooks_connection(connection, "kg_test")
