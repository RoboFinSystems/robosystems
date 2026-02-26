"""Tests for ProviderRegistry class."""

from contextlib import contextmanager
from unittest.mock import AsyncMock, Mock, patch

import pytest


def _make_mock_env(
  sec_enabled=True,
  quickbooks_enabled=True,
):
  """Create a mock env object with connection feature flags."""
  mock_env = Mock()
  mock_env.CONNECTION_SEC_ENABLED = sec_enabled
  mock_env.CONNECTION_QUICKBOOKS_ENABLED = quickbooks_enabled
  # QuickBooks provider needs these
  mock_env.INTUIT_ENVIRONMENT = "sandbox"
  return mock_env


@contextmanager
def _registry_context(env_mock):
  """Context manager: patch env for the full duration of build + use.

  Yields (registry, env_mock) so tests can call registry methods while
  env is still patched. This is required because get_provider() also
  reads env at call time (not just at init time).
  """
  mock_metrics_instance = Mock()
  mock_metrics_instance.record_business_event = Mock()

  with (
    patch("robosystems.operations.providers.registry.env", env_mock),
    patch("robosystems.operations.providers.quickbooks_provider.env", env_mock),
    patch(
      "robosystems.operations.providers.registry.get_endpoint_metrics",
      return_value=mock_metrics_instance,
    ),
  ):
    from robosystems.operations.providers.registry import ProviderRegistry

    registry = ProviderRegistry()
    yield registry, env_mock


def _build_registry(env_mock):
  """Build a ProviderRegistry without keeping env patched after return.

  Only suitable for tests that don't call registry methods that re-check env.
  (e.g. _providers dict inspection, init tests)
  """
  with _registry_context(env_mock) as (registry, _):
    pass
  return registry


@pytest.mark.unit
class TestProviderRegistryInit:
  """Tests for ProviderRegistry initialization."""

  def test_registry_registers_sec_when_enabled(self):
    """Test that SEC provider is registered when CONNECTION_SEC_ENABLED=True."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)
    registry = _build_registry(env_mock)

    assert "sec" in registry._providers

  def test_registry_does_not_register_sec_when_disabled(self):
    """Test that SEC provider is absent when CONNECTION_SEC_ENABLED=False."""
    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=False)
    registry = _build_registry(env_mock)

    assert "sec" not in registry._providers

  def test_registry_registers_quickbooks_when_enabled(self):
    """Test that QuickBooks provider is registered when CONNECTION_QUICKBOOKS_ENABLED=True."""
    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=True)
    registry = _build_registry(env_mock)

    assert "quickbooks" in registry._providers

  def test_registry_does_not_register_quickbooks_when_disabled(self):
    """Test that QB provider is absent when CONNECTION_QUICKBOOKS_ENABLED=False."""
    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=False)
    registry = _build_registry(env_mock)

    assert "quickbooks" not in registry._providers

  def test_registry_all_providers_registered_when_all_enabled(self):
    """Test all providers are present when all flags are True."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=True)
    registry = _build_registry(env_mock)

    assert "sec" in registry._providers
    assert "quickbooks" in registry._providers

  def test_registry_empty_when_all_disabled(self):
    """Test no providers registered when all flags are False."""
    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=False)
    registry = _build_registry(env_mock)

    assert len(registry._providers) == 0


@pytest.mark.unit
class TestGetProvider:
  """Tests for ProviderRegistry.get_provider()."""

  def test_get_provider_sec_returns_dict_with_expected_keys(self):
    """Test that get_provider('sec') returns dict with create/sync/cleanup/config_class."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with _registry_context(env_mock) as (registry, _):
      provider = registry.get_provider("sec")

    assert "create" in provider
    assert "sync" in provider
    assert "cleanup" in provider
    assert "config_class" in provider

  def test_get_provider_quickbooks_returns_dict_with_expected_keys(self):
    """Test that get_provider('quickbooks') returns dict with create/sync/cleanup/config_class."""
    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=True)

    with _registry_context(env_mock) as (registry, _):
      provider = registry.get_provider("quickbooks")

    assert "create" in provider
    assert "sync" in provider
    assert "cleanup" in provider
    assert "config_class" in provider

  def test_get_provider_case_insensitive(self):
    """Test that get_provider normalizes the provider type to lowercase."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with _registry_context(env_mock) as (registry, _):
      provider_upper = registry.get_provider("SEC")
      provider_lower = registry.get_provider("sec")
      provider_mixed = registry.get_provider("Sec")

    assert provider_upper is provider_lower
    assert provider_lower is provider_mixed

  def test_get_provider_disabled_sec_raises_value_error(self):
    """Test ValueError is raised with helpful message when SEC is disabled."""
    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=False)

    with _registry_context(env_mock) as (registry, _):
      with pytest.raises(ValueError, match="SEC provider is not enabled"):
        registry.get_provider("sec")

  def test_get_provider_disabled_quickbooks_raises_value_error(self):
    """Test ValueError is raised with helpful message when QuickBooks is disabled."""
    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=False)

    with _registry_context(env_mock) as (registry, _):
      with pytest.raises(ValueError, match="QuickBooks provider is not enabled"):
        registry.get_provider("quickbooks")

  def test_get_provider_unknown_raises_value_error(self):
    """Test ValueError with 'Unknown provider type' for completely unknown providers."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=True)

    with _registry_context(env_mock) as (registry, _):
      with pytest.raises(ValueError, match="Unknown provider type"):
        registry.get_provider("stripe")

  def test_get_provider_unknown_raises_value_error_with_provider_name(self):
    """Test that the unknown provider name is included in the error message."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=True)

    with _registry_context(env_mock) as (registry, _):
      with pytest.raises(ValueError, match="twitter"):
        registry.get_provider("twitter")


@pytest.mark.unit
class TestCreateConnection:
  """Tests for ProviderRegistry.create_connection()."""

  @pytest.mark.asyncio
  async def test_create_connection_delegates_to_sec_provider(self):
    """Test that create_connection calls the SEC create function."""
    from sqlalchemy.orm import Session

    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with _registry_context(env_mock) as (registry, _):
      mock_create = AsyncMock(return_value="conn_sec_123")
      registry._providers["sec"]["create"] = mock_create

      mock_db = Mock(spec=Session)
      mock_config = Mock()

      result = await registry.create_connection(
        provider_type="sec",
        entity_id="entity_1",
        config=mock_config,
        user_id="user_1",
        graph_id="kg_test",
        db=mock_db,
      )

    assert result == "conn_sec_123"
    mock_create.assert_awaited_once_with(
      "entity_1", mock_config, "user_1", "kg_test", mock_db
    )

  @pytest.mark.asyncio
  async def test_create_connection_delegates_to_quickbooks_provider(self):
    """Test that create_connection calls the QB create function."""
    from sqlalchemy.orm import Session

    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=True)

    with _registry_context(env_mock) as (registry, _):
      mock_create = AsyncMock(return_value="conn_qb_456")
      registry._providers["quickbooks"]["create"] = mock_create

      mock_db = Mock(spec=Session)
      mock_config = Mock()

      result = await registry.create_connection(
        provider_type="quickbooks",
        entity_id="entity_2",
        config=mock_config,
        user_id="user_2",
        graph_id="kg_test",
        db=mock_db,
      )

    assert result == "conn_qb_456"
    mock_create.assert_awaited_once()

  @pytest.mark.asyncio
  async def test_create_connection_raises_for_disabled_provider(self):
    """Test that create_connection raises ValueError for a disabled provider."""
    from sqlalchemy.orm import Session

    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=False)

    with _registry_context(env_mock) as (registry, _):
      mock_db = Mock(spec=Session)

      with pytest.raises(ValueError, match="SEC provider is not enabled"):
        await registry.create_connection(
          provider_type="sec",
          entity_id="entity_1",
          config=None,
          user_id="user_1",
          graph_id="kg_test",
          db=mock_db,
        )


@pytest.mark.unit
class TestSyncConnection:
  """Tests for ProviderRegistry.sync_connection()."""

  @pytest.mark.asyncio
  async def test_sync_connection_delegates_to_sec_provider(self):
    """Test that sync_connection calls the SEC sync function."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with _registry_context(env_mock) as (registry, _):
      mock_sync = AsyncMock(return_value="run_123")
      registry._providers["sec"]["sync"] = mock_sync

      connection = {"connection_id": "conn_1", "entity_id": "entity_1"}

      result = await registry.sync_connection(
        provider_type="sec",
        connection=connection,
        sync_options=None,
        graph_id="kg_test",
      )

    assert result == "run_123"
    mock_sync.assert_awaited_once_with(connection, None, "kg_test")

  @pytest.mark.asyncio
  async def test_sync_connection_delegates_to_quickbooks_provider(self):
    """Test that sync_connection calls the QB sync function."""
    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=True)

    with _registry_context(env_mock) as (registry, _):
      mock_sync = AsyncMock(return_value="run_qb_789")
      registry._providers["quickbooks"]["sync"] = mock_sync

      connection = {
        "connection_id": "conn_qb",
        "entity_id": "entity_qb",
        "metadata": {"realm_id": "realm_123"},
      }
      sync_options = {"full_rebuild": True}

      result = await registry.sync_connection(
        provider_type="quickbooks",
        connection=connection,
        sync_options=sync_options,
        graph_id="kg_test",
      )

    assert result == "run_qb_789"
    mock_sync.assert_awaited_once_with(connection, sync_options, "kg_test")

  @pytest.mark.asyncio
  async def test_sync_connection_passes_sync_options(self):
    """Test that sync_options are forwarded to the provider sync function."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with _registry_context(env_mock) as (registry, _):
      mock_sync = AsyncMock(return_value="run_opts_test")
      registry._providers["sec"]["sync"] = mock_sync

      sync_opts = {"lookback_days": 90, "full_rebuild": False}

      await registry.sync_connection(
        provider_type="sec",
        connection={"connection_id": "c1", "entity_id": "e1"},
        sync_options=sync_opts,
        graph_id="kg_test",
      )

    call_args = mock_sync.call_args
    assert call_args[0][1] == sync_opts

  @pytest.mark.asyncio
  async def test_sync_connection_raises_for_unknown_provider(self):
    """Test sync_connection raises ValueError for an unknown provider."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=True)

    with _registry_context(env_mock) as (registry, _):
      with pytest.raises(ValueError, match="Unknown provider type"):
        await registry.sync_connection(
          provider_type="coinbase",
          connection={},
          sync_options=None,
          graph_id="kg_test",
        )


@pytest.mark.unit
class TestCleanupConnection:
  """Tests for ProviderRegistry.cleanup_connection()."""

  @pytest.mark.asyncio
  async def test_cleanup_connection_delegates_to_sec_provider(self):
    """Test that cleanup_connection calls the SEC cleanup function."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with _registry_context(env_mock) as (registry, _):
      mock_cleanup = AsyncMock(return_value=None)
      registry._providers["sec"]["cleanup"] = mock_cleanup

      connection = {"connection_id": "conn_c1", "entity_id": "entity_c1"}

      await registry.cleanup_connection(
        provider_type="sec",
        connection=connection,
        graph_id="kg_test",
      )

    mock_cleanup.assert_awaited_once_with(connection, "kg_test")

  @pytest.mark.asyncio
  async def test_cleanup_connection_delegates_to_quickbooks_provider(self):
    """Test that cleanup_connection calls the QB cleanup function."""
    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=True)

    with _registry_context(env_mock) as (registry, _):
      mock_cleanup = AsyncMock(return_value=None)
      registry._providers["quickbooks"]["cleanup"] = mock_cleanup

      connection = {"connection_id": "conn_qb", "entity_id": "entity_qb"}

      await registry.cleanup_connection(
        provider_type="quickbooks",
        connection=connection,
        graph_id="kg_test",
      )

    mock_cleanup.assert_awaited_once_with(connection, "kg_test")

  @pytest.mark.asyncio
  async def test_cleanup_connection_raises_for_disabled_provider(self):
    """Test cleanup_connection raises ValueError for disabled provider."""
    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=False)

    with _registry_context(env_mock) as (registry, _):
      with pytest.raises(ValueError, match="SEC provider is not enabled"):
        await registry.cleanup_connection(
          provider_type="sec",
          connection={},
          graph_id="kg_test",
        )


@pytest.mark.unit
class TestProviderRegistryMetrics:
  """Tests for metrics recording in ProviderRegistry."""

  def test_metrics_recorded_at_init(self):
    """Test that feature flag metrics are recorded during initialization."""
    env_mock = _make_mock_env()
    mock_metrics_instance = Mock()
    mock_metrics_instance.record_business_event = Mock()

    with (
      patch("robosystems.operations.providers.registry.env", env_mock),
      patch("robosystems.operations.providers.quickbooks_provider.env", env_mock),
      patch(
        "robosystems.operations.providers.registry.get_endpoint_metrics",
        return_value=mock_metrics_instance,
      ),
    ):
      from robosystems.operations.providers.registry import ProviderRegistry

      ProviderRegistry()

    # At minimum 2 calls: one per provider feature flag
    assert mock_metrics_instance.record_business_event.call_count >= 2

  def test_metrics_failure_does_not_prevent_init(self):
    """Test that a metrics failure during init doesn't raise."""
    env_mock = _make_mock_env()

    with (
      patch("robosystems.operations.providers.registry.env", env_mock),
      patch("robosystems.operations.providers.quickbooks_provider.env", env_mock),
      patch(
        "robosystems.operations.providers.registry.get_endpoint_metrics",
        side_effect=Exception("Metrics unavailable"),
      ),
    ):
      from robosystems.operations.providers.registry import ProviderRegistry

      # Should not raise
      registry = ProviderRegistry()

    assert registry is not None

  def test_metrics_failure_on_get_provider_does_not_raise(self):
    """Test that metrics recording failure during get_provider doesn't propagate."""
    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    call_count = {"n": 0}
    original_metrics = Mock()
    original_metrics.record_business_event = Mock()

    def flaky_get_metrics():
      call_count["n"] += 1
      if call_count["n"] > 3:
        raise Exception("Metrics down")
      return original_metrics

    with (
      patch("robosystems.operations.providers.registry.env", env_mock),
      patch("robosystems.operations.providers.quickbooks_provider.env", env_mock),
      patch(
        "robosystems.operations.providers.registry.get_endpoint_metrics",
        side_effect=flaky_get_metrics,
      ),
    ):
      from robosystems.operations.providers.registry import ProviderRegistry

      registry = ProviderRegistry()
      # get_provider will try to record metrics; if it fails, it should not raise
      provider = registry.get_provider("sec")

    assert provider is not None


@pytest.mark.unit
class TestProviderConfigClasses:
  """Tests that providers are associated with the correct config classes."""

  def test_sec_provider_uses_sec_connection_config(self):
    """Test that SEC provider's config_class is SECConnectionConfig."""
    from robosystems.models.api.graphs.connections import SECConnectionConfig

    env_mock = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)
    registry = _build_registry(env_mock)

    assert registry._providers["sec"]["config_class"] is SECConnectionConfig

  def test_quickbooks_provider_uses_quickbooks_connection_config(self):
    """Test that QB provider's config_class is QuickBooksConnectionConfig."""
    from robosystems.models.api.graphs.connections import QuickBooksConnectionConfig

    env_mock = _make_mock_env(sec_enabled=False, quickbooks_enabled=True)
    registry = _build_registry(env_mock)

    assert (
      registry._providers["quickbooks"]["config_class"] is QuickBooksConnectionConfig
    )
