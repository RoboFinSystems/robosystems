"""
Unit tests for the connection options endpoint.

Tests that providers are included or excluded based on feature flags
via robosystems.config.env attributes.
"""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.routers.graphs.connections.options import get_connection_options

OPTIONS_MODULE = "robosystems.routers.graphs.connections.options"

GRAPH_ID = "kg01234567890abcdef"
USER_ID = "usr_test123"


def _make_mock_user(user_id: str = USER_ID):
  user = MagicMock()
  user.id = user_id
  user.email = "test@example.com"
  return user


def _make_mock_env(
  sec_enabled: bool = True,
  quickbooks_enabled: bool = True,
  external_enabled: bool = False,
):
  """Create a mock env object with connection feature flags.

  ``external_enabled`` defaults False so the pre-external assertions
  about provider counts stay literal; external-specific tests opt in."""
  mock_env = MagicMock()
  mock_env.CONNECTION_SEC_ENABLED = sec_enabled
  mock_env.CONNECTION_QUICKBOOKS_ENABLED = quickbooks_enabled
  mock_env.CONNECTION_EXTERNAL_ENABLED = external_enabled
  return mock_env


# ---------------------------------------------------------------------------
# get_connection_options — feature flag combinations
# ---------------------------------------------------------------------------


class TestGetConnectionOptions:
  """Tests for the get_connection_options endpoint."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_all_providers_enabled_returns_two_providers(self):
    """All providers enabled → two providers in response."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=True)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    assert result.total_providers == 2
    provider_names = [p.provider for p in result.providers]
    assert "sec" in provider_names
    assert "quickbooks" in provider_names

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_all_providers_disabled_returns_empty_list(self):
    """All providers disabled → empty providers list."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(
      sec_enabled=False, quickbooks_enabled=False, external_enabled=False
    )

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    assert result.total_providers == 0
    assert result.providers == []

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_only_sec_enabled(self):
    """Only SEC enabled → single SEC provider in response."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    assert result.total_providers == 1
    assert result.providers[0].provider == "sec"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_only_quickbooks_enabled(self):
    """Only QuickBooks enabled → single QB provider in response."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=False, quickbooks_enabled=True)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    assert result.total_providers == 1
    assert result.providers[0].provider == "quickbooks"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_total_providers_matches_providers_list_length(self):
    """total_providers field always matches the length of the providers list."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    assert result.total_providers == len(result.providers)

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_provider_ordering_is_sec_then_qb(self):
    """Providers appear in canonical order: SEC, QuickBooks."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=True)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    provider_names = [p.provider for p in result.providers]
    assert provider_names == ["sec", "quickbooks"]

  # ---------------------------------------------------------------------------
  # Verify provider metadata for each enabled provider
  # ---------------------------------------------------------------------------

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sec_provider_metadata(self):
    """SEC provider includes correct display name and auth_type."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    sec = result.providers[0]
    assert sec.provider == "sec"
    assert sec.display_name == "SEC EDGAR"
    assert sec.auth_type == "none"
    assert "cik" in sec.required_config
    assert sec.documentation_url is not None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_quickbooks_provider_metadata(self):
    """QuickBooks provider includes OAuth auth_type and correct features."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=False, quickbooks_enabled=True)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    qb = result.providers[0]
    assert qb.provider == "quickbooks"
    assert qb.display_name == "QuickBooks Online"
    assert qb.auth_type == "oauth"
    assert "trial_balance" in qb.features
    assert "chart_of_accounts" in qb.features
    assert qb.documentation_url is not None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_sec_provider_has_xbrl_features(self):
    """SEC provider advertises XBRL parsing as a feature."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    sec = result.providers[0]
    assert "xbrl_parsing" in sec.features

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_quickbooks_has_entity_id_in_required_config(self):
    """QuickBooks provider requires entity_id configuration."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=False, quickbooks_enabled=True)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    qb = result.providers[0]
    assert "entity_id" in qb.required_config

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_response_is_connection_options_response_type(self):
    """Return value is a ConnectionOptionsResponse instance."""
    from robosystems.models.api.graphs.connections import ConnectionOptionsResponse

    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=False)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    assert isinstance(result, ConnectionOptionsResponse)

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_providers_is_list_of_connection_provider_info(self):
    """Each element in providers is a ConnectionProviderInfo instance."""
    from robosystems.models.api.graphs.connections import ConnectionProviderInfo

    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=True)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    for provider in result.providers:
      assert isinstance(provider, ConnectionProviderInfo)

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_all_providers_have_setup_instructions(self):
    """Every enabled provider includes setup_instructions."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=True)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    for provider in result.providers:
      assert provider.setup_instructions is not None
      assert len(provider.setup_instructions) > 0

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_all_providers_have_data_types(self):
    """Every enabled provider includes a non-empty data_types list."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=True)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    for provider in result.providers:
      assert len(provider.data_types) > 0

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_all_providers_have_sync_frequency(self):
    """Every enabled provider includes sync_frequency."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(sec_enabled=True, quickbooks_enabled=True)

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    for provider in result.providers:
      assert provider.sync_frequency is not None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_external_provider_listed_when_enabled(self):
    """External provider appears with source_name as required config."""
    mock_user = _make_mock_user()
    mock_env = _make_mock_env(
      sec_enabled=True, quickbooks_enabled=True, external_enabled=True
    )

    with patch(f"{OPTIONS_MODULE}.env", mock_env):
      result = await get_connection_options(
        graph_id=GRAPH_ID,
        current_user=mock_user,
        _rate_limit=None,
      )

    assert result.total_providers == 3
    external = next(p for p in result.providers if p.provider == "external")
    assert external.auth_type == "none"
    assert external.required_config == ["source_name"]
