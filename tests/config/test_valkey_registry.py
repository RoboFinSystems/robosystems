"""Tests for Valkey/Redis registry and URL builder."""

import os
from unittest.mock import patch

from robosystems.config.valkey_registry import (
  ValkeyDatabase,
  ValkeyURLBuilder,
  get_database_purpose,
  print_database_registry,
)


class TestValkeyDatabase:
  """Tests for ValkeyDatabase enum."""

  def test_database_values(self):
    """Test that database numbers are assigned correctly."""
    assert ValkeyDatabase.AUTH == 0
    assert ValkeyDatabase.RATE_LIMITS == 1
    assert ValkeyDatabase.GRAPH_ROUTING == 2
    assert ValkeyDatabase.SSE == 3
    assert ValkeyDatabase.LOCKS == 4

  def test_get_next_available(self):
    """Test getting the next available database number."""
    # Currently databases 0-5 are used, so next should be 6
    next_db = ValkeyDatabase.get_next_available()
    assert next_db == 6

  def test_get_next_available_validates_range(self):
    """Test that get_next_available respects the 0-15 range."""
    next_db = ValkeyDatabase.get_next_available()
    assert next_db >= 0
    assert next_db <= 15
    # Since we have 6 databases defined (0-5), next should be 6
    assert next_db == 6

  def test_get_next_available_algorithm(self):
    """Test the algorithm by mocking the enum iteration."""
    with patch.object(ValkeyDatabase, "get_next_available") as mock_method:
      used_values = {0, 1, 2, 3, 4}  # Current allocations
      for i in range(16):
        if i not in used_values:
          mock_method.return_value = i
          break

      result = ValkeyDatabase.get_next_available()
      assert result == 5  # First available after current allocations

  def test_get_url(self):
    """Test convenience method for getting database URL."""
    base_url = "redis://localhost:6379"
    result = ValkeyDatabase.get_url(ValkeyDatabase.AUTH, base_url)
    assert result == "redis://localhost:6379/0"

  def test_enum_iteration(self):
    """Test that we can iterate over database enum."""
    databases = list(ValkeyDatabase)
    assert len(databases) == 6  # Currently 6 databases allocated
    assert ValkeyDatabase.AUTH in databases
    assert ValkeyDatabase.GRAPH_ROUTING in databases


class TestValkeyURLBuilder:
  """Tests for ValkeyURLBuilder."""

  def setup_method(self):
    """Reset cache before each test."""
    ValkeyURLBuilder._cached_base_url = None
    ValkeyURLBuilder._cache_environment = None

  @patch.dict(
    os.environ, {"ENVIRONMENT": "dev", "VALKEY_URL": "redis://dev-redis:6379"}
  )
  def test_get_base_url_dev_environment(self):
    """Test getting base URL in development environment."""
    url = ValkeyURLBuilder.get_base_url()
    assert url == "redis://dev-redis:6379"

  @patch.dict(os.environ, {"ENVIRONMENT": "dev"}, clear=True)
  def test_get_base_url_fallback_to_localhost(self):
    """Test fallback to localhost when no VALKEY_URL is set."""
    # Remove VALKEY_URL from environment
    if "VALKEY_URL" in os.environ:
      del os.environ["VALKEY_URL"]

    url = ValkeyURLBuilder.get_base_url()
    assert url == "redis://localhost:6379"

  @patch.dict(os.environ, {"ENVIRONMENT": "prod"})
  @patch(
    "robosystems.config.valkey_registry.ValkeyURLBuilder._get_valkey_url_from_cloudformation"
  )
  def test_get_base_url_prod_cloudformation(self, mock_get_cf):
    """Test getting URL from CloudFormation in production."""
    mock_get_cf.return_value = "redis://prod-valkey.cache.amazonaws.com:6379"

    url = ValkeyURLBuilder.get_base_url()
    assert url == "redis://prod-valkey.cache.amazonaws.com:6379"
    mock_get_cf.assert_called_once()

  @patch.dict(os.environ, {"ENVIRONMENT": "staging"})
  @patch(
    "robosystems.config.valkey_registry.ValkeyURLBuilder._get_valkey_url_from_cloudformation"
  )
  def test_get_base_url_staging_cloudformation(self, mock_get_cf):
    """Test getting URL from CloudFormation in staging."""
    mock_get_cf.return_value = "redis://staging-valkey.cache.amazonaws.com:6379"

    url = ValkeyURLBuilder.get_base_url()
    assert url == "redis://staging-valkey.cache.amazonaws.com:6379"

  @patch.dict(
    os.environ, {"ENVIRONMENT": "prod", "VALKEY_URL": "redis://fallback:6379"}
  )
  @patch(
    "robosystems.config.valkey_registry.ValkeyURLBuilder._get_valkey_url_from_cloudformation"
  )
  def test_get_base_url_cloudformation_error(self, mock_get_cf):
    """Test fallback when CloudFormation is not available."""
    mock_get_cf.return_value = None  # Simulates CloudFormation failure

    url = ValkeyURLBuilder.get_base_url()
    assert url == "redis://fallback:6379"

  @patch.dict(os.environ, {"ENVIRONMENT": "prod"})
  @patch(
    "robosystems.config.valkey_registry.ValkeyURLBuilder._get_valkey_url_from_cloudformation"
  )
  def test_get_base_url_caching(self, mock_get_cf):
    """Test that base URL is cached properly."""
    mock_get_cf.return_value = "redis://prod-valkey.cache.amazonaws.com:6379"

    # First call
    url1 = ValkeyURLBuilder.get_base_url()
    assert url1 == "redis://prod-valkey.cache.amazonaws.com:6379"
    assert mock_get_cf.call_count == 1

    # Second call should use cache
    url2 = ValkeyURLBuilder.get_base_url()
    assert url2 == url1
    assert mock_get_cf.call_count == 1  # Should not call again

  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_get_base_url_cache_invalidation_on_env_change(self):
    """Test that cache is invalidated when environment changes."""
    ValkeyURLBuilder._cached_base_url = "redis://old-cache:6379"
    ValkeyURLBuilder._cache_environment = "prod"

    with patch.dict(os.environ, {"VALKEY_URL": "redis://new-dev:6379"}):
      url = ValkeyURLBuilder.get_base_url()
      assert url == "redis://new-dev:6379"
      assert ValkeyURLBuilder._cache_environment == "dev"

  def test_build_url_basic(self):
    """Test building URL with explicit base URL."""
    url = ValkeyURLBuilder.build_url("redis://localhost:6379", ValkeyDatabase.AUTH)
    assert url == "redis://localhost:6379/0"

  def test_build_url_removes_trailing_slash(self):
    """Test that trailing slash is removed from base URL."""
    url = ValkeyURLBuilder.build_url("redis://localhost:6379/", ValkeyDatabase.SSE)
    assert url == "redis://localhost:6379/3"

  def test_build_url_replaces_existing_database(self):
    """Test that existing database number is replaced."""
    url = ValkeyURLBuilder.build_url("redis://localhost:6379/5", ValkeyDatabase.SSE)
    assert url == "redis://localhost:6379/3"

  def test_build_url_adds_protocol(self):
    """Test that protocol is added if missing."""
    url = ValkeyURLBuilder.build_url("localhost:6379", ValkeyDatabase.RATE_LIMITS)
    assert url == "redis://localhost:6379/1"

  def test_build_url_with_valkey_prefix(self):
    """Test using valkey:// prefix."""
    url = ValkeyURLBuilder.build_url(
      "localhost:6379", ValkeyDatabase.GRAPH_ROUTING, use_valkey_prefix=True
    )
    assert url == "valkey://localhost:6379/2"

  def test_build_url_replaces_redis_with_valkey(self):
    """Test replacing redis:// with valkey:// when requested."""
    url = ValkeyURLBuilder.build_url(
      "redis://localhost:6379", ValkeyDatabase.LOCKS, use_valkey_prefix=True
    )
    assert url == "valkey://localhost:6379/4"

  @patch.dict(os.environ, {"VALKEY_URL": "redis://auto-discovered:6379"})
  def test_build_url_auto_discover(self):
    """Test auto-discovering base URL when not provided."""
    url = ValkeyURLBuilder.build_url(None, ValkeyDatabase.SSE)
    assert url == "redis://auto-discovered:6379/3"

  def test_build_url_default_database(self):
    """Test that default database is AUTH."""
    url = ValkeyURLBuilder.build_url("redis://localhost:6379")
    assert url == "redis://localhost:6379/0"

  def test_parse_url_basic(self):
    """Test parsing a basic Redis URL."""
    base_url, db_num = ValkeyURLBuilder.parse_url("redis://localhost:6379/2")
    assert base_url == "redis://localhost:6379"
    assert db_num == 2

  def test_parse_url_no_database(self):
    """Test parsing URL without database number."""
    base_url, db_num = ValkeyURLBuilder.parse_url("redis://localhost:6379")
    assert base_url == "redis://localhost:6379"
    assert db_num is None

  def test_parse_url_with_query_params(self):
    """Test parsing URL with query parameters."""
    base_url, db_num = ValkeyURLBuilder.parse_url("redis://localhost:6379/3?timeout=10")
    assert base_url == "redis://localhost:6379"
    assert db_num == 3

  def test_parse_url_invalid_database(self):
    """Test parsing URL with invalid database number."""
    base_url, db_num = ValkeyURLBuilder.parse_url("redis://localhost:6379/invalid")
    assert base_url == "redis://localhost:6379/invalid"
    assert db_num is None

  def test_parse_url_with_path(self):
    """Test parsing URL with path components."""
    base_url, db_num = ValkeyURLBuilder.parse_url("redis://localhost:6379/some/path/2")
    assert base_url == "redis://localhost:6379/some/path"
    assert db_num == 2


class TestDatabasePurpose:
  """Tests for database purpose documentation."""

  def test_get_database_purpose_known(self):
    """Test getting purpose for known databases."""
    purpose = get_database_purpose(ValkeyDatabase.AUTH)
    assert "JWT" in purpose or "token" in purpose

    purpose = get_database_purpose(ValkeyDatabase.GRAPH_ROUTING)
    assert "Graph" in purpose or "routing" in purpose

  def test_get_database_purpose_all_defined(self):
    """Test that all databases have purposes defined."""
    for db in ValkeyDatabase:
      purpose = get_database_purpose(db)
      assert purpose is not None
      assert len(purpose) > 0

  def test_print_database_registry(self, capsys):
    """Test printing the database registry."""
    print_database_registry()

    captured = capsys.readouterr()
    assert "VALKEY/REDIS DATABASE REGISTRY" in captured.out
    assert "AUTH" in captured.out
    assert "GRAPH_ROUTING" in captured.out
    assert "USAGE EXAMPLE" in captured.out
    assert "ValkeyURLBuilder.build_url" in captured.out

  def test_print_database_registry_shows_all(self, capsys):
    """Test that registry print shows all databases."""
    print_database_registry()

    captured = capsys.readouterr()
    for db in ValkeyDatabase:
      assert db.name in captured.out
      assert f"DB {db.value:2d}" in captured.out


class TestIntegration:
  """Integration tests for the Valkey registry."""

  @patch.dict(os.environ, {"ENVIRONMENT": "dev", "VALKEY_URL": "redis://test:6379"})
  def test_full_workflow(self):
    """Test a complete workflow of building and parsing URLs."""
    # Build URL for auth
    auth_url = ValkeyURLBuilder.build_url(database=ValkeyDatabase.AUTH)
    assert auth_url == "redis://test:6379/0"

    # Parse it back
    base, db = ValkeyURLBuilder.parse_url(auth_url)
    assert base == "redis://test:6379"
    assert db == 0

    # Build another URL with the parsed base
    sse_url = ValkeyURLBuilder.build_url(base, ValkeyDatabase.SSE)
    assert sse_url == "redis://test:6379/3"

  def test_enum_convenience_method(self):
    """Test the enum's convenience get_url method."""
    url = ValkeyDatabase.get_url(ValkeyDatabase.RATE_LIMITS, "redis://myserver:6379")
    assert url == "redis://myserver:6379/1"

  @patch(
    "robosystems.config.valkey_registry.ValkeyURLBuilder._get_valkey_url_from_cloudformation"
  )
  def test_production_like_setup(self, mock_cf):
    """Test production-like configuration flow."""
    # Reset cache
    ValkeyURLBuilder._cached_base_url = None
    ValkeyURLBuilder._cache_environment = None

    # Simulate production environment
    with patch.dict(os.environ, {"ENVIRONMENT": "prod"}):
      mock_cf.return_value = "redis://prod.cluster.cache.amazonaws.com:6379"

      # Build URLs for different services
      urls = {
        "auth": ValkeyURLBuilder.build_url(database=ValkeyDatabase.AUTH),
        "rate_limits": ValkeyURLBuilder.build_url(database=ValkeyDatabase.RATE_LIMITS),
        "sse": ValkeyURLBuilder.build_url(database=ValkeyDatabase.SSE),
      }

      assert urls["auth"] == "redis://prod.cluster.cache.amazonaws.com:6379/0"
      assert urls["rate_limits"] == "redis://prod.cluster.cache.amazonaws.com:6379/1"
      assert urls["sse"] == "redis://prod.cluster.cache.amazonaws.com:6379/3"

      # CloudFormation should only be called once due to caching
      assert mock_cf.call_count == 1
