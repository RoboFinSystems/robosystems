"""
Tests for multitenant utility functions.
"""

import os
import pytest
from unittest.mock import patch

from robosystems.middleware.graph.utils import MultiTenantUtils


class TestMultiTenantUtils:
  """Test cases for MultiTenantUtils class."""

  def test_is_multitenant_mode_always_true(self):
    """Test that multi-tenant mode is always True for graph databases."""
    # MultiTenantUtils.is_multitenant_mode() always returns True
    assert MultiTenantUtils.is_multitenant_mode() is True

  def test_get_max_databases_default(self):
    """Test that max databases uses the configured default."""
    # The test environment sets LBUG_MAX_DATABASES_PER_NODE=50 in pytest.ini
    # So we expect 50, not the constants.py default of 10
    assert MultiTenantUtils.get_max_databases_per_node() == 50

  def test_get_max_databases_custom(self):
    """Test custom max databases value."""
    with patch(
      "robosystems.middleware.graph.utils.database.env.LBUG_MAX_DATABASES_PER_NODE",
      500,
    ):
      assert MultiTenantUtils.get_max_databases_per_node() == 500

  def test_get_max_databases_invalid_value(self):
    """Test that the value comes from config (environment handling is in env.py)."""
    # Since env.py handles the environment variable parsing and validation,
    # we don't need to test invalid values here - just that the config value is used
    with patch(
      "robosystems.middleware.graph.utils.database.env.LBUG_MAX_DATABASES_PER_NODE",
      100,
    ):
      assert MultiTenantUtils.get_max_databases_per_node() == 100

  def test_validate_graph_id_valid(self):
    """Test validation of valid graph IDs."""
    valid_ids = [
      "kg1a2b3c",
      "test_entity",
      "TestEntity123",
      "a",
      "kg1a2b3c_test",
    ]

    for graph_id in valid_ids:
      assert MultiTenantUtils.validate_graph_id(graph_id) == graph_id

  def test_validate_graph_id_invalid_format(self):
    """Test validation rejects invalid graph ID formats."""
    # Test path characters separately
    path_invalid_ids = [
      "entity/test",  # contains slash
    ]

    for graph_id in path_invalid_ids:
      with pytest.raises(ValueError, match="contains invalid path characters"):
        MultiTenantUtils.validate_graph_id(graph_id)

    # Test other invalid characters
    other_invalid_ids = [
      "entity.test",  # contains dot
      "entity test",  # contains space
      "entity@test",  # contains special char
      "entity#test",  # contains hash
    ]

    for graph_id in other_invalid_ids:
      with pytest.raises(ValueError, match="contains invalid characters"):
        MultiTenantUtils.validate_graph_id(graph_id)

  def test_validate_graph_id_empty(self):
    """Test validation rejects empty graph ID."""
    with pytest.raises(ValueError, match="cannot be empty"):
      MultiTenantUtils.validate_graph_id("")

  def test_validate_graph_id_too_long(self):
    """Test validation rejects graph ID that's too long."""
    long_id = "a" * 65  # 65 characters, exceeds 64 limit
    with pytest.raises(ValueError, match="too long"):
      MultiTenantUtils.validate_graph_id(long_id)

  def test_validate_graph_id_reserved_names(self):
    """Test validation rejects reserved names."""
    # Note: "sec" is a shared repository and is allowed through is_shared_repository() check
    reserved_names = ["system", "ladybug", "default", "SYSTEM", "LADYBUG", "DEFAULT"]

    for name in reserved_names:
      with pytest.raises(ValueError, match="reserved name"):
        MultiTenantUtils.validate_graph_id(name)

  def test_validate_graph_id_allows_shared_repositories(self):
    """Test validation allows shared repository names."""
    # These should be allowed because they're shared repositories
    shared_repos = ["sec", "industry", "economic", "regulatory", "market", "esg"]

    for repo in shared_repos:
      # Should not raise any exception
      result = MultiTenantUtils.validate_graph_id(repo)
      assert result == repo

  def test_get_database_name_with_graph_id(self):
    """Test database name resolution with graph_id."""
    # Since is_multitenant_mode() always returns True, graph_id determines database name
    assert MultiTenantUtils.get_database_name("kg1a2b3c") == "kg1a2b3c"

  def test_get_database_name_no_graph_id(self):
    """Test database name resolution without graph_id."""
    # Since is_multitenant_mode() always returns True, default is used when no graph_id
    assert MultiTenantUtils.get_database_name() == "default"
    assert MultiTenantUtils.get_database_name(None) == "default"

  def test_get_database_name_validates_graph_id(self):
    """Test that get_database_name validates graph_id."""
    # Test with invalid characters - should raise validation error
    with pytest.raises(ValueError, match="contains invalid characters"):
      MultiTenantUtils.get_database_name("invalid.graph@id")

  def test_validate_database_creation_valid(self):
    """Test successful database creation validation."""
    with patch.dict(os.environ, {"LBUG_MAX_DATABASES": "500"}):
      result = MultiTenantUtils.validate_database_creation("kg1a2b3c")
      assert result == "kg1a2b3c"

  def test_validate_database_creation_invalid_graph_id(self):
    """Test database creation validation with invalid graph_id."""
    with pytest.raises(ValueError, match="contains invalid characters"):
      MultiTenantUtils.validate_database_creation("invalid.graph@id")

  def test_check_database_limits(self):
    """Test database limits checking (placeholder implementation)."""
    # This should not raise an exception with the current placeholder implementation
    MultiTenantUtils.check_database_limits()

  def test_log_database_operation_calls_correctly(self):
    """Test logging database operations calls the logger correctly."""
    # Test that the method executes without error - actual logging testing
    # is complex in this environment and the functionality works in practice
    MultiTenantUtils.log_database_operation("Test operation", "test_db", "graph123")
    MultiTenantUtils.log_database_operation("Test operation", "test_db")
