"""Tests for subgraph utility functions."""

import pytest
from robosystems.middleware.graph.subgraph_utils import (
  parse_subgraph_id,
  validate_subgraph_name,
  validate_parent_graph_id,
  construct_subgraph_id,
  get_database_name,
  split_graph_hierarchy,
  is_subgraph,
  is_parent_graph,
  generate_unique_subgraph_name,
  SubgraphInfo,
)


class TestSubgraphParsing:
  """Test subgraph ID parsing."""

  def test_parse_valid_subgraph(self):
    """Test parsing a valid subgraph ID."""
    result = parse_subgraph_id("kg5f2e5e0da65d45d69645_dev")

    assert result is not None
    assert isinstance(result, SubgraphInfo)
    assert result.parent_graph_id == "kg5f2e5e0da65d45d69645"
    assert result.subgraph_name == "dev"
    assert result.graph_id == "kg5f2e5e0da65d45d69645_dev"
    assert result.database_name == "kg5f2e5e0da65d45d69645_dev"

  def test_parse_invalid_subgraph(self):
    """Test parsing invalid subgraph IDs."""
    # Not a subgraph
    assert parse_subgraph_id("kg5f2e5e0da65d45d69645") is None

    # Invalid format
    assert parse_subgraph_id("kg123.dev") is None  # Wrong separator
    assert parse_subgraph_id("invalid_dev") is None  # Invalid parent
    assert parse_subgraph_id("kg5f2e5e0da65d45d69645_") is None  # Missing name
    assert (
      parse_subgraph_id("kg5f2e5e0da65d45d69645_dev-test") is None
    )  # Invalid chars in name


class TestSubgraphValidation:
  """Test subgraph validation functions."""

  def test_validate_subgraph_name(self):
    """Test subgraph name validation."""
    # Valid names
    assert validate_subgraph_name("dev") is True
    assert validate_subgraph_name("staging") is True
    assert validate_subgraph_name("prod1") is True
    assert validate_subgraph_name("test123") is True
    assert validate_subgraph_name("a" * 20) is True  # Max length

    # Invalid names
    assert validate_subgraph_name("") is False  # Empty
    assert validate_subgraph_name("dev-test") is False  # Has dash
    assert validate_subgraph_name("dev_test") is False  # Has underscore
    assert validate_subgraph_name("dev.test") is False  # Has dot
    assert validate_subgraph_name("a" * 21) is False  # Too long
    assert validate_subgraph_name("dev@test") is False  # Special char

  def test_validate_parent_graph_id(self):
    """Test parent graph ID validation."""
    # Valid parent IDs (only user graphs can be parents)
    assert validate_parent_graph_id("kg5f2e5e0da65d45d69645") is True
    assert validate_parent_graph_id("kg1234567890abcdef") is True  # 16 chars
    assert (
      validate_parent_graph_id("kg1234567890abcdef1234567890abcdef") is True
    )  # Longer

    # Invalid parent IDs - shared repositories CANNOT be parents
    assert validate_parent_graph_id("sec") is False  # Shared repository
    assert validate_parent_graph_id("industry") is False  # Shared repository
    assert validate_parent_graph_id("economic") is False  # Shared repository

    # Invalid parent IDs - other reasons
    assert (
      validate_parent_graph_id("kg5f2e5e0da65d45d69645_dev") is False
    )  # Is a subgraph
    assert validate_parent_graph_id("invalid") is False
    assert validate_parent_graph_id("kg123") is False  # Too short
    assert validate_parent_graph_id("") is False


class TestSubgraphConstruction:
  """Test subgraph ID construction."""

  def test_construct_valid_subgraph_id(self):
    """Test constructing valid subgraph IDs."""
    result = construct_subgraph_id("kg5f2e5e0da65d45d69645", "dev")
    assert result == "kg5f2e5e0da65d45d69645_dev"

    result = construct_subgraph_id("kg5f2e5e0da65d45d69645", "staging")
    assert result == "kg5f2e5e0da65d45d69645_staging"

  def test_construct_invalid_subgraph_id(self):
    """Test constructing with invalid inputs."""
    # Invalid parent
    with pytest.raises(ValueError, match="Invalid parent graph ID"):
      construct_subgraph_id("invalid", "dev")

    # Invalid subgraph name
    with pytest.raises(ValueError, match="Invalid subgraph name"):
      construct_subgraph_id("kg5f2e5e0da65d45d69645", "dev-test")

    # Parent is already a subgraph
    with pytest.raises(ValueError, match="Invalid parent graph ID"):
      construct_subgraph_id("kg5f2e5e0da65d45d69645_dev", "test")


class TestDatabaseNames:
  """Test database name generation."""

  def test_get_database_name(self):
    """Test getting database names."""
    # Regular graph
    assert get_database_name("kg5f2e5e0da65d45d69645") == "kg5f2e5e0da65d45d69645"

    # Subgraph
    assert (
      get_database_name("kg5f2e5e0da65d45d69645_dev") == "kg5f2e5e0da65d45d69645_dev"
    )

    # Shared repository
    assert get_database_name("sec") == "sec"


class TestGraphHierarchy:
  """Test graph hierarchy functions."""

  def test_split_graph_hierarchy(self):
    """Test splitting graph IDs into components."""
    # Regular graph
    parent, subgraph = split_graph_hierarchy("kg5f2e5e0da65d45d69645")
    assert parent == "kg5f2e5e0da65d45d69645"
    assert subgraph is None

    # Subgraph
    parent, subgraph = split_graph_hierarchy("kg5f2e5e0da65d45d69645_dev")
    assert parent == "kg5f2e5e0da65d45d69645"
    assert subgraph == "dev"

  def test_is_subgraph(self):
    """Test subgraph detection."""
    assert is_subgraph("kg5f2e5e0da65d45d69645_dev") is True
    assert is_subgraph("kg5f2e5e0da65d45d69645_staging") is True
    assert is_subgraph("kg5f2e5e0da65d45d69645") is False
    assert is_subgraph("sec") is False

  def test_is_parent_graph(self):
    """Test parent graph detection."""
    assert is_parent_graph("kg5f2e5e0da65d45d69645") is True
    assert is_parent_graph("kg1234567890abcdef") is True
    # Shared repositories cannot be parents
    assert is_parent_graph("sec") is False
    assert is_parent_graph("industry") is False
    # Subgraphs cannot be parents
    assert is_parent_graph("kg5f2e5e0da65d45d69645_dev") is False
    assert is_parent_graph("invalid") is False


class TestUniqueNameGeneration:
  """Test unique subgraph name generation."""

  def test_generate_unique_name(self):
    """Test generating unique subgraph names."""
    existing = ["dev", "staging", "prod"]

    # Base name not taken
    result = generate_unique_subgraph_name("kg5f2e5e0da65d45d69645", "test", existing)
    assert result == "test"

    # Base name taken
    result = generate_unique_subgraph_name("kg5f2e5e0da65d45d69645", "dev", existing)
    assert result == "dev1"

    # Multiple variations taken
    existing.extend(["dev1", "dev2"])
    result = generate_unique_subgraph_name("kg5f2e5e0da65d45d69645", "dev", existing)
    assert result == "dev3"

  def test_generate_unique_name_cleanup(self):
    """Test name cleanup during generation."""
    existing = []

    # Clean special characters
    result = generate_unique_subgraph_name(
      "kg5f2e5e0da65d45d69645", "dev-test", existing
    )
    assert result == "devtest"

    # Truncate long names
    result = generate_unique_subgraph_name(
      "kg5f2e5e0da65d45d69645", "verylongsubgraphname12345", existing
    )
    assert len(result) <= 20
    assert result.startswith("verylongsubgraph")
