"""Full graph types tests — GraphIdentity, GraphTypeRegistry, subgraph utilities."""

import pytest

from robosystems.middleware.graph.types import (
  AccessPattern,
  ConnectionPattern,
  GraphCategory,
  GraphIdentity,
  GraphTypeRegistry,
  NodeType,
  RepositoryType,
  UserGraphType,
  construct_subgraph_id,
  is_subgraph_id,
  parse_graph_id,
)


class TestEnums:
  def test_graph_category(self):
    assert GraphCategory.USER.value == "user"
    assert GraphCategory.SHARED.value == "shared"
    assert GraphCategory.SYSTEM.value == "system"

  def test_user_graph_type(self):
    assert UserGraphType.ENTITY.value == "entity"
    assert UserGraphType.CUSTOM.value == "custom"

  def test_access_pattern(self):
    assert AccessPattern.READ_WRITE.value == "read_write"
    assert AccessPattern.READ_ONLY.value == "read_only"
    assert AccessPattern.RESTRICTED.value == "restricted"

  def test_connection_pattern(self):
    assert ConnectionPattern.API_AUTO.value == "api_auto"
    assert ConnectionPattern.API_WRITER.value == "api_writer"

  def test_node_type(self):
    assert NodeType.WRITER.value == "writer"
    assert NodeType.SHARED_REPLICA.value == "shared_replica"

  def test_repository_type(self):
    assert RepositoryType.ENTITY.value == "entity"
    assert RepositoryType.SHARED.value == "shared"


class TestGraphIdentity:
  def test_user_graph_identity(self):
    identity = GraphIdentity(
      graph_id="kg0123456789abcdef",
      category=GraphCategory.USER,
      graph_type="entity",
    )
    assert identity.is_user_graph is True
    assert identity.is_shared_repository is False
    assert identity.is_system_graph is False

  def test_shared_repo_identity(self):
    identity = GraphIdentity(
      graph_id="sec",
      category=GraphCategory.SHARED,
      graph_type="sec",
    )
    assert identity.is_shared_repository is True
    assert identity.is_user_graph is False

  def test_system_graph_identity(self):
    identity = GraphIdentity(
      graph_id="system",
      category=GraphCategory.SYSTEM,
    )
    assert identity.is_system_graph is True

  def test_get_access_pattern_user(self):
    identity = GraphIdentity(graph_id="kg123", category=GraphCategory.USER)
    assert identity.get_access_pattern() == AccessPattern.READ_WRITE

  def test_get_access_pattern_shared(self):
    identity = GraphIdentity(graph_id="sec", category=GraphCategory.SHARED)
    assert identity.get_access_pattern() == AccessPattern.READ_ONLY

  def test_get_access_pattern_system(self):
    identity = GraphIdentity(graph_id="system", category=GraphCategory.SYSTEM)
    assert identity.get_access_pattern() == AccessPattern.RESTRICTED

  def test_get_access_pattern_explicit_override(self):
    identity = GraphIdentity(
      graph_id="kg123",
      category=GraphCategory.USER,
      access_pattern=AccessPattern.READ_ONLY,
    )
    assert identity.get_access_pattern() == AccessPattern.READ_ONLY

  def test_get_routing_info_user(self):
    identity = GraphIdentity(graph_id="kg123", category=GraphCategory.USER)
    info = identity.get_routing_info()
    assert info["cluster_type"] == "user_writer"
    assert info["requires_allocation"] is True

  def test_get_routing_info_shared(self):
    identity = GraphIdentity(graph_id="sec", category=GraphCategory.SHARED)
    info = identity.get_routing_info()
    assert info["cluster_type"] == "shared_writer"
    assert info["cache_enabled"] is True

  def test_get_routing_info_system(self):
    identity = GraphIdentity(graph_id="system", category=GraphCategory.SYSTEM)
    info = identity.get_routing_info()
    assert info["cluster_type"] == "system"


class TestGraphTypeRegistry:
  def test_identify_shared_repo_no_session(self):
    identity = GraphTypeRegistry.identify_graph("sec")
    assert identity.category == GraphCategory.SHARED

  def test_identify_user_graph_no_session(self):
    identity = GraphTypeRegistry.identify_graph("kg0123456789abcdef")
    assert identity.category == GraphCategory.USER

  def test_identify_system_graph(self):
    identity = GraphTypeRegistry.identify_graph("system")
    assert identity.category == GraphCategory.SYSTEM

  def test_identify_metadata_graph(self):
    identity = GraphTypeRegistry.identify_graph("metadata")
    assert identity.category == GraphCategory.SYSTEM

  def test_is_valid_shared(self):
    assert GraphTypeRegistry.is_valid_graph_id("sec", GraphCategory.SHARED) is True

  def test_is_valid_user(self):
    assert GraphTypeRegistry.is_valid_graph_id("kg_test", GraphCategory.USER) is True

  def test_is_valid_system(self):
    assert GraphTypeRegistry.is_valid_graph_id("system", GraphCategory.SYSTEM) is True

  def test_list_shared_repositories(self):
    repos = GraphTypeRegistry.list_shared_repositories()
    assert isinstance(repos, list)
    assert "sec" in repos

  def test_get_graph_id_pattern(self):
    pattern = GraphTypeRegistry.get_graph_id_pattern()
    assert "kg" in pattern
    assert "sec" in pattern


class TestIsSubgraphId:
  def test_user_subgraph(self):
    assert is_subgraph_id("kg0123456789abcdef_dev") is True

  def test_shared_repo_subgraph(self):
    assert is_subgraph_id("sec_historical") is True

  def test_parent_graph(self):
    assert is_subgraph_id("kg0123456789abcdef") is False

  def test_shared_repo(self):
    assert is_subgraph_id("sec") is False

  def test_empty(self):
    assert is_subgraph_id("") is False

  def test_just_underscore(self):
    assert is_subgraph_id("_") is False

  def test_long_subgraph_name_rejected(self):
    assert is_subgraph_id("kg0123456789abcdef_" + "a" * 21) is False

  def test_special_chars_in_subgraph_rejected(self):
    assert is_subgraph_id("kg0123456789abcdef_dev-1") is False


class TestParseGraphId:
  def test_parent_graph(self):
    parent, sub = parse_graph_id("kg0123456789abcdef")
    assert parent == "kg0123456789abcdef"
    assert sub is None

  def test_subgraph(self):
    parent, sub = parse_graph_id("kg0123456789abcdef_dev")
    assert parent == "kg0123456789abcdef"
    assert sub == "dev"

  def test_shared_repo(self):
    parent, sub = parse_graph_id("sec")
    assert parent == "sec"
    assert sub is None

  def test_shared_repo_subgraph(self):
    parent, sub = parse_graph_id("sec_historical")
    assert parent == "sec"
    assert sub == "historical"


class TestConstructSubgraphId:
  def test_valid(self):
    result = construct_subgraph_id("kg0123456789abcdef", "dev")
    assert result == "kg0123456789abcdef_dev"

  def test_empty_parent_raises(self):
    with pytest.raises(ValueError, match="parent_graph_id"):
      construct_subgraph_id("", "dev")

  def test_empty_name_raises(self):
    with pytest.raises(ValueError, match="subgraph_name"):
      construct_subgraph_id("kg123", "")

  def test_underscore_in_parent_raises(self):
    with pytest.raises(ValueError, match="underscore"):
      construct_subgraph_id("kg_123", "dev")

  def test_invalid_subgraph_name_raises(self):
    with pytest.raises(ValueError, match="alphanumeric"):
      construct_subgraph_id("kg123", "dev-1")

  def test_long_subgraph_name_raises(self):
    with pytest.raises(ValueError, match="alphanumeric"):
      construct_subgraph_id("kg123", "a" * 21)
