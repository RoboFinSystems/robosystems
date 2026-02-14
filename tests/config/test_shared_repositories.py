"""Tests for shared repository registry functions."""

import pytest

from robosystems.config.shared_repositories import (
  is_shared_repository,
  is_shared_repository_or_subgraph,
  resolve_shared_repository_parent,
)


class TestIsSharedRepository:
  """Test exact-match shared repository check."""

  def test_registered_repo(self):
    assert is_shared_repository("sec") is True

  def test_unregistered_repo(self):
    assert is_shared_repository("nonexistent") is False

  def test_subgraph_not_matched(self):
    assert is_shared_repository("sec_historical") is False

  def test_user_graph(self):
    assert is_shared_repository("kg1a2b3c4d5e") is False

  def test_none(self):
    assert is_shared_repository(None) is False


class TestIsSharedRepositoryOrSubgraph:
  """Test subgraph-aware shared repository check."""

  def test_parent_repo(self):
    assert is_shared_repository_or_subgraph("sec") is True

  def test_subgraph(self):
    assert is_shared_repository_or_subgraph("sec_historical") is True

  def test_subgraph_deep_name(self):
    """Multiple underscores — only first segment is checked as parent."""
    assert is_shared_repository_or_subgraph("sec_a_b_c") is True

  def test_user_graph(self):
    assert is_shared_repository_or_subgraph("kg1a2b3c4d5e") is False

  def test_user_subgraph(self):
    assert is_shared_repository_or_subgraph("kg1a2b3c4d5e_dev") is False

  def test_unknown_parent(self):
    assert is_shared_repository_or_subgraph("nosuchrepo_sub") is False

  def test_none(self):
    assert is_shared_repository_or_subgraph(None) is False

  def test_empty_string(self):
    assert is_shared_repository_or_subgraph("") is False

  def test_underscore_only(self):
    assert is_shared_repository_or_subgraph("_") is False

  def test_trailing_underscore(self):
    assert is_shared_repository_or_subgraph("sec_") is True

  def test_no_underscore_unregistered(self):
    assert is_shared_repository_or_subgraph("industry") is False


class TestResolveSharedRepositoryParent:
  """Test subgraph-to-parent resolution."""

  def test_parent_returns_itself(self):
    assert resolve_shared_repository_parent("sec") == "sec"

  def test_subgraph_returns_parent(self):
    assert resolve_shared_repository_parent("sec_historical") == "sec"

  def test_deep_subgraph_returns_parent(self):
    assert resolve_shared_repository_parent("sec_a_b_c") == "sec"

  def test_user_graph_raises(self):
    with pytest.raises(ValueError, match="Not a shared repository"):
      resolve_shared_repository_parent("kg1a2b3c4d5e")

  def test_user_subgraph_raises(self):
    with pytest.raises(ValueError, match="Not a shared repository"):
      resolve_shared_repository_parent("kg1a2b3c4d5e_dev")

  def test_unknown_repo_raises(self):
    with pytest.raises(ValueError, match="Not a shared repository"):
      resolve_shared_repository_parent("nosuchrepo_sub")
