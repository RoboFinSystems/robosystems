"""Tests for OrgLimits model with graph creation scenarios."""

from unittest.mock import Mock

import pytest

from robosystems.models.core import OrgLimits


class TestOrgLimitsModel:
  """Test OrgLimits model with various graph limit scenarios."""

  @pytest.fixture
  def mock_session(self):
    """Create a mock database session."""
    session = Mock()
    return session

  @pytest.fixture
  def org_limits(self, mock_session):
    """Create an OrgLimits instance for testing."""
    limits = OrgLimits(
      org_id="test_org_123",
      max_graphs=5,  # Default test limit
    )
    return limits

  def test_can_create_graph_with_zero_limit(self, mock_session):
    """Test that orgs with limit=0 cannot create any graphs."""
    # Create org with 0 graph limit
    limits = OrgLimits(org_id="zero_limit_org", max_graphs=0)

    # Mock the OrgUser query to return empty list (no users)
    mock_org_user_query = Mock()
    mock_org_user_query.filter.return_value.all.return_value = []

    # Mock the GraphUser query to return 0 existing graphs
    mock_graph_user_query = Mock()
    mock_graph_user_query.filter.return_value.count.return_value = 0

    # Return different mocks for OrgUser and GraphUser queries
    def query_side_effect(model):
      if model.__name__ == "OrgUser":
        return mock_org_user_query
      return mock_graph_user_query

    mock_session.query.side_effect = query_side_effect

    # Should not be able to create a graph with limit=0
    can_create, reason = limits.can_create_graph(mock_session)

    assert can_create is False
    assert "graph limit" in reason.lower()

  def test_can_create_graph_under_limit(self, mock_session):
    """Test that orgs can create graphs when under limit."""
    limits = OrgLimits(org_id="test_org", max_graphs=5)

    # Mock the OrgUser query to return a user
    mock_org_user = Mock()
    mock_org_user.user_id = "user_123"
    mock_org_user_query = Mock()
    mock_org_user_query.filter.return_value.all.return_value = [mock_org_user]

    # Mock the GraphUser query to return 2 existing graphs
    mock_graph_user_query = Mock()
    mock_graph_user_query.filter.return_value.count.return_value = 2

    # Return different mocks for OrgUser and GraphUser queries
    def query_side_effect(model):
      if model.__name__ == "OrgUser":
        return mock_org_user_query
      return mock_graph_user_query

    mock_session.query.side_effect = query_side_effect

    # Should be able to create a graph (2 < 5)
    can_create, reason = limits.can_create_graph(mock_session)

    assert can_create is True
    assert "can create" in reason.lower()

  def test_can_create_graph_at_limit(self, mock_session):
    """Test that orgs cannot create graphs when at limit."""
    limits = OrgLimits(org_id="test_org", max_graphs=3)

    # Mock the OrgUser query to return a user
    mock_org_user = Mock()
    mock_org_user.user_id = "user_123"
    mock_org_user_query = Mock()
    mock_org_user_query.filter.return_value.all.return_value = [mock_org_user]

    # Mock the GraphUser query to return 3 existing graphs
    mock_graph_user_query = Mock()
    mock_graph_user_query.filter.return_value.count.return_value = 3

    # Return different mocks for OrgUser and GraphUser queries
    def query_side_effect(model):
      if model.__name__ == "OrgUser":
        return mock_org_user_query
      return mock_graph_user_query

    mock_session.query.side_effect = query_side_effect

    # Should not be able to create a graph (3 >= 3)
    can_create, reason = limits.can_create_graph(mock_session)

    assert can_create is False
    assert "reached" in reason.lower()

  def test_can_create_graph_unlimited(self, mock_session):
    """Test that orgs with unlimited graphs (-1) can always create."""
    limits = OrgLimits(org_id="unlimited_org", max_graphs=-1)

    # Should be able to create a graph (unlimited)
    can_create, reason = limits.can_create_graph(mock_session)

    assert can_create is True
    assert "unlimited" in reason.lower()

  def test_create_default_limits(self, mock_session):
    """Test creation of default limits for an org."""
    # Mock the add and commit operations
    mock_session.add = Mock()
    mock_session.commit = Mock()
    mock_session.refresh = Mock()

    # Create default limits
    limits = OrgLimits.create_default_limits("new_org_id", mock_session)

    # Check that defaults were applied. Assert against the configured default
    # rather than a literal — the safe code default is intentionally the floor
    # (1) so an SSM blip cannot durably over-provision an org.
    from robosystems.config.tuning import TuningConfig

    assert limits.org_id == "new_org_id"
    assert limits.max_graphs == TuningConfig.get_org_graphs_default_limit()

    # Verify database operations were called
    mock_session.add.assert_called_once()
    mock_session.commit.assert_called_once()
    mock_session.refresh.assert_called_once()

  def test_get_or_create_for_org(self, mock_session):
    """Test get_or_create_for_org creates limits when none exist."""
    # Mock no existing limits
    mock_session.query.return_value.filter.return_value.first.return_value = None
    mock_session.add = Mock()
    mock_session.commit = Mock()
    mock_session.refresh = Mock()

    # Get or create limits
    limits = OrgLimits.get_or_create_for_org("test_org", mock_session)

    # Should have created new limits
    assert limits.org_id == "test_org"
    mock_session.add.assert_called_once()
    mock_session.commit.assert_called_once()


class TestOrgLimitsGraphCounting:
  """Quota counts org-owned graphs, not per-user access rows."""

  def test_multi_user_graph_counts_once(self, test_db, test_user, test_org):
    from uuid import uuid4

    from robosystems.models.core import (
      Graph,
      GraphUser,
      OrgLimits,
      OrgRole,
      OrgUser,
      User,
    )

    limits = OrgLimits.get_or_create_for_org(test_org.id, test_db)
    limits.max_graphs = 2
    test_db.commit()

    graph = Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=test_org.id,
      graph_name="Shared Graph",
      graph_type="generic",
      session=test_db,
    )

    second_user = User(
      id=f"limit-user-{uuid4().hex[:8]}",
      email=f"limit+{uuid4().hex[:8]}@example.com",
      name="Second Member",
      password_hash=test_user.password_hash,
    )
    test_db.add(second_user)
    test_db.commit()
    OrgUser.create(
      org_id=test_org.id, user_id=second_user.id, role=OrgRole.MEMBER, session=test_db
    )

    # Two access rows on one graph must still count as a single graph
    GraphUser.create(
      user_id=test_user.id, graph_id=graph.graph_id, role="admin", session=test_db
    )
    GraphUser.create(
      user_id=second_user.id, graph_id=graph.graph_id, role="member", session=test_db
    )

    can_create, _ = limits.can_create_graph(test_db)
    assert can_create is True
    assert limits.get_current_usage(test_db)["graphs"]["current"] == 1

  def test_deprovisioned_graphs_do_not_consume_quota(
    self, test_db, test_user, test_org
  ):
    from uuid import uuid4

    from robosystems.models.core import Graph, GraphStatus, OrgLimits

    limits = OrgLimits.get_or_create_for_org(test_org.id, test_db)
    limits.max_graphs = 1
    test_db.commit()

    graph = Graph.create(
      graph_id=f"graph_{uuid4().hex[:8]}",
      org_id=test_org.id,
      graph_name="Old Graph",
      graph_type="generic",
      session=test_db,
    )
    graph.status = GraphStatus.DEPROVISIONED.value
    test_db.commit()

    can_create, _ = limits.can_create_graph(test_db)
    assert can_create is True
    assert limits.get_current_usage(test_db)["graphs"]["current"] == 0
