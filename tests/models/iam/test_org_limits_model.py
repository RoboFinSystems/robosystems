"""Tests for OrgLimits model with graph creation scenarios."""

from unittest.mock import Mock

import pytest

from robosystems.models.iam import OrgLimits


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

    # Check that defaults were applied
    assert limits.org_id == "new_org_id"
    assert limits.max_graphs == 100

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
