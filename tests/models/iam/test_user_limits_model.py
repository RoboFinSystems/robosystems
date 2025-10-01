"""Comprehensive tests for UserLimits model with graph creation scenarios."""

import pytest
from unittest.mock import Mock, patch
from robosystems.models.iam import UserLimits


class TestUserLimitsModel:
  """Test UserLimits model with various graph limit scenarios."""

  @pytest.fixture
  def mock_session(self):
    """Create a mock database session."""
    session = Mock()
    return session

  @pytest.fixture
  def user_limits(self, mock_session):
    """Create a UserLimits instance for testing."""
    limits = UserLimits(
      user_id="test_user_123",
      max_user_graphs=5,  # Default test limit
    )
    return limits

  def test_can_create_graph_with_zero_limit(self, mock_session):
    """Test that users with limit=0 cannot create any graphs."""
    # Create user with 0 graph limit
    limits = UserLimits(user_id="zero_limit_user", max_user_graphs=0)

    # Mock query to return 0 existing graphs
    mock_query = Mock()
    mock_query.filter.return_value.count.return_value = 0
    mock_session.query.return_value = mock_query

    # Check if user can create graph
    can_create, reason = limits.can_create_user_graph(mock_session)

    # Assert user cannot create any graphs
    assert can_create is False
    assert "Safety limit reached (0 graphs)" in reason
    assert "contact support" in reason.lower()

  def test_can_create_graph_at_limit(self, mock_session, user_limits):
    """Test that users at their limit cannot create more graphs."""
    # Mock query to return 5 existing graphs (at the limit)
    mock_query = Mock()
    mock_query.filter.return_value.count.return_value = 5
    mock_session.query.return_value = mock_query

    # Check if user can create graph
    can_create, reason = user_limits.can_create_user_graph(mock_session)

    # Assert user cannot create more graphs
    assert can_create is False
    assert "Safety limit reached (5 graphs)" in reason
    assert "contact support" in reason.lower()

  def test_can_create_graph_above_limit(self, mock_session, user_limits):
    """Test that users above their limit cannot create graphs."""
    # Mock query to return 6 existing graphs (above the limit of 5)
    mock_query = Mock()
    mock_query.filter.return_value.count.return_value = 6
    mock_session.query.return_value = mock_query

    # Check if user can create graph
    can_create, reason = user_limits.can_create_user_graph(mock_session)

    # Assert user cannot create more graphs
    assert can_create is False
    assert "Safety limit reached (5 graphs)" in reason

  def test_can_create_graph_below_limit(self, mock_session, user_limits):
    """Test that users below their limit can create graphs."""
    # Mock query to return 3 existing graphs (below limit of 5)
    mock_query = Mock()
    mock_query.filter.return_value.count.return_value = 3
    mock_session.query.return_value = mock_query

    # Check if user can create graph
    can_create, reason = user_limits.can_create_user_graph(mock_session)

    # Assert user can create more graphs
    assert can_create is True
    assert "Can create graph" in reason

  def test_can_create_graph_with_high_limit(self, mock_session):
    """Test users with high limits work correctly."""
    limits = UserLimits(user_id="enterprise_user", max_user_graphs=1000)

    # Mock query to return 50 existing graphs
    mock_query = Mock()
    mock_query.filter.return_value.count.return_value = 50
    mock_session.query.return_value = mock_query

    # Check if user can create graph
    can_create, reason = limits.can_create_user_graph(mock_session)

    # Assert user can create many more graphs
    assert can_create is True
    assert "Can create graph" in reason

  def test_get_current_usage_at_limit(self, mock_session, user_limits):
    """Test usage statistics when at limit."""
    # Mock query to return 5 graphs (at limit)
    mock_query = Mock()
    mock_query.filter.return_value.count.return_value = 5
    mock_session.query.return_value = mock_query

    usage = user_limits.get_current_usage(mock_session)

    assert usage["graphs"]["current"] == 5
    assert usage["graphs"]["limit"] == 5
    assert usage["graphs"]["remaining"] == 0

  def test_get_current_usage_with_zero_limit(self, mock_session):
    """Test usage statistics with zero limit."""
    limits = UserLimits(user_id="zero_user", max_user_graphs=0)

    # Mock query to return 0 graphs
    mock_query = Mock()
    mock_query.filter.return_value.count.return_value = 0
    mock_session.query.return_value = mock_query

    usage = limits.get_current_usage(mock_session)

    assert usage["graphs"]["current"] == 0
    assert usage["graphs"]["limit"] == 0
    assert usage["graphs"]["remaining"] == 0

  def test_get_current_usage_below_limit(self, mock_session, user_limits):
    """Test usage statistics when below limit."""
    # Mock query to return 2 graphs (below limit of 5)
    mock_query = Mock()
    mock_query.filter.return_value.count.return_value = 2
    mock_session.query.return_value = mock_query

    usage = user_limits.get_current_usage(mock_session)

    assert usage["graphs"]["current"] == 2
    assert usage["graphs"]["limit"] == 5
    assert usage["graphs"]["remaining"] == 3

  def test_update_limit(self, mock_session, user_limits):
    """Test updating user's graph limit."""
    # Update limit from 5 to 10
    user_limits.update_limit(10, mock_session)

    assert user_limits.max_user_graphs == 10
    mock_session.commit.assert_called_once()
    mock_session.refresh.assert_called_once_with(user_limits)

  def test_update_limit_to_zero(self, mock_session, user_limits):
    """Test updating limit to zero (blocking all new graphs)."""
    # Update limit to 0
    user_limits.update_limit(0, mock_session)

    assert user_limits.max_user_graphs == 0
    mock_session.commit.assert_called_once()

  def test_create_default_limits_with_zero_env(self, mock_session):
    """Test creating default limits when environment variable is 0."""
    with patch("robosystems.models.iam.user_limits.env.USER_GRAPHS_DEFAULT_LIMIT", 0):
      limits = UserLimits.create_default_limits("new_user", mock_session)

      assert limits.user_id == "new_user"
      assert limits.max_user_graphs == 0
      mock_session.add.assert_called_once()
      mock_session.commit.assert_called_once()

  def test_create_default_limits_with_env_value(self, mock_session):
    """Test that default limits use environment variable."""
    # Test with different environment values
    test_values = [0, 1, 50, 100, 500]

    for test_value in test_values:
      with patch(
        "robosystems.models.iam.user_limits.env.USER_GRAPHS_DEFAULT_LIMIT", test_value
      ):
        mock_session.reset_mock()
        limits = UserLimits.create_default_limits(f"user_{test_value}", mock_session)

        assert limits.max_user_graphs == test_value
        mock_session.add.assert_called_once()
        mock_session.commit.assert_called_once()
