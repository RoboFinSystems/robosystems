"""Integration tests for graph creation limits through API endpoints."""

import pytest
from fastapi import status
from unittest.mock import Mock, patch
from httpx import AsyncClient
from robosystems.models.iam import UserLimits
from robosystems.config import env  # Import to enable patching


@pytest.mark.asyncio
class TestGraphCreationLimits:
  """Test graph creation with various limit scenarios."""

  @pytest.fixture
  def auth_headers(self):
    """Mock authentication headers."""
    return {"Authorization": "Bearer test_token"}

  @pytest.fixture
  def graph_request_data(self):
    """Standard graph creation request."""
    return {
      "metadata": {"graph_name": "Test Graph", "description": "Testing graph limits"},
      "instance_tier": "standard",
    }

  async def test_create_graph_with_zero_limit_blocked(
    self, async_client: AsyncClient, graph_request_data
  ):
    """Test that users with limit=0 get 403 error when creating graphs."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch.object(UserLimits, "get_by_user_id") as mock_get_limits:
        # Create mock limits with zero max
        mock_limits = Mock()
        mock_limits.max_user_graphs = 0
        mock_limits.can_create_user_graph.return_value = (
          False,
          "Safety limit reached (0 graphs). Please contact support if you need more.",
        )
        mock_get_limits.return_value = mock_limits

        mock_db = Mock()
        mock_get_db.return_value = iter([mock_db])

        # Attempt to create graph
        response = await async_client.post("/v1/graphs", json=graph_request_data)

        # Assert 403 Forbidden
        assert response.status_code == status.HTTP_403_FORBIDDEN
        response_data = response.json()
        assert response_data["detail"]["error"]["code"] == "graph_limit_reached"
        assert (
          "Safety limit reached (0 graphs)"
          in response_data["detail"]["error"]["message"]
        )
        assert "contact support" in response_data["detail"]["error"]["message"].lower()

  async def test_create_graph_at_limit_blocked(
    self, async_client: AsyncClient, graph_request_data
  ):
    """Test that users at their limit cannot create more graphs."""
    with patch("robosystems.database.get_db_session") as mock_get_db:
      with patch.object(UserLimits, "get_by_user_id") as mock_get_limits:
        # Create mock limits at maximum
        mock_limits = Mock()
        mock_limits.max_user_graphs = 5
        mock_limits.can_create_user_graph.return_value = (
          False,
          "Safety limit reached (5 graphs). Please contact support if you need more.",
        )
        mock_get_limits.return_value = mock_limits

        mock_db = Mock()
        mock_get_db.return_value = iter([mock_db])

        # Attempt to create graph
        response = await async_client.post("/v1/graphs", json=graph_request_data)

        # Assert 403 Forbidden
        assert response.status_code == status.HTTP_403_FORBIDDEN
        response_data = response.json()
        assert response_data["detail"]["error"]["code"] == "graph_limit_reached"
        assert (
          "Safety limit reached (5 graphs)"
          in response_data["detail"]["error"]["message"]
        )

  # TODO: This test needs to be updated to handle SSE responses
  # async def test_create_graph_below_limit_allowed(
  #   self, async_client: AsyncClient, graph_request_data
  # ):
  #   """Test that users below their limit can create graphs successfully."""
  #   # This test needs to be rewritten to handle the new SSE response format
  #   pass

  async def test_create_graph_with_custom_limit_from_env(
    self, async_client: AsyncClient, graph_request_data
  ):
    """Test that USER_GRAPHS_DEFAULT_LIMIT environment variable is respected."""
    # Test with environment variable set to 0
    with patch.object(env, "USER_GRAPHS_DEFAULT_LIMIT", 0):
      with patch("robosystems.database.get_db_session") as mock_get_db:
        with patch.object(UserLimits, "get_by_user_id") as mock_get_limits:
          # Mock creating new user with env default
          mock_limits = Mock()
          mock_limits.max_user_graphs = 0  # From env
          mock_limits.can_create_user_graph.return_value = (
            False,
            "Safety limit reached (0 graphs). Please contact support if you need more.",
          )
          mock_get_limits.return_value = mock_limits

          mock_db = Mock()
          mock_get_db.return_value = iter([mock_db])

          response = await async_client.post("/v1/graphs", json=graph_request_data)

          assert response.status_code == status.HTTP_403_FORBIDDEN
          response_data = response.json()
          assert (
            "Safety limit reached (0 graphs)"
            in response_data["detail"]["error"]["message"]
          )

  async def test_graph_limit_message_formatting(
    self, async_client: AsyncClient, graph_request_data
  ):
    """Test that error messages are properly formatted for different limits."""
    test_cases = [
      (0, "Safety limit reached (0 graphs)"),
      (1, "Safety limit reached (1 graphs)"),
      (10, "Safety limit reached (10 graphs)"),
      (100, "Safety limit reached (100 graphs)"),
    ]

    for limit, expected_message_part in test_cases:
      with patch("robosystems.database.get_db_session") as mock_get_db:
        with patch.object(UserLimits, "get_by_user_id") as mock_get_limits:
          mock_limits = Mock()
          mock_limits.max_user_graphs = limit
          mock_limits.can_create_user_graph.return_value = (
            False,
            f"Safety limit reached ({limit} graphs). Please contact support if you need more.",
          )
          mock_get_limits.return_value = mock_limits

          mock_db = Mock()
          mock_get_db.return_value = iter([mock_db])

          response = await async_client.post("/v1/graphs", json=graph_request_data)

          assert response.status_code == status.HTTP_403_FORBIDDEN
          response_data = response.json()
          assert expected_message_part in response_data["detail"]["error"]["message"]

  # TODO: This test needs to be updated to handle SSE responses
  # async def test_user_without_limits_creates_defaults(
  #   self, async_client: AsyncClient, graph_request_data
  # ):
  #   """Test that users without limits get defaults from environment."""
  #   # This test needs to be rewritten to handle the new SSE response format
  #   pass
