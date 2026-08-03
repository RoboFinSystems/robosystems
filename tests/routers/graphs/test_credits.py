"""Comprehensive unit tests for the credits router."""

from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.core import GraphRole
from robosystems.routers.graphs import credits as credits_module
from robosystems.routers.graphs.credits import (
  get_graph_access,
)


def _make_mock_user(user_id="user-123"):
  user = Mock()
  user.id = user_id
  return user


def _make_mock_identity(is_shared_repository=False, is_user_graph=True):
  identity = Mock()
  identity.is_shared_repository = is_shared_repository
  identity.is_user_graph = is_user_graph
  return identity


@pytest.mark.unit
class TestGetGraphAccess:
  """Test the get_graph_access dependency helper."""

  def test_shared_repo_access_granted(self):
    """Test that shared repository access creates synthetic GraphUser."""
    identity = _make_mock_identity(is_shared_repository=True, is_user_graph=False)

    with (
      patch(
        "robosystems.middleware.graph.utils.MultiTenantUtils.get_graph_identity",
        return_value=identity,
      ),
      patch(
        "robosystems.models.core.user.user_repository.UserRepository.user_has_access",
        return_value=True,
      ),
    ):
      result = get_graph_access(
        graph_id="sec",
        current_user=_make_mock_user(),
        db=Mock(),
      )
      assert result.role == "reader"
      assert result.graph_id == "sec"

  def test_shared_repo_access_denied(self):
    """Test shared repository access denied when user lacks permission."""
    identity = _make_mock_identity(is_shared_repository=True, is_user_graph=False)

    with (
      patch(
        "robosystems.middleware.graph.utils.MultiTenantUtils.get_graph_identity",
        return_value=identity,
      ),
      patch(
        "robosystems.models.core.user.user_repository.UserRepository.user_has_access",
        return_value=False,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        get_graph_access(
          graph_id="sec",
          current_user=_make_mock_user(),
          db=Mock(),
        )
      assert exc_info.value.status_code == 403

  def test_user_graph_access_granted(self):
    """Test user graph access with existing GraphUser."""
    identity = _make_mock_identity(is_shared_repository=False, is_user_graph=True)
    mock_user_graph = Mock()
    mock_user_graph.user_id = "user-123"
    mock_user_graph.graph_id = "kg01234567890abcdef"

    mock_db = Mock()
    mock_query = Mock()
    mock_query.filter.return_value = mock_query
    mock_query.first.return_value = mock_user_graph
    mock_db.query.return_value = mock_query

    with (
      patch(
        "robosystems.middleware.graph.utils.MultiTenantUtils.get_graph_identity",
        return_value=identity,
      ),
      patch(
        "robosystems.models.core.graph.graph_user.GraphUser.get_effective_role",
        return_value=(GraphRole.MEMBER, False),
      ),
    ):
      result = get_graph_access(
        graph_id="kg01234567890abcdef",
        current_user=_make_mock_user(),
        db=mock_db,
      )
      assert result == mock_user_graph

  def test_user_graph_access_denied(self):
    """Test user graph access denied when user has no access."""
    identity = _make_mock_identity(is_shared_repository=False, is_user_graph=True)

    with (
      patch(
        "robosystems.middleware.graph.utils.MultiTenantUtils.get_graph_identity",
        return_value=identity,
      ),
      patch(
        "robosystems.models.core.graph.graph_user.GraphUser.get_effective_role",
        return_value=(None, False),
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        get_graph_access(
          graph_id="kg01234567890abcdef",
          current_user=_make_mock_user(),
          db=Mock(),
        )
      assert exc_info.value.status_code == 403

  def test_unknown_graph_type(self):
    """Test error handling for unknown graph type."""
    identity = _make_mock_identity(is_shared_repository=False, is_user_graph=False)

    with patch(
      "robosystems.middleware.graph.utils.MultiTenantUtils.get_graph_identity",
      return_value=identity,
    ):
      with pytest.raises(HTTPException) as exc_info:
        get_graph_access(
          graph_id="unknown_graph",
          current_user=_make_mock_user(),
          db=Mock(),
        )
      assert exc_info.value.status_code == 400

  def test_user_graph_access_validation_fail_safety(self):
    """Test safety check when user_has_access returns True but no GraphUser found."""
    identity = _make_mock_identity(is_shared_repository=False, is_user_graph=True)

    mock_db = Mock()
    mock_query = Mock()
    mock_query.filter.return_value = mock_query
    mock_query.first.return_value = None
    mock_db.query.return_value = mock_query

    with (
      patch(
        "robosystems.middleware.graph.utils.MultiTenantUtils.get_graph_identity",
        return_value=identity,
      ),
      patch(
        "robosystems.models.core.graph.graph_user.GraphUser.user_has_access",
        return_value=True,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        get_graph_access(
          graph_id="kg01234567890abcdef",
          current_user=_make_mock_user(),
          db=mock_db,
        )
      assert exc_info.value.status_code == 403


@pytest.mark.unit
class TestGetCreditSummary:
  """Test the credit summary endpoint."""

  @pytest.mark.asyncio
  async def test_returns_credit_summary(self):
    """Test successful credit summary response."""
    mock_summary = {
      "graph_id": "kg01234567890abcdef",
      "graph_tier": "ladybug-standard",
      "monthly_allocation": 8000.0,
      "current_balance": 7500.0,
      "consumed_this_month": 500.0,
      "transaction_count": 10,
      "usage_percentage": 6.25,
    }

    with patch.object(credits_module, "CreditService") as MockCreditService:
      mock_service = MockCreditService.return_value
      mock_service.get_credit_summary.return_value = mock_summary

      result = await credits_module.get_credit_summary(
        graph_id="kg01234567890abcdef",
        current_user=_make_mock_user(),
        user_graph=Mock(),
        db=Mock(),
        _rate_limit=None,
      )

    assert result.graph_id == "kg01234567890abcdef"
    assert result.current_balance == 7500.0

  @pytest.mark.asyncio
  async def test_returns_404_when_no_credit_pool(self):
    """Test 404 when credit pool not found."""
    with patch.object(credits_module, "CreditService") as MockCreditService:
      mock_service = MockCreditService.return_value
      mock_service.get_credit_summary.return_value = {"error": "No credit pool found"}

      with pytest.raises(HTTPException) as exc_info:
        await credits_module.get_credit_summary(
          graph_id="kg01234567890abcdef",
          current_user=_make_mock_user(),
          user_graph=Mock(),
          db=Mock(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_handles_service_error(self):
    """Test 500 on internal error."""
    with patch.object(credits_module, "CreditService") as MockCreditService:
      mock_service = MockCreditService.return_value
      mock_service.get_credit_summary.side_effect = RuntimeError("DB error")

      with pytest.raises(HTTPException) as exc_info:
        await credits_module.get_credit_summary(
          graph_id="kg01234567890abcdef",
          current_user=_make_mock_user(),
          user_graph=Mock(),
          db=Mock(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 500
