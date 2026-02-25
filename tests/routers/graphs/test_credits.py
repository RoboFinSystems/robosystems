"""Comprehensive unit tests for the credits router."""

from decimal import Decimal
from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

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
        "robosystems.models.iam.user_repository.UserRepository.user_has_access",
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
        "robosystems.models.iam.user_repository.UserRepository.user_has_access",
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
        "robosystems.models.iam.graph_user.GraphUser.user_has_access",
        return_value=True,
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
        "robosystems.models.iam.graph_user.GraphUser.user_has_access",
        return_value=False,
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
        "robosystems.models.iam.graph_user.GraphUser.user_has_access",
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


@pytest.mark.unit
class TestCheckCreditBalance:
  """Test the balance check endpoint."""

  @pytest.mark.asyncio
  async def test_balance_check_sufficient(self):
    """Test balance check returns sufficient credits."""
    with patch.object(credits_module, "CreditService") as MockCreditService:
      mock_service = MockCreditService.return_value
      mock_service.check_credit_balance.return_value = {
        "has_sufficient_credits": True,
        "required_credits": 1.5,
        "available_credits": 1000.0,
      }

      result = await credits_module.check_credit_balance(
        graph_id="kg01234567890abcdef",
        operation_type="entity_lookup",
        base_cost=None,
        current_user=_make_mock_user(),
        user_graph=Mock(),
        db=Mock(),
        _rate_limit=None,
      )

    assert result["has_sufficient_credits"] is True

  @pytest.mark.asyncio
  async def test_balance_check_insufficient(self):
    """Test balance check returns insufficient credits."""
    with patch.object(credits_module, "CreditService") as MockCreditService:
      mock_service = MockCreditService.return_value
      mock_service.check_credit_balance.return_value = {
        "has_sufficient_credits": False,
        "required_credits": 500.0,
        "available_credits": 100.0,
      }

      result = await credits_module.check_credit_balance(
        graph_id="kg01234567890abcdef",
        operation_type="bulk_operation",
        base_cost=Decimal("500.0"),
        current_user=_make_mock_user(),
        user_graph=Mock(),
        db=Mock(),
        _rate_limit=None,
      )

    assert result["has_sufficient_credits"] is False

  @pytest.mark.asyncio
  async def test_balance_check_uses_default_cost_when_none(self):
    """Test that default operation cost is used when base_cost is None."""
    with (
      patch.object(credits_module, "CreditService") as MockCreditService,
      patch.object(
        credits_module, "get_operation_cost", return_value=Decimal("1.5")
      ) as mock_get_cost,
    ):
      mock_service = MockCreditService.return_value
      mock_service.check_credit_balance.return_value = {
        "has_sufficient_credits": True,
      }

      await credits_module.check_credit_balance(
        graph_id="kg01234567890abcdef",
        operation_type="cypher_query",
        base_cost=None,
        current_user=_make_mock_user(),
        user_graph=Mock(),
        db=Mock(),
        _rate_limit=None,
      )

      mock_get_cost.assert_called_once_with("cypher_query")

  @pytest.mark.asyncio
  async def test_balance_check_error_returns_404(self):
    """Test error in balance check returns 404."""
    with patch.object(credits_module, "CreditService") as MockCreditService:
      mock_service = MockCreditService.return_value
      mock_service.check_credit_balance.return_value = {"error": "No credit pool found"}

      with pytest.raises(HTTPException) as exc_info:
        await credits_module.check_credit_balance(
          graph_id="kg01234567890abcdef",
          operation_type="entity_lookup",
          base_cost=None,
          current_user=_make_mock_user(),
          user_graph=Mock(),
          db=Mock(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 404


@pytest.mark.unit
class TestCheckStorageLimits:
  """Test the storage limits endpoint."""

  @pytest.mark.asyncio
  async def test_returns_storage_limit_info(self):
    """Test successful storage limit response."""
    mock_limit_info = {
      "graph_id": "kg01234567890abcdef",
      "current_storage_gb": 450.5,
      "effective_limit_gb": 500.0,
      "usage_percentage": 90.1,
      "within_limit": True,
      "approaching_limit": True,
      "needs_warning": True,
      "has_override": False,
      "recommendations": ["Monitor storage closely"],
    }

    with patch.object(credits_module, "CreditService") as MockCreditService:
      mock_service = MockCreditService.return_value
      mock_service.check_storage_limit.return_value = mock_limit_info

      result = await credits_module.check_storage_limits(
        graph_id="kg01234567890abcdef",
        current_user=_make_mock_user(),
        user_graph=Mock(),
        db=Mock(),
        _rate_limit=None,
      )

    assert result.graph_id == "kg01234567890abcdef"
    assert result.usage_percentage == 90.1
    assert result.approaching_limit is True

  @pytest.mark.asyncio
  async def test_storage_limit_not_found(self):
    """Test 404 when no credit pool for storage limits."""
    with patch.object(credits_module, "CreditService") as MockCreditService:
      mock_service = MockCreditService.return_value
      mock_service.check_storage_limit.return_value = {"error": "No credit pool found"}

      with pytest.raises(HTTPException) as exc_info:
        await credits_module.check_storage_limits(
          graph_id="kg01234567890abcdef",
          current_user=_make_mock_user(),
          user_graph=Mock(),
          db=Mock(),
          _rate_limit=None,
        )
      assert exc_info.value.status_code == 404


@pytest.mark.unit
class TestGetStorageUsage:
  """Test the storage usage endpoint."""

  @pytest.mark.asyncio
  async def test_returns_storage_usage_data(self):
    """Test successful storage usage response with records."""
    mock_credits = Mock()
    mock_credits.graph_tier = "ladybug-large"

    mock_usage_row = Mock()
    mock_usage_row.usage_date = Mock()
    mock_usage_row.usage_date.isoformat.return_value = "2024-01-30"
    mock_usage_row.avg_storage_gb = 18.5
    mock_usage_row.measurement_count = 24

    mock_db = Mock()
    mock_query = Mock()
    mock_query.filter.return_value = mock_query
    mock_query.group_by.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.all.return_value = [mock_usage_row]
    mock_db.query.return_value = mock_query

    with (
      patch(
        "robosystems.models.iam.GraphCredits.get_by_graph_id",
        return_value=mock_credits,
      ),
      patch(
        "robosystems.operations.graph.credit_service.get_operation_cost",
        return_value=Decimal("0.05"),
      ),
    ):
      result = await credits_module.get_storage_usage(
        graph_id="kg01234567890abcdef",
        days=30,
        current_user=_make_mock_user(),
        user_graph=Mock(),
        db=mock_db,
        _rate_limit=None,
      )

    assert result["graph_id"] == "kg01234567890abcdef"
    assert result["graph_tier"] == "ladybug-large"
    assert result["storage_multiplier"] == 1.0
    assert len(result["recent_usage"]) == 1

  @pytest.mark.asyncio
  async def test_returns_empty_usage_data(self):
    """Test storage usage response with no records."""
    mock_credits = Mock()
    mock_credits.graph_tier = "ladybug-standard"

    mock_db = Mock()
    mock_query = Mock()
    mock_query.filter.return_value = mock_query
    mock_query.group_by.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.all.return_value = []
    mock_db.query.return_value = mock_query

    with (
      patch(
        "robosystems.models.iam.GraphCredits.get_by_graph_id",
        return_value=mock_credits,
      ),
      patch(
        "robosystems.operations.graph.credit_service.get_operation_cost",
        return_value=Decimal("0.05"),
      ),
    ):
      result = await credits_module.get_storage_usage(
        graph_id="kg01234567890abcdef",
        days=30,
        current_user=_make_mock_user(),
        user_graph=Mock(),
        db=mock_db,
        _rate_limit=None,
      )

    assert result["recent_usage"] == []
    assert result["summary"]["average_daily_storage_gb"] == 0
