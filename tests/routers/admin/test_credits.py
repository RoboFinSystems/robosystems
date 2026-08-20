"""Tests for admin credits router endpoints."""

from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from unittest.mock import MagicMock, patch

import pytest
from fastapi import Request

MODULE = "robosystems.routers.admin.credits"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def patch_admin_auth(monkeypatch):
  """Bypass admin authentication for all tests in this module."""

  async def mock_admin_auth(self, request, credentials=None):
    request.state.admin = {
      "key_id": "test_admin_key",
      "name": "Test Admin",
      "permissions": ["credits:read", "credits:write"],
    }
    request.state.admin_key_id = "test_admin_key"

  monkeypatch.setattr(
    "robosystems.middleware.auth.admin.AdminAuthMiddleware.__call__",
    mock_admin_auth,
  )


@pytest.fixture
def mock_request():
  """Create a mock FastAPI request with admin state pre-populated."""
  request = MagicMock(spec=Request)
  request.state.admin = {
    "key_id": "test_admin_key",
    "name": "Test Admin",
    "permissions": ["credits:read", "credits:write"],
  }
  request.state.admin_key_id = "test_admin_key"
  return request


@pytest.fixture
def mock_session():
  """Create a mock SQLAlchemy session."""
  return MagicMock()


def _make_graph_credit_pool(
  graph_id="kg01234567890abcdef",
  user_id="usr_123",
  graph_tier="ladybug-standard",
  current_balance=Decimal("800.0"),
  monthly_allocation=Decimal("1000.0"),
  storage_override_gb=None,
):
  """Helper to create a mock GraphCredits pool object."""
  pool = MagicMock()
  pool.graph_id = graph_id
  pool.user_id = user_id
  pool.graph_tier = graph_tier
  pool.current_balance = current_balance
  pool.monthly_allocation = monthly_allocation
  pool.storage_override_gb = storage_override_gb
  pool.created_at = datetime(2024, 1, 1, tzinfo=UTC)
  pool.updated_at = datetime(2024, 1, 15, tzinfo=UTC)
  return pool


class _FakePlan(str, Enum):
  """Fake enum so .value works like the router expects."""

  STARTER = "starter"


def _make_repo_credit_pool(
  user_repository_id="ur_abc123",
  user_id="usr_456",
  repository_type="sec",
  repository_plan=_FakePlan.STARTER,
  current_balance=Decimal("500.0"),
  monthly_allocation=Decimal("1000.0"),
  credits_consumed_this_month=Decimal("200.0"),
  allows_rollover=False,
  rollover_credits=Decimal("0"),
  is_active=True,
  last_allocation_date=None,
  next_allocation_date=None,
):
  """Helper to create a mock UserRepositoryCredits pool object."""
  pool = MagicMock()
  pool.user_repository_id = user_repository_id
  pool.current_balance = current_balance
  pool.monthly_allocation = monthly_allocation
  pool.credits_consumed_this_month = credits_consumed_this_month
  pool.allows_rollover = allows_rollover
  pool.rollover_credits = rollover_credits
  pool.is_active = is_active
  pool.last_allocation_date = last_allocation_date
  pool.next_allocation_date = next_allocation_date
  pool.created_at = datetime(2024, 2, 1, tzinfo=UTC)
  pool.updated_at = datetime(2024, 2, 10, tzinfo=UTC)
  pool.id = "crd_pool_001"

  # The related UserRepository object
  user_repo = MagicMock()
  user_repo.user_id = user_id
  user_repo.repository_type = repository_type
  user_repo.repository_plan = repository_plan
  pool.user_repository = user_repo

  return pool


# ===========================================================================
# TestListGraphCreditPools
# ===========================================================================


class TestListGraphCreditPools:
  """Tests for GET /admin/v1/credits/graphs."""

  @pytest.mark.unit
  async def test_basic_list(self, mock_request, mock_session):
    """List graph credit pools with no filters returns results."""
    from robosystems.routers.admin.credits import list_graph_credit_pools

    pool = _make_graph_credit_pool()

    mock_query = mock_session.query.return_value
    mock_query.count.return_value = 1
    mock_query.offset.return_value.limit.return_value.all.return_value = [pool]

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await list_graph_credit_pools(
        request=mock_request,
        user_email=None,
        tier=None,
        low_balance_only=False,
        limit=100,
        offset=0,
      )

    assert len(result) == 1
    item = result[0]
    assert item.graph_id == "kg01234567890abcdef"
    assert item.user_id == "usr_123"
    assert item.graph_tier == "ladybug-standard"
    assert item.current_balance == 800.0
    assert item.monthly_allocation == 1000.0
    assert item.credit_multiplier == 1.0
    assert item.storage_limit_override_gb is None
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_filter_by_tier(self, mock_request, mock_session):
    """Filtering by tier adds a join and filter on Graph.graph_tier."""
    from robosystems.routers.admin.credits import list_graph_credit_pools

    pool = _make_graph_credit_pool(graph_tier="ladybug-large")

    mock_query = mock_session.query.return_value
    mock_joined = mock_query.join.return_value
    mock_filtered = mock_joined.filter.return_value
    mock_filtered.count.return_value = 1
    mock_filtered.offset.return_value.limit.return_value.all.return_value = [pool]

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await list_graph_credit_pools(
        request=mock_request,
        user_email=None,
        tier="ladybug-large",
        low_balance_only=False,
        limit=100,
        offset=0,
      )

    assert len(result) == 1
    assert result[0].graph_tier == "ladybug-large"
    mock_query.join.assert_called_once()

  @pytest.mark.unit
  async def test_low_balance_filter(self, mock_request, mock_session):
    """low_balance_only=True adds a filter for balance < 10% of allocation."""
    from robosystems.routers.admin.credits import list_graph_credit_pools

    pool = _make_graph_credit_pool(
      current_balance=Decimal("50.0"), monthly_allocation=Decimal("1000.0")
    )

    mock_query = mock_session.query.return_value
    mock_filtered = mock_query.filter.return_value
    mock_filtered.count.return_value = 1
    mock_filtered.offset.return_value.limit.return_value.all.return_value = [pool]

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await list_graph_credit_pools(
        request=mock_request,
        user_email=None,
        tier=None,
        low_balance_only=True,
        limit=100,
        offset=0,
      )

    assert len(result) == 1
    assert result[0].current_balance == 50.0
    mock_query.filter.assert_called_once()

  @pytest.mark.unit
  async def test_empty_list(self, mock_request, mock_session):
    """When no pools exist, returns empty list."""
    from robosystems.routers.admin.credits import list_graph_credit_pools

    mock_query = mock_session.query.return_value
    mock_query.count.return_value = 0
    mock_query.offset.return_value.limit.return_value.all.return_value = []

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await list_graph_credit_pools(
        request=mock_request,
        user_email=None,
        tier=None,
        low_balance_only=False,
        limit=100,
        offset=0,
      )

    assert result == []

  @pytest.mark.unit
  async def test_storage_override_gb_present(self, mock_request, mock_session):
    """When storage_override_gb is set, it appears in the response."""
    from robosystems.routers.admin.credits import list_graph_credit_pools

    pool = _make_graph_credit_pool(storage_override_gb=Decimal("50.0"))

    mock_query = mock_session.query.return_value
    mock_query.count.return_value = 1
    mock_query.offset.return_value.limit.return_value.all.return_value = [pool]

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await list_graph_credit_pools(
        request=mock_request,
        user_email=None,
        tier=None,
        low_balance_only=False,
        limit=100,
        offset=0,
      )

    assert result[0].storage_limit_override_gb == 50.0


# ===========================================================================
# TestGetGraphCreditPool
# ===========================================================================


class TestGetGraphCreditPool:
  """Tests for GET /admin/v1/credits/graphs/{graph_id}."""

  @pytest.mark.unit
  async def test_found(self, mock_request, mock_session):
    """When pool exists, return a CreditPoolResponse."""
    from robosystems.routers.admin.credits import get_graph_credit_pool

    pool = _make_graph_credit_pool()

    with (
      patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])),
      patch(f"{MODULE}.GraphCredits") as MockGraphCredits,
    ):
      MockGraphCredits.get_by_graph_id.return_value = pool
      result = await get_graph_credit_pool(
        request=mock_request, graph_id="kg01234567890abcdef"
      )

    assert result.graph_id == "kg01234567890abcdef"
    assert result.current_balance == 800.0
    assert result.monthly_allocation == 1000.0
    assert result.credit_multiplier == 1.0
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_not_found_raises_404(self, mock_request, mock_session):
    """When pool does not exist, raise HTTPException 404."""
    from fastapi import HTTPException

    from robosystems.routers.admin.credits import get_graph_credit_pool

    with (
      patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])),
      patch(f"{MODULE}.GraphCredits") as MockGraphCredits,
    ):
      MockGraphCredits.get_by_graph_id.return_value = None
      with pytest.raises(HTTPException) as exc_info:
        await get_graph_credit_pool(request=mock_request, graph_id="kg_nonexistent")

    assert exc_info.value.status_code == 404
    assert "not found" in exc_info.value.detail.lower()
    mock_session.close.assert_called_once()


# ===========================================================================
# TestAddBonusCreditsToGraph
# ===========================================================================


class TestAddBonusCreditsToGraph:
  """Tests for POST /admin/v1/credits/graphs/{graph_id}/bonus."""

  @pytest.mark.unit
  async def test_happy_path(self, mock_request, mock_session):
    """Successfully add bonus credits to an existing pool."""
    from robosystems.routers.admin.credits import add_bonus_credits_to_graph

    pool = _make_graph_credit_pool(current_balance=Decimal("1300.0"))

    bonus_request = MagicMock()
    bonus_request.amount = 500.0
    bonus_request.description = "Customer retention bonus"
    bonus_request.metadata = {"ticket": "SUP-123"}

    with (
      patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])),
      patch(f"{MODULE}.GraphCredits") as MockGraphCredits,
      patch(f"{MODULE}.CreditService") as MockCreditService,
    ):
      MockGraphCredits.get_by_graph_id.return_value = pool
      mock_credit_svc = MagicMock()
      MockCreditService.return_value = mock_credit_svc

      result = await add_bonus_credits_to_graph(
        request=mock_request,
        graph_id="kg01234567890abcdef",
        data=bonus_request,
      )

    mock_credit_svc.add_bonus_credits.assert_called_once()
    call_kwargs = mock_credit_svc.add_bonus_credits.call_args
    assert call_kwargs.kwargs["graph_id"] == "kg01234567890abcdef"
    assert call_kwargs.kwargs["amount"] == Decimal("500.0")
    assert call_kwargs.kwargs["description"] == "Customer retention bonus"
    assert call_kwargs.kwargs["metadata"]["source"] == "admin_api"
    assert call_kwargs.kwargs["metadata"]["admin_key_id"] == "test_admin_key"
    assert call_kwargs.kwargs["metadata"]["ticket"] == "SUP-123"
    assert result.graph_id == "kg01234567890abcdef"
    mock_session.refresh.assert_called_once_with(pool)
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_not_found_raises_404(self, mock_request, mock_session):
    """When pool does not exist, raise HTTPException 404."""
    from fastapi import HTTPException

    from robosystems.routers.admin.credits import add_bonus_credits_to_graph

    bonus_request = MagicMock()
    bonus_request.amount = 500.0
    bonus_request.description = "Bonus"
    bonus_request.metadata = None

    with (
      patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])),
      patch(f"{MODULE}.GraphCredits") as MockGraphCredits,
    ):
      MockGraphCredits.get_by_graph_id.return_value = None
      with pytest.raises(HTTPException) as exc_info:
        await add_bonus_credits_to_graph(
          request=mock_request,
          graph_id="kg_nonexistent",
          data=bonus_request,
        )

    assert exc_info.value.status_code == 404
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_null_metadata_defaults_to_empty(self, mock_request, mock_session):
    """When metadata is None on the request, admin metadata is still added."""
    from robosystems.routers.admin.credits import add_bonus_credits_to_graph

    pool = _make_graph_credit_pool()

    bonus_request = MagicMock()
    bonus_request.amount = 100.0
    bonus_request.description = "Small bonus"
    bonus_request.metadata = None

    with (
      patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])),
      patch(f"{MODULE}.GraphCredits") as MockGraphCredits,
      patch(f"{MODULE}.CreditService") as MockCreditService,
    ):
      MockGraphCredits.get_by_graph_id.return_value = pool
      mock_credit_svc = MagicMock()
      MockCreditService.return_value = mock_credit_svc

      await add_bonus_credits_to_graph(
        request=mock_request,
        graph_id="kg01234567890abcdef",
        data=bonus_request,
      )

    call_kwargs = mock_credit_svc.add_bonus_credits.call_args.kwargs
    assert call_kwargs["metadata"]["source"] == "admin_api"
    assert call_kwargs["metadata"]["admin_name"] == "Test Admin"


# ===========================================================================
# TestResetGraphCreditPool
# ===========================================================================


class TestResetGraphCreditPool:
  """Tests for POST /admin/v1/credits/graphs/{graph_id}/reset."""

  @pytest.mark.unit
  async def test_happy_path(self, mock_request, mock_session):
    """Reset delegates to the service with the admin identity and reason."""
    from robosystems.routers.admin.credits import reset_graph_credit_pool

    pool = _make_graph_credit_pool(current_balance=Decimal("1000.0"))

    reset_request = MagicMock()
    reset_request.reason = "Customer goodwill mid-month refresh"

    with (
      patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])),
      patch(f"{MODULE}.GraphCredits") as MockGraphCredits,
      patch(f"{MODULE}.CreditService") as MockCreditService,
    ):
      MockGraphCredits.get_by_graph_id.return_value = pool
      mock_credit_svc = MagicMock()
      mock_credit_svc.reset_graph_pool.return_value = {
        "success": True,
        "credits_forfeited": 400.0,
        "new_balance": 1000.0,
      }
      MockCreditService.return_value = mock_credit_svc

      result = await reset_graph_credit_pool(
        request=mock_request,
        graph_id="kg01234567890abcdef",
        data=reset_request,
      )

    call_kwargs = mock_credit_svc.reset_graph_pool.call_args.kwargs
    assert call_kwargs["graph_id"] == "kg01234567890abcdef"
    assert call_kwargs["initiated_by"] == "admin:test_admin_key"
    assert call_kwargs["reason"] == "Customer goodwill mid-month refresh"
    assert result.graph_id == "kg01234567890abcdef"
    mock_session.refresh.assert_called_once_with(pool)
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_not_found_raises_404(self, mock_request, mock_session):
    """When pool does not exist, raise HTTPException 404."""
    from fastapi import HTTPException

    from robosystems.routers.admin.credits import reset_graph_credit_pool

    with (
      patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])),
      patch(f"{MODULE}.GraphCredits") as MockGraphCredits,
    ):
      MockGraphCredits.get_by_graph_id.return_value = None
      with pytest.raises(HTTPException) as exc_info:
        await reset_graph_credit_pool(
          request=mock_request,
          graph_id="kg_nonexistent",
          data=None,
        )

    assert exc_info.value.status_code == 404
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_no_body_passes_null_reason(self, mock_request, mock_session):
    """The body is optional; a bare POST resets with the default reason."""
    from robosystems.routers.admin.credits import reset_graph_credit_pool

    pool = _make_graph_credit_pool()

    with (
      patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])),
      patch(f"{MODULE}.GraphCredits") as MockGraphCredits,
      patch(f"{MODULE}.CreditService") as MockCreditService,
    ):
      MockGraphCredits.get_by_graph_id.return_value = pool
      mock_credit_svc = MagicMock()
      mock_credit_svc.reset_graph_pool.return_value = {
        "success": True,
        "credits_forfeited": 0.0,
        "new_balance": 1000.0,
      }
      MockCreditService.return_value = mock_credit_svc

      await reset_graph_credit_pool(
        request=mock_request,
        graph_id="kg01234567890abcdef",
        data=None,
      )

    assert mock_credit_svc.reset_graph_pool.call_args.kwargs["reason"] is None


# ===========================================================================
# TestListRepositoryCreditPools
# ===========================================================================


class TestListRepositoryCreditPools:
  """Tests for GET /admin/v1/credits/repositories."""

  @pytest.mark.unit
  async def test_basic_list(self, mock_request, mock_session):
    """List repository credit pools with no filters."""
    from robosystems.routers.admin.credits import list_repository_credit_pools

    pool = _make_repo_credit_pool()

    mock_query = mock_session.query.return_value
    mock_joined = mock_query.join.return_value
    mock_joined.count.return_value = 1
    mock_joined.offset.return_value.limit.return_value.all.return_value = [pool]

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await list_repository_credit_pools(
        request=mock_request,
        user_email=None,
        repository_type=None,
        low_balance_only=False,
        limit=100,
        offset=0,
      )

    assert len(result) == 1
    item = result[0]
    assert item.user_repository_id == "ur_abc123"
    assert item.user_id == "usr_456"
    assert item.repository_type == "sec"
    assert item.repository_plan == "starter"
    assert item.current_balance == 500.0
    assert item.consumed_this_month == 200.0
    assert item.allows_rollover is False
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_filter_by_repository_type(self, mock_request, mock_session):
    """Filtering by repository_type adds a filter clause."""
    from robosystems.routers.admin.credits import list_repository_credit_pools

    pool = _make_repo_credit_pool(repository_type="sec")

    mock_query = mock_session.query.return_value
    mock_joined = mock_query.join.return_value
    mock_filtered = mock_joined.filter.return_value
    mock_filtered.count.return_value = 1
    mock_filtered.offset.return_value.limit.return_value.all.return_value = [pool]

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await list_repository_credit_pools(
        request=mock_request,
        user_email=None,
        repository_type="sec",
        low_balance_only=False,
        limit=100,
        offset=0,
      )

    assert len(result) == 1
    mock_joined.filter.assert_called_once()


# ===========================================================================
# TestGetRepositoryCreditPool
# ===========================================================================


class TestGetRepositoryCreditPool:
  """Tests for GET /admin/v1/credits/repositories/{user_repository_id}."""

  @pytest.mark.unit
  async def test_found(self, mock_request, mock_session):
    """When pool exists, return a RepositoryCreditPoolResponse."""
    from robosystems.routers.admin.credits import get_repository_credit_pool

    pool = _make_repo_credit_pool()

    mock_query = mock_session.query.return_value
    mock_query.filter.return_value.first.return_value = pool

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await get_repository_credit_pool(
        request=mock_request, user_repository_id="ur_abc123"
      )

    assert result.user_repository_id == "ur_abc123"
    assert result.repository_type == "sec"
    assert result.is_active is True
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_not_found_raises_404(self, mock_request, mock_session):
    """When pool does not exist, raise HTTPException 404."""
    from fastapi import HTTPException

    from robosystems.routers.admin.credits import get_repository_credit_pool

    mock_query = mock_session.query.return_value
    mock_query.filter.return_value.first.return_value = None

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      with pytest.raises(HTTPException) as exc_info:
        await get_repository_credit_pool(
          request=mock_request, user_repository_id="ur_nonexistent"
        )

    assert exc_info.value.status_code == 404
    assert "not found" in exc_info.value.detail.lower()


# ===========================================================================
# TestAddBonusCreditsToRepository
# ===========================================================================


class TestAddBonusCreditsToRepository:
  """Tests for POST /admin/v1/credits/repositories/{user_repository_id}/bonus."""

  @pytest.mark.unit
  async def test_happy_path(self, mock_request, mock_session):
    """Successfully add bonus credits to an existing repository pool."""
    from robosystems.routers.admin.credits import add_bonus_credits_to_repository

    pool = _make_repo_credit_pool(current_balance=Decimal("700.0"))

    bonus_request = MagicMock()
    bonus_request.amount = 200.0
    bonus_request.description = "Repo bonus"
    bonus_request.metadata = None

    mock_query = mock_session.query.return_value
    mock_query.filter.return_value.first.return_value = pool

    # refresh() reloads the server-computed balance from the DB — simulate that,
    # capturing what was assigned before the reload.
    captured = {}

    def fake_refresh(obj):
      captured["pre_reload"] = obj.current_balance
      obj.current_balance = Decimal("900.0")

    mock_session.refresh.side_effect = fake_refresh

    with (
      patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])),
      patch(f"{MODULE}.UserRepositoryCreditTransaction") as MockTransaction,
    ):
      result = await add_bonus_credits_to_repository(
        request=mock_request,
        user_repository_id="ur_abc123",
        data=bonus_request,
      )

    # The fix: balance is incremented server-side (SET current_balance =
    # current_balance + :amount), not by a Python read-modify-write that could
    # revert a concurrent debit — so what was assigned before the reload is a
    # SQLAlchemy expression, not a pre-computed Decimal. The arithmetic itself is
    # exercised by the model tests against a real DB.
    assert not isinstance(captured["pre_reload"], Decimal)
    assert "current_balance" in str(captured["pre_reload"])
    assert result.current_balance == 900.0
    MockTransaction.create_transaction.assert_called_once()
    call_kwargs = MockTransaction.create_transaction.call_args.kwargs
    assert call_kwargs["credit_pool_id"] == "crd_pool_001"
    assert call_kwargs["amount"] == Decimal("200.0")
    assert call_kwargs["description"] == "Repo bonus"
    assert call_kwargs["metadata"]["source"] == "admin_api"
    mock_session.refresh.assert_called_once_with(pool)
    assert result.user_repository_id == "ur_abc123"
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_not_found_raises_404(self, mock_request, mock_session):
    """When pool does not exist, raise HTTPException 404."""
    from fastapi import HTTPException

    from robosystems.routers.admin.credits import add_bonus_credits_to_repository

    bonus_request = MagicMock()
    bonus_request.amount = 100.0
    bonus_request.description = "Bonus"
    bonus_request.metadata = None

    mock_query = mock_session.query.return_value
    mock_query.filter.return_value.first.return_value = None

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      with pytest.raises(HTTPException) as exc_info:
        await add_bonus_credits_to_repository(
          request=mock_request,
          user_repository_id="ur_nonexistent",
          data=bonus_request,
        )

    assert exc_info.value.status_code == 404


# ===========================================================================
# TestGetCreditAnalytics
# ===========================================================================


class TestGetCreditAnalytics:
  """Tests for GET /admin/v1/credits/analytics."""

  @pytest.mark.unit
  async def test_returns_analytics(self, mock_request, mock_session):
    """Returns combined analytics for both graph and repository pools."""
    from robosystems.routers.admin.credits import get_credit_analytics

    graph_pool = _make_graph_credit_pool(
      current_balance=Decimal("600.0"),
      monthly_allocation=Decimal("1000.0"),
    )

    # The analytics function makes these session.query() calls:
    # 1. session.query(GraphCredits).all() -> graph pools
    # 2. session.query(func.sum(...)).filter(...).scalar() -> consumed
    # 3. session.query(...).filter().group_by().order_by().limit(10).all()
    # 4. session.query(UserRepositoryCredits).all() -> repo pools
    # 5. session.query(func.sum(...)).filter(...).scalar() -> consumed
    #
    # All return the same query_mock because session.query.return_value
    # is one object. We use side_effect on .all() to vary responses.
    # The .limit() chain returns a separate mock so its .all() is independent.
    query_mock = MagicMock()
    mock_session.query.return_value = query_mock

    # Direct .all() calls: graph_pools (call 1), repo_pools (call 4)
    query_mock.all.side_effect = [
      [graph_pool],  # graph_query.all()
      [],  # repo_pools = session.query(UserRepositoryCredits).all()
    ]

    # .filter() chains back to self for .scalar() and for group_by chain
    query_mock.filter.return_value = query_mock
    query_mock.scalar.return_value = Decimal("-150.0")

    # .group_by().order_by().limit(10).all() -- use a separate terminal mock
    # so that .limit().all() does not consume from query_mock.all.side_effect
    limit_result_mock = MagicMock()
    limit_result_mock.all.return_value = []  # no top consumers

    query_mock.group_by.return_value = query_mock
    query_mock.order_by.return_value = query_mock
    query_mock.limit.return_value = limit_result_mock

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await get_credit_analytics(request=mock_request, tier=None)

    assert result.total_pools == 1
    assert result.graph_credits["total_pools"] == 1
    assert result.graph_credits["total_allocated_monthly"] == 1000.0
    assert result.graph_credits["total_current_balance"] == 600.0
    assert isinstance(result.total_consumed_month, float)
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_empty_system(self, mock_request, mock_session):
    """Analytics on empty system returns zeros."""
    from robosystems.routers.admin.credits import get_credit_analytics

    query_mock = MagicMock()
    mock_session.query.return_value = query_mock
    query_mock.all.return_value = []
    query_mock.filter.return_value = query_mock
    query_mock.scalar.return_value = None
    query_mock.group_by.return_value = query_mock
    query_mock.order_by.return_value = query_mock
    query_mock.limit.return_value = query_mock
    query_mock.limit.return_value.all.return_value = []

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await get_credit_analytics(request=mock_request, tier=None)

    assert result.total_pools == 0
    assert result.total_allocated_monthly == 0.0
    assert result.total_current_balance == 0.0
    assert result.total_consumed_month == 0.0
    mock_session.close.assert_called_once()


# ===========================================================================
# TestCheckCreditHealth
# ===========================================================================


class TestCheckCreditHealth:
  """Tests for GET /admin/v1/credits/health."""

  @pytest.mark.unit
  async def test_healthy_system(self, mock_request, mock_session):
    """No issues returns status='healthy'."""
    from robosystems.routers.admin.credits import check_credit_health

    pool = _make_graph_credit_pool(
      current_balance=Decimal("800.0"),
      monthly_allocation=Decimal("1000.0"),
    )

    query_mock = MagicMock()
    mock_session.query.return_value = query_mock
    # First .all() returns graph pools, second returns repo pools
    query_mock.all.side_effect = [[pool], []]
    query_mock.outerjoin.return_value.filter.return_value.all.return_value = []

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await check_credit_health(request=mock_request)

    assert result.status == "healthy"
    assert result.total_pools == 1
    assert result.pools_with_issues == 0
    assert result.graph_health["total_pools"] == 1
    assert result.graph_health["negative_balance_pools"] == []
    assert result.graph_health["low_balance_pools"] == []
    mock_session.close.assert_called_once()

  @pytest.mark.unit
  async def test_missing_pool_triggers_critical(self, mock_request, mock_session):
    """An active graph with no credit pool is invisible to per-pool scans yet
    has every AI run denied — it must surface as a critical missing_pools row."""
    from robosystems.routers.admin.credits import check_credit_health

    orphan = MagicMock()
    orphan.graph_id = "kg_orphan"
    orphan.org_id = "org_1"
    orphan.graph_tier = "ladybug-standard"

    query_mock = MagicMock()
    mock_session.query.return_value = query_mock
    query_mock.all.side_effect = [[], []]  # no pools of either kind
    query_mock.outerjoin.return_value.filter.return_value.all.return_value = [orphan]

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await check_credit_health(request=mock_request)

    assert result.status == "critical"
    assert result.graph_health["missing_pools"] == [
      {"graph_id": "kg_orphan", "org_id": "org_1", "tier": "ladybug-standard"}
    ]
    assert result.pools_with_issues >= 1

  @pytest.mark.unit
  async def test_critical_with_negative_balance(self, mock_request, mock_session):
    """A pool with negative balance triggers status='critical'."""
    from robosystems.routers.admin.credits import check_credit_health

    negative_pool = _make_graph_credit_pool(
      graph_id="kg_negative",
      current_balance=Decimal("-50.0"),
      monthly_allocation=Decimal("1000.0"),
    )

    query_mock = MagicMock()
    mock_session.query.return_value = query_mock
    query_mock.all.side_effect = [[negative_pool], []]
    query_mock.outerjoin.return_value.filter.return_value.all.return_value = []

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await check_credit_health(request=mock_request)

    assert result.status == "critical"
    assert result.pools_with_issues >= 1
    assert len(result.graph_health["negative_balance_pools"]) == 1
    neg_entry = result.graph_health["negative_balance_pools"][0]
    assert neg_entry["graph_id"] == "kg_negative"
    assert neg_entry["balance"] == -50.0

  @pytest.mark.unit
  async def test_low_balance_detected(self, mock_request, mock_session):
    """A pool with balance < 10% of allocation appears in low_balance_pools."""
    from robosystems.routers.admin.credits import check_credit_health

    low_pool = _make_graph_credit_pool(
      graph_id="kg_low",
      current_balance=Decimal("5.0"),
      monthly_allocation=Decimal("1000.0"),
    )

    query_mock = MagicMock()
    mock_session.query.return_value = query_mock
    query_mock.all.side_effect = [[low_pool], []]
    query_mock.outerjoin.return_value.filter.return_value.all.return_value = []

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await check_credit_health(request=mock_request)

    assert len(result.graph_health["low_balance_pools"]) == 1
    assert result.graph_health["low_balance_pools"][0]["graph_id"] == "kg_low"

  @pytest.mark.unit
  async def test_zero_allocation_detected(self, mock_request, mock_session):
    """A pool with zero allocation appears in allocation_issues."""
    from robosystems.routers.admin.credits import check_credit_health

    zero_pool = _make_graph_credit_pool(
      graph_id="kg_zero",
      current_balance=Decimal("0.0"),
      monthly_allocation=Decimal("0.0"),
    )

    query_mock = MagicMock()
    mock_session.query.return_value = query_mock
    query_mock.all.side_effect = [[zero_pool], []]
    query_mock.outerjoin.return_value.filter.return_value.all.return_value = []

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await check_credit_health(request=mock_request)

    assert len(result.graph_health["allocation_issues"]) == 1
    issue = result.graph_health["allocation_issues"][0]
    assert issue["graph_id"] == "kg_zero"
    assert issue["issue"] == "Zero or negative allocation"

  @pytest.mark.unit
  async def test_repo_negative_balance_triggers_critical(
    self, mock_request, mock_session
  ):
    """A repository pool with negative balance also triggers status='critical'."""
    from robosystems.routers.admin.credits import check_credit_health

    repo_pool = _make_repo_credit_pool(
      user_repository_id="ur_negative",
      current_balance=Decimal("-10.0"),
    )

    query_mock = MagicMock()
    mock_session.query.return_value = query_mock
    query_mock.all.side_effect = [[], [repo_pool]]
    query_mock.outerjoin.return_value.filter.return_value.all.return_value = []

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await check_credit_health(request=mock_request)

    assert result.status == "critical"
    assert len(result.repository_health["negative_balance_pools"]) == 1

  @pytest.mark.unit
  async def test_inactive_repo_pool_detected(self, mock_request, mock_session):
    """An inactive repository pool appears in inactive_pools."""
    from robosystems.routers.admin.credits import check_credit_health

    inactive_pool = _make_repo_credit_pool(
      user_repository_id="ur_inactive",
      is_active=False,
      current_balance=Decimal("500.0"),
      monthly_allocation=Decimal("1000.0"),
    )

    query_mock = MagicMock()
    mock_session.query.return_value = query_mock
    query_mock.all.side_effect = [[], [inactive_pool]]
    query_mock.outerjoin.return_value.filter.return_value.all.return_value = []

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await check_credit_health(request=mock_request)

    assert len(result.repository_health["inactive_pools"]) == 1
    assert (
      result.repository_health["inactive_pools"][0]["user_repository_id"]
      == "ur_inactive"
    )

  @pytest.mark.unit
  async def test_last_checked_is_recent(self, mock_request, mock_session):
    """The last_checked timestamp should be approximately now."""
    from robosystems.routers.admin.credits import check_credit_health

    query_mock = MagicMock()
    mock_session.query.return_value = query_mock
    query_mock.all.side_effect = [[], []]
    query_mock.outerjoin.return_value.filter.return_value.all.return_value = []

    before = datetime.now(UTC)

    with patch(f"{MODULE}.get_db_session", return_value=iter([mock_session])):
      result = await check_credit_health(request=mock_request)

    after = datetime.now(UTC)
    assert before <= result.last_checked <= after
