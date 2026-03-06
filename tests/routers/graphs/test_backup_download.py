"""
Tests for backup download endpoint with rate limiting.

This test suite validates the download URL generation endpoint,
especially rate limiting for shared repository downloads.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from main import app


class TestBackupDownloadEndpoint:
  """Test suite for backup download REST API endpoints."""

  @pytest.fixture
  def client(self):
    """Create test client with rate limiting disabled."""
    from robosystems.middleware.rate_limits import (
      analytics_rate_limit_dependency,
      auth_rate_limit_dependency,
      auth_status_rate_limit_dependency,
      backup_operations_rate_limit_dependency,
      connection_management_rate_limit_dependency,
      general_api_rate_limit_dependency,
      graph_scoped_rate_limit_dependency,
      sensitive_auth_rate_limit_dependency,
      sso_rate_limit_dependency,
      subscription_aware_rate_limit_dependency,
      sync_operations_rate_limit_dependency,
      tasks_management_rate_limit_dependency,
      user_management_rate_limit_dependency,
    )

    # Save original overrides
    original_overrides = app.dependency_overrides.copy()

    # Disable rate limiting during tests
    app.dependency_overrides[auth_rate_limit_dependency] = lambda: None
    app.dependency_overrides[user_management_rate_limit_dependency] = lambda: None
    app.dependency_overrides[sync_operations_rate_limit_dependency] = lambda: None
    app.dependency_overrides[connection_management_rate_limit_dependency] = lambda: None
    app.dependency_overrides[analytics_rate_limit_dependency] = lambda: None
    app.dependency_overrides[backup_operations_rate_limit_dependency] = lambda: None
    app.dependency_overrides[sensitive_auth_rate_limit_dependency] = lambda: None
    app.dependency_overrides[tasks_management_rate_limit_dependency] = lambda: None
    app.dependency_overrides[general_api_rate_limit_dependency] = lambda: None
    app.dependency_overrides[subscription_aware_rate_limit_dependency] = lambda: None
    app.dependency_overrides[auth_status_rate_limit_dependency] = lambda: None
    app.dependency_overrides[sso_rate_limit_dependency] = lambda: None
    app.dependency_overrides[graph_scoped_rate_limit_dependency] = lambda: None

    client = TestClient(app)
    yield client

    # Restore original overrides
    app.dependency_overrides = original_overrides

  @pytest.fixture
  def mock_auth_user(self):
    """Mock authenticated user."""
    user = MagicMock()
    user.id = "test-user-123"
    user.email = "test@example.com"
    return user

  def test_download_endpoint_requires_authentication(self, client):
    """Test that download endpoint requires authentication."""
    response = client.get("/v1/graphs/sec/backups/backup123/download")
    assert response.status_code in [401, 403, 422]

  @patch("robosystems.routers.graphs.backups.download.get_backup_manager")
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.increment_download_count",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.check_download_limit",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.routers.graphs.backups.download.UserRepository.get_by_user_and_repository"
  )
  @patch(
    "robosystems.routers.graphs.backups.download.MultiTenantUtils.is_shared_repository"
  )
  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_download_requires_subscription_for_shared_repo(
    self,
    mock_get_by_user_id,
    mock_is_shared,
    mock_get_user_repo,
    mock_check_limit,
    mock_increment,
    mock_get_backup_manager,
    client,
    mock_auth_user,
  ):
    """Test that download requires subscription for shared repositories."""
    from robosystems.database import get_db_session
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph

    mock_session = MagicMock()
    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_auth_user
    app.dependency_overrides[get_db_session] = lambda: mock_session

    try:
      # This IS a shared repository
      mock_is_shared.return_value = True

      # User has graph access
      mock_user_graph = MagicMock()
      mock_user_graph.graph_id = "sec"
      mock_user_graph.role = "viewer"
      mock_get_by_user_id.return_value = [mock_user_graph]

      # But NO repository subscription
      mock_get_user_repo.return_value = None

      mock_auth_user.id = "test-user-123"

      response = client.get("/v1/graphs/sec/backups/backup123/download")

      assert response.status_code == 403
      assert "subscription required" in response.json()["detail"].lower()
    finally:
      if get_current_user_with_graph in app.dependency_overrides:
        del app.dependency_overrides[get_current_user_with_graph]
      if get_db_session in app.dependency_overrides:
        del app.dependency_overrides[get_db_session]

  @patch("robosystems.routers.graphs.backups.download.get_backup_manager")
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.increment_download_count",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.check_download_limit",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.get_shared_repo_monthly_limit"
  )
  @patch(
    "robosystems.routers.graphs.backups.download.UserRepository.get_by_user_and_repository"
  )
  @patch(
    "robosystems.routers.graphs.backups.download.MultiTenantUtils.is_shared_repository"
  )
  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_download_returns_403_for_starter_plan(
    self,
    mock_get_by_user_id,
    mock_is_shared,
    mock_get_user_repo,
    mock_get_monthly_limit,
    mock_check_limit,
    mock_increment,
    mock_get_backup_manager,
    client,
    mock_auth_user,
  ):
    """Test that starter plan users get 403 (downloads not available)."""
    from robosystems.database import get_db_session
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph

    mock_session = MagicMock()
    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_auth_user
    app.dependency_overrides[get_db_session] = lambda: mock_session

    try:
      mock_is_shared.return_value = True

      mock_user_graph = MagicMock()
      mock_user_graph.graph_id = "sec"
      mock_user_graph.role = "viewer"
      mock_get_by_user_id.return_value = [mock_user_graph]

      mock_user_repo = MagicMock()
      mock_user_repo.repository_plan = "starter"
      mock_get_user_repo.return_value = mock_user_repo

      # Starter plan has 0 downloads
      mock_get_monthly_limit.return_value = 0

      mock_auth_user.id = "test-user-123"

      response = client.get("/v1/graphs/sec/backups/backup123/download")

      assert response.status_code == 403, (
        f"Expected 403, got {response.status_code}: {response.text}"
      )
      assert "not available" in response.json()["detail"].lower()

      # Should NOT have checked rate limit or incremented
      mock_check_limit.assert_not_called()
      mock_increment.assert_not_called()
    finally:
      if get_current_user_with_graph in app.dependency_overrides:
        del app.dependency_overrides[get_current_user_with_graph]
      if get_db_session in app.dependency_overrides:
        del app.dependency_overrides[get_db_session]

  @patch("robosystems.routers.graphs.backups.download.get_backup_manager")
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.increment_download_count",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.check_download_limit",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.get_shared_repo_monthly_limit"
  )
  @patch(
    "robosystems.routers.graphs.backups.download.UserRepository.get_by_user_and_repository"
  )
  @patch(
    "robosystems.routers.graphs.backups.download.MultiTenantUtils.is_shared_repository"
  )
  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_download_rate_limit_exceeded_returns_429(
    self,
    mock_get_by_user_id,
    mock_is_shared,
    mock_get_user_repo,
    mock_get_monthly_limit,
    mock_check_limit,
    mock_increment,
    mock_get_backup_manager,
    client,
    mock_auth_user,
  ):
    """Test that exceeding download rate limit returns 429."""
    from robosystems.database import get_db_session
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph

    mock_session = MagicMock()
    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_auth_user
    app.dependency_overrides[get_db_session] = lambda: mock_session

    try:
      mock_is_shared.return_value = True

      mock_user_graph = MagicMock()
      mock_user_graph.graph_id = "sec"
      mock_user_graph.role = "viewer"
      mock_get_by_user_id.return_value = [mock_user_graph]

      mock_user_repo = MagicMock()
      mock_user_repo.repository_plan = "advanced"
      mock_get_user_repo.return_value = mock_user_repo

      # Advanced plan has downloads available
      mock_get_monthly_limit.return_value = 1

      # Rate limit exceeded
      reset_time = datetime.now(UTC) + timedelta(hours=5)
      mock_check_limit.return_value = (False, 0, reset_time)

      mock_auth_user.id = "test-user-123"

      response = client.get("/v1/graphs/sec/backups/backup123/download")

      assert response.status_code == 429, (
        f"Expected 429, got {response.status_code}: {response.text}"
      )
      assert "limit exceeded" in response.json()["detail"].lower()
      assert "X-RateLimit-Remaining" in response.headers
      assert response.headers["X-RateLimit-Remaining"] == "0"
    finally:
      if get_current_user_with_graph in app.dependency_overrides:
        del app.dependency_overrides[get_current_user_with_graph]
      if get_db_session in app.dependency_overrides:
        del app.dependency_overrides[get_db_session]

  @patch("robosystems.routers.graphs.backups.download.get_backup_manager")
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.increment_download_count",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.check_download_limit",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.get_shared_repo_monthly_limit"
  )
  @patch(
    "robosystems.routers.graphs.backups.download.UserRepository.get_by_user_and_repository"
  )
  @patch(
    "robosystems.routers.graphs.backups.download.MultiTenantUtils.is_shared_repository"
  )
  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_download_allowed_when_under_rate_limit(
    self,
    mock_get_by_user_id,
    mock_is_shared,
    mock_get_user_repo,
    mock_get_monthly_limit,
    mock_check_limit,
    mock_increment,
    mock_get_backup_manager,
    client,
    mock_auth_user,
  ):
    """Test that download is allowed when under rate limit."""
    from robosystems.database import get_db_session
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph

    mock_session = MagicMock()
    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_auth_user
    app.dependency_overrides[get_db_session] = lambda: mock_session

    try:
      mock_is_shared.return_value = True

      mock_user_graph = MagicMock()
      mock_user_graph.graph_id = "sec"
      mock_user_graph.role = "viewer"
      mock_get_by_user_id.return_value = [mock_user_graph]

      mock_user_repo = MagicMock()
      mock_user_repo.repository_plan = "advanced"
      mock_get_user_repo.return_value = mock_user_repo

      # Advanced plan has downloads available
      mock_get_monthly_limit.return_value = 1

      # Rate limit NOT exceeded
      reset_time = datetime.now(UTC) + timedelta(hours=5)
      mock_check_limit.return_value = (True, 4, reset_time)

      # Mock backup manager to return a download URL
      mock_backup_mgr = MagicMock()
      mock_backup_mgr.get_backup_download_url = AsyncMock(
        return_value="https://s3.amazonaws.com/bucket/backup.lbug?signature=xyz"
      )
      mock_get_backup_manager.return_value = mock_backup_mgr

      mock_auth_user.id = "test-user-123"

      response = client.get("/v1/graphs/sec/backups/backup123/download")

      assert response.status_code == 200
      data = response.json()
      assert "download_url" in data
      assert data["backup_id"] == "backup123"
      assert data["graph_id"] == "sec"

      # Verify increment was called
      mock_increment.assert_called_once_with(
        user_id="test-user-123",
        resource_id="sec",
      )
    finally:
      if get_current_user_with_graph in app.dependency_overrides:
        del app.dependency_overrides[get_current_user_with_graph]
      if get_db_session in app.dependency_overrides:
        del app.dependency_overrides[get_db_session]

  @patch("robosystems.routers.graphs.backups.download.get_backup_manager")
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.increment_download_count",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.check_graph_download_limit",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.graphs.backups.download.Graph.get_by_id")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils.is_shared_repository")
  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_download_allowed_for_user_graph_under_limit(
    self,
    mock_get_by_user_id,
    mock_is_shared,
    mock_get_graph,
    mock_check_graph_limit,
    mock_increment,
    mock_get_backup_manager,
    client,
    mock_auth_user,
  ):
    """Test that user graph downloads are allowed when under tier limit."""
    from robosystems.database import get_db_session
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph

    mock_session = MagicMock()
    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_auth_user
    app.dependency_overrides[get_db_session] = lambda: mock_session

    try:
      mock_is_shared.return_value = False

      mock_user_graph = MagicMock()
      mock_user_graph.graph_id = "kg0123456789abcdef"
      mock_user_graph.role = "admin"
      mock_get_by_user_id.return_value = [mock_user_graph]

      # Graph has standard tier
      mock_graph_record = MagicMock()
      mock_graph_record.graph_tier = "ladybug-standard"
      mock_get_graph.return_value = mock_graph_record

      # Under limit
      reset_time = datetime.now(UTC) + timedelta(hours=5)
      mock_check_graph_limit.return_value = (True, 1, reset_time)

      mock_backup_mgr = MagicMock()
      mock_backup_mgr.get_backup_download_url = AsyncMock(
        return_value="https://s3.amazonaws.com/bucket/backup.lbug?signature=xyz"
      )
      mock_get_backup_manager.return_value = mock_backup_mgr

      mock_auth_user.id = "test-user-123"

      response = client.get("/v1/graphs/kg0123456789abcdef/backups/backup123/download")

      assert response.status_code == 200
      data = response.json()
      assert "download_url" in data

      # Verify download count was incremented (standard tier has limit > 0)
      mock_increment.assert_called_once()
    finally:
      if get_current_user_with_graph in app.dependency_overrides:
        del app.dependency_overrides[get_current_user_with_graph]
      if get_db_session in app.dependency_overrides:
        del app.dependency_overrides[get_db_session]

  @patch("robosystems.routers.graphs.backups.download.get_backup_manager")
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.check_graph_download_limit",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.graphs.backups.download.Graph.get_by_id")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils.is_shared_repository")
  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_download_rate_limit_exceeded_for_user_graph(
    self,
    mock_get_by_user_id,
    mock_is_shared,
    mock_get_graph,
    mock_check_graph_limit,
    mock_get_backup_manager,
    client,
    mock_auth_user,
  ):
    """Test that exceeding tier download limit returns 429 for user graphs."""
    from robosystems.database import get_db_session
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph

    mock_session = MagicMock()
    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_auth_user
    app.dependency_overrides[get_db_session] = lambda: mock_session

    try:
      mock_is_shared.return_value = False

      mock_user_graph = MagicMock()
      mock_user_graph.graph_id = "kg0123456789abcdef"
      mock_user_graph.role = "admin"
      mock_get_by_user_id.return_value = [mock_user_graph]

      # Graph has standard tier (2 downloads/month)
      mock_graph_record = MagicMock()
      mock_graph_record.graph_tier = "ladybug-standard"
      mock_get_graph.return_value = mock_graph_record

      # Limit exceeded
      reset_time = datetime.now(UTC) + timedelta(hours=5)
      mock_check_graph_limit.return_value = (False, 0, reset_time)

      mock_auth_user.id = "test-user-123"

      response = client.get("/v1/graphs/kg0123456789abcdef/backups/backup123/download")

      assert response.status_code == 429
      assert "limit" in response.json()["detail"].lower()
      assert "X-RateLimit-Remaining" in response.headers
    finally:
      if get_current_user_with_graph in app.dependency_overrides:
        del app.dependency_overrides[get_current_user_with_graph]
      if get_db_session in app.dependency_overrides:
        del app.dependency_overrides[get_db_session]

  @patch("robosystems.routers.graphs.backups.download.get_backup_manager")
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.increment_download_count",
    new_callable=AsyncMock,
  )
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.check_graph_download_limit",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.graphs.backups.download.Graph.get_by_id")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils.is_shared_repository")
  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_download_allowed_for_xlarge_tier(
    self,
    mock_get_by_user_id,
    mock_is_shared,
    mock_get_graph,
    mock_check_graph_limit,
    mock_increment,
    mock_get_backup_manager,
    client,
    mock_auth_user,
  ):
    """Test that xlarge tier downloads work and count is tracked."""
    from robosystems.database import get_db_session
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph

    mock_session = MagicMock()
    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_auth_user
    app.dependency_overrides[get_db_session] = lambda: mock_session

    try:
      mock_is_shared.return_value = False

      mock_user_graph = MagicMock()
      mock_user_graph.graph_id = "kg0123456789abcdef"
      mock_user_graph.role = "admin"
      mock_get_by_user_id.return_value = [mock_user_graph]

      # Graph has xlarge tier (10 downloads/month)
      mock_graph_record = MagicMock()
      mock_graph_record.graph_tier = "ladybug-xlarge"
      mock_get_graph.return_value = mock_graph_record

      # Under limit
      reset_time = datetime.now(UTC) + timedelta(hours=5)
      mock_check_graph_limit.return_value = (True, 9, reset_time)

      mock_backup_mgr = MagicMock()
      mock_backup_mgr.get_backup_download_url = AsyncMock(
        return_value="https://s3.amazonaws.com/bucket/backup.lbug?signature=xyz"
      )
      mock_get_backup_manager.return_value = mock_backup_mgr

      mock_auth_user.id = "test-user-123"

      response = client.get("/v1/graphs/kg0123456789abcdef/backups/backup123/download")

      assert response.status_code == 200

      # All tiers now track download count
      mock_increment.assert_called_once()
    finally:
      if get_current_user_with_graph in app.dependency_overrides:
        del app.dependency_overrides[get_current_user_with_graph]
      if get_db_session in app.dependency_overrides:
        del app.dependency_overrides[get_db_session]

  @patch("robosystems.routers.graphs.backups.download.get_backup_manager")
  @patch(
    "robosystems.routers.graphs.backups.download.DownloadRateLimiter.check_graph_download_limit",
    new_callable=AsyncMock,
  )
  @patch("robosystems.routers.graphs.backups.download.Graph.get_by_id")
  @patch("robosystems.middleware.graph.utils.MultiTenantUtils.is_shared_repository")
  @patch("robosystems.models.iam.GraphUser.get_by_user_id")
  def test_download_returns_404_when_backup_not_found(
    self,
    mock_get_by_user_id,
    mock_is_shared,
    mock_get_graph,
    mock_check_graph_limit,
    mock_get_backup_manager,
    client,
    mock_auth_user,
  ):
    """Test that 404 is returned when backup doesn't exist."""
    from robosystems.database import get_db_session
    from robosystems.middleware.auth.dependencies import get_current_user_with_graph

    mock_session = MagicMock()
    app.dependency_overrides[get_current_user_with_graph] = lambda: mock_auth_user
    app.dependency_overrides[get_db_session] = lambda: mock_session

    try:
      mock_is_shared.return_value = False

      mock_user_graph = MagicMock()
      mock_user_graph.graph_id = "kg0123456789abcdef"
      mock_user_graph.role = "admin"
      mock_get_by_user_id.return_value = [mock_user_graph]

      # Graph with xlarge tier (10 downloads/month)
      mock_graph_record = MagicMock()
      mock_graph_record.graph_tier = "ladybug-xlarge"
      mock_get_graph.return_value = mock_graph_record

      reset_time = datetime.now(UTC) + timedelta(hours=5)
      mock_check_graph_limit.return_value = (True, 9, reset_time)

      # Backup not found
      mock_backup_mgr = MagicMock()
      mock_backup_mgr.get_backup_download_url = AsyncMock(return_value=None)
      mock_get_backup_manager.return_value = mock_backup_mgr

      mock_auth_user.id = "test-user-123"

      response = client.get(
        "/v1/graphs/kg0123456789abcdef/backups/nonexistent/download"
      )

      assert response.status_code == 404
      assert "not found" in response.json()["detail"].lower()
    finally:
      if get_current_user_with_graph in app.dependency_overrides:
        del app.dependency_overrides[get_current_user_with_graph]
      if get_db_session in app.dependency_overrides:
        del app.dependency_overrides[get_db_session]
