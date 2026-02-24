from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.iam.user_repository import RepositoryType
from robosystems.operations.graph.repository_subscription_service import (
  RepositorySubscriptionService,
  get_available_plans_for_repository,
  get_available_repositories,
)

MODULE = "robosystems.operations.graph.repository_subscription_service"
MOCK_USER_ID = "usr_test123"


@pytest.fixture
def mock_session():
  return MagicMock()


@pytest.fixture
def service(mock_session):
  return RepositorySubscriptionService(mock_session)


class TestGetAvailableRepositories:
  @pytest.mark.unit
  def test_returns_repository_types(self):
    mock_manifest1 = MagicMock()
    mock_manifest1.id = "sec"

    with patch(f"{MODULE}._get_available_manifests", return_value=[mock_manifest1]):
      result = get_available_repositories()

    assert len(result) == 1
    assert RepositoryType("sec") in result

  @pytest.mark.unit
  def test_empty_when_no_manifests(self):
    with patch(f"{MODULE}._get_available_manifests", return_value=[]):
      result = get_available_repositories()
    assert result == []


class TestGetAvailablePlansForRepository:
  @pytest.mark.unit
  def test_returns_plan_names(self):
    mock_manifest = MagicMock()
    mock_manifest.plans = {"starter": {}, "advanced": {}}

    with (
      patch(f"{MODULE}._is_repository_enabled", return_value=True),
      patch(f"{MODULE}._get_manifest", return_value=mock_manifest),
    ):
      result = get_available_plans_for_repository(RepositoryType("sec"))

    assert "starter" in result
    assert "advanced" in result

  @pytest.mark.unit
  def test_disabled_returns_empty(self):
    with patch(f"{MODULE}._is_repository_enabled", return_value=False):
      result = get_available_plans_for_repository(RepositoryType("sec"))
    assert result == []

  @pytest.mark.unit
  def test_no_manifest_returns_empty(self):
    with (
      patch(f"{MODULE}._is_repository_enabled", return_value=True),
      patch(f"{MODULE}._get_manifest", return_value=None),
    ):
      result = get_available_plans_for_repository(RepositoryType("sec"))
    assert result == []


class TestEnsureRepositoryGraphExists:
  @pytest.mark.unit
  def test_graph_already_exists(self, service, mock_session):
    mock_session.query.return_value.filter.return_value.first.return_value = MagicMock()

    with patch("robosystems.models.iam.graph.Graph"):
      service.ensure_repository_graph_exists(RepositoryType("sec"))

    mock_session.add.assert_not_called()

  @pytest.mark.unit
  def test_creates_graph_when_missing(self, service, mock_session):
    mock_session.query.return_value.filter.return_value.first.return_value = None

    with (
      patch("robosystems.models.iam.graph.Graph"),
      patch(
        f"{MODULE}._get_repository_metadata",
        return_value={
          "name": "SEC Financial Data",
          "graph_tier": "ladybug-shared",
          "graph_instance_id": "shared-sec",
          "data_source_type": "sec",
          "data_source_url": "https://efts.sec.gov/LATEST/",
          "sync_frequency": "daily",
        },
      ),
    ):
      service.ensure_repository_graph_exists(RepositoryType("sec"))

    mock_session.add.assert_called_once()
    mock_session.commit.assert_called_once()

  @pytest.mark.unit
  def test_invalid_config_raises(self, service, mock_session):
    mock_session.query.return_value.filter.return_value.first.return_value = None

    with (
      patch("robosystems.models.iam.graph.Graph"),
      patch(f"{MODULE}._get_repository_metadata", return_value=None),
    ):
      with pytest.raises(ValueError, match="No configuration found"):
        service.ensure_repository_graph_exists(RepositoryType("sec"))


class TestCreateRepositorySubscription:
  @pytest.mark.unit
  def test_happy_path(self, service):
    mock_access = MagicMock()

    with (
      patch(f"{MODULE}._is_repository_enabled", return_value=True),
      patch(
        f"{MODULE}.get_available_plans_for_repository",
        return_value=["starter", "advanced"],
      ),
      patch(
        f"{MODULE}._get_plan_details",
        return_value={
          "price_monthly": 9.99,
          "monthly_credits": 1000,
          "access_level": "READ",
          "features": ["basic_access"],
        },
      ),
      patch(f"{MODULE}.UserRepository") as MockUR,
    ):
      MockUR.get_by_user_and_repository.return_value = None
      MockUR.create_access.return_value = mock_access

      result = service.create_repository_subscription(
        MOCK_USER_ID, RepositoryType("sec"), "starter"
      )

    assert result == mock_access
    MockUR.create_access.assert_called_once()

  @pytest.mark.unit
  def test_disabled_repo_raises(self, service):
    with patch(f"{MODULE}._is_repository_enabled", return_value=False):
      with pytest.raises(ValueError, match="not available for subscription"):
        service.create_repository_subscription(
          MOCK_USER_ID, RepositoryType("sec"), "starter"
        )

  @pytest.mark.unit
  def test_invalid_plan_raises(self, service):
    with (
      patch(f"{MODULE}._is_repository_enabled", return_value=True),
      patch(
        f"{MODULE}.get_available_plans_for_repository",
        return_value=["starter"],
      ),
    ):
      with pytest.raises(ValueError, match=r"Plan .* not available"):
        service.create_repository_subscription(
          MOCK_USER_ID, RepositoryType("sec"), "enterprise"
        )

  @pytest.mark.unit
  def test_duplicate_returns_existing(self, service):
    mock_existing = MagicMock()

    with (
      patch(f"{MODULE}._is_repository_enabled", return_value=True),
      patch(
        f"{MODULE}.get_available_plans_for_repository",
        return_value=["starter"],
      ),
      patch(
        f"{MODULE}._get_plan_details",
        return_value={"price_monthly": 9.99, "monthly_credits": 1000},
      ),
      patch(f"{MODULE}.UserRepository") as MockUR,
    ):
      MockUR.get_by_user_and_repository.return_value = mock_existing

      result = service.create_repository_subscription(
        MOCK_USER_ID, RepositoryType("sec"), "starter"
      )

    assert result == mock_existing


class TestUpgradeRepositorySubscription:
  @pytest.mark.unit
  def test_happy_path(self, service):
    mock_access = MagicMock()

    with (
      patch(f"{MODULE}._is_repository_enabled", return_value=True),
      patch(
        f"{MODULE}.get_available_plans_for_repository",
        return_value=["starter", "advanced"],
      ),
      patch(
        f"{MODULE}._get_plan_details",
        return_value={"price_monthly": 29.99, "monthly_credits": 5000},
      ),
      patch(f"{MODULE}.UserRepository") as MockUR,
    ):
      MockUR.get_by_user_and_repository.return_value = mock_access

      result = service.upgrade_repository_subscription(
        MOCK_USER_ID, RepositoryType("sec"), "advanced"
      )

    assert result == mock_access
    mock_access.upgrade_tier.assert_called_once()

  @pytest.mark.unit
  def test_no_existing_raises(self, service):
    with patch(f"{MODULE}.UserRepository") as MockUR:
      MockUR.get_by_user_and_repository.return_value = None

      with pytest.raises(ValueError, match="No subscription found"):
        service.upgrade_repository_subscription(
          MOCK_USER_ID, RepositoryType("sec"), "advanced"
        )

  @pytest.mark.unit
  def test_disabled_repo_raises(self, service):
    mock_access = MagicMock()

    with (
      patch(f"{MODULE}._is_repository_enabled", return_value=False),
      patch(f"{MODULE}.UserRepository") as MockUR,
    ):
      MockUR.get_by_user_and_repository.return_value = mock_access

      with pytest.raises(ValueError, match="no longer available"):
        service.upgrade_repository_subscription(
          MOCK_USER_ID, RepositoryType("sec"), "advanced"
        )


class TestCancelRepositorySubscription:
  @pytest.mark.unit
  def test_happy_path(self, service):
    mock_access = MagicMock()

    with patch(f"{MODULE}.UserRepository") as MockUR:
      MockUR.get_by_user_and_repository.return_value = mock_access

      result = service.cancel_repository_subscription(
        MOCK_USER_ID, RepositoryType("sec")
      )

    assert result is True
    mock_access.revoke_access.assert_called_once()

  @pytest.mark.unit
  def test_no_existing_raises(self, service):
    with patch(f"{MODULE}.UserRepository") as MockUR:
      MockUR.get_by_user_and_repository.return_value = None

      with pytest.raises(ValueError, match="No subscription found"):
        service.cancel_repository_subscription(MOCK_USER_ID, RepositoryType("sec"))


class TestGetUserRepositorySubscriptions:
  @pytest.mark.unit
  def test_returns_list(self, service):
    mock_records = [MagicMock(), MagicMock()]

    with patch(f"{MODULE}.UserRepository") as MockUR:
      MockUR.get_user_repositories.return_value = mock_records

      result = service.get_user_repository_subscriptions(MOCK_USER_ID)

    assert len(result) == 2


class TestGetRepositoryCreditsSummary:
  @pytest.mark.unit
  def test_specific_repository(self, service):
    mock_credits = MagicMock()
    mock_credits.get_summary.return_value = {
      "current_balance": 800,
      "monthly_allocation": 1000,
    }

    with patch(f"{MODULE}.UserRepositoryCredits") as MockURC:
      MockURC.get_user_repository_credits.return_value = mock_credits

      result = service.get_repository_credits_summary(
        MOCK_USER_ID, RepositoryType("sec")
      )

    assert result["current_balance"] == 800

  @pytest.mark.unit
  def test_specific_repository_no_credits(self, service):
    with patch(f"{MODULE}.UserRepositoryCredits") as MockURC:
      MockURC.get_user_repository_credits.return_value = None

      result = service.get_repository_credits_summary(
        MOCK_USER_ID, RepositoryType("sec")
      )

    assert result == {}

  @pytest.mark.unit
  def test_all_repositories(self, service):
    mock_access = MagicMock()
    mock_access.repository_type = "sec"
    mock_access.repository_plan = "starter"
    mock_access.user_credits = MagicMock()
    mock_access.user_credits.get_summary.return_value = {
      "current_balance": 800,
    }

    with patch.object(
      service, "get_user_repository_subscriptions", return_value=[mock_access]
    ):
      result = service.get_repository_credits_summary(MOCK_USER_ID)

    assert result["total_subscriptions"] == 1
    assert result["total_credits"] == 800
    assert len(result["repositories"]) == 1


class TestAllocateCredits:
  @pytest.mark.unit
  def test_existing_record_with_credits(self, service):
    mock_access = MagicMock()
    mock_access.user_credits = MagicMock()

    with (
      patch(
        f"{MODULE}._get_plan_details",
        return_value={"monthly_credits": 1000},
      ),
      patch(f"{MODULE}.UserRepository") as MockUR,
    ):
      MockUR.get_by_user_and_repository.return_value = mock_access

      result = service.allocate_credits(RepositoryType("sec"), "starter", MOCK_USER_ID)

    assert result == 1000
    mock_access.user_credits.update_monthly_allocation.assert_called_once_with(
      new_allocation=1000, session=service.session
    )

  @pytest.mark.unit
  def test_existing_record_no_credits_creates(self, service):
    mock_access = MagicMock()
    mock_access.user_credits = None
    mock_access.id = 42

    with (
      patch(
        f"{MODULE}._get_plan_details",
        return_value={"monthly_credits": 1000},
      ),
      patch(f"{MODULE}.UserRepository") as MockUR,
      patch(f"{MODULE}.UserRepositoryCredits") as MockURC,
    ):
      MockUR.get_by_user_and_repository.return_value = mock_access

      result = service.allocate_credits(RepositoryType("sec"), "starter", MOCK_USER_ID)

    assert result == 1000
    MockURC.create_for_access.assert_called_once()

  @pytest.mark.unit
  def test_no_access_record_returns_credits(self, service):
    with (
      patch(
        f"{MODULE}._get_plan_details",
        return_value={"monthly_credits": 1000},
      ),
      patch(f"{MODULE}.UserRepository") as MockUR,
    ):
      MockUR.get_by_user_and_repository.return_value = None

      result = service.allocate_credits(RepositoryType("sec"), "starter", MOCK_USER_ID)

    assert result == 1000

  @pytest.mark.unit
  def test_invalid_plan_raises(self, service):
    with patch(f"{MODULE}._get_plan_details", return_value=None):
      with pytest.raises(ValueError, match=r"Plan .* not available"):
        service.allocate_credits(RepositoryType("sec"), "nonexistent", MOCK_USER_ID)


class TestGrantAccess:
  @pytest.mark.unit
  def test_new_access(self, service):
    with (
      patch.object(service, "ensure_repository_graph_exists"),
      patch(f"{MODULE}.UserRepository") as MockUR,
      patch(
        f"{MODULE}._get_plan_details",
        return_value={
          "access_level": "READ",
          "price_cents": 999,
          "monthly_credits": 1000,
        },
      ),
    ):
      MockUR.get_by_user_and_repository.return_value = None

      result = service.grant_access(RepositoryType("sec"), MOCK_USER_ID)

    assert result is True
    MockUR.create_access.assert_called_once()

  @pytest.mark.unit
  def test_existing_active_access(self, service, mock_session):
    mock_existing = MagicMock()
    mock_existing.is_active = True

    with (
      patch.object(service, "ensure_repository_graph_exists"),
      patch(f"{MODULE}.UserRepository") as MockUR,
    ):
      MockUR.get_by_user_and_repository.return_value = mock_existing

      result = service.grant_access(RepositoryType("sec"), MOCK_USER_ID)

    assert result is True
    mock_session.commit.assert_not_called()

  @pytest.mark.unit
  def test_existing_inactive_reactivates(self, service, mock_session):
    mock_existing = MagicMock()
    mock_existing.is_active = False

    with (
      patch.object(service, "ensure_repository_graph_exists"),
      patch(f"{MODULE}.UserRepository") as MockUR,
    ):
      MockUR.get_by_user_and_repository.return_value = mock_existing

      result = service.grant_access(RepositoryType("sec"), MOCK_USER_ID)

    assert result is True
    assert mock_existing.is_active is True
    mock_session.commit.assert_called_once()

  @pytest.mark.unit
  def test_invalid_plan_raises(self, service):
    with (
      patch.object(service, "ensure_repository_graph_exists"),
      patch(f"{MODULE}.UserRepository") as MockUR,
      patch(f"{MODULE}._get_plan_details", return_value=None),
    ):
      MockUR.get_by_user_and_repository.return_value = None

      with pytest.raises(ValueError, match=r"Plan .* not available"):
        service.grant_access(RepositoryType("sec"), MOCK_USER_ID)


class TestRevokeAccess:
  @pytest.mark.unit
  def test_happy_path(self, service):
    mock_access = MagicMock()

    with patch(f"{MODULE}.UserRepository") as MockUR:
      MockUR.get_by_user_and_repository.return_value = mock_access

      result = service.revoke_access(RepositoryType("sec"), MOCK_USER_ID)

    assert result is True
    mock_access.revoke_access.assert_called_once()

  @pytest.mark.unit
  def test_no_access_record_returns_false(self, service):
    with patch(f"{MODULE}.UserRepository") as MockUR:
      MockUR.get_by_user_and_repository.return_value = None

      result = service.revoke_access(RepositoryType("sec"), MOCK_USER_ID)

    assert result is False
