"""Test UserRepository model functionality."""

import pytest
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.iam import UserRepository, User
from robosystems.models.iam.user_repository import (
  RepositoryType,
  RepositoryAccessLevel,
  RepositoryPlan,
  safe_str,
  safe_bool,
)


class TestUserRepository:
  """Test cases for UserRepository model."""

  @pytest.fixture(autouse=True)
  def setup(self, db_session):
    """Set up test fixtures."""
    self.session = db_session

    # Create test users with unique emails
    import uuid

    unique_id = str(uuid.uuid4())[:8]
    self.user = User(
      email=f"user_repo_user_{unique_id}@example.com",
      name="Test User",
      password_hash="hashed_password",
    )
    self.admin = User(
      email=f"user_repo_admin_{unique_id}@example.com",
      name="Admin User",
      password_hash="hashed_password",
    )
    self.session.add_all([self.user, self.admin])
    self.session.commit()

  def test_safe_helper_functions(self):
    """Test safe conversion helper functions."""
    # safe_str
    assert safe_str(None) == ""
    assert safe_str("test") == "test"
    assert safe_str(123) == "123"

    # safe_bool
    assert safe_bool(None) is False
    assert safe_bool(True) is True
    assert safe_bool(False) is False
    assert safe_bool(1) is True
    assert safe_bool(0) is False

  def test_repository_type_enum(self):
    """Test RepositoryType enum values."""
    assert RepositoryType.SEC.value == "sec"
    assert RepositoryType.INDUSTRY.value == "industry"
    assert RepositoryType.ECONOMIC.value == "economic"

  def test_repository_access_level_enum(self):
    """Test RepositoryAccessLevel enum values."""
    assert RepositoryAccessLevel.NONE.value == "none"
    assert RepositoryAccessLevel.READ.value == "read"
    assert RepositoryAccessLevel.WRITE.value == "write"
    assert RepositoryAccessLevel.ADMIN.value == "admin"

  def test_repository_plan_enum(self):
    """Test RepositoryPlan enum values."""
    assert RepositoryPlan.STARTER.value == "starter"
    assert RepositoryPlan.ADVANCED.value == "advanced"
    assert RepositoryPlan.UNLIMITED.value == "unlimited"

  def test_create_user_repository(self):
    """Test creating a UserRepository instance."""
    repo = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
    )

    assert repo.user_id == self.user.id
    assert repo.repository_type == RepositoryType.SEC
    assert repo.repository_name == "sec"
    assert repo.access_level == RepositoryAccessLevel.READ
    assert repo.repository_plan == RepositoryPlan.STARTER
    assert repo.is_active is None  # Not set until session add

    self.session.add(repo)
    self.session.commit()

    assert repo.id is not None
    assert repo.id.startswith("usra_")
    assert repo.is_active is True
    assert repo.created_at is not None
    assert repo.updated_at is not None

  def test_create_access_new(self):
    """Test creating new repository access."""
    access = UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
      granted_by=self.admin.id,
      monthly_price_cents=2999,
      monthly_credits=5000,
      metadata={"test": "data"},
    )

    assert access.id is not None
    assert access.user_id == self.user.id
    assert access.repository_type == RepositoryType.SEC
    assert access.repository_name == "sec"
    assert access.access_level == RepositoryAccessLevel.READ
    assert access.repository_plan == RepositoryPlan.STARTER
    assert access.granted_by == self.admin.id
    assert access.granted_at is not None
    assert access.is_active is True
    assert access.monthly_price_cents == 2999
    assert access.monthly_credit_allocation == 5000
    assert access.billing_cycle_day is not None
    assert access.next_billing_at is not None

    # Check metadata
    metadata = json.loads(access.extra_metadata)
    assert metadata["test"] == "data"

  def test_create_access_update_existing(self):
    """Test updating existing repository access."""
    # Create initial access
    initial = UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    initial_id = initial.id

    # Update the access
    updated = UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.WRITE,
      repository_plan=RepositoryPlan.ADVANCED,
      session=self.session,
      granted_by=self.admin.id,
      monthly_price_cents=9999,
      monthly_credits=25000,
    )

    # Should be the same record, updated
    assert updated.id == initial_id
    assert updated.access_level == RepositoryAccessLevel.WRITE
    assert updated.repository_plan == RepositoryPlan.ADVANCED
    assert updated.monthly_price_cents == 9999
    assert updated.monthly_credit_allocation == 25000

  def test_create_access_with_expiration(self):
    """Test creating access with expiration date."""
    expires = datetime.now(timezone.utc) + timedelta(days=30)

    access = UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec_temp",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
      expires_at=expires,
    )

    assert access.expires_at == expires

  def test_create_access_rollback_on_error(self):
    """Test rollback on error during access creation."""
    with patch.object(self.session, "commit", side_effect=SQLAlchemyError("DB error")):
      with pytest.raises(SQLAlchemyError):
        UserRepository.create_access(
          user_id=self.user.id,
          repository_type=RepositoryType.SEC,
          repository_name="sec_error",
          access_level=RepositoryAccessLevel.READ,
          repository_plan=RepositoryPlan.STARTER,
          session=self.session,
        )

  def test_get_by_user_and_repository(self):
    """Test getting access by user and repository."""
    access = UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    result = UserRepository.get_by_user_and_repository(
      self.user.id, "sec", self.session
    )

    assert result is not None
    assert result.id == access.id

    # Test non-existent
    result = UserRepository.get_by_user_and_repository(
      self.user.id, "non_existent", self.session
    )
    assert result is None

  def test_user_has_access(self):
    """Test checking if user has access."""
    # Create access
    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    # Should have access
    assert UserRepository.user_has_access(self.user.id, "sec", self.session) is True

    # Should not have access to different repo
    assert UserRepository.user_has_access(self.user.id, "other", self.session) is False

  def test_user_has_access_expired(self):
    """Test access check with expired access."""
    past = datetime.now(timezone.utc) - timedelta(days=1)

    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec_expired",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
      expires_at=past,
    )

    # Should not have access (expired)
    assert (
      UserRepository.user_has_access(self.user.id, "sec_expired", self.session) is False
    )

  def test_user_has_access_none_level(self):
    """Test access check with NONE access level."""
    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec_none",
      access_level=RepositoryAccessLevel.NONE,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    # Should not have access (NONE level)
    assert (
      UserRepository.user_has_access(self.user.id, "sec_none", self.session) is False
    )

  def test_get_user_access_level(self):
    """Test getting user's access level."""
    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.WRITE,
      repository_plan=RepositoryPlan.ADVANCED,
      session=self.session,
    )

    level = UserRepository.get_user_access_level(self.user.id, "sec", self.session)
    assert level == RepositoryAccessLevel.WRITE

    # Non-existent returns NONE
    level = UserRepository.get_user_access_level(self.user.id, "other", self.session)
    assert level == RepositoryAccessLevel.NONE

  def test_get_user_repositories(self):
    """Test getting all repositories for a user."""
    # Create multiple accesses
    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.INDUSTRY,
      repository_name="industry_tech",
      access_level=RepositoryAccessLevel.WRITE,
      repository_plan=RepositoryPlan.ADVANCED,
      session=self.session,
    )

    repos = UserRepository.get_user_repositories(self.user.id, self.session)
    assert len(repos) == 2

    repo_names = {repo.repository_name for repo in repos}
    assert repo_names == {"sec", "industry_tech"}

  def test_get_user_repositories_active_only(self):
    """Test getting only active repositories."""
    # Create active and inactive access
    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec_active",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    inactive = UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec_inactive",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    # Deactivate one
    inactive.is_active = False
    self.session.commit()

    # Get active only
    repos = UserRepository.get_user_repositories(
      self.user.id, self.session, active_only=True
    )
    assert len(repos) == 1
    assert repos[0].repository_name == "sec_active"

    # Get all
    repos = UserRepository.get_user_repositories(
      self.user.id, self.session, active_only=False
    )
    assert len(repos) == 2

  def test_get_repository_users(self):
    """Test getting all users with access to a repository."""
    # Clean up any existing user repository records to ensure test isolation
    # Delete in dependency order due to foreign key constraints
    from robosystems.models.iam.user_repository_credits import (
      UserRepositoryCredits,
      UserRepositoryCreditTransaction,
    )

    self.session.query(UserRepositoryCreditTransaction).delete()
    self.session.query(UserRepositoryCredits).delete()
    self.session.query(UserRepository).delete()
    self.session.commit()

    # Create access for multiple users
    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    UserRepository.create_access(
      user_id=self.admin.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.ADMIN,
      repository_plan=RepositoryPlan.UNLIMITED,
      session=self.session,
    )

    users = UserRepository.get_repository_users("sec", self.session)
    assert len(users) == 2

    user_ids = {u.user_id for u in users}
    assert user_ids == {self.user.id, self.admin.id}

  def test_get_by_repository_type(self):
    """Test getting all access records by repository type."""
    # Clean up any existing user repository records to ensure test isolation
    # Delete in dependency order due to foreign key constraints
    from robosystems.models.iam.user_repository_credits import (
      UserRepositoryCredits,
      UserRepositoryCreditTransaction,
    )

    self.session.query(UserRepositoryCreditTransaction).delete()
    self.session.query(UserRepositoryCredits).delete()
    self.session.query(UserRepository).delete()
    self.session.commit()

    # Create different types
    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec1",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    UserRepository.create_access(
      user_id=self.admin.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec2",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.INDUSTRY,
      repository_name="industry1",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    sec_repos = UserRepository.get_by_repository_type(RepositoryType.SEC, self.session)
    assert len(sec_repos) == 2

    industry_repos = UserRepository.get_by_repository_type(
      RepositoryType.INDUSTRY, self.session
    )
    assert len(industry_repos) == 1

  def test_revoke_access(self):
    """Test revoking repository access."""
    access = UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    assert access.is_active is True
    assert access.expires_at is None

    access.revoke_access(self.session)

    assert access.is_active is False
    assert access.expires_at is not None

  def test_upgrade_tier(self):
    """Test upgrading repository tier."""
    access = UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
      monthly_price_cents=2999,
      monthly_credits=5000,
    )

    with patch("robosystems.models.iam.user_repository.logger") as mock_logger:
      access.upgrade_tier(
        new_plan=RepositoryPlan.ADVANCED,
        session=self.session,
        new_price_cents=9999,
        new_credits=25000,
      )

    assert access.repository_plan == RepositoryPlan.ADVANCED
    assert access.monthly_price_cents == 9999
    assert access.monthly_credit_allocation == 25000
    mock_logger.info.assert_called_once()

  def test_is_expired(self):
    """Test checking if access is expired."""
    past = datetime.now(timezone.utc) - timedelta(days=1)
    future = datetime.now(timezone.utc) + timedelta(days=1)

    # Not expired (no expiration)
    access1 = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec1",
      expires_at=None,
    )
    assert access1.is_expired() is False

    # Expired
    access2 = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec2",
      expires_at=past,
    )
    assert access2.is_expired() is True

    # Not expired yet
    access3 = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec3",
      expires_at=future,
    )
    assert access3.is_expired() is False

  def test_can_read(self):
    """Test can_read permission check."""
    # Active with READ access
    access = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      is_active=True,
    )
    assert access.can_read() is True

    # WRITE access can also read
    access.access_level = RepositoryAccessLevel.WRITE
    assert access.can_read() is True

    # ADMIN access can also read
    access.access_level = RepositoryAccessLevel.ADMIN
    assert access.can_read() is True

    # NONE cannot read
    access.access_level = RepositoryAccessLevel.NONE
    assert access.can_read() is False

    # Inactive cannot read
    access.access_level = RepositoryAccessLevel.READ
    access.is_active = False
    assert access.can_read() is False

    # Expired cannot read
    access.is_active = True
    access.expires_at = datetime.now(timezone.utc) - timedelta(days=1)
    assert access.can_read() is False

  def test_can_write(self):
    """Test can_write permission check."""
    # Active with WRITE access
    access = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.WRITE,
      is_active=True,
    )
    assert access.can_write() is True

    # ADMIN can also write
    access.access_level = RepositoryAccessLevel.ADMIN
    assert access.can_write() is True

    # READ cannot write
    access.access_level = RepositoryAccessLevel.READ
    assert access.can_write() is False

    # NONE cannot write
    access.access_level = RepositoryAccessLevel.NONE
    assert access.can_write() is False

  def test_can_admin(self):
    """Test can_admin permission check."""
    # Active with ADMIN access
    access = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.ADMIN,
      is_active=True,
    )
    assert access.can_admin() is True

    # WRITE cannot admin
    access.access_level = RepositoryAccessLevel.WRITE
    assert access.can_admin() is False

    # READ cannot admin
    access.access_level = RepositoryAccessLevel.READ
    assert access.can_admin() is False

  def test_get_graph_connection_info(self):
    """Test getting graph connection information."""
    access = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      graph_instance_id="instance123",
      graph_cluster_region="us-east-1",
      instance_tier="shared",
      read_preference="primary",
    )

    info = access.get_graph_connection_info()

    assert info["instance_id"] == "instance123"
    assert info["cluster_region"] == "us-east-1"
    assert info["instance_tier"] == "shared"
    assert info["read_preference"] == "primary"
    assert info["repository_name"] == "sec"
    assert info["repository_type"] == "sec"

  def test_get_repository_plan_config(self):
    """Test getting repository plan configuration."""
    access = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      repository_plan=RepositoryPlan.STARTER,
    )

    config = access.get_repository_plan_config()

    assert config["name"] == "SEC Data Starter"
    assert config["monthly_credits"] == 5000
    assert config["price_monthly"] == 29.99
    assert config["access_level"] == RepositoryAccessLevel.READ

  def test_is_repository_enabled(self):
    """Test checking if repository type is enabled."""
    assert UserRepository.is_repository_enabled(RepositoryType.SEC) is True
    assert UserRepository.is_repository_enabled(RepositoryType.INDUSTRY) is False
    assert UserRepository.is_repository_enabled(RepositoryType.ECONOMIC) is False

  def test_get_all_repository_configs(self):
    """Test getting all repository configurations."""
    configs = UserRepository.get_all_repository_configs()

    assert RepositoryType.SEC in configs
    assert configs[RepositoryType.SEC]["enabled"] is True

    assert RepositoryType.INDUSTRY in configs
    assert configs[RepositoryType.INDUSTRY]["enabled"] is False
    assert configs[RepositoryType.INDUSTRY]["coming_soon"] is True

  def test_to_dict(self):
    """Test conversion to dictionary."""
    access = UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
      monthly_price_cents=2999,
      monthly_credits=5000,
      metadata={"test": "data"},
    )

    result = access.to_dict()

    assert result["id"] == access.id
    assert result["user_id"] == self.user.id
    assert result["repository_type"] == "sec"
    assert result["repository_name"] == "sec"
    assert result["access_level"] == "read"
    assert result["repository_plan"] == "starter"
    assert result["is_active"] is True
    assert result["monthly_price_cents"] == 2999
    assert result["monthly_credit_allocation"] == 5000
    assert result["metadata"]["test"] == "data"
    assert "config" in result
    assert "graph_connection" in result

  def test_repr_method(self):
    """Test string representation."""
    access = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.WRITE,
      repository_plan=RepositoryPlan.ADVANCED,
    )

    repr_str = repr(access)
    assert f"<UserRepository(user={self.user.id}" in repr_str
    assert "repo=sec" in repr_str
    assert "level=RepositoryAccessLevel.WRITE" in repr_str
    assert "plan=RepositoryPlan.ADVANCED" in repr_str

  def test_unique_constraint(self):
    """Test unique constraint on user/repository combination."""
    # Create first access
    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec_unique",
      access_level=RepositoryAccessLevel.READ,
      repository_plan=RepositoryPlan.STARTER,
      session=self.session,
    )

    # Try to create duplicate (should update, not create new)
    UserRepository.create_access(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec_unique",
      access_level=RepositoryAccessLevel.WRITE,
      repository_plan=RepositoryPlan.ADVANCED,
      session=self.session,
    )

    # Should only have one record
    all_access = (
      self.session.query(UserRepository)
      .filter_by(user_id=self.user.id, repository_name="sec_unique")
      .all()
    )
    assert len(all_access) == 1
    assert all_access[0].access_level == RepositoryAccessLevel.WRITE
