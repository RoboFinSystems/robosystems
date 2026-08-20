"""
Comprehensive tests for authentication endpoints.

Tests cover the refactored auth endpoints with metrics decorators,
including registration, login, and error scenarios.
"""

import os
from unittest.mock import patch

import bcrypt
from fastapi.testclient import TestClient

from robosystems.models.core import User


class TestAuthRegistration:
  """Test user registration endpoint."""

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_success(self, client: TestClient):
    """Test successful user registration."""
    registration_data = {
      "name": "Test User",
      "email": "test@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }

    response = client.post("/v1/auth/register", json=registration_data)

    assert response.status_code == 201
    data = response.json()

    # Check response structure
    assert "user" in data
    assert "message" in data
    assert (
      data["message"]
      == "User registered successfully. Please check your email to verify your account."
    )

    # Check user data
    user_data = data["user"]
    assert user_data["name"] == "Test User"
    assert user_data["email"] == "test@example.com"
    assert "id" in user_data

    # Check JWT token is returned
    assert "token" in data
    assert data["token"] is not None
    assert len(data["token"]) > 0

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_duplicate_email(self, client: TestClient):
    """Test registration with already existing email."""
    # First registration
    registration_data = {
      "name": "Test User",
      "email": "duplicate@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }

    response1 = client.post("/v1/auth/register", json=registration_data)
    assert response1.status_code == 201

    # Second registration with same email
    registration_data["name"] = "Another User"
    response2 = client.post("/v1/auth/register", json=registration_data)

    assert response2.status_code == 409
    data = response2.json()
    assert data["detail"] == "Email already registered"

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_invalid_email(self, client: TestClient):
    """Test registration with invalid email format."""
    registration_data = {
      "name": "Test User",
      "email": "invalid-email",
      "password": "S3cur3P@ssw0rd!2024",
    }

    response = client.post("/v1/auth/register", json=registration_data)
    assert response.status_code == 422

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_weak_password(self, client: TestClient):
    """Test registration with weak password."""
    registration_data = {
      "name": "Test User",
      "email": "test@example.com",
      "password": "123",  # Too short
    }

    response = client.post("/v1/auth/register", json=registration_data)
    assert response.status_code == 422

  def test_register_missing_fields(self, client: TestClient):
    """Test registration with missing required fields."""
    # Missing name
    response = client.post(
      "/v1/auth/register",
      json={"email": "test@example.com", "password": "S3cur3P@ssw0rd!2024"},
    )
    assert response.status_code == 422

    # Missing email
    response = client.post(
      "/v1/auth/register", json={"name": "Test User", "password": "S3cur3P@ssw0rd!2024"}
    )
    assert response.status_code == 422

    # Missing password
    response = client.post(
      "/v1/auth/register", json={"name": "Test User", "email": "test@example.com"}
    )
    assert response.status_code == 422

  @patch("robosystems.routers.auth.register.record_auth_metrics")
  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_metrics_recorded(self, mock_record_auth, client: TestClient):
    """Test that auth metrics are properly recorded during registration."""
    registration_data = {
      "name": "Metrics Test User",
      "email": "metrics@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }

    response = client.post("/v1/auth/register", json=registration_data)
    assert response.status_code == 201

    # Verify auth metrics were recorded
    assert mock_record_auth.call_count >= 2  # Should record attempt and success

    # Check calls include success and failure attempts
    calls = mock_record_auth.call_args_list
    success_call = None

    for call in calls:
      args, kwargs = call
      if kwargs.get("success"):
        success_call = kwargs
      elif not kwargs.get("success"):
        pass  # Just skip failed attempts

    # Verify success call
    assert success_call is not None
    assert success_call["endpoint"] == "/v1/auth/register"
    assert success_call["auth_type"] == "email_password_registration"
    assert "user_id" in success_call


class TestAuthLogin:
  """Test user login endpoint."""

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_login_success(self, client: TestClient):
    """Test successful user login."""
    # First register a user
    registration_data = {
      "name": "Login Test User",
      "email": "login@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }
    register_response = client.post("/v1/auth/register", json=registration_data)
    assert register_response.status_code == 201

    # Now login with same credentials
    login_data = {"email": "login@example.com", "password": "S3cur3P@ssw0rd!2024"}

    response = client.post("/v1/auth/login", json=login_data)

    assert response.status_code == 200
    data = response.json()

    # Check response structure
    assert "user" in data
    assert "message" in data
    assert data["message"] == "Login successful"

    # Check user data
    user_data = data["user"]
    assert user_data["email"] == "login@example.com"
    assert user_data["name"] == "Login Test User"

    # Check JWT token is returned
    assert "token" in data
    assert data["token"] is not None
    assert len(data["token"]) > 0

  def test_login_invalid_email(self, client: TestClient):
    """Test login with non-existent email."""
    login_data = {"email": "nonexistent@example.com", "password": "somePassword123"}

    response = client.post("/v1/auth/login", json=login_data)

    assert response.status_code == 401
    data = response.json()
    assert data["detail"] == "Invalid email or password"

  def test_login_unknown_email_burns_bcrypt_to_equalize_timing(
    self, client: TestClient
  ):
    """The miss path must spend a verification's worth of work so it is
    indistinguishable from a wrong password (no enumeration oracle)."""
    from unittest.mock import AsyncMock

    with patch(
      "robosystems.security.password.PasswordSecurity.equalize_verify_timing",
      new=AsyncMock(),
    ) as equalizer:
      response = client.post(
        "/v1/auth/login",
        json={"email": "no-such-user@example.com", "password": "whatever123"},
      )

    assert response.status_code == 401
    equalizer.assert_awaited_once()

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_login_invalid_password(self, client: TestClient):
    """Test login with wrong password."""
    # First register a user
    registration_data = {
      "name": "Wrong Password Test",
      "email": "wrongpwd@example.com",
      "password": "C0rr3ctP@ssw0rd!123",
    }
    register_response = client.post("/v1/auth/register", json=registration_data)
    assert register_response.status_code == 201

    # Try login with wrong password
    login_data = {"email": "wrongpwd@example.com", "password": "wrongPassword123"}

    response = client.post("/v1/auth/login", json=login_data)

    assert response.status_code == 401
    data = response.json()
    assert data["detail"] == "Invalid email or password"

  def test_login_missing_fields(self, client: TestClient):
    """Test login with missing required fields."""
    # Missing email
    response = client.post("/v1/auth/login", json={"password": "password123"})
    assert response.status_code == 422

    # Missing password
    response = client.post("/v1/auth/login", json={"email": "test@example.com"})
    assert response.status_code == 422

  @patch("robosystems.routers.auth.login.record_auth_metrics")
  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_login_metrics_recorded(self, mock_record_auth, client: TestClient):
    """Test that auth metrics are properly recorded during login."""
    # Register user first
    registration_data = {
      "name": "Metrics Login User",
      "email": "loginmetrics@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }
    client.post("/v1/auth/register", json=registration_data)

    # Reset mock for login test
    mock_record_auth.reset_mock()

    # Login
    login_data = {
      "email": "loginmetrics@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }

    response = client.post("/v1/auth/login", json=login_data)
    assert response.status_code == 200

    # Verify auth metrics were recorded
    assert mock_record_auth.call_count >= 2  # Should record attempt and success

    # Check for success call
    calls = mock_record_auth.call_args_list
    success_call = None

    for call in calls:
      args, kwargs = call
      if kwargs.get("success"):
        success_call = kwargs
        break

    assert success_call is not None
    assert success_call["endpoint"] == "/v1/auth/login"
    assert success_call["auth_type"] == "email_password_login"

  @patch("robosystems.routers.auth.login.record_auth_metrics")
  def test_login_failure_metrics_recorded(self, mock_record_auth, client: TestClient):
    """Test that auth failure metrics are recorded for invalid login."""
    login_data = {"email": "nonexistent@example.com", "password": "somePassword123"}

    response = client.post("/v1/auth/login", json=login_data)
    assert response.status_code == 401

    # Verify failure metrics were recorded
    calls = mock_record_auth.call_args_list
    failure_call = None

    for call in calls:
      args, kwargs = call
      if not kwargs.get("success") and "failure_reason" in kwargs:
        failure_call = kwargs
        break

    assert failure_call is not None
    assert failure_call["endpoint"] == "/v1/auth/login"
    assert failure_call["auth_type"] == "email_password_login"
    assert failure_call["failure_reason"] == "user_not_found_or_inactive"


class TestAuthRateLimit:
  """Test rate limiting on auth endpoints."""

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_rate_limit(self, client: TestClient):
    """Test rate limiting on registration endpoint."""
    registration_data = {
      "name": "Rate Limit Test",
      "email": "ratelimit{i}@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }

    # Make multiple rapid requests (rate limit implementation may vary)
    responses = []
    for i in range(5):
      data = registration_data.copy()
      data["email"] = f"ratelimit{i}@example.com"
      response = client.post("/v1/auth/register", json=data)
      responses.append(response)

    # All should succeed since different emails
    for response in responses:
      assert response.status_code in [201, 429]  # 429 if rate limited

  def test_login_rate_limit(self, client: TestClient):
    """Test rate limiting on login endpoint."""
    login_data = {"email": "nonexistent@example.com", "password": "wrongPassword123"}

    # Make multiple rapid failed login attempts
    responses = []
    for i in range(10):
      response = client.post("/v1/auth/login", json=login_data)
      responses.append(response)

    # Should eventually get rate limited
    status_codes = [r.status_code for r in responses]
    assert 401 in status_codes  # Invalid credentials
    # May also have 429 if rate limiting kicks in


class TestAuthSecurity:
  """Test security aspects of auth endpoints."""

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_password_hashing(self, client: TestClient, test_db):
    """Test that passwords are properly hashed."""
    registration_data = {
      "name": "Hash Test User",
      "email": "hashtest@example.com",
      "password": "MyS3cr3tP@ssw0rd!123",
    }

    response = client.post("/v1/auth/register", json=registration_data)
    assert response.status_code == 201

    # Retrieve user from database
    user = User.get_by_email("hashtest@example.com", test_db)
    assert user is not None
    assert user.password_hash != "MyS3cr3tP@ssw0rd!123"  # Password should be hashed
    assert len(user.password_hash) > 50  # bcrypt hashes are long

    # Verify password can be checked
    assert bcrypt.checkpw(b"MyS3cr3tP@ssw0rd!123", user.password_hash.encode("utf-8"))

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_cookie_security(self, client: TestClient):
    """Test that auth cookies have proper security settings."""
    registration_data = {
      "name": "Cookie Test User",
      "email": "cookietest@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }

    response = client.post("/v1/auth/register", json=registration_data)
    assert response.status_code == 201

    # JWT tokens are returned in response body, not cookies
    data = response.json()
    assert "token" in data
    assert data["token"] is not None
    assert len(data["token"]) > 0

    # Verify the token is a JWT (has 3 parts separated by dots)
    token_parts = data["token"].split(".")
    assert len(token_parts) == 3

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_sql_injection_prevention(self, client: TestClient):
    """Test that SQL injection attempts are prevented."""
    malicious_data = {
      "name": "'; DROP TABLE users; --",
      "email": "'; DROP TABLE users; --@example.com",
      "password": "password123",
    }

    # Should not cause errors and should be treated as normal data
    response = client.post("/v1/auth/register", json=malicious_data)
    # May succeed (422 due to email format) or fail gracefully
    assert response.status_code in [201, 422, 400]

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_xss_prevention(self, client: TestClient):
    """Test that XSS attempts in names are handled."""
    xss_data = {
      "name": "<script>alert('xss')</script>",
      "email": "xsstest@example.com",
      "password": "S3cur3P@ssw0rd!2024",
    }

    response = client.post("/v1/auth/register", json=xss_data)

    if response.status_code == 201:
      data = response.json()
      # Should sanitize the XSS attempt by HTML escaping
      assert (
        data["user"]["name"] == "&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;"
      )


class TestInvitedRegistration:
  """Registration via an org invitation token joins the org instead of
  creating a personal org."""

  @staticmethod
  def _make_invitation(test_db, test_user, email, role=None):
    from robosystems.models.core import Org, OrgInvitation, OrgRole, OrgType, OrgUser

    org = Org.create(
      name="Inviting Org",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.OWNER, session=test_db
    )
    invitation, raw_token = OrgInvitation.create_invitation(
      org_id=org.id,
      email=email,
      role=role or OrgRole.MEMBER,
      invited_by=test_user.id,
      session=test_db,
    )
    return org, invitation, raw_token

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_with_invitation_joins_org(
    self, client: TestClient, test_db, test_user
  ):
    from robosystems.models.core import InvitationStatus, OrgRole, OrgUser

    org, invitation, raw_token = self._make_invitation(
      test_db, test_user, "invited@example.com", role=OrgRole.ADMIN
    )

    response = client.post(
      "/v1/auth/register",
      json={
        "name": "Invited User",
        "email": "invited@example.com",
        "password": "S3cur3P@ssw0rd!2024",
        "invite_token": raw_token,
      },
    )

    assert response.status_code == 201
    data = response.json()
    assert data["org"]["id"] == org.id
    assert data["user"]["email_verified"] is True
    assert "joined" in data["message"]

    new_user = User.get_by_email("invited@example.com", test_db)
    assert new_user is not None

    memberships = OrgUser.get_user_orgs(new_user.id, test_db)
    assert len(memberships) == 1
    assert memberships[0].org_id == org.id
    assert memberships[0].role == OrgRole.ADMIN

    test_db.refresh(invitation)
    assert invitation.status == InvitationStatus.ACCEPTED.value
    assert invitation.accepted_user_id == new_user.id

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_with_invitation_audits_the_org_join(
    self, client: TestClient, test_db, test_user
  ):
    """The invited join is the org-side "member added" — a privileged access
    change that must leave a structured record carrying the inviter."""
    from robosystems.models.core import OrgRole
    from robosystems.security import SecurityEventType

    org, invitation, raw_token = self._make_invitation(
      test_db, test_user, "audited-invitee@example.com", role=OrgRole.MEMBER
    )

    with patch("robosystems.routers.auth.register.SecurityAuditLogger") as audit:
      response = client.post(
        "/v1/auth/register",
        json={
          "name": "Audited Invitee",
          "email": "audited-invitee@example.com",
          "password": "S3cur3P@ssw0rd!2024",
          "invite_token": raw_token,
        },
      )

    assert response.status_code == 201
    joined = [
      c.kwargs
      for c in audit.log_security_event.call_args_list
      if c.kwargs["event_type"] is SecurityEventType.ORG_MEMBER_ADDED
    ]
    assert len(joined) == 1
    new_user = User.get_by_email("audited-invitee@example.com", test_db)
    assert joined[0]["user_id"] == new_user.id
    assert joined[0]["details"] == {
      "action": "org_member_added",
      "org_id": org.id,
      "target_user_id": new_user.id,
      "new_role": "member",
      "via": "invitation",
      "invitation_id": invitation.id,
      "invited_by": test_user.id,
    }

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_with_invitation_email_mismatch(
    self, client: TestClient, test_db, test_user
  ):
    _, _, raw_token = self._make_invitation(test_db, test_user, "intended@example.com")

    response = client.post(
      "/v1/auth/register",
      json={
        "name": "Wrong Person",
        "email": "someoneelse@example.com",
        "password": "S3cur3P@ssw0rd!2024",
        "invite_token": raw_token,
      },
    )

    assert response.status_code == 400
    assert "different email address" in response.json()["detail"]
    assert User.get_by_email("someoneelse@example.com", test_db) is None

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_with_invalid_invite_token(self, client: TestClient, test_db):
    response = client.post(
      "/v1/auth/register",
      json={
        "name": "No Invite",
        "email": "noinvite@example.com",
        "password": "S3cur3P@ssw0rd!2024",
        "invite_token": "bogus-token",
      },
    )

    assert response.status_code == 400
    assert "invalid or has expired" in response.json()["detail"]
    assert User.get_by_email("noinvite@example.com", test_db) is None

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    True,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_register_with_expired_invite_token(
    self, client: TestClient, test_db, test_user
  ):
    from datetime import UTC, datetime, timedelta

    _, invitation, raw_token = self._make_invitation(
      test_db, test_user, "late@example.com"
    )
    invitation.expires_at = datetime.now(UTC) - timedelta(days=1)
    test_db.commit()

    response = client.post(
      "/v1/auth/register",
      json={
        "name": "Too Late",
        "email": "late@example.com",
        "password": "S3cur3P@ssw0rd!2024",
        "invite_token": raw_token,
      },
    )

    assert response.status_code == 400
    assert "invalid or has expired" in response.json()["detail"]

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    False,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_invited_user_can_register_while_registration_is_closed(
    self, client: TestClient, test_db, test_user
  ):
    """Invite-only mode: closing registration stops unsolicited signups, not
    the people an admin deliberately invited."""
    from robosystems.models.core import OrgUser

    org, _invitation, raw_token = self._make_invitation(
      test_db, test_user, "closedreg@example.com"
    )

    response = client.post(
      "/v1/auth/register",
      json={
        "name": "Invited User",
        "email": "closedreg@example.com",
        "password": "S3cur3P@ssw0rd!2024",
        "invite_token": raw_token,
      },
    )

    assert response.status_code == 201
    new_user = User.get_by_email("closedreg@example.com", test_db)
    assert new_user is not None
    assert OrgUser.get_user_orgs(new_user.id, test_db)[0].org_id == org.id

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    False,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_uninvited_registration_still_blocked_when_closed(
    self, client: TestClient, test_db
  ):
    response = client.post(
      "/v1/auth/register",
      json={
        "name": "Walk In",
        "email": "walkin@example.com",
        "password": "S3cur3P@ssw0rd!2024",
      },
    )

    assert response.status_code == 503
    assert User.get_by_email("walkin@example.com", test_db) is None

  @patch.object(
    __import__("robosystems.config", fromlist=["env"]).env,
    "USER_REGISTRATION_ENABLED",
    False,
  )
  @patch.dict(os.environ, {"ENVIRONMENT": "dev"})
  def test_bogus_token_does_not_open_closed_registration(
    self, client: TestClient, test_db
  ):
    """A made-up token must not buy passage past the gate — otherwise any
    string would reach the checks below, including the duplicate-email probe
    that closed registration exists to deny."""
    response = client.post(
      "/v1/auth/register",
      json={
        "name": "Probe",
        "email": "probe@example.com",
        "password": "S3cur3P@ssw0rd!2024",
        "invite_token": "not-a-real-token",
      },
    )

    assert response.status_code == 503
    assert User.get_by_email("probe@example.com", test_db) is None
