"""
Tests for the auth providers (posture) endpoint.
"""

from unittest.mock import patch

from robosystems.config import env
from robosystems.routers.auth.providers import get_auth_providers


class TestAuthProviders:
  """Test the /providers endpoint."""

  async def test_default_posture(self):
    with (
      patch.object(env, "PASSWORD_AUTH_ENABLED", True),
      patch.object(env, "SSO_OIDC_ENABLED", False),
      patch.object(env, "PASSKEYS_ENABLED", False),
      patch.object(env, "USER_REGISTRATION_ENABLED", True),
    ):
      result = await get_auth_providers()

    assert result.password_auth is True
    assert result.oidc.enabled is False
    assert result.oidc.provider_label is None
    assert result.registration is True
    assert result.passkeys is False

  async def test_oidc_primary_posture(self):
    with (
      patch.object(env, "PASSWORD_AUTH_ENABLED", True),
      patch.object(env, "SSO_OIDC_ENABLED", True),
      patch.object(env, "SSO_OIDC_PROVIDER_LABEL", "Okta"),
      patch.object(env, "PASSKEYS_ENABLED", True),
      patch.object(env, "USER_REGISTRATION_ENABLED", False),
    ):
      result = await get_auth_providers()

    assert result.oidc.enabled is True
    assert result.oidc.provider_label == "Okta"
    assert result.registration is False
    assert result.passkeys is True

  async def test_label_hidden_when_oidc_disabled(self):
    with (
      patch.object(env, "SSO_OIDC_ENABLED", False),
      patch.object(env, "SSO_OIDC_PROVIDER_LABEL", "Okta"),
    ):
      result = await get_auth_providers()

    assert result.oidc.provider_label is None
