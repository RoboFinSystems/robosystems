"""Token-type enforcement in verify_jwt_claims.

Single-use SSO handoff tokens (``create_sso_token`` → {"sso": true}, no jti,
unrevocable) must never authenticate as a full session bearer. They are
consumed only at /sso-exchange, which decodes them directly. Session tokens
carry ``type: "access"``; legacy tokens minted before that claim existed are
grandfathered so a deploy doesn't invalidate active sessions.
"""

from unittest.mock import MagicMock, patch

import jwt
import pytest

from robosystems.config import env
from robosystems.middleware.auth.jwt import (
  create_jwt_token,
  create_sso_token,
  verify_jwt_claims,
)


@pytest.mark.unit
class TestJWTTokenType:
  @pytest.fixture(autouse=True)
  def _stub_session_version(self):
    with patch(
      "robosystems.middleware.auth.jwt._get_user_session_version", return_value=0
    ):
      yield

  @pytest.fixture(autouse=True)
  def _redis_not_revoked(self):
    """is_jwt_token_revoked() reads Redis; return an empty hash (not revoked)."""
    mock_redis = MagicMock()
    mock_redis.hgetall.return_value = {}
    with patch(
      "robosystems.middleware.auth.jwt.get_redis_client", return_value=mock_redis
    ):
      yield

  def _encode(self, **overrides) -> str:
    """Mint a raw token with the standard claims, minus/plus overrides."""
    payload = {
      "user_id": "usr_1",
      "session_version": 0,
      "iss": env.JWT_ISSUER,
      "aud": env.JWT_AUDIENCE,
    }
    payload.update(overrides)
    return jwt.encode(payload, env.JWT_SECRET_KEY, algorithm="HS256")

  def test_create_jwt_token_embeds_access_type(self):
    token = create_jwt_token("usr_1")
    payload = jwt.decode(
      token,
      env.JWT_SECRET_KEY,
      algorithms=["HS256"],
      options={"verify_exp": False, "verify_aud": False, "verify_iss": False},
    )
    assert payload["type"] == "access"

  def test_access_token_is_accepted(self):
    token = create_jwt_token("usr_1")
    result = verify_jwt_claims(token)
    assert result is not None
    assert result[0] == "usr_1"

  def test_sso_token_is_rejected_as_bearer(self):
    token, _token_id = create_sso_token("usr_1")
    # The SSO token is valid at /sso-exchange, but must not pass the general
    # bearer verification path.
    assert verify_jwt_claims(token) is None

  def test_sso_bearer_rejection_is_audited(self):
    token, _token_id = create_sso_token("usr_1")
    with patch(
      "robosystems.security.SecurityAuditLogger.log_security_event"
    ) as mock_audit:
      assert verify_jwt_claims(token) is None
    assert mock_audit.call_count == 1
    kwargs = mock_audit.call_args.kwargs
    assert kwargs["details"]["reason"] == "non_access_token_as_bearer"
    assert kwargs["details"]["token_type"] == "sso"

  def test_legacy_token_without_type_is_grandfathered(self):
    from datetime import UTC, datetime, timedelta

    # A session token minted before the `type` claim existed — no `type`, no
    # `sso`. Must still authenticate so a deploy doesn't log everyone out.
    token = self._encode(
      exp=datetime.now(UTC) + timedelta(hours=1),
      iat=datetime.now(UTC),
    )
    result = verify_jwt_claims(token)
    assert result is not None
    assert result[0] == "usr_1"

  def test_non_access_type_is_rejected(self):
    from datetime import UTC, datetime, timedelta

    token = self._encode(
      type="stream",
      exp=datetime.now(UTC) + timedelta(hours=1),
      iat=datetime.now(UTC),
    )
    assert verify_jwt_claims(token) is None
