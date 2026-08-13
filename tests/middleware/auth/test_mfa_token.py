"""Direct unit tests for the MFA challenge token helpers.

The HTTP-level handshake tests (tests/routers/auth/test_login_mfa.py) cover
the flows; these pin the token contract itself — purpose scoping, expiry,
claim requirements, and the bearer refusal — since these tokens gate the
login handshake.
"""

from datetime import UTC, datetime, timedelta

import jwt as pyjwt
import pytest

from robosystems.config import env
from robosystems.middleware.auth.jwt import (
  JWTConfig,
  create_mfa_token,
  create_sso_token,
  decode_mfa_token,
  verify_jwt_claims,
)


def _mint_raw(**overrides) -> str:
  """Mint an mfa-shaped token directly so individual claims can be broken."""
  payload = {
    "user_id": "user_test",
    "jti": "jti-test",
    "type": "mfa",
    "purpose": "login",
    "session_version": 0,
    "exp": datetime.now(UTC) + timedelta(seconds=300),
    "iat": datetime.now(UTC),
    "iss": env.JWT_ISSUER,
    "aud": env.JWT_AUDIENCE,
  }
  payload.update(overrides)
  payload = {k: v for k, v in payload.items() if v is not None}
  return pyjwt.encode(payload, JWTConfig.get_jwt_secret(), algorithm="HS256")


class TestCreateMfaToken:
  def test_roundtrip_both_purposes(self):
    for purpose in ("login", "enroll"):
      token, jti = create_mfa_token("user_test", purpose)
      payload = decode_mfa_token(token, purpose)
      assert payload is not None
      assert payload["user_id"] == "user_test"
      assert payload["jti"] == jti
      assert payload["purpose"] == purpose
      assert payload["type"] == "mfa"

  def test_invalid_purpose_refused_at_mint(self):
    with pytest.raises(ValueError):
      create_mfa_token("user_test", "bearer")


class TestDecodeMfaToken:
  def test_purpose_mismatch_returns_none(self):
    login_token, _ = create_mfa_token("user_test", "login")
    enroll_token, _ = create_mfa_token("user_test", "enroll")
    assert decode_mfa_token(login_token, "enroll") is None
    assert decode_mfa_token(enroll_token, "login") is None

  def test_expired_token_returns_none(self):
    token = _mint_raw(exp=datetime.now(UTC) - timedelta(seconds=1))
    assert decode_mfa_token(token, "login") is None

  def test_wrong_type_returns_none(self):
    token = _mint_raw(type="access")
    assert decode_mfa_token(token, "login") is None

  def test_missing_user_id_or_jti_returns_none(self):
    assert decode_mfa_token(_mint_raw(user_id=None), "login") is None
    assert decode_mfa_token(_mint_raw(jti=None), "login") is None

  def test_wrong_issuer_or_audience_returns_none(self):
    assert decode_mfa_token(_mint_raw(iss="https://evil.example"), "login") is None
    assert decode_mfa_token(_mint_raw(aud="other-audience"), "login") is None

  def test_tampered_and_garbage_return_none(self):
    token, _ = create_mfa_token("user_test", "login")
    assert decode_mfa_token(token[:-4] + "AAAA", "login") is None
    assert decode_mfa_token("not-a-jwt", "login") is None

  def test_sso_token_is_not_an_mfa_token(self):
    sso_token, _ = create_sso_token("user_test")
    assert decode_mfa_token(sso_token, "login") is None


class TestMfaTokenBearerRefusal:
  @pytest.mark.parametrize("purpose", ["login", "enroll"])
  def test_mfa_token_refused_as_session_bearer(self, purpose):
    """verify_jwt_claims must refuse type:"mfa" like every non-access type —
    the generic guard, exercised with this specific token shape."""
    token, _ = create_mfa_token("user_test", purpose)
    assert verify_jwt_claims(token) is None
