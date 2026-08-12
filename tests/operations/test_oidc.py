"""Tests for the OIDC login kernel.

The ID-token validation tests use real RSA crypto (sign with pyjwt, verify
through the module's own JWKS path) rather than mocked verification — this is
the classic account-takeover surface, so the tests must prove the rejections,
not assert that a mock was called.
"""

import json
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from robosystems.models.core import User, UserIdentity
from robosystems.operations import oidc as oidc_ops
from robosystems.operations.oidc import (
  OIDCConnection,
  OIDCState,
  OIDCTokenInvalidError,
  OIDCUnavailableError,
  OIDCUserInactiveError,
  OIDCUserNotProvisionedError,
  parse_return_to,
  resolve_oidc_user,
  validate_id_token,
)

ISSUER = "https://test-org.okta.example"
CLIENT_ID = "client-abc-123"
KID = "test-signing-key"

CONNECTION = OIDCConnection(
  issuer=ISSUER,
  client_id=CLIENT_ID,
  client_secret="secret",
  redirect_uri="http://localhost:8000/v1/auth/oidc/callback",
)
METADATA = {"issuer": ISSUER, "jwks_uri": f"{ISSUER}/jwks"}


@pytest.fixture(scope="module")
def rsa_key():
  return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="module")
def private_pem(rsa_key):
  return rsa_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
  )


@pytest.fixture(scope="module")
def jwks(rsa_key):
  jwk = json.loads(pyjwt.algorithms.RSAAlgorithm.to_jwk(rsa_key.public_key()))
  jwk.update({"kid": KID, "alg": "RS256", "use": "sig"})
  return {"keys": [jwk]}


def _make_id_token(private_pem, *, kid=KID, **overrides):
  now = datetime.now(UTC)
  claims = {
    "iss": ISSUER,
    "aud": CLIENT_ID,
    "sub": "okta|user-1",
    "email": "staff@customer.example",
    "nonce": "expected-nonce",
    "iat": now,
    "exp": now + timedelta(minutes=5),
  }
  claims.update(overrides)
  claims = {k: v for k, v in claims.items() if v is not None}
  return pyjwt.encode(claims, private_pem, algorithm="RS256", headers={"kid": kid})


class TestValidateIdToken:
  async def _validate(self, token, jwks, nonce="expected-nonce"):
    with patch.object(oidc_ops, "_fetch_jwks", new=AsyncMock(return_value=jwks)):
      return await validate_id_token(
        token, metadata=METADATA, connection=CONNECTION, nonce=nonce
      )

  async def test_valid_token_returns_claims(self, private_pem, jwks):
    claims = await self._validate(_make_id_token(private_pem), jwks)
    assert claims["sub"] == "okta|user-1"
    assert claims["email"] == "staff@customer.example"

  async def test_nonce_mismatch_rejected(self, private_pem, jwks):
    token = _make_id_token(private_pem, nonce="attacker-nonce")
    with pytest.raises(OIDCTokenInvalidError, match="nonce"):
      await self._validate(token, jwks)

  async def test_missing_nonce_rejected(self, private_pem, jwks):
    token = _make_id_token(private_pem, nonce=None)
    with pytest.raises(OIDCTokenInvalidError, match="nonce"):
      await self._validate(token, jwks)

  async def test_non_string_nonce_rejected_not_raised(self, private_pem, jwks):
    """A non-string nonce claim must reject as OIDCTokenInvalidError, not
    escape as a TypeError from compare_digest (which would 500 the callback)."""
    token = _make_id_token(private_pem, nonce=12345)
    with pytest.raises(OIDCTokenInvalidError, match="nonce"):
      await self._validate(token, jwks)

  async def test_wrong_audience_rejected(self, private_pem, jwks):
    token = _make_id_token(private_pem, aud="some-other-client")
    with pytest.raises(OIDCTokenInvalidError):
      await self._validate(token, jwks)

  async def test_wrong_issuer_rejected(self, private_pem, jwks):
    token = _make_id_token(private_pem, iss="https://evil.example")
    with pytest.raises(OIDCTokenInvalidError):
      await self._validate(token, jwks)

  async def test_expired_token_rejected(self, private_pem, jwks):
    now = datetime.now(UTC)
    token = _make_id_token(
      private_pem, iat=now - timedelta(hours=2), exp=now - timedelta(hours=1)
    )
    with pytest.raises(OIDCTokenInvalidError):
      await self._validate(token, jwks)

  async def test_symmetric_alg_confusion_rejected(self, jwks):
    """An HS256 token must never pass RS256-only validation, even one signed
    with knowledge of the published public key material."""
    now = datetime.now(UTC)
    token = pyjwt.encode(
      {
        "iss": ISSUER,
        "aud": CLIENT_ID,
        "sub": "okta|user-1",
        "nonce": "expected-nonce",
        "iat": now,
        "exp": now + timedelta(minutes=5),
      },
      "shared-secret",
      algorithm="HS256",
      headers={"kid": KID},
    )
    with pytest.raises(OIDCTokenInvalidError):
      await self._validate(token, jwks)

  async def test_unknown_kid_triggers_one_refetch(self, private_pem, jwks):
    """Key rotation: an unknown kid refetches the JWKS once before failing."""
    fetches = AsyncMock(side_effect=[{"keys": []}, jwks])
    with patch.object(oidc_ops, "_fetch_jwks", new=fetches):
      claims = await validate_id_token(
        _make_id_token(private_pem),
        metadata=METADATA,
        connection=CONNECTION,
        nonce="expected-nonce",
      )
    assert claims["sub"] == "okta|user-1"
    assert fetches.call_count == 2
    assert fetches.call_args_list[1].kwargs.get("force") or (
      len(fetches.call_args_list[1].args) > 1 and fetches.call_args_list[1].args[1]
    )


class _FakeRedis:
  def __init__(self, fail=False):
    self.store: dict[str, str] = {}
    self.fail = fail

  def setex(self, key, ttl, value):
    if self.fail:
      raise ConnectionError("valkey down")
    self.store[key] = value

  def getdel(self, key):
    if self.fail:
      raise ConnectionError("valkey down")
    return self.store.pop(key, None)


class TestOIDCState:
  def test_roundtrip_and_single_use(self):
    fake = _FakeRedis()
    with patch.object(oidc_ops, "create_redis_client", return_value=fake):
      state = OIDCState.create(
        nonce="n1",
        code_verifier="cv1",
        return_to="roboledger:/x",
        browser_state="bs1",
      )
      # The raw state token is never stored as a key.
      assert all(state not in key for key in fake.store)

      payload = OIDCState.validate(state)
      assert payload is not None
      assert payload["nonce"] == "n1"
      assert payload["code_verifier"] == "cv1"
      assert payload["return_to"] == "roboledger:/x"
      assert payload["browser_state"] == "bs1"

      # Consumed: a second redemption (replay) fails.
      assert OIDCState.validate(state) is None

  def test_unknown_state_is_invalid(self):
    with patch.object(oidc_ops, "create_redis_client", return_value=_FakeRedis()):
      assert OIDCState.validate("never-issued") is None

  def test_store_down_at_create_raises(self):
    with patch.object(
      oidc_ops, "create_redis_client", return_value=_FakeRedis(fail=True)
    ):
      with pytest.raises(OIDCUnavailableError):
        OIDCState.create(
          nonce="n", code_verifier="cv", return_to=None, browser_state="bs"
        )

  def test_store_down_at_validate_fails_closed(self):
    with patch.object(
      oidc_ops, "create_redis_client", return_value=_FakeRedis(fail=True)
    ):
      assert OIDCState.validate("anything") is None

  def test_malformed_payload_is_invalid(self):
    fake = _FakeRedis()
    with patch.object(oidc_ops, "create_redis_client", return_value=fake):
      state = OIDCState.create(
        nonce="n", code_verifier="cv", return_to=None, browser_state="bs"
      )
      key = next(iter(fake.store))
      fake.store[key] = json.dumps({"return_to": "roboledger"})  # no nonce/verifier
      assert OIDCState.validate(state) is None

  def test_missing_browser_state_is_invalid(self):
    fake = _FakeRedis()
    with patch.object(oidc_ops, "create_redis_client", return_value=fake):
      state = OIDCState.create(
        nonce="n", code_verifier="cv", return_to=None, browser_state="bs"
      )
      key = next(iter(fake.store))
      fake.store[key] = json.dumps({"nonce": "n", "code_verifier": "cv"})
      assert OIDCState.validate(state) is None


class TestParseReturnTo:
  APPS = ["roboledger", "roboinvestor", "robosystems"]

  @pytest.mark.parametrize(
    "raw,expected",
    [
      (None, None),
      ("", None),
      ("roboledger", "roboledger"),
      ("roboledger:/reports/x", "roboledger:/reports/x"),
      ("unknownapp", None),
      ("unknownapp:/x", None),
      ("roboledger://evil.example", None),
      ("roboledger:/ok\\evil", None),
      ("roboledger:relative-no-slash", None),
      ("https://evil.example", None),
      ("roboledger:/x\nSet-Cookie: a=b", None),
    ],
  )
  def test_matrix(self, raw, expected):
    assert parse_return_to(raw, self.APPS) == expected


def _create_user(session, *, external_id=None, is_active=True, password_hash=None):
  user = User.create(
    email=f"idp+{uuid4().hex[:8]}@customer.example",
    name="IdP Staffer",
    password_hash=password_hash,
    session=session,
    external_id=external_id,
  )
  if not is_active:
    user.update(session, is_active=False)
  return user


class TestResolveOidcUser:
  def test_existing_identity_resolves(self, test_db):
    user = _create_user(test_db, external_id="okta-ext-1")
    UserIdentity.create(
      user_id=str(user.id),
      issuer=ISSUER,
      subject="okta|sub-1",
      email_at_link=user.email,
      session=test_db,
    )

    resolution = resolve_oidc_user(
      test_db, issuer=ISSUER, subject="okta|sub-1", email=user.email
    )

    assert resolution.user.id == user.id
    assert resolution.linked is False
    identity = UserIdentity.get_by_issuer_subject(ISSUER, "okta|sub-1", test_db)
    assert identity is not None and identity.last_login_at is not None

  def test_email_match_links_scim_provisioned_user(self, test_db):
    user = _create_user(test_db, external_id="okta-ext-2")

    resolution = resolve_oidc_user(
      test_db, issuer=ISSUER, subject="okta|sub-2", email=user.email.upper()
    )

    assert resolution.user.id == user.id
    assert resolution.linked is True
    identity = UserIdentity.get_by_issuer_subject(ISSUER, "okta|sub-2", test_db)
    assert identity is not None
    assert identity.user_id == user.id

  def test_email_match_refused_without_external_id(self, test_db):
    """The provenance predicate: a locally-created account (invited user,
    future portal guest) must never be bindable by email match alone."""
    user = _create_user(test_db, external_id=None, password_hash="some-hash")

    with pytest.raises(OIDCUserNotProvisionedError):
      resolve_oidc_user(test_db, issuer=ISSUER, subject="okta|sub-3", email=user.email)
    assert UserIdentity.get_by_issuer_subject(ISSUER, "okta|sub-3", test_db) is None

  def test_email_match_refused_when_email_verified_false(self, test_db):
    """An explicitly-unverified email must not link (OIDC Core §5.7)."""
    user = _create_user(test_db, external_id="okta-ext-ev")

    with pytest.raises(OIDCUserNotProvisionedError):
      resolve_oidc_user(
        test_db,
        issuer=ISSUER,
        subject="okta|sub-ev",
        email=user.email,
        email_verified=False,
      )
    assert UserIdentity.get_by_issuer_subject(ISSUER, "okta|sub-ev", test_db) is None

  def test_email_match_links_when_email_verified_absent(self, test_db):
    """Entra omits email_verified entirely — None must still be allowed to
    link (only an explicit False is refused)."""
    user = _create_user(test_db, external_id="okta-ext-ev2")

    resolution = resolve_oidc_user(
      test_db,
      issuer=ISSUER,
      subject="okta|sub-ev2",
      email=user.email,
      email_verified=None,
    )
    assert resolution.user.id == user.id
    assert resolution.linked is True

  def test_unknown_email_refused(self, test_db):
    with pytest.raises(OIDCUserNotProvisionedError):
      resolve_oidc_user(
        test_db, issuer=ISSUER, subject="okta|sub-4", email="nobody@nowhere.example"
      )

  def test_missing_email_claim_refused(self, test_db):
    with pytest.raises(OIDCUserNotProvisionedError):
      resolve_oidc_user(test_db, issuer=ISSUER, subject="okta|sub-5", email=None)

  def test_second_subject_cannot_rebind_by_email(self, test_db):
    """One identity per issuer per user: once linked, a different subject
    presenting the same email must not steal the account."""
    user = _create_user(test_db, external_id="okta-ext-6")
    resolve_oidc_user(test_db, issuer=ISSUER, subject="okta|sub-6a", email=user.email)

    with pytest.raises(OIDCUserNotProvisionedError):
      resolve_oidc_user(test_db, issuer=ISSUER, subject="okta|sub-6b", email=user.email)

  def test_deactivated_user_with_valid_identity_refused(self, test_db):
    """The Okta two-app assignment-drift case: SCIM deactivated the user but
    the OIDC app still authenticates them — the callback must refuse."""
    user = _create_user(test_db, external_id="okta-ext-7")
    UserIdentity.create(
      user_id=str(user.id),
      issuer=ISSUER,
      subject="okta|sub-7",
      email_at_link=user.email,
      session=test_db,
    )
    user.update(test_db, is_active=False)

    with pytest.raises(OIDCUserInactiveError):
      resolve_oidc_user(test_db, issuer=ISSUER, subject="okta|sub-7", email=user.email)

  def test_deactivated_user_cannot_link_by_email(self, test_db):
    user = _create_user(test_db, external_id="okta-ext-8", is_active=False)

    with pytest.raises(OIDCUserNotProvisionedError):
      resolve_oidc_user(test_db, issuer=ISSUER, subject="okta|sub-8", email=user.email)


class TestDiscoveryMetadata:
  async def test_issuer_mismatch_rejected(self):
    oidc_ops._metadata_cache.clear()
    response = AsyncMock()
    response.json = lambda: {"issuer": "https://impostor.example"}
    response.raise_for_status = lambda: None

    client = AsyncMock()
    client.get.return_value = response
    client.__aenter__.return_value = client

    with patch.object(oidc_ops.httpx, "AsyncClient", return_value=client):
      with pytest.raises(OIDCTokenInvalidError, match="issuer mismatch"):
        await oidc_ops.fetch_server_metadata(ISSUER)

  async def test_metadata_is_cached(self):
    oidc_ops._metadata_cache[ISSUER] = (
      time.monotonic(),
      {"issuer": ISSUER, "authorization_endpoint": "cached"},
    )
    metadata = await oidc_ops.fetch_server_metadata(ISSUER)
    assert metadata["authorization_endpoint"] == "cached"
    oidc_ops._metadata_cache.clear()
