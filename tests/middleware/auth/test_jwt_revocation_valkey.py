"""Revocation against a live Valkey: an entry must cover the refresh window."""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import jwt
import pytest
import redis

from robosystems.config import env
from robosystems.config.constants import (
  JWT_REVOCATION_KEY_PREFIX,
  TOKEN_GRACE_PERIOD_MINUTES,
)
from robosystems.middleware.auth.jwt import is_jwt_token_revoked, revoke_jwt_token


@pytest.fixture
def live_valkey():
  client = redis.Redis.from_url(
    "redis://:valkey@localhost:6379/15", decode_responses=True
  )
  try:
    client.ping()
  except Exception:
    pytest.skip("Valkey not reachable")
  with patch("robosystems.middleware.auth.jwt.get_redis_client", return_value=client):
    yield client
  client.close()


def _token(exp: datetime) -> tuple[str, str]:
  jti = uuid.uuid4().hex
  payload = {
    "user_id": "usr_revocation",
    "jti": jti,
    "type": "access",
    "exp": exp,
    "iat": exp - timedelta(minutes=30),
  }
  return jwt.encode(payload, env.JWT_SECRET_KEY, algorithm="HS256"), jti


@pytest.mark.unit
class TestRevocationCoversRefreshWindow:
  def test_expired_token_inside_refresh_window_is_revoked(self, live_valkey):
    token, jti = _token(datetime.now(UTC) - timedelta(minutes=1))

    assert revoke_jwt_token(token, reason="user_logout")

    assert live_valkey.exists(f"{JWT_REVOCATION_KEY_PREFIX}{jti}")
    assert is_jwt_token_revoked(token)

  def test_entry_outlives_exp_by_the_refresh_window(self, live_valkey):
    token, jti = _token(datetime.now(UTC) + timedelta(minutes=10))

    assert revoke_jwt_token(token, reason="user_logout")

    ttl = live_valkey.ttl(f"{JWT_REVOCATION_KEY_PREFIX}{jti}")
    assert ttl >= 10 * 60 + TOKEN_GRACE_PERIOD_MINUTES * 60 - 5

  def test_token_past_refresh_window_writes_nothing(self, live_valkey):
    token, jti = _token(
      datetime.now(UTC) - timedelta(minutes=TOKEN_GRACE_PERIOD_MINUTES + 5)
    )

    assert revoke_jwt_token(token, reason="user_logout")

    assert not live_valkey.exists(f"{JWT_REVOCATION_KEY_PREFIX}{jti}")
