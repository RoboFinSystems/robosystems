"""Fixtures for the OAuth authorization-server kernel tests."""

from unittest.mock import patch

import pytest


class FakeRedis:
  """The three Valkey verbs the OAuth stores use, in memory."""

  def __init__(self):
    self.store: dict[str, str] = {}

  def setex(self, key, ttl, value):
    self.store[key] = value
    return True

  def get(self, key):
    return self.store.get(key)

  def getdel(self, key):
    return self.store.pop(key, None)


@pytest.fixture
def fake_redis():
  redis = FakeRedis()
  with patch(
    "robosystems.operations.oauth_server.authorization.create_redis_client",
    return_value=redis,
  ):
    yield redis


@pytest.fixture
def oauth_env():
  """A deterministic issuer for canonical-resource assertions."""
  app_urls = {
    "robosystems": "https://app.test.example",
    "roboledger": "https://ledger.test.example",
    "roboinvestor": "https://investor.test.example",
  }
  with (
    patch("robosystems.operations.oauth_server.resources.env") as env,
    patch("robosystems.operations.oauth_server.authorization.env") as authz_env,
    patch("robosystems.routers.auth.utils.Config.get_app_urls", return_value=app_urls),
  ):
    env.ROBOSYSTEMS_API_URL = "https://api.test.example"
    env.ROBOSYSTEMS_URL = "https://app.test.example"
    authz_env.LOGIN_HOME_APP = "robosystems"
    yield env
