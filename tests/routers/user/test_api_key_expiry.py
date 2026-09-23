"""A key created with an expiry authenticates until that expiry."""

from datetime import UTC, datetime, timedelta

import pytest

from robosystems.middleware.auth.jwt import create_jwt_token


@pytest.mark.parametrize("suffix", ["Z", ""], ids=["utc-offset", "no-offset"])
def test_key_with_future_expiry_authenticates(client, test_user, suffix):
  expires = (datetime.now(UTC) + timedelta(days=30)).replace(tzinfo=None)
  created = client.post(
    "/v1/user/api-keys",
    json={"name": "expiring", "expires_at": expires.isoformat() + suffix},
    headers={"Authorization": f"Bearer {create_jwt_token(test_user.id)}"},
  )
  assert created.status_code == 201, created.text

  with_key = client.get("/v1/user", headers={"X-API-Key": created.json()["key"]})
  assert with_key.status_code == 200, with_key.text
