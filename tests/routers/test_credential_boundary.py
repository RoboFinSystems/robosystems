"""Routes that hand out access take a signed-in session, never an API key."""

import pytest

from robosystems.middleware.auth.jwt import create_jwt_token
from robosystems.models.core import UserAPIKey

ORG = "org_boundary"
GRAPH = "kg0123456789abcdef01"

GRANTING_ROUTES = [
  ("post", "/v1/user/api-keys", {"name": "second key"}),
  ("post", f"/v1/orgs/{ORG}/invitations", {"email": "new@example.com"}),
  ("post", f"/v1/orgs/{ORG}/invitations/inv_1/resend", None),
  ("put", f"/v1/orgs/{ORG}/members/user_1", {"role": "admin"}),
  ("post", f"/v1/graphs/{GRAPH}/members", {"user_id": "user_1", "role": "admin"}),
  ("put", f"/v1/graphs/{GRAPH}/members/user_1", {"role": "admin"}),
]


@pytest.fixture
def api_key(test_db, test_user) -> str:
  _, plain_key = UserAPIKey.create(
    user_id=test_user.id, name="boundary", session=test_db
  )
  return plain_key


@pytest.mark.parametrize(
  ("method", "path", "body"), GRANTING_ROUTES, ids=[r[1] for r in GRANTING_ROUTES]
)
def test_api_key_is_refused(client, api_key, method, path, body):
  # The key itself is good: the refusal is the route's, not the credential's.
  assert client.get("/v1/user", headers={"X-API-Key": api_key}).status_code == 200

  response = client.request(method, path, json=body, headers={"X-API-Key": api_key})

  assert response.status_code == 401, response.text
  assert "API keys are not accepted" in response.json()["detail"]
  assert response.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.parametrize(
  ("method", "path", "body"), GRANTING_ROUTES, ids=[r[1] for r in GRANTING_ROUTES]
)
def test_session_passes_authentication(client, test_user, method, path, body):
  headers = {"Authorization": f"Bearer {create_jwt_token(test_user.id)}"}

  response = client.request(method, path, json=body, headers=headers)

  # Whatever the route decides next (403, 404, 501), it is not an auth refusal.
  assert response.status_code != 401, response.text


# Reading, and taking access away, stay open to a key.
KEY_ROUTES = [
  ("get", "/v1/user/api-keys"),
  ("delete", "/v1/user/api-keys/uak_missing"),
  ("get", f"/v1/orgs/{ORG}/members"),
  ("delete", f"/v1/orgs/{ORG}/members/user_1"),
  ("get", f"/v1/orgs/{ORG}/invitations"),
  ("delete", f"/v1/orgs/{ORG}/invitations/inv_1"),
  ("get", f"/v1/graphs/{GRAPH}/members"),
  ("delete", f"/v1/graphs/{GRAPH}/members/user_1"),
]


@pytest.mark.parametrize(
  ("method", "path"), KEY_ROUTES, ids=[f"{m} {p}" for m, p in KEY_ROUTES]
)
def test_api_key_still_reads_and_removes(client, api_key, method, path):
  response = client.request(method, path, headers={"X-API-Key": api_key})

  # Whatever the route decides next (403, 404, 501), the key authenticated.
  assert response.status_code != 401, response.text
