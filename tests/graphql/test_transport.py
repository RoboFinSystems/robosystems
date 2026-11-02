"""The extensions GraphQL endpoint is served over HTTP only."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from main import app

pytestmark = pytest.mark.unit

GRAPHQL_PATH = "/extensions/kg0123456789abcdef/graphql"


def test_a_websocket_upgrade_is_refused_cleanly():
  with pytest.raises(WebSocketDisconnect):
    with TestClient(app).websocket_connect(GRAPHQL_PATH):
      pytest.fail("the GraphQL endpoint accepted a WebSocket")


def test_queries_still_run_over_http():
  response = TestClient(app).post(GRAPHQL_PATH, json={"query": "{ __typename }"})

  assert response.status_code == 200
  assert response.json()["data"]["__typename"] == "Query"
