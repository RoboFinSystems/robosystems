"""Request-scoped identity: the request id and the authenticated principal
reach the audit trail below the route handler, and the access log names the
caller."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.security.request_context import (
  RequestPrincipal,
  audit_context,
  bind_principal,
  bind_request_id,
  current_principal,
  current_request_id,
  publish_principal,
  reset_principal,
  reset_request_id,
)


class TestPublishPrincipal:
  def test_publishes_to_state_and_context(self):
    request = SimpleNamespace(state=SimpleNamespace())
    principal = publish_principal(
      request, "usr_1", "api_key", api_key="rfs_abcdefghijklmnop"
    )
    try:
      assert request.state.user_id == "usr_1"
      assert request.state.auth_user_id == "usr_1"
      assert request.state.auth_method == "api_key"
      assert request.state.api_key_prefix == "rfs_abcd"
      assert current_principal() == principal
      assert principal.api_key_prefix == "rfs_abcd"
    finally:
      bind_principal(None)

  def test_jwt_principal_has_no_key_prefix(self):
    request = SimpleNamespace(state=SimpleNamespace())
    principal = publish_principal(request, "usr_2", "jwt_token")
    try:
      assert principal.api_key_prefix is None
      assert not hasattr(request.state, "api_key_prefix")
      assert audit_context() == {"auth_method": "jwt_token"}
    finally:
      bind_principal(None)

  def test_audit_context_is_empty_outside_a_request(self):
    assert current_request_id() is None
    assert current_principal() is None
    assert audit_context() == {}

  def test_audit_context_carries_request_id_and_credential(self):
    rid = bind_request_id("req_1")
    ptoken = bind_principal(
      RequestPrincipal("usr_3", "api_key", api_key_prefix="rfs_1234")
    )
    try:
      assert audit_context() == {
        "request_id": "req_1",
        "auth_method": "api_key",
        "api_key_prefix": "rfs_1234",
      }
    finally:
      reset_principal(ptoken)
      reset_request_id(rid)
    assert audit_context() == {}


class TestContextPropagation:
  @pytest.mark.asyncio
  async def test_follows_the_request_into_a_runner_thread(self):
    """`run_off_loop` copies the request context into the worker thread, so
    an operation running there still sees the principal."""
    from robosystems.middleware.operations import run_off_loop

    token = bind_principal(RequestPrincipal("usr_t", "api_key", "rfs_tttt"))
    try:
      seen = await run_off_loop(current_principal)
    finally:
      reset_principal(token)
    assert seen is not None and seen.user_id == "usr_t"

  @pytest.mark.asyncio
  async def test_does_not_leak_between_concurrent_tasks(self):
    async def one_request(user_id: str) -> str | None:
      bind_principal(RequestPrincipal(user_id, "jwt_token"))
      await asyncio.sleep(0.01)
      principal = current_principal()
      return principal.user_id if principal else None

    seen = await asyncio.gather(one_request("a"), one_request("b"))
    assert seen == ["a", "b"]
    assert current_principal() is None


class TestSecurityEventsCarryTheContext:
  @patch("robosystems.security.audit_logger._publish_security_metrics")
  @patch("robosystems.security.audit_logger.env")
  @patch("robosystems.security.audit_logger.logger")
  def test_authorization_denied_carries_request_id_and_key(
    self, mock_logger, mock_env, _publish
  ):
    mock_env.ENVIRONMENT = "prod"
    mock_env.SECURITY_AUDIT_ENABLED = True
    rid = bind_request_id("req_9")
    ptoken = bind_principal(RequestPrincipal("usr_9", "api_key", "rfs_9999"))
    try:
      SecurityAuditLogger.log_authorization_denied(
        user_id="usr_9", resource="graph_database:kg1", action="read"
      )
    finally:
      reset_principal(ptoken)
      reset_request_id(rid)
    line = mock_logger.warning.call_args.args[0]
    payload = json.loads(line.removeprefix("SECURITY_AUDIT: "))
    assert payload["event_type"] == SecurityEventType.AUTHORIZATION_DENIED.value
    assert payload["user_id"] == "usr_9"
    assert payload["request_id"] == "req_9"
    assert payload["api_key_prefix"] == "rfs_9999"
    assert payload["auth_method"] == "api_key"

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  @patch("robosystems.security.audit_logger.env")
  @patch("robosystems.security.audit_logger.logger")
  def test_outside_a_request_the_fields_are_absent(self, mock_logger, mock_env, _p):
    mock_env.ENVIRONMENT = "prod"
    mock_env.SECURITY_AUDIT_ENABLED = True
    SecurityAuditLogger.log_authorization_denied(
      user_id="usr_0", resource="graph_database:kg1", action="read"
    )
    payload = json.loads(
      mock_logger.warning.call_args.args[0].removeprefix("SECURITY_AUDIT: ")
    )
    assert "request_id" not in payload
    assert "api_key_prefix" not in payload


class TestAccessLogAttribution:
  """The access-log line reads the caller after the route ran, and the
  request id bound by the middleware is visible inside the route."""

  def _app(self) -> FastAPI:
    from robosystems.middleware.logging import StructuredLoggingMiddleware

    app = FastAPI()
    app.add_middleware(StructuredLoggingMiddleware)

    @app.get("/v1/things")
    async def things(request: Request):
      publish_principal(request, "usr_route", "api_key", api_key="rfs_routeKEY")
      return {"request_id_in_route": current_request_id()}

    return app

  def test_user_id_is_read_after_the_route_ran(self):
    with patch("robosystems.middleware.logging.log_api") as mock_log_api:
      response = TestClient(self._app()).get("/v1/things")
    assert response.status_code == 200
    kwargs = mock_log_api.call_args.kwargs
    assert kwargs["user_id"] == "usr_route"
    assert kwargs["request_id"] == response.headers["X-Request-ID"]
    assert response.json()["request_id_in_route"] == response.headers["X-Request-ID"]

  def test_request_id_binding_is_reset_after_the_request(self):
    with patch("robosystems.middleware.logging.log_api"):
      TestClient(self._app()).get("/v1/things")
    assert current_request_id() is None
