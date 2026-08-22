"""Tests for the admin-surface action audit trail.

Covers both halves of the trail: ``SecurityAuditLogger.log_admin_action``,
which shapes the record, and the ``SecurityLoggingMiddleware`` branch that
decides which requests earn one.

The middleware tests run through a real ASGI stack on purpose. The branch
depends on ``request.state.admin_key_id`` — set inside the route by
``AdminAuthMiddleware`` — being visible to middleware *after* ``call_next``
returns. That works because Starlette backs ``request.state`` with
``scope["state"]``, and it is exactly the kind of framework assumption that a
mocked Request would assert away instead of verifying.
"""

import json
from unittest.mock import patch

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from robosystems.middleware.logging import SecurityLoggingMiddleware
from robosystems.security.audit_logger import (
  _ADMIN_FLAGGABLE_EVENTS,
  _METRIC_FOR_EVENT,
  SecurityAuditLogger,
  SecurityEventType,
)


def _build_app() -> FastAPI:
  """An app whose routes stand in for the admin surface and the public API."""
  app = FastAPI()
  app.add_middleware(SecurityLoggingMiddleware)

  @app.get("/admin/v1/subscriptions")
  async def authenticated_admin_read(request: Request):
    request.state.admin_key_id = "admin"
    return {"ok": True}

  @app.post("/admin/v1/invoices/inv_123/mark-paid")
  async def authenticated_admin_write(request: Request):
    request.state.admin_key_id = "admin"
    return {"ok": True}

  @app.get("/admin/v1/users/boom")
  async def authenticated_admin_failure(request: Request):
    request.state.admin_key_id = "admin"
    from fastapi import HTTPException

    raise HTTPException(status_code=403, detail="nope")

  @app.get("/admin/v1/webhooks/stripe")
  async def unauthenticated_admin_path():
    """Stands in for the Stripe webhook: admin path, no admin credential."""
    return {"ok": True}

  @app.get("/v1/graphs")
  async def public_route():
    return {"ok": True}

  return app


class TestAdminActionRecordShape:
  """``log_admin_action`` records the actor, the action and its outcome."""

  def test_emits_admin_action_event_with_attribution(self):
    with patch("robosystems.security.audit_logger.logger") as mock_logger:
      SecurityAuditLogger.log_admin_action(
        admin_key_id="admin",
        method="POST",
        endpoint="/admin/v1/invoices/inv_123/mark-paid",
        status_code=200,
        ip_address="10.0.0.4",
        user_agent="robosystems-admin-cli",
      )

    payload = json.loads(
      mock_logger.warning.call_args[0][0].removeprefix("SECURITY_AUDIT: ")
    )

    assert payload["event_type"] == "admin_action"
    assert payload["user_id"] == "admin"
    assert payload["endpoint"] == "/admin/v1/invoices/inv_123/mark-paid"
    assert payload["ip_address"] == "10.0.0.4"
    assert payload["details"]["method"] == "POST"
    assert payload["details"]["status_code"] == 200
    assert payload["details"]["success"] is True
    assert payload["details"]["admin_key_id"] == "admin"

  def test_failed_action_is_still_recorded(self):
    """A 500 on the admin surface belongs in the trail as much as a 200."""
    with patch("robosystems.security.audit_logger.logger") as mock_logger:
      SecurityAuditLogger.log_admin_action(
        admin_key_id="admin",
        method="DELETE",
        endpoint="/admin/v1/users/u_1",
        status_code=500,
      )

    payload = json.loads(
      mock_logger.warning.call_args[0][0].removeprefix("SECURITY_AUDIT: ")
    )
    assert payload["details"]["success"] is False
    assert payload["details"]["status_code"] == 500

  def test_reads_and_writes_carry_different_risk(self):
    for method, expected in (("GET", "low"), ("POST", "medium"), ("DELETE", "medium")):
      with patch("robosystems.security.audit_logger.logger") as mock_logger:
        SecurityAuditLogger.log_admin_action(
          admin_key_id="admin",
          method=method,
          endpoint="/admin/v1/users",
          status_code=200,
        )
      payload = json.loads(
        mock_logger.warning.call_args[0][0].removeprefix("SECURITY_AUDIT: ")
      )
      assert payload["risk_level"] == expected, method

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  def test_publishes_no_metric(self, mock_publish):
    """Evidence, not an alert — the admin surface is not internet-reachable,
    so request volume here is not a detective-control signal."""
    SecurityAuditLogger.log_admin_action(
      admin_key_id="admin",
      method="GET",
      endpoint="/admin/v1/users",
      status_code=200,
    )
    mock_publish.assert_called_once_with([])

  def test_admin_action_is_not_flaggable_as_auth_failure(self):
    """The ``details["admin"]`` flag must not drag ADMIN_ACTION into the
    FailedAdminAuth alarm, which would fire on ordinary admin work."""
    assert SecurityEventType.ADMIN_ACTION not in _ADMIN_FLAGGABLE_EVENTS
    assert SecurityEventType.ADMIN_ACTION not in _METRIC_FOR_EVENT


class TestAdminActionMiddlewareBranch:
  """The middleware records authenticated admin requests, and only those."""

  def test_authenticated_admin_request_is_recorded(self):
    with patch.object(SecurityAuditLogger, "log_admin_action") as mock_log:
      TestClient(_build_app()).get("/admin/v1/subscriptions")

    mock_log.assert_called_once()
    kwargs = mock_log.call_args.kwargs
    assert kwargs["admin_key_id"] == "admin"
    assert kwargs["method"] == "GET"
    assert kwargs["endpoint"] == "/admin/v1/subscriptions"
    assert kwargs["status_code"] == 200

  def test_write_records_literal_path_including_identifiers(self):
    """Which invoice was marked paid is the point of the record."""
    with patch.object(SecurityAuditLogger, "log_admin_action") as mock_log:
      TestClient(_build_app()).post("/admin/v1/invoices/inv_123/mark-paid")

    kwargs = mock_log.call_args.kwargs
    assert kwargs["endpoint"] == "/admin/v1/invoices/inv_123/mark-paid"
    assert kwargs["method"] == "POST"

  def test_authenticated_request_that_fails_is_recorded(self):
    with patch.object(SecurityAuditLogger, "log_admin_action") as mock_log:
      TestClient(_build_app()).get("/admin/v1/users/boom")

    mock_log.assert_called_once()
    assert mock_log.call_args.kwargs["status_code"] == 403

  def test_admin_path_without_credential_is_not_recorded(self):
    """Auth failures are recorded — and alarmed — by log_admin_auth_failure.
    Logging them here too would double-count them in the audit stream."""
    with patch.object(SecurityAuditLogger, "log_admin_action") as mock_log:
      TestClient(_build_app()).get("/admin/v1/webhooks/stripe")

    mock_log.assert_not_called()

  def test_non_admin_path_is_not_recorded(self):
    with patch.object(SecurityAuditLogger, "log_admin_action") as mock_log:
      TestClient(_build_app()).get("/v1/graphs")

    mock_log.assert_not_called()

  def test_query_parameters_are_redacted(self):
    with patch.object(SecurityAuditLogger, "log_admin_action") as mock_log:
      TestClient(_build_app()).get(
        "/admin/v1/subscriptions",
        params={"api_key": "sk_live_secret", "status": "active"},
      )

    query = mock_log.call_args.kwargs["query"]
    assert "sk_live_secret" not in query
    assert "api_key=REDACTED" in query
    assert "status=active" in query

  def test_absent_query_is_none_not_empty_string(self):
    with patch.object(SecurityAuditLogger, "log_admin_action") as mock_log:
      TestClient(_build_app()).get("/admin/v1/subscriptions")

    assert mock_log.call_args.kwargs["query"] is None
