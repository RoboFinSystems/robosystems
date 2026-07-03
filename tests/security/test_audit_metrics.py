"""Tests for security audit metric emission.

Covers the CloudWatch metric side of ``SecurityAuditLogger`` — the detective-
control signal that powers the alarms in ``cloudformation/api.yaml`` — and the
admin auth-failure path that feeds the ``FailedAdminAuth`` metric.
"""

from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.security.audit_logger import (
  SecurityAuditLogger,
  SecurityEventType,
  _publish_security_metrics,
)


class TestSecurityMetricMapping:
  """``log_security_event`` maps the compliance subset to CloudWatch metrics."""

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  def test_injection_attempt_emits_metric(self, mock_publish):
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.INJECTION_ATTEMPT,
    )
    mock_publish.assert_called_once_with(["InjectionAttempt"])

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  def test_path_traversal_maps_to_injection(self, mock_publish):
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.PATH_TRAVERSAL_ATTEMPT,
    )
    mock_publish.assert_called_once_with(["InjectionAttempt"])

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  def test_authorization_denied_emits_metric(self, mock_publish):
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTHORIZATION_DENIED,
    )
    mock_publish.assert_called_once_with(["AuthorizationDenied"])

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  def test_privilege_escalation_emits_metric(self, mock_publish):
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.PRIVILEGE_ESCALATION_ATTEMPT,
    )
    mock_publish.assert_called_once_with(["PrivilegeEscalationAttempt"])

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  def test_auth_failure_variants_map_to_auth_failure(self, mock_publish):
    for event_type in (
      SecurityEventType.AUTH_FAILURE,
      SecurityEventType.API_KEY_INVALID,
      SecurityEventType.API_KEY_EXPIRED,
      SecurityEventType.AUTH_TOKEN_EXPIRED,
      SecurityEventType.AUTH_TOKEN_INVALID,
    ):
      mock_publish.reset_mock()
      SecurityAuditLogger.log_security_event(event_type=event_type)
      mock_publish.assert_called_once_with(["AuthFailure"])

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  def test_operational_events_emit_no_metric(self, mock_publish):
    for event_type in (
      SecurityEventType.EMAIL_SENT,
      SecurityEventType.AUTH_SUCCESS,
      SecurityEventType.OPERATION_TIMEOUT,
      SecurityEventType.RATE_LIMIT_EXCEEDED,
    ):
      mock_publish.reset_mock()
      SecurityAuditLogger.log_security_event(event_type=event_type)
      mock_publish.assert_called_once_with([])

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  def test_admin_flag_adds_failed_admin_metric(self, mock_publish):
    SecurityAuditLogger.log_admin_auth_failure(
      reason="invalid_admin_key",
      ip_address="203.0.113.7",
      endpoint="/admin/v1/graphs",
    )
    mock_publish.assert_called_once_with(["AuthFailure", "FailedAdminAuth"])

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  def test_non_admin_auth_failure_omits_admin_metric(self, mock_publish):
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_FAILURE,
      details={"failure_reason": "bad password"},
    )
    mock_publish.assert_called_once_with(["AuthFailure"])

  @patch("robosystems.security.audit_logger._publish_security_metrics")
  def test_admin_flag_ignored_for_non_auth_events(self, mock_publish):
    # A stray admin flag on an unrelated event must not synthesize FailedAdminAuth.
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.INJECTION_ATTEMPT,
      details={"admin": True},
    )
    mock_publish.assert_called_once_with(["InjectionAttempt"])


class TestPublishSecurityMetrics:
  """``_publish_security_metrics`` is prod/staging-only and best-effort."""

  @patch("robosystems.security.audit_logger.env")
  def test_skipped_when_not_prod_or_staging(self, mock_env):
    mock_env.is_production.return_value = False
    mock_env.is_staging.return_value = False
    with patch(
      "robosystems.security.audit_logger._get_cloudwatch_client"
    ) as mock_client:
      _publish_security_metrics(["AuthFailure"])
      mock_client.assert_not_called()

  @patch("robosystems.security.audit_logger.env")
  def test_empty_metric_list_is_noop(self, mock_env):
    mock_env.is_production.return_value = True
    mock_env.is_staging.return_value = False
    with patch(
      "robosystems.security.audit_logger._get_cloudwatch_client"
    ) as mock_client:
      _publish_security_metrics([])
      mock_client.assert_not_called()

  @patch("robosystems.security.audit_logger.env")
  def test_publishes_to_security_namespace_in_production(self, mock_env):
    mock_env.is_production.return_value = True
    mock_env.is_staging.return_value = False
    mock_env.ENVIRONMENT = "prod"
    cw = Mock()
    with patch(
      "robosystems.security.audit_logger._get_cloudwatch_client", return_value=cw
    ):
      _publish_security_metrics(["AuthFailure", "FailedAdminAuth"])
    cw.put_metric_data.assert_called_once()
    kwargs = cw.put_metric_data.call_args.kwargs
    assert kwargs["Namespace"] == "RoboSystems/Security/prod"
    names = {m["MetricName"] for m in kwargs["MetricData"]}
    assert names == {"AuthFailure", "FailedAdminAuth"}
    assert all(m["Value"] == 1 and m["Unit"] == "Count" for m in kwargs["MetricData"])

  @patch("robosystems.security.audit_logger.env")
  def test_publishes_in_staging(self, mock_env):
    mock_env.is_production.return_value = False
    mock_env.is_staging.return_value = True
    mock_env.ENVIRONMENT = "staging"
    cw = Mock()
    with patch(
      "robosystems.security.audit_logger._get_cloudwatch_client", return_value=cw
    ):
      _publish_security_metrics(["InjectionAttempt"])
    assert cw.put_metric_data.call_args.kwargs["Namespace"] == (
      "RoboSystems/Security/staging"
    )

  @patch("robosystems.security.audit_logger.env")
  def test_publish_failure_is_swallowed(self, mock_env):
    mock_env.is_production.return_value = True
    mock_env.is_staging.return_value = False
    mock_env.ENVIRONMENT = "prod"
    cw = Mock()
    cw.put_metric_data.side_effect = RuntimeError("cloudwatch unavailable")
    with patch(
      "robosystems.security.audit_logger._get_cloudwatch_client", return_value=cw
    ):
      # Must not raise — metrics are best-effort and never break the caller.
      _publish_security_metrics(["AuthFailure"])


class TestAdminAuthFailurePath:
  """``AdminAuthMiddleware`` records + metricizes invalid admin key attempts."""

  async def test_invalid_admin_key_logs_and_raises(self):
    from robosystems.middleware.auth.admin import AdminAuthMiddleware

    mw = AdminAuthMiddleware()
    request = Mock()
    request.headers = {"Authorization": "Bearer wrong-key", "User-Agent": "pytest"}
    request.client.host = "203.0.113.7"
    request.url.path = "/admin/v1/graphs"

    with (
      patch.object(mw, "verify_admin_key", return_value=None),
      patch(
        "robosystems.middleware.auth.admin.SecurityAuditLogger.log_admin_auth_failure"
      ) as mock_log,
    ):
      with pytest.raises(HTTPException) as exc_info:
        await mw(request)

    assert exc_info.value.status_code == 401
    mock_log.assert_called_once()
    kwargs = mock_log.call_args.kwargs
    assert kwargs["reason"] == "invalid_admin_key"
    assert kwargs["ip_address"] == "203.0.113.7"
    assert kwargs["endpoint"] == "/admin/v1/graphs"

  async def test_valid_admin_key_does_not_log_failure(self):
    from robosystems.middleware.auth.admin import AdminAuthMiddleware

    mw = AdminAuthMiddleware()
    request = Mock()
    request.headers = {"Authorization": "Bearer good-key", "User-Agent": "pytest"}
    request.client.host = "203.0.113.7"
    request.url.path = "/admin/v1/graphs"

    with (
      patch.object(
        mw,
        "verify_admin_key",
        return_value={"key_id": "admin", "name": "Admin API Key", "permissions": ["*"]},
      ),
      patch(
        "robosystems.middleware.auth.admin.SecurityAuditLogger.log_admin_auth_failure"
      ) as mock_log,
    ):
      await mw(request)

    mock_log.assert_not_called()
