"""
Security Audit Logger

Provides structured logging for security events including authentication failures,
authorization violations, and suspicious activities.
"""

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from ..config import env
from ..logger import logger


class SecurityEventType(Enum):
  """Security event types for audit logging."""

  AUTH_FAILURE = "auth_failure"
  AUTH_SUCCESS = "auth_success"
  AUTH_TOKEN_EXPIRED = "auth_token_expired"
  AUTH_TOKEN_INVALID = "auth_token_invalid"
  API_KEY_INVALID = "api_key_invalid"
  API_KEY_EXPIRED = "api_key_expired"
  AUTHORIZATION_DENIED = "authorization_denied"
  RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
  SUSPICIOUS_ACTIVITY = "suspicious_activity"
  INPUT_VALIDATION_FAILURE = "input_validation_failure"
  INJECTION_ATTEMPT = "injection_attempt"
  PRIVILEGE_ESCALATION_ATTEMPT = "privilege_escalation_attempt"
  FINANCIAL_TRANSACTION = "financial_transaction"
  # New event types for S3 security
  INVALID_INPUT = "invalid_input"
  PATH_TRAVERSAL_ATTEMPT = "path_traversal_attempt"
  # Event types for data operations
  DATA_IMPORT = "data_import"
  OPERATION_TIMEOUT = "operation_timeout"
  OPERATION_FAILED = "operation_failed"
  # Email and password reset events
  EMAIL_SENT = "email_sent"
  EMAIL_VERIFIED = "email_verified"
  PASSWORD_RESET_REQUESTED = "password_reset_requested"
  PASSWORD_RESET_COMPLETED = "password_reset_completed"
  # Token management events
  TOKEN_REFRESH = "token_refresh"
  # OIDC login refused after the IdP authenticated (unprovisioned or
  # deactivated user) — alarm-worthy, unlike ordinary auth noise
  OIDC_LOGIN_DENIED = "oidc_login_denied"
  # SCIM provisioning lifecycle (operational; only the auth failure alarms)
  SCIM_USER_PROVISIONED = "scim_user_provisioned"
  SCIM_USER_UPDATED = "scim_user_updated"
  SCIM_USER_DEACTIVATED = "scim_user_deactivated"
  SCIM_USER_REACTIVATED = "scim_user_reactivated"
  SCIM_AUTH_FAILURE = "scim_auth_failure"
  # Passkey MFA lifecycle (operational; only MFA_FAILED alarms — a spike is
  # the second-factor brute-force / phishing-relay signal)
  PASSKEY_ENROLLED = "passkey_enrolled"
  PASSKEY_REMOVED = "passkey_removed"
  MFA_CHALLENGE_ISSUED = "mfa_challenge_issued"
  MFA_VERIFIED = "mfa_verified"
  MFA_FAILED = "mfa_failed"
  MFA_RECOVERY_USED = "mfa_recovery_used"
  # Subgraph events
  SUBGRAPH_CREATED = "subgraph_created"
  SUBGRAPH_DELETED = "subgraph_deleted"
  # Graph lifecycle events
  GRAPH_DELETED = "graph_deleted"
  # Shared-repository query telemetry
  QUERY_ABUSE_SIGNAL = "query_abuse_signal"
  QUERY_ENGINE_DISRUPTION = "query_engine_disruption"


# CloudWatch metrics: the compliance/alerting-relevant subset of security
# events is published to RoboSystems/Security/{env} so the detective-control
# alarms (cloudformation/api.yaml, graph-ladybug-replicas.yaml) can actually
# fire. Operational events (email_sent, auth_success, operation_timeout, …)
# are deliberately excluded to keep the metric stream low-volume and the
# alarms meaningful.
_METRIC_FOR_EVENT: dict[SecurityEventType, str] = {
  SecurityEventType.AUTH_FAILURE: "AuthFailure",
  SecurityEventType.AUTH_TOKEN_EXPIRED: "AuthFailure",
  SecurityEventType.AUTH_TOKEN_INVALID: "AuthFailure",
  SecurityEventType.API_KEY_INVALID: "AuthFailure",
  SecurityEventType.API_KEY_EXPIRED: "AuthFailure",
  SecurityEventType.AUTHORIZATION_DENIED: "AuthorizationDenied",
  SecurityEventType.OIDC_LOGIN_DENIED: "AuthorizationDenied",
  SecurityEventType.SCIM_AUTH_FAILURE: "AuthFailure",
  SecurityEventType.MFA_FAILED: "AuthFailure",
  SecurityEventType.INJECTION_ATTEMPT: "InjectionAttempt",
  SecurityEventType.PATH_TRAVERSAL_ATTEMPT: "InjectionAttempt",
  SecurityEventType.PRIVILEGE_ESCALATION_ATTEMPT: "PrivilegeEscalationAttempt",
  SecurityEventType.QUERY_ABUSE_SIGNAL: "QueryAbuseSignal",
  SecurityEventType.QUERY_ENGINE_DISRUPTION: "QueryEngineDisruption",
}

# Emitted in addition to AuthFailure when the failure is on the admin surface,
# so FailedAdminAuthAlarm has a dedicated, low-noise trigger.
_ADMIN_AUTH_FAILURE_METRIC = "FailedAdminAuth"

# Auth-failure event types that count as an admin failure when details.admin is set.
_ADMIN_FLAGGABLE_EVENTS = frozenset(
  {
    SecurityEventType.AUTH_FAILURE,
    SecurityEventType.API_KEY_INVALID,
    SecurityEventType.AUTH_TOKEN_INVALID,
  }
)

# Metric publishing runs OFF the request path. log_security_event is called
# synchronously from async auth handlers (get_current_user, AdminAuthMiddleware),
# so a blocking put_metric_data would stall the whole event loop — worst during
# an auth-failure spike, which is exactly when these alarms matter. Publish on a
# small bounded thread pool instead, with short client timeouts, and shed load
# past _METRIC_MAX_INFLIGHT rather than let work queue without bound under a flood.
_METRIC_MAX_INFLIGHT = 256

_cloudwatch_client: Any = None
_metric_executor: ThreadPoolExecutor | None = None
_metric_inflight = 0
_metric_lock = threading.Lock()


def _get_cloudwatch_client() -> Any:
  global _cloudwatch_client
  if _cloudwatch_client is None:
    import boto3
    from botocore.config import Config

    # Short timeouts + no retries: a stalled CloudWatch endpoint must fail fast,
    # never hold a pool thread (or, historically, the event loop) for ~60s.
    _cloudwatch_client = boto3.client(
      "cloudwatch",
      region_name=env.AWS_REGION,
      config=Config(connect_timeout=2, read_timeout=3, retries={"max_attempts": 1}),
    )
  return _cloudwatch_client


def _get_metric_executor() -> ThreadPoolExecutor:
  global _metric_executor
  if _metric_executor is None:
    _metric_executor = ThreadPoolExecutor(
      max_workers=2, thread_name_prefix="security-metrics"
    )
  return _metric_executor


def _put_metric_data(namespace: str, metric_names: list[str]) -> None:
  """Blocking CloudWatch publish — runs on the metric pool, never the caller."""
  global _metric_inflight
  try:
    _get_cloudwatch_client().put_metric_data(
      Namespace=namespace,
      MetricData=[
        {"MetricName": name, "Value": 1, "Unit": "Count"} for name in metric_names
      ],
    )
  except Exception as e:
    # Metrics are best-effort; a publish failure must not surface anywhere.
    logger.debug(f"Failed to publish security metric(s) {metric_names}: {e}")
  finally:
    with _metric_lock:
      _metric_inflight -= 1


def _publish_security_metrics(metric_names: list[str]) -> None:
  """Queue security alerting metrics for CloudWatch, off the request path.

  Best-effort and prod/staging-only: skipped where there is no CloudWatch or
  credentials (dev/test). The actual publish runs on a bounded thread pool so it
  never blocks the (often async) auth path, and load is shed past
  ``_METRIC_MAX_INFLIGHT`` so a flood can't grow the queue without bound. Never
  raises into the caller.
  """
  global _metric_inflight
  if not metric_names:
    return
  if not (env.is_production() or env.is_staging()):
    return
  with _metric_lock:
    if _metric_inflight >= _METRIC_MAX_INFLIGHT:
      return
    _metric_inflight += 1
  namespace = f"RoboSystems/Security/{env.ENVIRONMENT}"
  try:
    _get_metric_executor().submit(_put_metric_data, namespace, list(metric_names))
  except Exception as e:
    # Executor rejected the task (e.g. interpreter shutdown) — release the slot.
    with _metric_lock:
      _metric_inflight -= 1
    logger.debug(f"Failed to queue security metric(s) {metric_names}: {e}")


class SecurityAuditLogger:
  """Centralized security audit logging."""

  @staticmethod
  def log_security_event(
    event_type: SecurityEventType,
    user_id: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
    endpoint: str | None = None,
    details: dict[str, Any] | None = None,
    risk_level: str = "medium",
  ):
    """
    Log a security event as structured JSON, and publish its CloudWatch metric.

    ``risk_level`` is one of low, medium, high, critical. Setting
    ``details["admin"]`` on an auth-failure event also emits the dedicated
    admin-surface metric.
    """
    environment = env.ENVIRONMENT.lower()
    audit_enabled = env.SECURITY_AUDIT_ENABLED

    if environment == "dev" and not audit_enabled:
      return

    audit_data = {
      "timestamp": datetime.now(UTC).isoformat(),
      "event_type": event_type.value,
      "risk_level": risk_level,
      "user_id": user_id,
      "ip_address": ip_address,
      "user_agent": user_agent,
      "endpoint": endpoint,
      "details": details or {},
    }

    # Log as structured JSON for security monitoring
    logger.warning(f"SECURITY_AUDIT: {json.dumps(audit_data)}")

    # Publish the alerting metric for the compliance-relevant subset so the
    # CloudWatch detective-control alarms can fire (no-op outside prod/staging).
    metric_names: list[str] = []
    metric = _METRIC_FOR_EVENT.get(event_type)
    if metric:
      metric_names.append(metric)
    if (details or {}).get("admin") and event_type in _ADMIN_FLAGGABLE_EVENTS:
      metric_names.append(_ADMIN_AUTH_FAILURE_METRIC)
    _publish_security_metrics(metric_names)

  @staticmethod
  def log_admin_auth_failure(
    reason: str,
    ip_address: str | None = None,
    endpoint: str | None = None,
    user_agent: str | None = None,
  ):
    """Log an admin-surface authentication failure.

    Flags the event as admin so a dedicated ``FailedAdminAuth`` CloudWatch
    metric is emitted alongside the generic ``AuthFailure`` signal, giving
    ``FailedAdminAuthAlarm`` a low-noise trigger.
    """
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_FAILURE,
      ip_address=ip_address,
      user_agent=user_agent,
      endpoint=endpoint,
      details={"admin": True, "failure_reason": reason},
      risk_level="high",
    )

  @staticmethod
  def log_auth_failure(
    reason: str,
    user_id: str | None = None,
    ip_address: str | None = None,
    user_agent: str | None = None,
    endpoint: str | None = None,
  ):
    """Log authentication failure."""
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_FAILURE,
      user_id=user_id,
      ip_address=ip_address,
      user_agent=user_agent,
      endpoint=endpoint,
      details={"failure_reason": reason},
      risk_level="high",
    )

  @staticmethod
  def log_auth_success(
    user_id: str,
    ip_address: str | None = None,
    user_agent: str | None = None,
    auth_method: str = "api_key",
  ):
    """Log successful authentication."""
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      user_id=user_id,
      ip_address=ip_address,
      user_agent=user_agent,
      details={"auth_method": auth_method},
      risk_level="low",
    )

  @staticmethod
  def log_authorization_denied(
    user_id: str,
    resource: str,
    action: str,
    ip_address: str | None = None,
    endpoint: str | None = None,
  ):
    """Log authorization denial."""
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTHORIZATION_DENIED,
      user_id=user_id,
      ip_address=ip_address,
      endpoint=endpoint,
      details={"resource": resource, "action": action},
      risk_level="medium",
    )

  @staticmethod
  def log_rate_limit_exceeded(
    user_id: str | None = None,
    ip_address: str | None = None,
    endpoint: str | None = None,
    limit_type: str = "api",
    user_agent: str | None = None,
  ):
    """Log rate limit violation."""
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.RATE_LIMIT_EXCEEDED,
      user_id=user_id,
      ip_address=ip_address,
      endpoint=endpoint,
      user_agent=user_agent,
      details={"limit_type": limit_type},
      risk_level="medium",
    )

  @staticmethod
  def log_injection_attempt(
    user_id: str | None = None,
    ip_address: str | None = None,
    endpoint: str | None = None,
    payload: str = "",
    injection_type: str = "sql",
  ):
    """Log potential injection attempt."""
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.INJECTION_ATTEMPT,
      user_id=user_id,
      ip_address=ip_address,
      endpoint=endpoint,
      details={
        "injection_type": injection_type,
        "payload_snippet": payload[:100] if payload else "",  # Limit payload size
      },
      risk_level="critical",
    )

  @staticmethod
  def log_input_validation_failure(
    user_id: str | None = None,
    ip_address: str | None = None,
    endpoint: str | None = None,
    field_name: str = "",
    invalid_value: str = "",
    validation_error: str = "",
  ):
    """Log input validation failure."""
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.INPUT_VALIDATION_FAILURE,
      user_id=user_id,
      ip_address=ip_address,
      endpoint=endpoint,
      details={
        "field_name": field_name,
        "invalid_value": invalid_value[:50]
        if invalid_value
        else "",  # Limit value size
        "validation_error": validation_error,
      },
      risk_level="medium",
    )

  @staticmethod
  def log_financial_transaction(
    user_id: str,
    transaction_type: str,
    amount: float,
    balance_before: float | None = None,
    balance_after: float | None = None,
    metadata: dict[str, Any] | None = None,
    ip_address: str | None = None,
    endpoint: str | None = None,
  ):
    """Log financial transaction for audit trail."""
    details = {
      "transaction_type": transaction_type,
      "amount": amount,
      "balance_before": balance_before,
      "balance_after": balance_after,
    }
    if metadata:
      details.update(metadata)

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.FINANCIAL_TRANSACTION,
      user_id=user_id,
      ip_address=ip_address,
      endpoint=endpoint,
      details=details,
      risk_level="medium" if amount < 1000 else "high",
    )

  @staticmethod
  def log_query_abuse_signal(
    user_id: str | None,
    graph_id: str,
    signal: str,
    api_key_prefix: str | None = None,
    endpoint: str | None = None,
    metadata: dict[str, Any] | None = None,
    disruption: bool = False,
  ):
    """Log a shared-repository query outcome that counts toward abuse detection.

    ``signal`` names the outcome class (e.g. ``timeout``, ``admission_reject``,
    ``rate_limited``, ``analyzer_reject``, ``write_denied``). Set ``disruption``
    for engine-connection-loss outcomes, which publish their own metric so they
    can alarm at a lower threshold than routine pressure signals.
    """
    details: dict[str, Any] = {"graph_id": graph_id, "signal": signal}
    if api_key_prefix:
      details["api_key_prefix"] = api_key_prefix
    if metadata:
      details.update(metadata)

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.QUERY_ENGINE_DISRUPTION
      if disruption
      else SecurityEventType.QUERY_ABUSE_SIGNAL,
      user_id=user_id,
      endpoint=endpoint,
      details=details,
      risk_level="critical" if disruption else "medium",
    )

  @staticmethod
  def log_suspicious_activity(
    user_id: str | None = None,
    activity_type: str = "",
    description: str = "",
    ip_address: str | None = None,
    endpoint: str | None = None,
    metadata: dict[str, Any] | None = None,
  ):
    """Log suspicious activity."""
    details = {
      "activity_type": activity_type,
      "description": description,
    }
    if metadata:
      details.update(metadata)

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.SUSPICIOUS_ACTIVITY,
      user_id=user_id,
      ip_address=ip_address,
      endpoint=endpoint,
      details=details,
      risk_level="high",
    )


def log_security_event(event_type: SecurityEventType, **kwargs):
  """Module-level shorthand for :meth:`SecurityAuditLogger.log_security_event`."""
  return SecurityAuditLogger.log_security_event(event_type, **kwargs)
