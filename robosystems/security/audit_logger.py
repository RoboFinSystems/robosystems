"""
Security Audit Logger

Provides structured logging for security events including authentication failures,
authorization violations, and suspicious activities.
"""

import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from enum import Enum

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


class SecurityAuditLogger:
  """Centralized security audit logging."""

  @staticmethod
  def log_security_event(
    event_type: SecurityEventType,
    user_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    endpoint: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    risk_level: str = "medium",
  ):
    """
    Log a security event with structured data.

    Args:
        event_type: Type of security event
        user_id: User identifier (if available)
        ip_address: Client IP address
        user_agent: Client user agent
        endpoint: API endpoint accessed
        details: Additional event details
        risk_level: Risk level (low, medium, high, critical)
    """
    # Skip audit logging in dev environment if disabled
    environment = env.ENVIRONMENT.lower()
    audit_enabled = env.SECURITY_AUDIT_ENABLED

    if environment == "dev" and not audit_enabled:
      return

    audit_data = {
      "timestamp": datetime.now(timezone.utc).isoformat(),
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

  @staticmethod
  def log_auth_failure(
    reason: str,
    user_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    endpoint: Optional[str] = None,
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
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
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
    ip_address: Optional[str] = None,
    endpoint: Optional[str] = None,
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
    user_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    endpoint: Optional[str] = None,
    limit_type: str = "api",
    user_agent: Optional[str] = None,
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
    user_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    endpoint: Optional[str] = None,
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
    user_id: Optional[str] = None,
    ip_address: Optional[str] = None,
    endpoint: Optional[str] = None,
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
    balance_before: Optional[float] = None,
    balance_after: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None,
    ip_address: Optional[str] = None,
    endpoint: Optional[str] = None,
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
  def log_suspicious_activity(
    user_id: Optional[str] = None,
    activity_type: str = "",
    description: str = "",
    ip_address: Optional[str] = None,
    endpoint: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
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


# Convenience function for backward compatibility
def log_security_event(event_type: SecurityEventType, **kwargs):
  """Convenience function for logging security events."""
  return SecurityAuditLogger.log_security_event(event_type, **kwargs)
