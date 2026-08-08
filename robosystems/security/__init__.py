"""
Security utilities for RoboSystems.

- `cypher_analyzer.py` — Cypher read/write classification (the query write barrier)
- `error_handling.py` — generic client errors, full detail to logs only
- `audit_logger.py` — structured security events + CloudWatch alerting metrics
- `auth_protection.py`, `captcha.py`, `password.py`, `device_fingerprinting.py`,
  `input_validation.py` — authentication and input hardening
"""

from .audit_logger import SecurityAuditLogger, SecurityEventType
from .cypher_analyzer import analyze_cypher_query, is_write_operation
from .error_handling import (
  ErrorType,
  classify_exception,
  handle_exception_securely,
  is_safe_to_expose,
  raise_secure_error,
  sanitize_error_detail,
)

__all__ = [
  "ErrorType",
  "SecurityAuditLogger",
  "SecurityEventType",
  "analyze_cypher_query",
  "classify_exception",
  "handle_exception_securely",
  "is_safe_to_expose",
  "is_write_operation",
  "raise_secure_error",
  "sanitize_error_detail",
]
