"""
Security utilities for RoboSystems.

This package contains security-related utilities including:
- Secure Cypher query analysis (cypher_analyzer.py)
- Secure error handling and information disclosure prevention (error_handling.py)
- Data encryption for backups (encryption.py)
- Authentication and authorization helpers
- Input validation and sanitization
"""

from .audit_logger import SecurityAuditLogger, SecurityEventType
from .cypher_analyzer import analyze_cypher_query, is_write_operation
from .encryption import decrypt_data, encrypt_data, generate_encryption_key
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
  "decrypt_data",
  "encrypt_data",
  "generate_encryption_key",
  "handle_exception_securely",
  "is_safe_to_expose",
  "is_write_operation",
  "raise_secure_error",
  "sanitize_error_detail",
]
