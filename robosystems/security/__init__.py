"""
Security utilities for RoboSystems.

This package contains security-related utilities including:
- Secure Cypher query analysis (cypher_analyzer.py)
- Secure error handling and information disclosure prevention (error_handling.py)
- Data encryption for backups (encryption.py)
- Authentication and authorization helpers
- Input validation and sanitization
"""

from .cypher_analyzer import is_write_operation, analyze_cypher_query
from .error_handling import (
  ErrorType,
  raise_secure_error,
  handle_exception_securely,
  classify_exception,
  is_safe_to_expose,
  sanitize_error_detail,
)
from .audit_logger import SecurityAuditLogger, SecurityEventType
from .encryption import encrypt_data, decrypt_data, generate_encryption_key

__all__ = [
  "is_write_operation",
  "analyze_cypher_query",
  "ErrorType",
  "raise_secure_error",
  "handle_exception_securely",
  "classify_exception",
  "is_safe_to_expose",
  "sanitize_error_detail",
  "SecurityAuditLogger",
  "SecurityEventType",
  "encrypt_data",
  "decrypt_data",
  "generate_encryption_key",
]
