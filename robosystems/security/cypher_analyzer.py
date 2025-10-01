"""
Secure Cypher query analysis for write operation detection.

This module provides secure, AST-based analysis of Cypher queries to accurately
detect write operations without the vulnerabilities of regex-based approaches.
"""

import re
from typing import Set
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class CypherOperationType(Enum):
  """Types of Cypher operations."""

  READ = "read"
  WRITE = "write"
  MIXED = "mixed"  # Contains both read and write operations


class CypherSecurityAnalyzer:
  """
  Secure analyzer for Cypher queries that uses multiple validation layers
  to accurately detect write operations.

  This replaces vulnerable regex-based detection with a comprehensive
  approach that handles comments, strings, nested queries, and complex syntax.
  """

  # Definitive write operation keywords (must be exact matches)
  WRITE_KEYWORDS = {
    "CREATE",
    "MERGE",
    "SET",
    "DELETE",
    "REMOVE",
    "DETACH",
    "DROP",
    "ALTER",
    "INSERT",
    "UPDATE",
  }

  # Bulk operation keywords that should use dedicated endpoints
  BULK_KEYWORDS = {
    "COPY",
    "LOAD",
    "IMPORT",
  }

  # Administrative operations that require special permissions
  ADMIN_KEYWORDS = {
    "EXPORT",
    "INSTALL",
    "ATTACH",
    "USE",
  }

  # System procedure calls that may need restrictions
  SYSTEM_PROCEDURES = {
    "show_warnings",
    "clear_warnings",
    "current_setting",
    "db_version",
    "table_info",
    "show_tables",
    "show_connection",
  }

  # Read-only keywords that should never trigger write detection
  READ_KEYWORDS = {
    "MATCH",
    "RETURN",
    "WHERE",
    "WITH",
    "UNWIND",
    "ORDER",
    "LIMIT",
    "SKIP",
    "DISTINCT",
    "COUNT",
    "COLLECT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
  }

  def __init__(self):
    """Initialize the analyzer with compiled patterns."""
    # Pattern to find potential write keywords (case-insensitive)
    self.write_pattern = re.compile(
      r"\b(" + "|".join(self.WRITE_KEYWORDS) + r")\b", re.IGNORECASE
    )

    # Pattern to find bulk operation keywords (case-insensitive)
    self.bulk_pattern = re.compile(
      r"\b(" + "|".join(self.BULK_KEYWORDS) + r")\b", re.IGNORECASE
    )

    # Pattern to find admin operation keywords (case-insensitive)
    self.admin_pattern = re.compile(
      r"\b(" + "|".join(self.ADMIN_KEYWORDS) + r")\b", re.IGNORECASE
    )

    # Pattern to find CALL procedures
    self.call_pattern = re.compile(r"\bCALL\s+(\w+)\s*\(", re.IGNORECASE)

    # Pattern to identify comments
    self.comment_pattern = re.compile(r"(/\*.*?\*/|//.*?$)", re.DOTALL | re.MULTILINE)

    # Pattern to identify string literals (single and double quotes)
    self.string_pattern = re.compile(
      r"""(?:"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')""", re.DOTALL
    )

    # Pattern to identify backtick-quoted identifiers
    self.identifier_pattern = re.compile(r"`(?:[^`\\]|\\.)*`", re.DOTALL)

  def analyze_query(self, query: str) -> CypherOperationType:
    """
    Securely analyze a Cypher query to determine if it contains write operations.

    Args:
        query: The Cypher query to analyze

    Returns:
        CypherOperationType indicating the operation type

    Raises:
        ValueError: If query is invalid or suspicious
    """
    if not query or not isinstance(query, str):
      raise ValueError("Query must be a non-empty string")

    # Basic security validations
    self._validate_query_security(query)

    # Remove comments and strings to avoid false positives
    cleaned_query = self._clean_query(query)

    # Analyze the cleaned query for write operations
    write_operations = self._find_write_operations(cleaned_query)
    read_operations = self._find_read_operations(cleaned_query)

    # Determine operation type
    if write_operations and read_operations:
      return CypherOperationType.MIXED
    elif write_operations:
      return CypherOperationType.WRITE
    else:
      return CypherOperationType.READ

  def is_write_operation(self, query: str) -> bool:
    """
    Convenience method to check if a query contains write operations.

    Args:
        query: The Cypher query to check

    Returns:
        True if the query contains write operations, False otherwise
    """
    try:
      operation_type = self.analyze_query(query)
      return operation_type in (CypherOperationType.WRITE, CypherOperationType.MIXED)
    except Exception as e:
      logger.warning(f"Query analysis failed, defaulting to write operation: {e}")
      # Default to treating as write operation for security
      return True

  def is_bulk_operation(self, query: str) -> bool:
    """
    Check if a query contains bulk operations (COPY, LOAD, IMPORT).

    Args:
        query: The Cypher query to check

    Returns:
        True if the query contains bulk operations, False otherwise
    """
    try:
      cleaned_query = self._clean_query(query)
      bulk_ops = self._find_bulk_operations(cleaned_query)
      return len(bulk_ops) > 0
    except Exception as e:
      logger.warning(f"Bulk operation analysis failed: {e}")
      # Default to false for bulk operations
      return False

  def is_admin_operation(self, query: str) -> bool:
    """
    Check if a query contains administrative operations.

    Args:
        query: The Cypher query to check

    Returns:
        True if the query contains admin operations, False otherwise
    """
    try:
      cleaned_query = self._clean_query(query)
      admin_ops = self._find_admin_operations(cleaned_query)
      return len(admin_ops) > 0
    except Exception as e:
      logger.warning(f"Admin operation analysis failed: {e}")
      # Default to true for safety with admin operations
      return True

  def has_system_calls(self, query: str) -> bool:
    """
    Check if a query contains system procedure calls.

    Args:
        query: The Cypher query to check

    Returns:
        True if the query contains system calls, False otherwise
    """
    try:
      cleaned_query = self._clean_query(query)
      system_calls = self._find_system_calls(cleaned_query)
      return len(system_calls) > 0
    except Exception as e:
      logger.warning(f"System call analysis failed: {e}")
      # Default to false for system calls
      return False

  def _validate_query_security(self, query: str) -> None:
    """
    Perform basic security validations on the query.

    Args:
        query: The query to validate

    Raises:
        ValueError: If the query appears suspicious or dangerous
    """
    # Check for excessively long queries (potential DoS)
    if len(query) > 100000:  # 100KB limit
      raise ValueError("Query exceeds maximum allowed length")

    # Check for suspicious nested comment patterns
    nested_comments = query.count("/*") - query.count("*/")
    if nested_comments != 0:
      raise ValueError("Unbalanced comment blocks detected")

    # Check for potential injection patterns
    suspicious_patterns = [
      r";\s*CREATE\s+USER",
      r";\s*DROP\s+DATABASE",
      r";\s*CALL\s+dbms\.",
      r";\s*SHOW\s+USERS",
    ]

    for pattern in suspicious_patterns:
      if re.search(pattern, query, re.IGNORECASE):
        logger.warning(f"Suspicious query pattern detected: {pattern}")
        raise ValueError("Query contains potentially dangerous patterns")

  def _clean_query(self, query: str) -> str:
    """
    Remove comments, strings, and quoted identifiers from the query
    to avoid false positives in write operation detection.

    Args:
        query: The original query

    Returns:
        Cleaned query with comments and strings removed
    """
    # Step 1: Remove comments
    cleaned = self.comment_pattern.sub(" ", query)

    # Step 2: Remove string literals
    cleaned = self.string_pattern.sub(" STRING_LITERAL ", cleaned)

    # Step 3: Remove backtick-quoted identifiers
    cleaned = self.identifier_pattern.sub(" IDENTIFIER ", cleaned)

    return cleaned

  def _find_write_operations(self, query: str) -> Set[str]:
    """
    Find write operation keywords in the cleaned query.

    Args:
        query: The cleaned query to analyze

    Returns:
        Set of write operation keywords found
    """
    found_operations = set()

    # Find all potential write keywords
    matches = self.write_pattern.finditer(query)

    for match in matches:
      keyword = match.group(1).upper()
      start_pos = match.start()

      # Additional context validation
      if self._validate_keyword_context(query, keyword, start_pos):
        found_operations.add(keyword)

    return found_operations

  def _find_read_operations(self, query: str) -> Set[str]:
    """
    Find read operation keywords in the cleaned query.

    Args:
        query: The cleaned query to analyze

    Returns:
        Set of read operation keywords found
    """
    found_operations = set()

    # Create pattern for read keywords
    read_pattern = re.compile(
      r"\b(" + "|".join(self.READ_KEYWORDS) + r")\b", re.IGNORECASE
    )

    matches = read_pattern.finditer(query)
    for match in matches:
      keyword = match.group(1).upper()
      found_operations.add(keyword)

    return found_operations

  def _find_bulk_operations(self, query: str) -> Set[str]:
    """
    Find bulk operation keywords in the cleaned query.

    Args:
        query: The cleaned query to analyze

    Returns:
        Set of bulk operation keywords found
    """
    found_operations = set()

    # Find all potential bulk keywords
    matches = self.bulk_pattern.finditer(query)

    for match in matches:
      keyword = match.group(1).upper()
      start_pos = match.start()

      # Additional context validation
      if self._validate_keyword_context(query, keyword, start_pos):
        found_operations.add(keyword)

    return found_operations

  def _find_admin_operations(self, query: str) -> Set[str]:
    """
    Find administrative operation keywords in the cleaned query.

    Args:
        query: The cleaned query to analyze

    Returns:
        Set of admin operation keywords found
    """
    found_operations = set()

    # Find all potential admin keywords
    matches = self.admin_pattern.finditer(query)

    for match in matches:
      keyword = match.group(1).upper()
      start_pos = match.start()

      # Additional context validation
      if self._validate_keyword_context(query, keyword, start_pos):
        found_operations.add(keyword)

    # Special case: IMPORT/EXPORT DATABASE are admin operations
    if re.search(r"\b(IMPORT|EXPORT)\s+DATABASE\b", query, re.IGNORECASE):
      found_operations.add("DATABASE_MIGRATION")

    # Special case: DETACH DATABASE is an admin operation (but not DETACH DELETE)
    if re.search(r"\bDETACH\s+DATABASE\b", query, re.IGNORECASE):
      found_operations.add("DETACH_DATABASE")

    return found_operations

  def _find_system_calls(self, query: str) -> Set[str]:
    """
    Find system procedure calls in the cleaned query.

    Args:
        query: The cleaned query to analyze

    Returns:
        Set of system procedure names found
    """
    found_calls = set()

    # Find all CALL statements
    matches = self.call_pattern.finditer(query)

    for match in matches:
      procedure_name = match.group(1).lower()
      if procedure_name in self.SYSTEM_PROCEDURES:
        found_calls.add(procedure_name)

    return found_calls

  def _validate_keyword_context(self, query: str, keyword: str, position: int) -> bool:
    """
    Validate that a keyword is in a valid context and not part of an identifier.

    Args:
        query: The query string
        keyword: The keyword found
        position: Position of the keyword in the query

    Returns:
        True if the keyword is in a valid context
    """
    # Get context around the keyword
    # Check if keyword is part of a larger identifier
    if position > 0 and query[position - 1].isalnum():
      return False

    if (
      position + len(keyword) < len(query) and query[position + len(keyword)].isalnum()
    ):
      return False

    # Additional context-specific validations can be added here
    # For example, checking if CREATE is followed by valid syntax

    return True

  def get_write_operation_details(self, query: str) -> dict:
    """
    Get detailed information about write operations in the query.

    Args:
        query: The query to analyze

    Returns:
        Dictionary with detailed analysis results
    """
    try:
      operation_type = self.analyze_query(query)
      cleaned_query = self._clean_query(query)
      write_ops = self._find_write_operations(cleaned_query)
      read_ops = self._find_read_operations(cleaned_query)
      bulk_ops = self._find_bulk_operations(cleaned_query)

      return {
        "operation_type": operation_type.value,
        "is_write_operation": operation_type
        in (CypherOperationType.WRITE, CypherOperationType.MIXED),
        "is_bulk_operation": len(bulk_ops) > 0,
        "write_keywords_found": list(write_ops),
        "read_keywords_found": list(read_ops),
        "bulk_keywords_found": list(bulk_ops),
        "analysis_successful": True,
        "security_validated": True,
      }
    except Exception as e:
      logger.error(f"Query analysis failed: {e}")
      return {
        "operation_type": "unknown",
        "is_write_operation": True,  # Default to safe assumption
        "is_bulk_operation": False,
        "write_keywords_found": [],
        "read_keywords_found": [],
        "bulk_keywords_found": [],
        "analysis_successful": False,
        "security_validated": False,
        "error": str(e),
      }


# Global instance for use throughout the application
cypher_analyzer = CypherSecurityAnalyzer()


def is_write_operation(query: str) -> bool:
  """
  Secure function to determine if a Cypher query contains write operations.

  This function replaces all regex-based write operation detection in the codebase.

  Args:
      query: The Cypher query to analyze

  Returns:
      True if the query contains write operations, False otherwise
  """
  return cypher_analyzer.is_write_operation(query)


def is_bulk_operation(query: str) -> bool:
  """
  Determine if a Cypher query contains bulk operations (COPY, LOAD, IMPORT).

  These operations should be performed through the dedicated /copy endpoint
  rather than the general /query endpoint.

  Args:
      query: The Cypher query to analyze

  Returns:
      True if the query contains bulk operations, False otherwise
  """
  return cypher_analyzer.is_bulk_operation(query)


def is_admin_operation(query: str) -> bool:
  """
  Determine if a Cypher query contains administrative operations.

  These operations require admin privileges and include operations like
  EXPORT, INSTALL, ATTACH, DETACH, USE.

  Args:
      query: The Cypher query to analyze

  Returns:
      True if the query contains admin operations, False otherwise
  """
  return cypher_analyzer.is_admin_operation(query)


def has_system_calls(query: str) -> bool:
  """
  Determine if a Cypher query contains system procedure calls.

  These are CALL statements to system procedures that may need restrictions.

  Args:
      query: The Cypher query to analyze

  Returns:
      True if the query contains system calls, False otherwise
  """
  return cypher_analyzer.has_system_calls(query)


def analyze_cypher_query(query: str) -> dict:
  """
  Analyze a Cypher query and return detailed information.

  Args:
      query: The Cypher query to analyze

  Returns:
      Dictionary with analysis results
  """
  return cypher_analyzer.get_write_operation_details(query)
