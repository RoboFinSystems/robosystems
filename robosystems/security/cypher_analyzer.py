"""
Cypher query analysis for write operation detection.

The sole write barrier for the Cypher query surface: every read-only path
classifies its query here before execution. Analysis fails closed — an
unparseable query, or a `CALL` form outside the read-only allowlist, is
treated as a write.
"""

import logging
import re
from enum import Enum

logger = logging.getLogger(__name__)


class CypherOperationType(Enum):
  """Types of Cypher operations."""

  READ = "read"
  WRITE = "write"
  MIXED = "mixed"  # Contains both read and write operations


class CypherSecurityAnalyzer:
  """
  Classifies Cypher queries as read, write, or mixed.

  Comments, string literals, and backtick-quoted identifiers are masked before
  any keyword matching, so data can never hide code from the classifier.
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

  # Schema DDL operations that modify graph structure
  SCHEMA_DDL_KEYWORDS = {
    "CREATE NODE TABLE",
    "CREATE REL TABLE",
    "DROP NODE TABLE",
    "DROP REL TABLE",
    "ALTER TABLE",
    "ADD COLUMN",
    "DROP COLUMN",
    "RENAME TABLE",
    "RENAME COLUMN",
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

  # Procedures permitted on a read-only path. This is an ALLOWLIST and the
  # classifier fails closed: a `CALL` to anything not named here is treated
  # as a write, because the engine's procedure surface includes index DDL
  # and session-configuration verbs that the keyword patterns below cannot
  # see (those keywords are matched on `\b` word boundaries, and `_` is a
  # word character, so an underscore-joined procedure name never matches).
  #
  # Keep this list minimal. A legitimate read procedure that is missing
  # shows up as a refused query — safe, visible, and a one-line fix. The
  # inverse mistake is silent.
  READ_ONLY_PROCEDURES = {
    "show_tables",
    "table_info",
    "db_version",
    "current_setting",
    "show_connection",
    "show_warnings",
    "show_indexes",
    "show_functions",
    "query_vector_index",
    "query_fts_index",
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

    # Pattern to find CALL procedures. Dots are allowed so namespaced
    # procedure names resolve as one identifier rather than matching on
    # their prefix alone.
    self.call_pattern = re.compile(r"\bCALL\s+([\w.]+)\s*\(", re.IGNORECASE)

    # `CALL <setting> = <value>` is the session-configuration form. It takes
    # no parentheses, so the procedure pattern above never sees it.
    self.call_assignment_pattern = re.compile(r"\bCALL\s+([\w.]+)\s*=", re.IGNORECASE)

    # Comments, string literals, and backtick identifiers are masked by the
    # single-pass scanner in `_clean_query` (not by regex) so an in-string
    # comment marker can't hide a trailing write keyword.

  def analyze_query(self, query: str) -> CypherOperationType:
    """
    Classify a Cypher query as READ, WRITE, or MIXED.

    Raises:
        ValueError: If the query is empty, oversized, or matches an injection
            pattern.
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
    """Check whether a query contains write operations."""
    try:
      operation_type = self.analyze_query(query)
      return operation_type in (CypherOperationType.WRITE, CypherOperationType.MIXED)
    except Exception as e:
      logger.warning(f"Query analysis failed, defaulting to write operation: {e}")
      # Default to treating as write operation for security
      return True

  def is_schema_ddl(self, query: str) -> bool:
    """Check whether a query contains schema DDL that modifies graph structure."""
    try:
      cleaned_query = self._clean_query(query)
      schema_ops = self._find_schema_ddl(cleaned_query)
      return len(schema_ops) > 0
    except Exception as e:
      logger.warning(f"Schema DDL analysis failed: {e}")
      return False

  def is_bulk_operation(self, query: str) -> bool:
    """Check whether a query contains bulk operations (COPY, LOAD, IMPORT)."""
    try:
      cleaned_query = self._clean_query(query)
      bulk_ops = self._find_bulk_operations(cleaned_query)
      return len(bulk_ops) > 0
    except Exception as e:
      logger.warning(f"Bulk operation analysis failed: {e}")
      # Default to false for bulk operations
      return False

  def is_admin_operation(self, query: str) -> bool:
    """Check whether a query contains administrative operations."""
    try:
      cleaned_query = self._clean_query(query)
      admin_ops = self._find_admin_operations(cleaned_query)
      return len(admin_ops) > 0
    except Exception as e:
      logger.warning(f"Admin operation analysis failed: {e}")
      # Default to true for safety with admin operations
      return True

  def is_non_read_call(self, query: str) -> bool:
    """Check if a query contains CALL forms that are not read-only.

    True for procedure invocations outside READ_ONLY_PROCEDURES and for
    session-configuration assignments. Exists for validators that gate on
    the operation *family* (bulk / admin / DDL) rather than on
    `is_write_operation`, so they can still refuse the CALL surface without
    refusing ordinary graph writes.
    """
    try:
      cleaned_query = self._clean_query(query)
      return len(self._find_call_operations(cleaned_query)) > 0
    except Exception as e:
      logger.warning(f"CALL analysis failed, defaulting to non-read: {e}")
      return True

  def has_system_calls(self, query: str) -> bool:
    """Check whether a query calls a procedure in SYSTEM_PROCEDURES."""
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
    Mask comments, string literals, and backtick-quoted identifiers so that
    keyword detection only ever sees *code*, not data.

    This is a single left-to-right scan that decides context (string vs.
    comment vs. identifier vs. code) at each position. A staged
    strip-comments-then-strings approach is unsafe: a ``//`` (or ``/*``)
    sequence *inside* a string literal would be treated as a comment and eat a
    real write keyword that follows the closed string
    (e.g. ``... WHERE n.x = '//' CREATE (m) ...``), causing a write to be
    misclassified as a read. Scanning once, quotes toggle string context
    before any comment marker is honoured, so an in-string ``//`` can never
    hide the code after the string.

    Comments are blanked and strings/identifiers become neutral placeholder
    tokens.
    """
    out: list[str] = []
    i = 0
    n = len(query)
    while i < n:
      ch = query[i]

      # Line comment: // ... to end of line
      if ch == "/" and i + 1 < n and query[i + 1] == "/":
        nl = query.find("\n", i)
        i = n if nl == -1 else nl
        out.append(" ")
        continue

      # Block comment: /* ... */  (unbalanced -> consume to end; the security
      # pre-check already rejects unbalanced blocks fail-closed)
      if ch == "/" and i + 1 < n and query[i + 1] == "*":
        end = query.find("*/", i + 2)
        i = n if end == -1 else end + 2
        out.append(" ")
        continue

      # String literal: '...' or "..."  (backslash escapes the next char)
      if ch == "'" or ch == '"':
        quote = ch
        i += 1
        while i < n:
          c = query[i]
          if c == "\\":
            i += 2
            continue
          i += 1
          if c == quote:
            break
        out.append(" STRING_LITERAL ")
        continue

      # Backtick-quoted identifier. Kuzu/openCypher do NOT use backslash
      # escaping inside backtick identifiers: a backslash is a literal
      # character and the identifier closes at the next backtick (an escaped
      # backtick is written by doubling it). Treating backslash as an escape
      # here diverged from the engine lexer, letting ``... AS `x\` SET ...``
      # mask a trailing write keyword — the analyzer swallowed everything
      # after the escaped backtick while Kuzu closed the identifier and
      # executed the rest. Close at the FIRST backtick and do not honour
      # backslash. This is conservative w.r.t. doubled-backtick identifiers
      # (the analyzer may split them into two tokens), which only ever
      # over-classifies a write, never hides one.
      if ch == "`":
        i += 1
        while i < n:
          c = query[i]
          i += 1
          if c == "`":
            break
        out.append(" IDENTIFIER ")
        continue

      out.append(ch)
      i += 1

    return "".join(out)

  def _find_write_operations(self, query: str) -> set[str]:
    """Find write operation keywords in the cleaned query."""
    found_operations = set()

    # Find all potential write keywords
    matches = self.write_pattern.finditer(query)

    for match in matches:
      keyword = match.group(1).upper()
      start_pos = match.start()

      # Additional context validation
      if self._validate_keyword_context(query, keyword, start_pos):
        found_operations.add(keyword)

    # `CALL` is a verb the keyword patterns above cannot classify, so it is
    # scanned separately. Folding the result in here (rather than into a
    # separate public predicate) is deliberate: `is_write_operation` and
    # `analyze_query` are the two gates every surface actually calls, and
    # both read from this set.
    found_operations |= self._find_call_operations(query)

    return found_operations

  def _find_call_operations(self, query: str) -> set[str]:
    """Find `CALL` forms that must not be treated as reads.

    Two forms, neither reachable by the word-boundary keyword patterns:

    - ``CALL <name>(...)`` — a procedure invocation. Fails closed: anything
      outside ``READ_ONLY_PROCEDURES`` counts as a write, since the engine's
      procedure surface includes index DDL whose names are underscore-joined
      and therefore invisible to a ``\\b``-anchored keyword match.
    - ``CALL <name> = <value>`` — session configuration. Always a write: it
      mutates connection state that outlives the statement, and connections
      are pooled and shared.

    Returns normalized markers (``CALL:<name>`` / ``CALL_SET:<name>``).
    """
    found: set[str] = set()

    for match in self.call_assignment_pattern.finditer(query):
      found.add(f"CALL_SET:{match.group(1).lower()}")

    for match in self.call_pattern.finditer(query):
      name = match.group(1).lower()
      if name not in self.READ_ONLY_PROCEDURES:
        found.add(f"CALL:{name}")

    return found

  def _find_read_operations(self, query: str) -> set[str]:
    """Find read operation keywords in the cleaned query."""
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

  def _find_bulk_operations(self, query: str) -> set[str]:
    """Find bulk operation keywords in the cleaned query."""
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

  def _find_admin_operations(self, query: str) -> set[str]:
    """Find administrative operation keywords in the cleaned query."""
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

  def _find_system_calls(self, query: str) -> set[str]:
    """Find system procedure calls in the cleaned query."""
    found_calls = set()

    # Find all CALL statements
    matches = self.call_pattern.finditer(query)

    for match in matches:
      procedure_name = match.group(1).lower()
      if procedure_name in self.SYSTEM_PROCEDURES:
        found_calls.add(procedure_name)

    return found_calls

  def _find_schema_ddl(self, query: str) -> set[str]:
    """Find schema DDL keywords in the cleaned query."""
    found_operations = set()

    # Check for CREATE NODE/REL TABLE
    if re.search(r"\bCREATE\s+(NODE|REL)\s+TABLE\b", query, re.IGNORECASE):
      found_operations.add("CREATE_TABLE")

    # Check for DROP NODE/REL TABLE
    if re.search(r"\bDROP\s+(NODE|REL)\s+TABLE\b", query, re.IGNORECASE):
      found_operations.add("DROP_TABLE")

    # Check for bare CREATE/DROP TABLE. LadybugDB/Kuzu drops with `DROP TABLE
    # <name>` (no NODE/REL qualifier), which the qualified regexes above miss —
    # leaving a table-destroying statement classified as a plain write.
    if re.search(r"\b(CREATE|DROP)\s+TABLE\b", query, re.IGNORECASE):
      found_operations.add("TABLE_DDL")

    # Check for CREATE/DROP INDEX (incl. vector/FTS indexes) and SEQUENCE —
    # DDL that is_write_operation would otherwise pass through as a write.
    if re.search(r"\b(CREATE|DROP)\s+INDEX\b", query, re.IGNORECASE):
      found_operations.add("INDEX_DDL")
    if re.search(r"\b(CREATE|DROP)\s+SEQUENCE\b", query, re.IGNORECASE):
      found_operations.add("SEQUENCE_DDL")

    # Check for ALTER TABLE
    if re.search(r"\bALTER\s+TABLE\b", query, re.IGNORECASE):
      found_operations.add("ALTER_TABLE")

    # Check for ADD/DROP/RENAME COLUMN
    if re.search(r"\b(ADD|DROP|RENAME)\s+COLUMN\b", query, re.IGNORECASE):
      found_operations.add("MODIFY_COLUMN")

    # Check for RENAME TABLE
    if re.search(r"\bRENAME\s+TABLE\b", query, re.IGNORECASE):
      found_operations.add("RENAME_TABLE")

    return found_operations

  def _validate_keyword_context(self, query: str, keyword: str, position: int) -> bool:
    """Check that a keyword stands alone rather than being part of an identifier."""
    if position > 0 and query[position - 1].isalnum():
      return False

    if (
      position + len(keyword) < len(query) and query[position + len(keyword)].isalnum()
    ):
      return False

    return True

  def get_write_operation_details(self, query: str) -> dict:
    """
    Get detailed information about write operations in the query.

    On analysis failure the result reports ``is_write_operation: True`` and
    ``analysis_successful: False`` — callers must treat it as a write.
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
  """Determine whether a Cypher query contains write operations.

  The single entry point for write detection — read-only paths must gate on
  this rather than pattern-matching the query themselves.
  """
  return cypher_analyzer.is_write_operation(query)


def is_bulk_operation(query: str) -> bool:
  """
  Determine whether a Cypher query contains bulk operations (COPY, LOAD, IMPORT).

  These belong on the staging and materialization path, not the general
  /query endpoint.
  """
  return cypher_analyzer.is_bulk_operation(query)


def is_admin_operation(query: str) -> bool:
  """
  Determine whether a Cypher query contains administrative operations.

  EXPORT, INSTALL, ATTACH, DETACH, USE and the database-level forms; these
  require admin privileges.
  """
  return cypher_analyzer.is_admin_operation(query)


def is_non_read_call(query: str) -> bool:
  """
  Determine whether a Cypher query contains CALL forms that are not read-only.

  Procedure invocations outside the read-only allowlist and session
  configuration assignments both count. For validators gating on operation
  family rather than write-ness.
  """
  return cypher_analyzer.is_non_read_call(query)


def has_system_calls(query: str) -> bool:
  """Determine whether a Cypher query calls a system procedure."""
  return cypher_analyzer.has_system_calls(query)


def is_schema_ddl(query: str) -> bool:
  """
  Determine whether a Cypher query contains schema DDL operations.

  These modify graph structure (CREATE/DROP/ALTER TABLE, INDEX, SEQUENCE) and
  are restricted so a graph's schema stays immutable after creation.
  """
  return cypher_analyzer.is_schema_ddl(query)


def analyze_cypher_query(query: str) -> dict:
  """Analyze a Cypher query and return detailed classification results."""
  return cypher_analyzer.get_write_operation_details(query)
