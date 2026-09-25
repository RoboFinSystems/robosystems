"""
Cypher query classification: the sole write barrier for the Cypher query
surface. Fails closed — an unparseable query, or a `CALL` outside the
read-only allowlist, is treated as a write.
"""

import logging
import re
from collections.abc import Iterable
from enum import Enum
from typing import NamedTuple

logger = logging.getLogger(__name__)

# What `_clean_query` substitutes for a backtick-quoted identifier.
_IDENTIFIER_PLACEHOLDER = "IDENTIFIER"


class GuardedStringMatch(NamedTuple):
  """A string-match predicate applied to a guarded ``Label.property``.

  ``label_resolved`` is False when the variable's node label could not be read
  from the statement and the match rests on the property name alone.
  """

  guarded_property: str
  label_resolved: bool


class CypherOperationType(Enum):
  READ = "read"
  WRITE = "write"
  MIXED = "mixed"


class CypherSecurityAnalyzer:
  """
  Classifies Cypher queries as read, write, or mixed.

  Comments, string literals, and backtick-quoted identifiers are masked before
  any keyword matching, so data can never hide code from the classifier.
  """

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

  # Belong on the dedicated staging endpoints
  BULK_KEYWORDS = {
    "COPY",
    "LOAD",
    "IMPORT",
  }

  ADMIN_KEYWORDS = {
    "EXPORT",
    "INSTALL",
    "ATTACH",
    "USE",
  }

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

  SYSTEM_PROCEDURES = {
    "show_warnings",
    "clear_warnings",
    "current_setting",
    "db_version",
    "table_info",
    "show_tables",
    "show_connection",
  }

  # Fail-closed allowlist: a `CALL` to anything else is a write, since the
  # keyword patterns cannot see procedure-level DDL. Keep it minimal; a missing
  # read procedure fails visibly, an extra write one silently.
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

  # Procedures that run a statement passed as a string, which static analysis
  # cannot classify; refused outright on every surface, regardless of role.
  OPAQUE_STATEMENT_PROCEDURES = {
    "gql",
  }

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

  # Function spellings of the string-match operators (CONTAINS, STARTS WITH,
  # ENDS WITH, =~), matched case-insensitively when followed by `(`.
  STRING_MATCH_FUNCTIONS = {
    "starts_with",
    "prefix",
    "ends_with",
    "suffix",
    "regexp_matches",
    "regexp_full_match",
  }

  # Tokens that end an operand expression at parenthesis depth zero.
  OPERAND_BOUNDARIES = {
    "AND",
    "OR",
    "XOR",
    "NOT",
    "WHERE",
    "WITH",
    "RETURN",
    "MATCH",
    "OPTIONAL",
    "UNWIND",
    "ORDER",
    "BY",
    "SKIP",
    "LIMIT",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "AS",
    "UNION",
    "CALL",
    "YIELD",
  }

  # Tokens that end a predicate (a WHERE or a lambda body) at depth zero.
  PREDICATE_BOUNDARIES = {
    "RETURN",
    "WITH",
    "MATCH",
    "OPTIONAL",
    "UNWIND",
    "ORDER",
    "SKIP",
    "LIMIT",
    "UNION",
    "CALL",
    "YIELD",
    "CREATE",
    "MERGE",
    "SET",
    "DELETE",
    "DETACH",
    "REMOVE",
  }

  _TOKEN_PATTERN = re.compile(r"=~|[^\W\d]\w*|\d+(?:\.\d+)?|\S")
  _OPENERS = frozenset("([{")
  _CLOSERS = frozenset(")]}")

  def __init__(self):
    self.write_pattern = re.compile(
      r"\b(" + "|".join(self.WRITE_KEYWORDS) + r")\b", re.IGNORECASE
    )

    self.bulk_pattern = re.compile(
      r"\b(" + "|".join(self.BULK_KEYWORDS) + r")\b", re.IGNORECASE
    )

    self.admin_pattern = re.compile(
      r"\b(" + "|".join(self.ADMIN_KEYWORDS) + r")\b", re.IGNORECASE
    )

    # Dots allowed so a namespaced procedure name resolves as one identifier.
    self.call_pattern = re.compile(r"\bCALL\s+([\w.]+)\s*\(", re.IGNORECASE)

    # `CALL <setting> = <value>`: session configuration, no parentheses.
    self.call_assignment_pattern = re.compile(r"\bCALL\s+([\w.]+)\s*=", re.IGNORECASE)

    self.transaction_control_pattern = re.compile(
      r"(?:^|;)\s*(BEGIN|COMMIT|ROLLBACK|CHECKPOINT)\b", re.IGNORECASE
    )

  def analyze_query(self, query: str) -> CypherOperationType:
    """
    Classify a Cypher query as READ, WRITE, or MIXED.

    Raises:
        ValueError: If the query is empty, oversized, or matches an injection
            pattern.
    """
    if not query or not isinstance(query, str):
      raise ValueError("Query must be a non-empty string")

    self._validate_query_security(query)

    cleaned_query = self._clean_query(query)

    write_operations = self._find_write_operations(cleaned_query)
    read_operations = self._find_read_operations(cleaned_query)

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
      return False

  def is_admin_operation(self, query: str) -> bool:
    """Check whether a query contains administrative operations."""
    try:
      cleaned_query = self._clean_query(query)
      admin_ops = self._find_admin_operations(cleaned_query)
      return len(admin_ops) > 0
    except Exception as e:
      logger.warning(f"Admin operation analysis failed: {e}")
      return True

  def is_non_read_call(self, query: str) -> bool:
    """Check if a query contains CALL forms that are not read-only.

    For validators that gate on operation family rather than
    `is_write_operation`, so they can refuse the CALL surface without refusing
    ordinary graph writes. Fails closed.
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
      return False

  def has_opaque_statement_call(self, query: str) -> bool:
    """Check whether a query calls a procedure in OPAQUE_STATEMENT_PROCEDURES.

    A backtick-quoted CALL target also counts, since its name is masked and
    nothing legitimate quotes one. Fails closed.
    """
    try:
      cleaned_query = self._clean_query(query)
      for match in self.call_pattern.finditer(cleaned_query):
        name = match.group(1)
        if name == _IDENTIFIER_PLACEHOLDER:
          return True
        if name.lower() in self.OPAQUE_STATEMENT_PROCEDURES:
          return True
      return False
    except Exception as e:
      logger.warning(f"Opaque-statement call analysis failed: {e}")
      return True

  def find_guarded_string_match(
    self, query: str, guarded_properties: Iterable[str]
  ) -> GuardedStringMatch | None:
    """Find a string-match predicate applied to a guarded ``Label.property``.

    A guarded property counts anywhere in either operand of a string-match
    operator or STRING_MATCH_FUNCTIONS call, including through an ``AS`` alias;
    merely returning it does not. When the variable's label can't be read, the
    property name alone decides.

    Every span check is an index lookup over tables built once per statement,
    so the cost grows with the statement's length, not with its square.

    Returns None when nothing matches, and when analysis fails.
    """
    try:
      # property name -> {label -> the "Label.property" spelling as declared}
      guarded: dict[str, dict[str, str]] = {}
      for entry in guarded_properties:
        label, _, prop = entry.partition(".")
        if label and prop:
          guarded.setdefault(prop.lower(), {})[label.lower()] = entry
      if not guarded:
        return None

      tokens = self._TOKEN_PATTERN.findall(self._clean_query(query))
      labels_by_variable = self._labels_by_variable(tokens)
      self._carry_rebound_labels(tokens, labels_by_variable)
      index = _SpanIndex(self, tokens, guarded, labels_by_variable)
      index.bind_aliases()
      for start, end in index.string_match_operands():
        match = index.guarded_reference(start, end)
        if match:
          return match
      for start, end in index.predicates():
        match = index.scan_in_predicate(start, end)
        if match:
          return match
      return index.derived_alias_read()
    except Exception as e:
      logger.warning(f"Guarded string-match analysis failed: {e}")
      return None

  def _labels_by_variable(self, tokens: list[str]) -> dict[str, set[str] | None]:
    """Map each pattern variable to its labels; None when a label is quoted."""
    labels: dict[str, set[str] | None] = {}
    for i in range(len(tokens) - 3):
      if tokens[i] not in ("(", "[") or tokens[i + 2] != ":":
        continue
      variable = tokens[i + 1].lower()
      if not (variable[0].isalpha() or variable[0] == "_"):
        continue
      j = i + 3
      while j < len(tokens):
        name = tokens[j]
        if name == _IDENTIFIER_PLACEHOLDER:
          labels[variable] = None
        elif name[0].isalpha() or name[0] == "_":
          known = labels.setdefault(variable, set())
          if known is not None:
            known.add(name.lower())
        else:
          break
        if j + 1 < len(tokens) and tokens[j + 1] in (":", "|"):
          j += 2
        else:
          break
    return labels

  def _carry_rebound_labels(
    self, tokens: list[str], labels_by_variable: dict[str, set[str] | None]
  ) -> None:
    """``WITH g AS f`` makes ``f`` stand for ``g``: add ``g``'s labels to ``f``.

    Labels are keyed by name for the whole statement, so a rebound name keeps
    every label it was ever given (across ``UNION`` too) and a guarded label
    is never lost to an earlier binding of the same name.
    """
    for _ in range(2):
      for i in range(1, len(tokens) - 1):
        if tokens[i].upper() != "AS":
          continue
        source, target = tokens[i - 1].lower(), tokens[i + 1].lower()
        if i >= 2 and tokens[i - 2] == ".":
          continue
        if source not in labels_by_variable or source == target:
          continue
        source_labels = labels_by_variable[source]
        if source_labels is None or labels_by_variable.get(target, set()) is None:
          labels_by_variable[target] = None
        else:
          merged = set(labels_by_variable.get(target) or set())
          labels_by_variable[target] = merged | source_labels

  def _validate_query_security(self, query: str) -> None:
    """Raise ValueError for an oversized or suspicious query."""
    if len(query) > 100000:
      raise ValueError("Query exceeds maximum allowed length")

    nested_comments = query.count("/*") - query.count("*/")
    if nested_comments != 0:
      raise ValueError("Unbalanced comment blocks detected")

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
    Mask comments, string literals, and backtick-quoted identifiers so keyword
    detection only ever sees code, not data.

    One left-to-right scan so string context is decided before any comment
    marker; staged regex stripping lets data hide code.
    """
    out: list[str] = []
    i = 0
    n = len(query)
    while i < n:
      ch = query[i]

      if ch == "/" and i + 1 < n and query[i + 1] == "/":
        nl = query.find("\n", i)
        i = n if nl == -1 else nl
        out.append(" ")
        continue

      # Unbalanced blocks are already rejected by _validate_query_security.
      if ch == "/" and i + 1 < n and query[i + 1] == "*":
        end = query.find("*/", i + 2)
        i = n if end == -1 else end + 2
        out.append(" ")
        continue

      # Backslash escapes the next char inside a string literal.
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

      # Backtick identifiers have no backslash escaping in the engine lexer;
      # close at the first backtick to match it exactly. Doubled backticks
      # split into two tokens, which can only over-classify.
      if ch == "`":
        i += 1
        while i < n:
          c = query[i]
          i += 1
          if c == "`":
            break
        out.append(f" {_IDENTIFIER_PLACEHOLDER} ")
        continue

      out.append(ch)
      i += 1

    return "".join(out)

  def _find_write_operations(self, query: str) -> set[str]:
    """Find write operation keywords in the cleaned query."""
    found_operations = set()

    matches = self.write_pattern.finditer(query)

    for match in matches:
      keyword = match.group(1).upper()
      start_pos = match.start()

      # Additional context validation
      if self._validate_keyword_context(query, keyword, start_pos):
        found_operations.add(keyword)

    # Folded in here because every surface gates on the two callers of this.
    found_operations |= self._find_call_operations(query)

    return found_operations

  def _find_call_operations(self, query: str) -> set[str]:
    """Find `CALL` forms that must not be treated as reads.

    A procedure outside ``READ_ONLY_PROCEDURES`` (fail closed), or any
    ``CALL <name> = <value>`` (session state on a pooled connection). Returns
    ``CALL:<name>`` / ``CALL_SET:<name>`` markers.
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

    matches = self.admin_pattern.finditer(query)

    for match in matches:
      keyword = match.group(1).upper()
      start_pos = match.start()

      # Additional context validation
      if self._validate_keyword_context(query, keyword, start_pos):
        found_operations.add(keyword)

    if re.search(r"\b(IMPORT|EXPORT)\s+DATABASE\b", query, re.IGNORECASE):
      found_operations.add("DATABASE_MIGRATION")

    # DETACH DATABASE, not DETACH DELETE
    if re.search(r"\bDETACH\s+DATABASE\b", query, re.IGNORECASE):
      found_operations.add("DETACH_DATABASE")

    # Manual transactions would outlive the request on a pooled connection.
    # Anchored at statement start so a property named `begin` still reads.
    if self.transaction_control_pattern.search(query):
      found_operations.add("TRANSACTION_CONTROL")

    return found_operations

  def _find_system_calls(self, query: str) -> set[str]:
    """Find system procedure calls in the cleaned query."""
    found_calls = set()

    matches = self.call_pattern.finditer(query)

    for match in matches:
      procedure_name = match.group(1).lower()
      if procedure_name in self.SYSTEM_PROCEDURES:
        found_calls.add(procedure_name)

    return found_calls

  def _find_schema_ddl(self, query: str) -> set[str]:
    """Find schema DDL keywords in the cleaned query."""
    found_operations = set()

    if re.search(r"\bCREATE\s+(NODE|REL)\s+TABLE\b", query, re.IGNORECASE):
      found_operations.add("CREATE_TABLE")

    if re.search(r"\bDROP\s+(NODE|REL)\s+TABLE\b", query, re.IGNORECASE):
      found_operations.add("DROP_TABLE")

    # The engine also accepts unqualified `DROP TABLE <name>`.
    if re.search(r"\b(CREATE|DROP)\s+TABLE\b", query, re.IGNORECASE):
      found_operations.add("TABLE_DDL")

    if re.search(r"\b(CREATE|DROP)\s+INDEX\b", query, re.IGNORECASE):
      found_operations.add("INDEX_DDL")
    if re.search(r"\b(CREATE|DROP)\s+SEQUENCE\b", query, re.IGNORECASE):
      found_operations.add("SEQUENCE_DDL")

    if re.search(r"\bALTER\s+TABLE\b", query, re.IGNORECASE):
      found_operations.add("ALTER_TABLE")

    if re.search(r"\b(ADD|DROP|RENAME)\s+COLUMN\b", query, re.IGNORECASE):
      found_operations.add("MODIFY_COLUMN")

    if re.search(r"\bRENAME\s+TABLE\b", query, re.IGNORECASE):
      found_operations.add("RENAME_TABLE")

    # A catalog write the keyword set can't see (`comment` is a common
    # property name, so only the two-word form is matched).
    if re.search(r"\bCOMMENT\s+ON\b", query, re.IGNORECASE):
      found_operations.add("COMMENT_ON")

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
        "is_write_operation": True,
        "is_bulk_operation": False,
        "write_keywords_found": [],
        "read_keywords_found": [],
        "bulk_keywords_found": [],
        "analysis_successful": False,
        "security_validated": False,
        "error": str(e),
      }


cypher_analyzer = CypherSecurityAnalyzer()


class _MaxTree:
  """Point updates and "first position in [lo, hi) holding at least v"."""

  def __init__(self, values: list[int]) -> None:
    size = 1
    while size < max(len(values), 1):
      size *= 2
    self._size = size
    self._tree = [-(2**62)] * (2 * size)
    self._tree[size : size + len(values)] = values
    for node in range(size - 1, 0, -1):
      self._tree[node] = max(self._tree[2 * node], self._tree[2 * node + 1])

  def set(self, position: int, value: int) -> None:
    node = position + self._size
    self._tree[node] = value
    node //= 2
    while node:
      self._tree[node] = max(self._tree[2 * node], self._tree[2 * node + 1])
      node //= 2

  def first_at_least(self, lo: int, hi: int, threshold: int) -> int:
    if lo >= hi:
      return -1
    return self._descend(1, 0, self._size, lo, hi, threshold)

  def _descend(
    self, node: int, left: int, right: int, lo: int, hi: int, threshold: int
  ) -> int:
    if right <= lo or hi <= left or self._tree[node] < threshold:
      return -1
    if right - left == 1:
      return left
    middle = (left + right) // 2
    found = self._descend(2 * node, left, middle, lo, hi, threshold)
    if found != -1:
      return found
    return self._descend(2 * node + 1, middle, right, lo, hi, threshold)


_ABSENT = -2


class _SpanIndex:
  """One statement's tokens, indexed so each guard check is a lookup.

  Built once per statement: matching brackets, where each operand and each
  predicate ends, the innermost function call around every token, and which
  tokens read a guarded value. An operand, argument list or predicate is then
  a ``(start, end)`` span, never a copied and rescanned slice.
  """

  def __init__(
    self,
    analyzer: "CypherSecurityAnalyzer",
    tokens: list[str],
    guarded: dict[str, dict[str, str]],
    labels_by_variable: dict[str, set[str] | None],
  ) -> None:
    self.a = analyzer
    self.tokens = tokens
    self.n = n = len(tokens)
    self.upper = [t.upper() for t in tokens]
    self.lower = [t.lower() for t in tokens]
    openers, closers = analyzer._OPENERS, analyzer._CLOSERS

    operand_stop = [
      t == "," or u in analyzer.OPERAND_BOUNDARIES
      for t, u in zip(tokens, self.upper, strict=False)
    ]
    predicate_stop = [
      t in ("|", ";") or u in analyzer.PREDICATE_BOUNDARIES
      for t, u in zip(tokens, self.upper, strict=False)
    ]
    when_stop = [
      stop or u in ("THEN", "ELSE", "END")
      for stop, u in zip(predicate_stop, self.upper, strict=False)
    ]
    self.operand_end = self._ends(operand_stop)
    self.predicate_end = self._ends(predicate_stop)
    self.when_end = self._ends(when_stop)
    self.operand_begin = self._begins(operand_stop)

    # Matching closer of each opener (n when unclosed), the innermost unclosed
    # opener before each token, and the innermost call around each token.
    self.closer = [n] * n
    self.enclosing = [-1] * n
    self.call_name = [-1] * n
    stack: list[int] = []
    calls: list[int] = []
    for j, token in enumerate(tokens):
      self.enclosing[j] = stack[-1] if stack else -1
      if token in closers and stack:
        self.closer[stack.pop()] = j
        calls.pop()
      # A closer sits outside the argument list it ends.
      self.call_name[j] = calls[-1] if calls else -1
      if token in openers:
        stack.append(j)
        calls.append(j - 1 if self._is_call(j) else self.call_name[j])

    self.prop_match: list[GuardedStringMatch | None] = [None] * n
    for i in range(n - 2):
      if tokens[i + 1] != "." or (i and tokens[i - 1] == "$"):
        continue
      self.prop_match[i] = self._resolve_property(
        self.lower[i], tokens[i + 2], guarded, labels_by_variable
      )
    self.next_prop = self._next_positions(m is not None for m in self.prop_match)
    self.prop_calls = _MaxTree(
      [
        self.call_name[i] if m is not None else _ABSENT
        for i, m in enumerate(self.prop_match)
      ]
    )

    self.bare = [
      not (j and tokens[j - 1] in (".", "$"))
      and not (j + 1 < n and tokens[j + 1] == ".")
      for j in range(n)
    ]
    self.positions: dict[str, list[int]] = {}
    for j, name in enumerate(self.lower):
      self.positions.setdefault(name, []).append(j)

    self.aliases: dict[str, GuardedStringMatch] = {}
    self.derived: set[str] = set()
    # Bare readings of a bound alias (value: innermost call name, or -1).
    self.alias_calls = _MaxTree([_ABSENT] * n)
    # Any reading of a derived alias (value 0).
    self.derived_any = _MaxTree([_ABSENT] * n)
    self._next_derived: list[int] | None = None

  def _is_call(self, opener: int) -> bool:
    if self.tokens[opener] != "(" or opener == 0:
      return False
    name = self.tokens[opener - 1]
    if not (name[0].isalpha() or name[0] == "_"):
      return False
    upper = self.upper[opener - 1]
    return upper not in self.a.OPERAND_BOUNDARIES and upper not in ("IN", "IS")

  def _ends(self, stop: list[bool]) -> list[int]:
    """For each start, where an expression running right from it ends: the
    first stop or unmatched closer at its own bracket depth."""
    openers, closers = self.a._OPENERS, self.a._CLOSERS
    ends = [self.n] * self.n
    outer: list[int] = []
    current = self.n
    for j in range(self.n - 1, -1, -1):
      token = self.tokens[j]
      if token in closers:
        outer.append(current)
        current = j
      elif token in openers:
        current = outer.pop() if outer else self.n
      elif stop[j]:
        current = j
      ends[j] = current
    return ends

  def _begins(self, stop: list[bool]) -> list[int]:
    """For each end, the last stop or unmatched opener at its bracket depth
    before it (the mirror of ``_ends``, for an operand running left)."""
    openers, closers = self.a._OPENERS, self.a._CLOSERS
    begins = [-1] * self.n
    outer: list[int] = []
    current = -1
    for j in range(self.n):
      token = self.tokens[j]
      if token in openers:
        outer.append(current)
        current = j
      elif token in closers:
        current = outer.pop() if outer else -1
      elif stop[j]:
        current = j
      begins[j] = current
    return begins

  def _next_positions(self, flags: Iterable[bool]) -> list[int]:
    flags = list(flags)
    following = [self.n] * (self.n + 1)
    for j in range(self.n - 1, -1, -1):
      following[j] = j if flags[j] else following[j + 1]
    return following

  @staticmethod
  def _resolve_property(
    variable: str,
    prop: str,
    guarded: dict[str, dict[str, str]],
    labels_by_variable: dict[str, set[str] | None],
  ) -> GuardedStringMatch | None:
    labels = labels_by_variable.get(variable)
    # A backtick-quoted property name is masked, so on a known label it may
    # be any of that label's guarded properties.
    quoted = prop == _IDENTIFIER_PLACEHOLDER
    for name in guarded if quoted else (prop.lower(),):
      declared = guarded.get(name)
      if not declared:
        continue
      if labels:
        for label in sorted(declared.keys() & labels):
          return GuardedStringMatch(declared[label], label_resolved=True)
      elif not quoted:
        return GuardedStringMatch(declared[min(declared)], label_resolved=False)
    return None

  # ── Spans ────────────────────────────────────────────────────────────

  def right_operand(self, start: int) -> tuple[int, int]:
    if start >= self.n:
      return start, start
    return start, self.operand_end[start]

  def left_operand(self, end: int) -> tuple[int, int]:
    if end <= 0:
      return 0, 0
    return self.operand_begin[end - 1] + 1, end

  def bracketed(self, opener: int) -> tuple[int, int]:
    return opener + 1, self.closer[opener]

  def string_match_operands(self) -> list[tuple[int, int]]:
    """The operand spans of every string-match predicate."""
    tokens, spans = self.tokens, []
    for i, upper in enumerate(self.upper):
      following = tokens[i + 1] if i + 1 < self.n else ""
      if upper in ("STARTS", "ENDS") and following.upper() == "WITH":
        right_start = i + 2
      elif upper == "CONTAINS" or tokens[i] == "=~":
        right_start = i + 1
      elif self.lower[i] in self.a.STRING_MATCH_FUNCTIONS and following == "(":
        spans.append(self.right_operand(i + 1))
        continue
      else:
        continue
      spans.append(self.left_operand(i))
      spans.append(self.right_operand(right_start))
    return spans

  def predicates(self) -> list[tuple[int, int]]:
    """Every WHERE expression, CASE WHEN condition and lambda body, each up to
    its clause end (a WHEN condition ends at its THEN)."""
    spans = []
    for i, upper in enumerate(self.upper):
      if upper == "WHERE" or (
        self.tokens[i] == ">" and i and self.tokens[i - 1] == "-"
      ):
        ends = self.predicate_end
      elif upper == "WHEN":
        ends = self.when_end
      else:
        continue
      start = i + 1
      spans.append((start, ends[start] if start < self.n else start))
    return spans

  # ── Checks ───────────────────────────────────────────────────────────

  def guarded_reference(self, start: int, end: int) -> GuardedStringMatch | None:
    """A guarded ``variable.property`` read in the span, or an alias of one."""
    at = self.alias_calls.first_at_least(start, end, -1)
    if at != -1:
      return self.aliases[self.lower[at]]
    at = self.next_prop[start] if start < self.n else self.n
    if at + 2 < end:
      return self.prop_match[at]
    return None

  def function_over_guarded(self, start: int, end: int) -> GuardedStringMatch | None:
    """A call in the span whose arguments read a guarded value: a guarded
    read whose innermost enclosing call is named inside the span."""
    at = self.alias_calls.first_at_least(start, end, start)
    if at != -1:
      return self.aliases[self.lower[at]]
    at = self.prop_calls.first_at_least(start, end - 2, start)
    if at != -1:
      return self.prop_match[at]
    return None

  def bind_aliases(self) -> None:
    """Map every name bound to a guarded value, and flag the derived ones.

    A name is bound by ``expression AS name``, by a list-comprehension or
    quantifier variable (``x IN expression``), or by a lambda parameter
    (``x -> …`` inside a call whose arguments read one). Read in statement
    order, so an alias of an alias carries through; a name stays mapped even
    if a later clause reuses it. A name is *derived* when its expression runs
    a function over the guarded value, so using it in a predicate is a scan.
    """
    tokens = self.tokens
    for i in range(self.n - 1):
      upper = self.upper[i]
      if upper == "AS":
        start, end = self.left_operand(i)
        name = self.lower[i + 1]
      elif upper == "IN" and i >= 2 and tokens[i - 2] in ("(", "["):
        start, end = self.right_operand(i + 1)
        name = self.lower[i - 1]
      elif tokens[i] == "-" and tokens[i + 1] == ">" and i >= 1:
        opener = self.enclosing[i]
        start, end = self.bracketed(opener) if opener != -1 else (0, 0)
        name = self.lower[i - 1]
      else:
        continue
      match = self.guarded_reference(start, end)
      if not match:
        continue
      if name not in self.aliases:
        for j in self.positions.get(name, ()):
          if self.bare[j]:
            self.alias_calls.set(j, self.call_name[j])
      self.aliases[name] = match
      if (
        upper == "AS"
        and name not in self.derived
        and (
          self.function_over_guarded(start, end)
          or self.derived_any.first_at_least(start, end, 0) != -1
        )
      ):
        self.derived.add(name)
        for j in self.positions.get(name, ()):
          self.derived_any.set(j, 0)

  def scan_in_predicate(self, start: int, end: int) -> GuardedStringMatch | None:
    """A predicate that runs a function over a guarded value, directly or
    through a derived alias. A bare comparison (``f.value = $v``) is not one."""
    match = self.function_over_guarded(start, end)
    if match:
      return match
    if self._next_derived is None:
      self._next_derived = self._next_positions(
        name in self.derived and bare
        for name, bare in zip(self.lower, self.bare, strict=False)
      )
    at = self._next_derived[start] if start < self.n else self.n
    if at < end:
      return self.aliases[self.lower[at]]
    return None

  def derived_alias_read(self) -> GuardedStringMatch | None:
    """A function-derived alias read anywhere after it is bound.

    Projecting a function of the guarded text is served; carrying that result
    on to another clause (a filter, a comparison, a second projection) is how
    a per-row test of the text gets past the predicate check.
    """
    tokens = self.tokens
    for i, name in enumerate(self.lower):
      if name not in self.derived:
        continue
      before = tokens[i - 1] if i else ""
      after = tokens[i + 1] if i + 1 < self.n else ""
      if before.upper() == "AS" or before in (".", "$") or after == ".":
        continue
      return self.aliases[name]
    return None


def is_write_operation(query: str) -> bool:
  """The single write-detection gate; read-only paths must not pattern-match
  queries themselves."""
  return cypher_analyzer.is_write_operation(query)


def is_bulk_operation(query: str) -> bool:
  """COPY / LOAD / IMPORT, which belong on the staging path, not /query."""
  return cypher_analyzer.is_bulk_operation(query)


def is_admin_operation(query: str) -> bool:
  """EXPORT, INSTALL, ATTACH, DETACH, USE and the database-level forms."""
  return cypher_analyzer.is_admin_operation(query)


def is_non_read_call(query: str) -> bool:
  """See ``CypherSecurityAnalyzer.is_non_read_call``."""
  return cypher_analyzer.is_non_read_call(query)


def has_system_calls(query: str) -> bool:
  return cypher_analyzer.has_system_calls(query)


def has_opaque_statement_call(query: str) -> bool:
  """Whether the query calls a procedure that runs a string-supplied
  statement; callers refuse these outright rather than classifying them."""
  return cypher_analyzer.has_opaque_statement_call(query)


def find_guarded_string_match(
  query: str, guarded_properties: Iterable[str]
) -> GuardedStringMatch | None:
  """
  Find a string-match predicate on one of ``guarded_properties``
  (``"Label.property"`` entries); callers refuse a statement that matches.
  """
  return cypher_analyzer.find_guarded_string_match(query, guarded_properties)


def is_schema_ddl(query: str) -> bool:
  """Table, index, sequence and column DDL; a graph's schema is immutable
  after creation."""
  return cypher_analyzer.is_schema_ddl(query)


def analyze_cypher_query(query: str) -> dict:
  return cypher_analyzer.get_write_operation_details(query)
