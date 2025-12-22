"""
Graph Query Validator for MCP Tools.

Validates queries before execution to prevent common errors, detect Neo4j patterns,
and provide helpful suggestions for AI agents.
"""

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
  """Result of query validation."""

  is_valid: bool
  errors: list[str] = field(default_factory=list)
  warnings: list[str] = field(default_factory=list)
  suggestions: list[str] = field(default_factory=list)
  complexity_score: int = 0
  neo4j_patterns_found: list[str] = field(default_factory=list)
  fixed_query: str | None = None


class GraphQueryValidator:
  """Validates graph database queries before execution."""

  # Compiled regex patterns for performance
  COMPILED_PATTERNS = {
    "unbounded_path": re.compile(r"\[(?:\s*:\w+\s*)?\*\s*\]"),
    "where_in_match": re.compile(r"MATCH\s*\([^)]*\bWHERE\b[^)]*\)", re.IGNORECASE),
    "label_in_where": re.compile(r"WHERE\s+(\w+)\s*:\s*(\w+)"),
    "remove_command": re.compile(r"REMOVE\s+(\w+\.\w+)", re.IGNORECASE),
    "type_check": re.compile(r"(\w+(?:\.\w+)?)\s+IS\s*::\s*(\w+)"),
    "show_command": re.compile(r"\bSHOW\s+(\w+)", re.IGNORECASE),
    "property_append": re.compile(r"\+="),
    "foreach": re.compile(r"\bFOREACH\b", re.IGNORECASE),
    "label_pattern": re.compile(r":(\w+)"),
    "property_pattern": re.compile(r"(\w+)\.(\w+)"),
    "param_pattern": re.compile(r"\$(\w+)"),
    "var_length_path": re.compile(r"\[\s*\*\s*(\d+)?\.\.(\d+)\s*\]"),
    "generic_node": re.compile(r"MATCH\s*\(\s*\)"),
    "metadata_queries": [
      re.compile(r"CALL\s+show_tables", re.IGNORECASE),
      re.compile(r"CALL\s+table_info", re.IGNORECASE),
      re.compile(r"CALL\s+show_functions", re.IGNORECASE),
      re.compile(r"CALL\s+current_setting", re.IGNORECASE),
    ],
    "date_format": re.compile(r"\d{4}-\d{2}-\d{2}"),
  }

  # Neo4j to graph database function mappings
  NEO4J_FUNCTION_MAPPINGS = {
    "toInteger": "cast(value, 'INT64')",
    "toFloat": "cast(value, 'DOUBLE')",
    "toString": "cast(value, 'STRING')",
    "toBoolean": "cast(value, 'BOOL')",
    "size": "len",  # for collections
    "length": "len",  # for strings
    "labels": "label",  # single label support
    "type": "label",  # for relationships
    "keys": "properties",
    "timestamp": "epoch_ms",
    "collect": "list",  # aggregate function
    "avg": "mean",  # average function
  }

  # Common SEC/XBRL element qnames for validation
  COMMON_XBRL_ELEMENTS = {
    "us-gaap:Revenues",
    "us-gaap:Revenue",
    "us-gaap:NetIncomeLoss",
    "us-gaap:Assets",
    "us-gaap:Liabilities",
    "us-gaap:StockholdersEquity",
    "us-gaap:CashAndCashEquivalentsAtCarryingValue",
    "us-gaap:OperatingIncomeLoss",
    "us-gaap:GrossProfit",
    "us-gaap:EarningsPerShareBasic",
    "us-gaap:EarningsPerShareDiluted",
    "us-gaap:CommonStockSharesOutstanding",
  }

  # Known SEC node labels
  SEC_NODE_LABELS = {
    "Entity",
    "Company",
    "Report",
    "Fact",
    "Element",
    "Period",
    "Unit",
    "Dimension",
    "Member",
    "Context",
    "Structure",
    "FactDimension",
    "ArcRole",
    "CalculationArc",
    "ConceptMap",
    "Relationship",
  }

  # Known SEC relationship labels
  SEC_RELATIONSHIP_LABELS = {
    "ENTITY_HAS_REPORT",
    "COMPANY_HAS_REPORT",
    "REPORT_HAS_FACT",
    "FACT_HAS_ELEMENT",
    "FACT_HAS_PERIOD",
    "FACT_HAS_UNIT",
    "FACT_HAS_DIMENSION",
    "DIMENSION_HAS_MEMBER",
    "FACT_HAS_CONTEXT",
    "ENTITY_EVOLVED_FROM",
    "FACT_DIMENSION_AXIS_ELEMENT",
    "FACT_DIMENSION_AXIS_MEMBER",
    "FACT_DIMENSION_HAS_ELEMENT",
    "STRUCTURE_HAS_ELEMENT",
    "STRUCTURE_HAS_ASSOCIATION",
    "STRUCTURE_HAS_CHILD",
    "STRUCTURE_HAS_PARENT",
  }

  def __init__(self, schema: list[dict] | None = None):
    self.schema = schema or []
    self._node_labels: set[str] = set()
    self._rel_labels: set[str] = set()
    self._properties: dict[str, set[str]] = {}  # label -> set of properties

    if schema:
      self._parse_schema(schema)
    else:
      # Use known SEC labels if no schema provided
      self._node_labels = self.SEC_NODE_LABELS.copy()
      self._rel_labels = self.SEC_RELATIONSHIP_LABELS.copy()

  def _parse_schema(self, schema: list[dict]) -> None:
    """Parse schema to extract labels and properties."""
    for item in schema:
      label = item.get("label", "")
      item_type = item.get("type", "").lower()

      if item_type == "node":
        self._node_labels.add(label)
      elif item_type in ["relationship", "rel"]:
        self._rel_labels.add(label)

      # Extract properties
      if "properties" in item and isinstance(item["properties"], list):
        prop_names = {p.get("name", "") for p in item["properties"] if p.get("name")}
        if prop_names:
          self._properties[label] = prop_names

  def validate(self, query: str, params: dict | None = None) -> ValidationResult:
    """Validate a graph database query comprehensively."""
    result = ValidationResult(is_valid=True)

    # Skip validation for metadata queries
    if self._is_metadata_query(query):
      return result

    # 1. Basic syntax validation
    syntax_errors = self._validate_basic_syntax(query)
    result.errors.extend(syntax_errors)

    # 2. Neo4j pattern detection and fixes
    neo4j_issues = self._detect_neo4j_patterns(query)
    for issue in neo4j_issues:
      result.errors.append(issue["error"])
      result.suggestions.append(issue["fix"])
      result.neo4j_patterns_found.append(issue["pattern"])

    # 3. Schema validation (if available)
    if self._node_labels or self._rel_labels:
      schema_warnings = self._validate_against_schema(query)
      result.warnings.extend(schema_warnings)

    # 4. Performance validation
    perf_warnings, complexity = self._analyze_performance(query)
    result.warnings.extend(perf_warnings)
    result.complexity_score = complexity

    # 5. SEC/Financial best practices
    best_practices = self._check_financial_best_practices(query)
    result.warnings.extend(best_practices)

    # 6. Parameter validation
    if params:
      param_issues = self._validate_parameters(query, params)
      result.warnings.extend(param_issues)

    # 7. Generate fixed query if there are Neo4j patterns
    if result.neo4j_patterns_found:
      result.fixed_query = self.suggest_query_fix(query, result)

    result.is_valid = len(result.errors) == 0

    # Add severity to result
    if result.complexity_score > 50:
      result.warnings.append(
        f"⚠️ High complexity query (score: {result.complexity_score}). Consider optimization."
      )

    return result

  def _is_metadata_query(self, query: str) -> bool:
    """Check if query is a metadata/system query."""
    return any(
      pattern.search(query) for pattern in self.COMPILED_PATTERNS["metadata_queries"]
    )

  def _validate_basic_syntax(self, query: str) -> list[str]:
    """Validate basic query syntax."""
    errors = []

    # Check for empty query
    if not query.strip():
      errors.append("Query cannot be empty")
      return errors

    # Check for unclosed quotes
    single_quotes = query.count("'")
    double_quotes = query.count('"')
    if single_quotes % 2 != 0:
      errors.append("Unclosed single quote detected")
    if double_quotes % 2 != 0:
      errors.append("Unclosed double quote detected")

    # Check for unmatched parentheses
    open_parens = query.count("(")
    close_parens = query.count(")")
    if open_parens != close_parens:
      errors.append(
        f"Unmatched parentheses: {open_parens} opening, {close_parens} closing"
      )

    # Check for unmatched brackets
    open_brackets = query.count("[")
    close_brackets = query.count("]")
    if open_brackets != close_brackets:
      errors.append(
        f"Unmatched brackets: {open_brackets} opening, {close_brackets} closing"
      )

    return errors

  def _detect_neo4j_patterns(self, query: str) -> list[dict[str, str]]:
    """Detect Neo4j-specific patterns that will fail in graph database."""
    issues = []

    # Pattern 1: Unbounded paths
    for match in self.COMPILED_PATTERNS["unbounded_path"].finditer(query):
      issues.append(
        {
          "pattern": "unbounded_path",
          "error": f"Unbounded path '{match.group()}' not allowed",
          "fix": "Replace with bounded path like '[*1..5]' or '[*1..10]'",
        }
      )

    # Pattern 2: WHERE inside MATCH
    for match in self.COMPILED_PATTERNS["where_in_match"].finditer(query):
      issues.append(
        {
          "pattern": "where_in_match",
          "error": "WHERE clause inside MATCH pattern not supported",
          "fix": "Move WHERE clause after MATCH. Example: MATCH (n:Label) WHERE n.prop = value",
        }
      )

    # Pattern 3: Label checking in WHERE
    for match in self.COMPILED_PATTERNS["label_in_where"].finditer(query):
      var_name = match.group(1)
      label_name = match.group(2)
      issues.append(
        {
          "pattern": "label_in_where",
          "error": f"Neo4j-style label check '{var_name}:{label_name}' in WHERE",
          "fix": f"Use: WHERE label({var_name}) = '{label_name}'",
        }
      )

    # Pattern 4: REMOVE command
    for match in self.COMPILED_PATTERNS["remove_command"].finditer(query):
      prop = match.group(1)
      issues.append(
        {
          "pattern": "remove_command",
          "error": f"REMOVE {prop} not supported",
          "fix": f"Use: SET {prop} = NULL",
        }
      )

    # Pattern 5: IS :: type checking
    for match in self.COMPILED_PATTERNS["type_check"].finditer(query):
      expr = match.group(1)
      type_name = match.group(2)
      lbug_type = self._map_neo4j_type(type_name)
      issues.append(
        {
          "pattern": "type_check",
          "error": f"Neo4j type check '{expr} IS :: {type_name}'",
          "fix": f"Use: typeOf({expr}) = '{lbug_type}'",
        }
      )

    # Pattern 6: Neo4j functions
    for neo4j_func, lbug_func in self.NEO4J_FUNCTION_MAPPINGS.items():
      pattern = rf"\b{neo4j_func}\s*\("
      if re.search(pattern, query, re.IGNORECASE):
        issues.append(
          {
            "pattern": "neo4j_function",
            "error": f"Neo4j function '{neo4j_func}()' not available",
            "fix": f"Use: {lbug_func}",
          }
        )

    # Pattern 7: SHOW commands
    for match in self.COMPILED_PATTERNS["show_command"].finditer(query):
      command = match.group(1).lower()
      call_equivalent = {
        "tables": "CALL show_tables() RETURN *",
        "functions": "CALL show_functions() RETURN *",
        "settings": 'CALL current_setting("") RETURN *',
      }.get(command, f"CALL show_{command}() RETURN *")

      issues.append(
        {
          "pattern": "show_command",
          "error": f"SHOW {command.upper()} not supported",
          "fix": f"Use: {call_equivalent}",
        }
      )

    # Pattern 8: FOREACH loops
    if self.COMPILED_PATTERNS["foreach"].search(query):
      issues.append(
        {
          "pattern": "foreach",
          "error": "FOREACH not supported",
          "fix": "Use UNWIND instead. Example: UNWIND list AS item CREATE ...",
        }
      )

    # Pattern 9: Property += syntax
    if self.COMPILED_PATTERNS["property_append"].search(query):
      issues.append(
        {
          "pattern": "property_append",
          "error": "Property += syntax not supported",
          "fix": "Set properties individually: SET n.prop1 = val1, n.prop2 = val2",
        }
      )

    return issues

  def _map_neo4j_type(self, neo4j_type: str) -> str:
    """Map Neo4j types to graph database types."""
    type_mappings = {
      "INTEGER": "INT64",
      "INT": "INT64",
      "LONG": "INT64",
      "FLOAT": "DOUBLE",
      "DOUBLE": "DOUBLE",
      "STRING": "STRING",
      "BOOLEAN": "BOOL",
      "BOOL": "BOOL",
      "DATE": "DATE",
      "DATETIME": "TIMESTAMP",
      "TIME": "TIME",
    }
    return type_mappings.get(neo4j_type.upper(), neo4j_type.upper())

  def _validate_against_schema(self, query: str) -> list[str]:
    """Validate query against known schema."""
    warnings = []

    # Extract labels from query
    query_labels = set(self.COMPILED_PATTERNS["label_pattern"].findall(query))

    # Check for unknown labels (only warn if we have a real schema)
    if self.schema:  # Only if we have actual schema data
      unknown_labels = query_labels - self._node_labels - self._rel_labels
      if unknown_labels:
        warnings.append(f"Unknown labels in query: {', '.join(sorted(unknown_labels))}")

    # Extract property references
    for var, prop in self.COMPILED_PATTERNS["property_pattern"].findall(query):
      # Try to infer label from context
      label_match = re.search(rf"{var}\s*:\s*(\w+)", query)
      if label_match:
        label = label_match.group(1)
        if label in self._properties and prop not in self._properties[label]:
          warnings.append(f"Property '{prop}' not found in '{label}' schema")

    return warnings

  def _analyze_performance(self, query: str) -> tuple[list[str], int]:
    """Analyze query for performance issues."""
    warnings = []
    complexity_score = 0

    query_upper = query.upper()

    # 1. Missing LIMIT
    if "RETURN" in query_upper and "LIMIT" not in query_upper:
      warnings.append("No LIMIT clause - query may return large result set")
      complexity_score += 20

    # 2. Variable-length paths
    var_paths = self.COMPILED_PATTERNS["var_length_path"].findall(query)
    for lower, upper in var_paths:
      lower_bound = int(lower) if lower else 1
      upper_bound = int(upper)

      if upper_bound > 5:
        warnings.append(f"Path length up to {upper_bound} hops may be slow")
        complexity_score += (upper_bound - 5) * 10

      if upper_bound - lower_bound > 10:
        warnings.append(
          f"Wide path range [{lower_bound}..{upper_bound}] may impact performance"
        )
        complexity_score += 15

    # 3. Multiple MATCH without WITH
    match_count = query_upper.count("MATCH")
    with_count = query_upper.count("WITH")

    if match_count > 2 and with_count == 0:
      warnings.append(
        f"{match_count} MATCH clauses without WITH may create cartesian product"
      )
      complexity_score += (match_count - 1) * 20

    # 4. Generic node patterns
    generic_patterns = self.COMPILED_PATTERNS["generic_node"].findall(query)
    if generic_patterns:
      warnings.append(
        "Generic node pattern () will scan all nodes - add label for better performance"
      )
      complexity_score += 30

    # 5. Multiple ORDER BY
    order_by_count = query_upper.count("ORDER BY")
    if order_by_count > 1:
      warnings.append("Multiple ORDER BY clauses may impact performance")
      complexity_score += order_by_count * 10

    # 6. String operations in WHERE
    string_ops = ["CONTAINS", "STARTS WITH", "ENDS WITH", "=~"]
    for op in string_ops:
      if op in query_upper:
        warnings.append(
          f"String operation '{op}' in query may be slow on large datasets"
        )
        complexity_score += 10

    return warnings, complexity_score

  def _check_financial_best_practices(self, query: str) -> list[str]:
    """Check for SEC/financial query best practices."""
    warnings = []

    query_lower = query.lower()

    # 1. Fact queries without units
    if "fact" in query_lower and "unit" not in query_lower:
      if "value" in query_lower or "numeric" in query_lower:
        warnings.append(
          "💡 Fact value queries should include Unit relationships for proper context"
        )

    # 2. Fact queries without periods
    if "fact" in query_lower and "period" not in query_lower:
      if any(term in query_lower for term in ["trend", "time", "date", "historical"]):
        warnings.append(
          "💡 Consider including Period relationships for temporal analysis"
        )

    # 3. Element queries without qname
    if "element" in query_lower and "qname" not in query_lower:
      warnings.append(
        "💡 Element queries should filter by qname for better performance"
      )

    # 4. Report queries without date filtering
    if "report" in query_lower:
      if not any(
        term in query_lower
        for term in ["report_date", "filing_date", "period_end_date", "filed_date"]
      ):
        warnings.append("💡 Consider filtering reports by date to limit results")

    # 5. Check for common XBRL elements
    if "element" in query_lower:
      has_known_element = any(
        element.lower() in query_lower for element in self.COMMON_XBRL_ELEMENTS
      )
      if not has_known_element and "qname" in query_lower:
        warnings.append(
          "💡 Verify XBRL element names (e.g., 'us-gaap:Revenues', not 'Revenue')"
        )

    return warnings

  def _validate_parameters(self, query: str, params: dict) -> list[str]:
    """Validate query parameters."""
    warnings = []

    # Extract parameter placeholders from query
    query_params = set(self.COMPILED_PATTERNS["param_pattern"].findall(query))

    # Check for missing parameters
    provided_params = set(params.keys())
    missing_params = query_params - provided_params
    if missing_params:
      warnings.append(f"Missing parameters: {', '.join(sorted(missing_params))}")

    # Check for unused parameters
    unused_params = provided_params - query_params
    if unused_params:
      warnings.append(f"Unused parameters provided: {', '.join(sorted(unused_params))}")

    # Validate parameter types for common cases
    for param_name, param_value in params.items():
      if "cik" in param_name.lower() and param_value:
        # CIK should be numeric or padded string
        if isinstance(param_value, str):
          if not param_value.isdigit():
            warnings.append(
              f"Parameter '{param_name}' appears to be a CIK but contains non-numeric value"
            )

      if "date" in param_name.lower() and param_value:
        # Basic date format check
        if isinstance(param_value, str) and not self.COMPILED_PATTERNS[
          "date_format"
        ].match(param_value):
          warnings.append(f"Parameter '{param_name}' may need date format YYYY-MM-DD")

    return warnings

  def suggest_query_fix(self, query: str, validation_result: ValidationResult) -> str:
    """Generate a fixed version of the query based on validation results."""
    fixed_query = query

    # Apply automatic fixes for Neo4j patterns
    for pattern in validation_result.neo4j_patterns_found:
      if pattern == "unbounded_path":
        fixed_query = re.sub(r"\[\s*\*\s*\]", "[*1..5]", fixed_query)

      elif pattern == "remove_command":
        fixed_query = re.sub(
          r"REMOVE\s+(\w+\.\w+)", r"SET \1 = NULL", fixed_query, flags=re.IGNORECASE
        )

      elif pattern == "show_command":
        fixed_query = re.sub(
          r"SHOW\s+TABLES\s*;?",
          "CALL show_tables() RETURN *",
          fixed_query,
          flags=re.IGNORECASE,
        )

      elif pattern == "where_in_match":
        # Move WHERE out of MATCH
        fixed_query = re.sub(
          r"MATCH\s*\(([^)]*)\s+WHERE\s+([^)]*)\)",
          r"MATCH (\1) WHERE \2",
          fixed_query,
          flags=re.IGNORECASE,
        )

      elif pattern == "label_in_where":
        # Fix label checking
        fixed_query = re.sub(
          r"WHERE\s+(\w+)\s*:\s*(\w+)",
          r"WHERE label(\1) = '\2'",
          fixed_query,
        )

    return fixed_query

  def format_validation_errors(self, validation: ValidationResult) -> str:
    """Format validation errors for display to AI agents."""
    if validation.is_valid:
      return "✅ Query validation passed"

    error_msg = "❌ **Query Validation Failed**\n\n"

    # Add errors
    if validation.errors:
      error_msg += "**Errors (must fix):**\n"
      for i, error in enumerate(validation.errors, 1):
        error_msg += f"{i}. {error}\n"
      error_msg += "\n"

    # Add suggestions
    if validation.suggestions:
      error_msg += "**💡 Suggestions:**\n"
      for i, suggestion in enumerate(validation.suggestions, 1):
        error_msg += f"{i}. {suggestion}\n"
      error_msg += "\n"

    # Add warnings
    if validation.warnings:
      error_msg += "**⚠️ Warnings:**\n"
      for warning in validation.warnings:
        error_msg += f"- {warning}\n"
      error_msg += "\n"

    # Add fixed query if available
    if validation.fixed_query and validation.fixed_query != validation.fixed_query:
      error_msg += "**🔧 Auto-fixed query:**\n"
      error_msg += f"```cypher\n{validation.fixed_query}\n```\n"

    return error_msg
