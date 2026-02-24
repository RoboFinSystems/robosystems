"""Graph query validator tests."""

import pytest

from robosystems.middleware.mcp.query_validator import (
    GraphQueryValidator,
    ValidationResult,
)


@pytest.fixture
def validator():
    """Validator with default SEC labels."""
    return GraphQueryValidator()


@pytest.fixture
def schema_validator():
    """Validator with explicit schema."""
    schema = [
        {"label": "Entity", "type": "node", "properties": [{"name": "cik"}, {"name": "name"}]},
        {"label": "Report", "type": "node", "properties": [{"name": "filing_date"}]},
        {"label": "Fact", "type": "node", "properties": [{"name": "value"}, {"name": "qname"}]},
        {"label": "ENTITY_HAS_REPORT", "type": "relationship", "properties": []},
    ]
    return GraphQueryValidator(schema=schema)


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_defaults(self):
        result = ValidationResult(is_valid=True)
        assert result.errors == []
        assert result.warnings == []
        assert result.suggestions == []
        assert result.complexity_score == 0
        assert result.fixed_query is None


class TestBasicSyntaxValidation:
    """Tests for basic syntax validation."""

    def test_empty_query(self, validator):
        result = validator.validate("")
        assert result.is_valid is False
        assert any("empty" in e.lower() for e in result.errors)

    def test_unclosed_single_quote(self, validator):
        result = validator.validate("MATCH (n) WHERE n.name = 'test RETURN n")
        assert any("single quote" in e.lower() for e in result.errors)

    def test_unclosed_double_quote(self, validator):
        result = validator.validate('MATCH (n) WHERE n.name = "test RETURN n')
        assert any("double quote" in e.lower() for e in result.errors)

    def test_unmatched_parentheses(self, validator):
        result = validator.validate("MATCH (n WHERE n.name = 'test' RETURN n")
        assert any("parenthes" in e.lower() for e in result.errors)

    def test_unmatched_brackets(self, validator):
        result = validator.validate("MATCH (n)-[r RETURN n")
        assert any("bracket" in e.lower() for e in result.errors)

    def test_valid_query_passes(self, validator):
        result = validator.validate("MATCH (n:Entity) RETURN n LIMIT 10")
        assert result.is_valid is True


class TestNeo4jPatternDetection:
    """Tests for Neo4j pattern detection."""

    def test_unbounded_path(self, validator):
        result = validator.validate("MATCH (a)-[*]->(b) RETURN a, b")
        assert any("unbounded" in e.lower() for e in result.errors)
        assert "unbounded_path" in result.neo4j_patterns_found

    def test_remove_command(self, validator):
        result = validator.validate("MATCH (n) REMOVE n.prop RETURN n")
        assert any("remove" in e.lower() for e in result.errors)
        assert "remove_command" in result.neo4j_patterns_found

    def test_foreach_detected(self, validator):
        result = validator.validate("MATCH (n) FOREACH (x IN [1,2,3] | SET n.val = x)")
        assert any("foreach" in e.lower() for e in result.errors)
        assert "foreach" in result.neo4j_patterns_found

    def test_property_append(self, validator):
        result = validator.validate("MATCH (n) SET n += {name: 'test'} RETURN n")
        assert any("+=" in e for e in result.errors)

    def test_show_command(self, validator):
        result = validator.validate("SHOW TABLES")
        assert any("show" in e.lower() for e in result.errors)
        assert "show_command" in result.neo4j_patterns_found

    def test_neo4j_function_toInteger(self, validator):
        result = validator.validate("MATCH (n) RETURN toInteger(n.val)")
        assert any("toInteger" in e for e in result.errors)

    def test_neo4j_function_labels(self, validator):
        result = validator.validate("MATCH (n) RETURN labels(n)")
        assert any("labels" in e for e in result.errors)

    def test_type_check_pattern(self, validator):
        result = validator.validate("MATCH (n) WHERE n.val IS :: INTEGER RETURN n")
        assert any("type check" in e.lower() for e in result.errors)


class TestMetadataQuery:
    """Tests for metadata query detection."""

    def test_show_tables_is_metadata(self, validator):
        result = validator.validate("CALL show_tables() RETURN *")
        assert result.is_valid is True  # Metadata queries skip validation

    def test_table_info_is_metadata(self, validator):
        result = validator.validate("CALL table_info('Entity') RETURN *")
        assert result.is_valid is True


class TestPerformanceAnalysis:
    """Tests for performance analysis."""

    def test_no_limit_warning(self, validator):
        result = validator.validate("MATCH (n:Entity) RETURN n")
        assert any("limit" in w.lower() for w in result.warnings)

    def test_has_limit_no_warning(self, validator):
        result = validator.validate("MATCH (n:Entity) RETURN n LIMIT 10")
        assert not any("No LIMIT" in w for w in result.warnings)

    def test_generic_node_pattern(self, validator):
        result = validator.validate("MATCH () RETURN count(*)")
        assert any("generic" in w.lower() for w in result.warnings)

    def test_multiple_match_without_with(self, validator):
        result = validator.validate(
            "MATCH (a:Entity) MATCH (b:Report) MATCH (c:Fact) RETURN a, b, c"
        )
        assert any("cartesian" in w.lower() for w in result.warnings)

    def test_string_operation_warning(self, validator):
        result = validator.validate(
            "MATCH (n:Entity) WHERE n.name CONTAINS 'test' RETURN n LIMIT 10"
        )
        assert any("CONTAINS" in w for w in result.warnings)

    def test_high_complexity_warning(self, validator):
        # Trigger multiple complexity factors
        result = validator.validate(
            "MATCH () MATCH (a) MATCH (b) MATCH (c) MATCH (d) RETURN a, b, c, d"
        )
        assert result.complexity_score > 50


class TestSchemaValidation:
    """Tests for schema validation."""

    def test_unknown_label_with_schema(self, schema_validator):
        result = schema_validator.validate(
            "MATCH (n:UnknownLabel) RETURN n LIMIT 10"
        )
        assert any("unknown" in w.lower() for w in result.warnings)

    def test_known_label_no_warning(self, schema_validator):
        result = schema_validator.validate("MATCH (n:Entity) RETURN n LIMIT 10")
        # Should not warn about Entity since it's in schema
        assert not any("Unknown labels" in w and "Entity" in w for w in result.warnings)

    def test_unknown_property_with_schema(self, schema_validator):
        result = schema_validator.validate(
            "MATCH (n:Entity) WHERE n.unknown_prop = 'test' RETURN n LIMIT 10"
        )
        assert any("not found" in w.lower() for w in result.warnings)


class TestFinancialBestPractices:
    """Tests for financial query best practices."""

    def test_fact_without_unit(self, validator):
        result = validator.validate(
            "MATCH (f:Fact) WHERE f.value > 1000 RETURN f LIMIT 10"
        )
        assert any("unit" in w.lower() for w in result.warnings)

    def test_element_without_qname(self, validator):
        result = validator.validate("MATCH (e:Element) RETURN e LIMIT 10")
        assert any("qname" in w.lower() for w in result.warnings)

    def test_report_without_date(self, validator):
        result = validator.validate("MATCH (r:Report) RETURN r LIMIT 10")
        assert any("date" in w.lower() for w in result.warnings)


class TestParameterValidation:
    """Tests for parameter validation."""

    def test_missing_parameter(self, validator):
        result = validator.validate(
            "MATCH (n:Entity) WHERE n.cik = $cik RETURN n LIMIT 10",
            params={"other_param": "value"},
        )
        assert any("missing" in w.lower() for w in result.warnings)

    def test_unused_parameter(self, validator):
        result = validator.validate(
            "MATCH (n:Entity) RETURN n LIMIT 10",
            params={"unused_param": "value"},
        )
        assert any("unused" in w.lower() for w in result.warnings)

    def test_invalid_cik_parameter(self, validator):
        result = validator.validate(
            "MATCH (n:Entity) WHERE n.cik = $cik RETURN n LIMIT 10",
            params={"cik": "not_a_number"},
        )
        assert any("cik" in w.lower() for w in result.warnings)

    def test_valid_cik_parameter(self, validator):
        result = validator.validate(
            "MATCH (n:Entity) WHERE n.cik = $cik RETURN n LIMIT 10",
            params={"cik": "0000320193"},
        )
        # Should not warn about CIK format
        assert not any("non-numeric" in w.lower() for w in result.warnings)

    def test_invalid_date_format(self, validator):
        result = validator.validate(
            "MATCH (r:Report) WHERE r.filing_date = $date RETURN r LIMIT 10",
            params={"date": "Jan 15 2024"},
        )
        assert any("date format" in w.lower() for w in result.warnings)


class TestQueryFix:
    """Tests for query fix suggestions."""

    def test_fix_unbounded_path(self, validator):
        result = validator.validate("MATCH (a)-[*]->(b) RETURN a, b")
        assert result.fixed_query is not None
        assert "[*1..5]" in result.fixed_query

    def test_fix_remove_command(self, validator):
        result = validator.validate("MATCH (n) REMOVE n.prop RETURN n")
        assert result.fixed_query is not None
        assert "SET" in result.fixed_query
        assert "NULL" in result.fixed_query

    def test_fix_show_tables(self, validator):
        result = validator.validate("SHOW TABLES")
        assert result.fixed_query is not None
        assert "CALL show_tables()" in result.fixed_query


class TestFormatValidationErrors:
    """Tests for format_validation_errors."""

    def test_valid_query_format(self, validator):
        result = validator.validate("MATCH (n:Entity) RETURN n LIMIT 10")
        formatted = validator.format_validation_errors(result)
        assert "[OK]" in formatted

    def test_invalid_query_format(self, validator):
        result = validator.validate("")
        formatted = validator.format_validation_errors(result)
        assert "[FAILED]" in formatted
        assert "Errors" in formatted

    def test_format_with_suggestions(self, validator):
        result = validator.validate("MATCH (a)-[*]->(b) RETURN a, b")
        formatted = validator.format_validation_errors(result)
        assert "Suggestions" in formatted

    def test_format_with_fixed_query(self, validator):
        result = validator.validate("MATCH (a)-[*]->(b) RETURN a, b")
        formatted = validator.format_validation_errors(result)
        assert "Auto-fixed" in formatted


class TestNeo4jTypeMappings:
    """Tests for Neo4j type mappings."""

    def test_integer_mapping(self, validator):
        assert validator._map_neo4j_type("INTEGER") == "INT64"

    def test_float_mapping(self, validator):
        assert validator._map_neo4j_type("FLOAT") == "DOUBLE"

    def test_boolean_mapping(self, validator):
        assert validator._map_neo4j_type("BOOLEAN") == "BOOL"

    def test_datetime_mapping(self, validator):
        assert validator._map_neo4j_type("DATETIME") == "TIMESTAMP"

    def test_unknown_type_passthrough(self, validator):
        assert validator._map_neo4j_type("CUSTOM") == "CUSTOM"

    def test_case_insensitive(self, validator):
        assert validator._map_neo4j_type("integer") == "INT64"
