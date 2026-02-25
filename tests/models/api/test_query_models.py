"""Unit tests for graph query API models including Cypher query translation."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.query import (
  DEFAULT_QUERY_TIMEOUT,
  MAX_QUERY_LENGTH,
  CypherQueryRequest,
  CypherQueryResponse,
  translate_neo4j_to_lbug,
)


@pytest.mark.unit
class TestCypherQueryRequest:
  def test_valid_simple_query(self):
    model = CypherQueryRequest(query="MATCH (n) RETURN n LIMIT 10")
    assert model.query == "MATCH (n) RETURN n LIMIT 10"
    assert model.parameters is None
    assert model.timeout == DEFAULT_QUERY_TIMEOUT

  def test_query_with_parameters(self):
    model = CypherQueryRequest(
      query="MATCH (n:Entity {type: $type}) RETURN n",
      parameters={"type": "Company"},
    )
    assert model.parameters == {"type": "Company"}

  def test_query_with_timeout(self):
    model = CypherQueryRequest(
      query="MATCH (n) RETURN n",
      timeout=120,
    )
    assert model.timeout == 120

  def test_empty_query_rejected(self):
    with pytest.raises(ValidationError):
      CypherQueryRequest(query="")

  def test_whitespace_only_query_rejected(self):
    with pytest.raises(ValidationError) as exc_info:
      CypherQueryRequest(query="   ")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("query",) for e in errors)

  def test_query_max_length(self):
    long_query = "MATCH (n) RETURN n" + " " * (MAX_QUERY_LENGTH - 18)
    model = CypherQueryRequest(query=long_query)
    assert len(model.query) == MAX_QUERY_LENGTH

  def test_query_exceeds_max_length(self):
    too_long_query = "x" * (MAX_QUERY_LENGTH + 1)
    with pytest.raises(ValidationError) as exc_info:
      CypherQueryRequest(query=too_long_query)
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("query",) for e in errors)

  def test_timeout_minimum(self):
    model = CypherQueryRequest(query="MATCH (n) RETURN n", timeout=1)
    assert model.timeout == 1

  def test_timeout_below_minimum(self):
    with pytest.raises(ValidationError) as exc_info:
      CypherQueryRequest(query="MATCH (n) RETURN n", timeout=0)
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("timeout",) for e in errors)

  def test_timeout_maximum(self):
    model = CypherQueryRequest(query="MATCH (n) RETURN n", timeout=300)
    assert model.timeout == 300

  def test_timeout_above_maximum(self):
    with pytest.raises(ValidationError) as exc_info:
      CypherQueryRequest(query="MATCH (n) RETURN n", timeout=301)
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("timeout",) for e in errors)

  def test_extra_fields_forbidden(self):
    with pytest.raises(ValidationError) as exc_info:
      CypherQueryRequest(query="MATCH (n) RETURN n", extra="bad")  # type: ignore[call-arg]
    errors = exc_info.value.errors()
    assert any("extra" in e["type"] for e in errors)

  def test_query_required(self):
    with pytest.raises(ValidationError):
      CypherQueryRequest()  # type: ignore[call-arg]

  def test_model_dump(self):
    model = CypherQueryRequest(query="MATCH (n) RETURN n")
    dumped = model.model_dump()
    assert dumped["query"] == "MATCH (n) RETURN n"
    assert dumped["parameters"] is None
    assert dumped["timeout"] == DEFAULT_QUERY_TIMEOUT

  def test_json_round_trip(self):
    model = CypherQueryRequest(
      query="MATCH (n) RETURN n",
      parameters={"limit": 10},
      timeout=30,
    )
    json_str = model.model_dump_json()
    restored = CypherQueryRequest.model_validate_json(json_str)
    assert restored.query == model.query
    assert restored.parameters == model.parameters
    assert restored.timeout == model.timeout


@pytest.mark.unit
class TestCypherQueryResponse:
  def test_successful_response(self):
    model = CypherQueryResponse(
      success=True,
      data=[{"n": {"name": "Test"}}],
      columns=["n"],
      row_count=1,
      execution_time_ms=45.3,
      graph_id="kg1a2b3c4d5",
      timestamp="2024-01-15T10:00:00Z",
    )
    assert model.success is True
    assert model.row_count == 1
    assert model.error is None

  def test_error_response(self):
    model = CypherQueryResponse(
      success=False,
      data=None,
      columns=None,
      row_count=0,
      execution_time_ms=5.0,
      graph_id="kg1a2b3c4d5",
      timestamp="2024-01-15T10:00:00Z",
      error="Syntax error at line 1",
    )
    assert model.success is False
    assert model.error == "Syntax error at line 1"

  def test_empty_result(self):
    model = CypherQueryResponse(
      success=True,
      data=[],
      columns=["n"],
      row_count=0,
      execution_time_ms=12.0,
      graph_id="kg1a2b3c4d5",
      timestamp="2024-01-15T10:00:00Z",
    )
    assert model.data == []
    assert model.row_count == 0


@pytest.mark.unit
class TestTranslateNeo4jToLbug:
  def test_no_translation_needed(self):
    query = "MATCH (n) RETURN n LIMIT 10"
    assert translate_neo4j_to_lbug(query) == query

  def test_translate_db_labels(self):
    query = "CALL db.labels()"
    result = translate_neo4j_to_lbug(query)
    assert "SHOW_TABLES()" in result
    assert "RETURN *" in result

  def test_translate_db_schema(self):
    query = "CALL db.schema()"
    result = translate_neo4j_to_lbug(query)
    assert "SHOW_TABLES()" in result

  def test_translate_db_relationships(self):
    query = "CALL db.relationships()"
    result = translate_neo4j_to_lbug(query)
    assert "SHOW_TABLES()" in result

  def test_translate_db_relationship_types(self):
    query = "CALL db.relationshipTypes()"
    result = translate_neo4j_to_lbug(query)
    assert "SHOW_TABLES()" in result

  def test_translate_db_property_keys(self):
    query = "CALL db.propertyKeys()"
    result = translate_neo4j_to_lbug(query)
    assert "SHOW_TABLES()" in result
    assert "RETURN *" in result

  def test_translate_db_indexes(self):
    query = "CALL db.indexes()"
    result = translate_neo4j_to_lbug(query)
    assert "SHOW_TABLES()" in result

  def test_translate_db_constraints(self):
    query = "CALL db.constraints()"
    result = translate_neo4j_to_lbug(query)
    assert "SHOW_TABLES()" in result

  def test_case_insensitive_translation(self):
    query = "CALL DB.LABELS()"
    result = translate_neo4j_to_lbug(query)
    assert "SHOW_TABLES()" in result

  def test_query_with_existing_return(self):
    # If there's already a RETURN clause, it shouldn't add another
    query = "CALL db.labels() RETURN *"
    result = translate_neo4j_to_lbug(query)
    assert "SHOW_TABLES()" in result

  def test_non_neo4j_call_not_translated(self):
    query = "CALL myCustomProcedure()"
    result = translate_neo4j_to_lbug(query)
    assert result == query
