"""Tests for CypherTool read-only validation.

Regression guard for the MCP read-path bypass: `read-graph-cypher` previously
validated with a hand-rolled keyword list that omitted LOAD / COPY / ATTACH /
INSTALL / EXPORT / USE, allowing a "read-only" tool to read local files, reach
instance metadata (SSRF), and ATTACH other tenants' databases. It now delegates
those categories to the central security.cypher_analyzer (the same analyzer the
REST /query endpoint uses).
"""

import pytest

from robosystems.middleware.mcp.tools.cypher_tool import CypherTool


@pytest.fixture
def tool() -> CypherTool:
  # _validate_read_only uses no instance state, so bypass __init__ (which needs
  # a live GraphMCPClient) and exercise the validator directly.
  return CypherTool.__new__(CypherTool)


class TestValidateReadOnly:
  """`_validate_read_only` must reject write, bulk, admin, and DDL operations."""

  @pytest.mark.parametrize(
    "query",
    [
      "LOAD FROM '/etc/passwd' RETURN *",
      "COPY (MATCH (n) RETURN n) TO '/tmp/x.csv'",
      "IMPORT DATABASE '/tmp/dump'",
      "ATTACH '/data/other_tenant' AS x (dbtype kuzu)",
      "INSTALL httpfs",
      "INSTALL httpfs; LOAD FROM 'http://169.254.169.254/latest/meta-data/' RETURN *",
      "EXPORT DATABASE '/tmp/dump'",
      "USE other_database",
    ],
  )
  def test_blocks_bulk_and_admin_operations(self, tool, query):
    """The previously-missed bulk/admin verbs are now rejected."""
    with pytest.raises(ValueError, match="read-only"):
      tool._validate_read_only(query)

  @pytest.mark.parametrize(
    "query",
    [
      "CREATE (n:Foo)",
      "MATCH (n) SET n.x = 1",
      "MATCH (n) DELETE n",
      "MERGE (n:Foo {id: 1})",
      "MATCH (n) REMOVE n.x",
      "DROP TABLE Foo",
    ],
  )
  def test_still_blocks_writes(self, tool, query):
    """Existing write protection is preserved (no regression)."""
    with pytest.raises(ValueError, match="read-only"):
      tool._validate_read_only(query)

  @pytest.mark.parametrize(
    "query",
    [
      "MATCH (n:Entity) RETURN n LIMIT 10",
      "MATCH (a)-[r]->(b) RETURN DISTINCT labels(a)[0], type(r) LIMIT 5",
      # Bulk keyword appears only inside a string literal — must not false-positive
      "MATCH (n) WHERE n.name CONTAINS 'load' RETURN n",
    ],
  )
  def test_allows_legitimate_reads(self, tool, query):
    """Pure reads pass, including bulk keywords quoted inside string literals."""
    tool._validate_read_only(query)  # should not raise
