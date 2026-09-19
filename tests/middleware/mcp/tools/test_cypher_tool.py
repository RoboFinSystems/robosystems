"""Tests for CypherTool read-only validation.

Regression guard for the MCP read-path bypass: `read-graph-cypher` previously
validated with a hand-rolled keyword list that omitted LOAD / COPY / ATTACH /
INSTALL / EXPORT / USE, allowing a "read-only" tool to read local files, reach
instance metadata (SSRF), and ATTACH other tenants' databases. It now delegates
those categories to the central security.cypher_analyzer (the same analyzer the
REST /query endpoint uses).
"""

from types import SimpleNamespace

import pytest

from robosystems.middleware.mcp.tools.constants import (
  INVESTOR_AMOUNT_GUIDANCE,
  LEDGER_AMOUNT_GUIDANCE,
  LEDGER_STATUS_GUIDANCE,
)
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


class TestLedgerAmountGuidance:
  """Graph ledger amounts are dollars; the close and schedule tools speak cents.

  Two different models divided graph amounts by 100 after reading a cents rule
  in another tool's description, so the unit is stated wherever a ledger query
  is written.
  """

  @staticmethod
  def _tool(graph_id: str, extensions: tuple[str, ...]) -> CypherTool:
    return CypherTool(SimpleNamespace(graph_id=graph_id), schema_extensions=extensions)

  def test_ledger_graph_states_dollars(self):
    description = self._tool(
      "kg1a0b70352e2fdcc071f1", ("roboledger",)
    ).get_tool_definition()["description"]
    assert LEDGER_AMOUNT_GUIDANCE in description
    assert description.index(LEDGER_STATUS_GUIDANCE) < description.index(
      LEDGER_AMOUNT_GUIDANCE
    )

  def test_non_ledger_graph_omits_it(self):
    description = self._tool("kg1a0b70352e2fdcc071f1", ()).get_tool_definition()[
      "description"
    ]
    assert LEDGER_AMOUNT_GUIDANCE not in description

  def test_sec_repository_omits_it(self):
    description = self._tool("sec", ("roboledger",)).get_tool_definition()[
      "description"
    ]
    assert LEDGER_AMOUNT_GUIDANCE not in description


class TestInvestorAmountGuidance:
  """The graph's Position amounts are dollars; the investor API returns the
  same field names in cents, so the unit is stated where Cypher is written."""

  @staticmethod
  def _description(graph_id: str, extensions: tuple[str, ...]) -> str:
    tool = CypherTool(SimpleNamespace(graph_id=graph_id), schema_extensions=extensions)
    return tool.get_tool_definition()["description"]

  def test_investor_graph_states_dollars(self):
    assert INVESTOR_AMOUNT_GUIDANCE in self._description(
      "kg1a0b7042ceafb8156215", ("roboinvestor",)
    )

  def test_ledger_only_graph_omits_it(self):
    assert INVESTOR_AMOUNT_GUIDANCE not in self._description(
      "kg1a0b70352e2fdcc071f1", ("roboledger",)
    )

  def test_graph_with_both_extensions_states_both(self):
    description = self._description(
      "kg1a0b7042ceafb8156215", ("roboledger", "roboinvestor")
    )
    assert LEDGER_AMOUNT_GUIDANCE in description
    assert INVESTOR_AMOUNT_GUIDANCE in description

  def test_sec_repository_omits_it(self):
    assert INVESTOR_AMOUNT_GUIDANCE not in self._description(
      "sec", ("roboledger", "roboinvestor")
    )
