"""Tests for AI Memory MCP tools."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from robosystems.middleware.mcp.tools.memory import (
  AddNodeTableTool,
  AddRelationshipTableTool,
  WriteCypherTool,
  _validate_subgraph_context,
  _validate_write_query,
)


@pytest.fixture
def mock_subgraph_client():
  """Mock client pointing to a subgraph."""
  client = MagicMock()
  client.graph_id = "kg1234567890abcdef_memory"
  client.execute_query = AsyncMock(return_value=[])
  client.graph_client = MagicMock()
  client.graph_client.install_schema = AsyncMock(
    return_value={
      "success": True,
      "message": "Schema installed",
      "statements_executed": 1,
    }
  )
  return client


@pytest.fixture
def mock_parent_client():
  """Mock client pointing to a parent graph (not a subgraph)."""
  client = MagicMock()
  client.graph_id = "kg1234567890abcdef"
  client.execute_query = AsyncMock(return_value=[])
  client.graph_client = MagicMock()
  return client


class TestValidateSubgraphContext:
  """Tests for subgraph-only enforcement."""

  @pytest.mark.unit
  def test_subgraph_passes(self):
    assert _validate_subgraph_context("kg1234567890abcdef_memory") is None

  @pytest.mark.unit
  def test_parent_graph_blocked(self):
    result = _validate_subgraph_context("kg1234567890abcdef")
    assert result is not None
    assert result["error"] == "subgraph_required"

  @pytest.mark.unit
  def test_shared_repo_subgraph_passes(self):
    assert _validate_subgraph_context("sec_historical") is None


class TestValidateWriteQuery:
  """Tests for write query validation."""

  @pytest.mark.unit
  def test_create_allowed(self):
    assert _validate_write_query("CREATE (n:Concept {name: 'test'})") is None

  @pytest.mark.unit
  def test_merge_allowed(self):
    assert _validate_write_query("MERGE (n:Concept {identifier: 'c1'})") is None

  @pytest.mark.unit
  def test_set_allowed(self):
    assert _validate_write_query("MATCH (n:Concept) SET n.name = 'new'") is None

  @pytest.mark.unit
  def test_delete_allowed(self):
    assert _validate_write_query("MATCH (n:Concept) DELETE n") is None

  @pytest.mark.unit
  def test_detach_delete_allowed(self):
    assert _validate_write_query("MATCH (n:Concept) DETACH DELETE n") is None

  @pytest.mark.unit
  def test_remove_allowed(self):
    assert _validate_write_query("MATCH (n:Concept) REMOVE n.category") is None

  @pytest.mark.unit
  def test_read_only_blocked(self):
    result = _validate_write_query("MATCH (n) RETURN n")
    assert result is not None
    assert "write operation" in result

  @pytest.mark.unit
  def test_drop_blocked(self):
    result = _validate_write_query("DROP TABLE Concept")
    assert result is not None
    assert "Blocked" in result

  @pytest.mark.unit
  def test_alter_blocked(self):
    result = _validate_write_query("ALTER TABLE Concept ADD COLUMN x STRING")
    assert result is not None
    assert "Blocked" in result

  @pytest.mark.unit
  def test_create_node_table_blocked(self):
    result = _validate_write_query("CREATE NODE TABLE Foo(id STRING, PRIMARY KEY(id))")
    assert result is not None
    assert "Blocked" in result

  @pytest.mark.unit
  def test_load_csv_blocked(self):
    result = _validate_write_query("LOAD CSV FROM 'file.csv' AS row CREATE (n:Foo)")
    assert result is not None
    assert "Blocked" in result

  @pytest.mark.unit
  def test_keyword_in_string_literal_not_blocked(self):
    # "DROP" in a string value should not trigger the block
    result = _validate_write_query("CREATE (n:Concept {name: 'DROP this idea'})")
    assert result is None


class TestWriteCypherTool:
  """Tests for WriteCypherTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_tool_definition(self, mock_subgraph_client):
    tool = WriteCypherTool(mock_subgraph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "write-graph-cypher"
    assert "query" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_write_success(self, mock_subgraph_client):
    tool = WriteCypherTool(mock_subgraph_client)
    result = await tool.execute(
      {"query": "CREATE (n:Concept {identifier: 'c1', name: 'test'})"}
    )
    assert result["success"] is True
    mock_subgraph_client.execute_query.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_blocked_on_parent_graph(self, mock_parent_client):
    tool = WriteCypherTool(mock_parent_client)
    result = await tool.execute(
      {"query": "CREATE (n:Concept {identifier: 'c1', name: 'test'})"}
    )
    assert result["error"] == "subgraph_required"
    mock_parent_client.execute_query.assert_not_called()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_empty_query_rejected(self, mock_subgraph_client):
    tool = WriteCypherTool(mock_subgraph_client)
    result = await tool.execute({"query": ""})
    assert result["error"] == "invalid_query"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_read_query_rejected(self, mock_subgraph_client):
    tool = WriteCypherTool(mock_subgraph_client)
    result = await tool.execute({"query": "MATCH (n) RETURN n"})
    assert result["error"] == "invalid_query"


class TestAddNodeTableTool:
  """Tests for AddNodeTableTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_tool_definition(self, mock_subgraph_client):
    tool = AddNodeTableTool(mock_subgraph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "add-node-table"
    assert "table_name" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_node_table_success(self, mock_subgraph_client):
    tool = AddNodeTableTool(mock_subgraph_client)
    result = await tool.execute(
      {
        "table_name": "CompanyProfile",
        "properties": [
          {"name": "identifier", "type": "STRING", "is_primary_key": True},
          {"name": "ticker", "type": "STRING"},
          {"name": "sector", "type": "STRING"},
        ],
      }
    )
    assert result["success"] is True
    assert result["table_name"] == "CompanyProfile"
    mock_subgraph_client.graph_client.install_schema.assert_called_once()
    call_args = mock_subgraph_client.graph_client.install_schema.call_args
    ddl = call_args.kwargs["custom_ddl"]
    assert "CREATE NODE TABLE IF NOT EXISTS CompanyProfile" in ddl
    assert "PRIMARY KEY(identifier)" in ddl

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_blocked_on_parent_graph(self, mock_parent_client):
    tool = AddNodeTableTool(mock_parent_client)
    result = await tool.execute(
      {
        "table_name": "Foo",
        "properties": [
          {"name": "identifier", "type": "STRING", "is_primary_key": True}
        ],
      }
    )
    assert result["error"] == "subgraph_required"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_invalid_table_name(self, mock_subgraph_client):
    tool = AddNodeTableTool(mock_subgraph_client)
    result = await tool.execute(
      {
        "table_name": "123bad",
        "properties": [
          {"name": "identifier", "type": "STRING", "is_primary_key": True}
        ],
      }
    )
    assert result["error"] == "invalid_table_name"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_missing_primary_key(self, mock_subgraph_client):
    tool = AddNodeTableTool(mock_subgraph_client)
    result = await tool.execute(
      {
        "table_name": "Foo",
        "properties": [{"name": "name", "type": "STRING"}],
      }
    )
    assert result["error"] == "missing_primary_key"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_invalid_property_type(self, mock_subgraph_client):
    tool = AddNodeTableTool(mock_subgraph_client)
    result = await tool.execute(
      {
        "table_name": "Foo",
        "properties": [
          {"name": "identifier", "type": "BIGINT", "is_primary_key": True}
        ],
      }
    )
    assert result["error"] == "invalid_property_type"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_empty_properties(self, mock_subgraph_client):
    tool = AddNodeTableTool(mock_subgraph_client)
    result = await tool.execute({"table_name": "Foo", "properties": []})
    assert result["error"] == "invalid_properties"


class TestAddRelationshipTableTool:
  """Tests for AddRelationshipTableTool."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_tool_definition(self, mock_subgraph_client):
    tool = AddRelationshipTableTool(mock_subgraph_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "add-relationship-table"
    assert "from_node" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_create_relationship_success(self, mock_subgraph_client):
    tool = AddRelationshipTableTool(mock_subgraph_client)
    result = await tool.execute(
      {
        "table_name": "FINDING_SUPPORTS",
        "from_node": "ResearchFinding",
        "to_node": "Concept",
        "properties": [{"name": "strength", "type": "DOUBLE"}],
      }
    )
    assert result["success"] is True
    assert result["table_name"] == "FINDING_SUPPORTS"
    call_args = mock_subgraph_client.graph_client.install_schema.call_args
    ddl = call_args.kwargs["custom_ddl"]
    assert "CREATE REL TABLE IF NOT EXISTS FINDING_SUPPORTS" in ddl
    assert "FROM ResearchFinding TO Concept" in ddl
    assert "strength DOUBLE" in ddl

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_relationship_without_properties(self, mock_subgraph_client):
    tool = AddRelationshipTableTool(mock_subgraph_client)
    result = await tool.execute(
      {
        "table_name": "LINKS_TO",
        "from_node": "Concept",
        "to_node": "Concept",
      }
    )
    assert result["success"] is True
    call_args = mock_subgraph_client.graph_client.install_schema.call_args
    ddl = call_args.kwargs["custom_ddl"]
    assert "FROM Concept TO Concept)" in ddl

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_blocked_on_parent_graph(self, mock_parent_client):
    tool = AddRelationshipTableTool(mock_parent_client)
    result = await tool.execute(
      {
        "table_name": "REL",
        "from_node": "A",
        "to_node": "B",
      }
    )
    assert result["error"] == "subgraph_required"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_invalid_from_node(self, mock_subgraph_client):
    tool = AddRelationshipTableTool(mock_subgraph_client)
    result = await tool.execute(
      {
        "table_name": "REL",
        "from_node": "123bad",
        "to_node": "Concept",
      }
    )
    assert result["error"] == "invalid_from_node"
