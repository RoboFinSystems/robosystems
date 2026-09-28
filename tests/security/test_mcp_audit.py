"""The MCP mutation audit: every mutating MCP call, from an external client
or an in-app operator, leaves one row — never its arguments, only their
fingerprint — and a failure to record never breaks the call."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.orm import sessionmaker

from robosystems.middleware.mcp.tools.classification import is_mutating_tool
from robosystems.middleware.mcp.tools.manager import GraphMCPTools
from robosystems.models.core import McpMutationAudit
from robosystems.security.mcp_audit import (
  McpCaller,
  build_record,
  fingerprint,
  object_ids,
  write_record,
)
from robosystems.security.request_context import (
  RequestPrincipal,
  bind_principal,
  bind_request_id,
  reset_principal,
  reset_request_id,
)

GRAPH = "kg1a2b3c4d5e6f7a8b9c0"


def _record(**overrides):
  kwargs = {
    "graph_id": GRAPH,
    "tool_name": "create-agent",
    "arguments": {"name": "Linear", "agent_type": "vendor"},
    "result": {"id": "agt_1", "name": "Linear"},
    "error": None,
    "duration_ms": 12.345,
    "user_id": "user_1",
    "caller": None,
  }
  kwargs.update(overrides)
  return build_record(**kwargs)


class TestClassification:
  @pytest.mark.parametrize(
    "name", ["create-agent", "remember", "write-graph-cypher", "close-period"]
  )
  def test_writes_are_mutating(self, name):
    assert is_mutating_tool(name)

  @pytest.mark.parametrize(
    "name", ["read-graph-cypher", "get-graph-schema", "recall", "list-agents"]
  )
  def test_reads_are_not(self, name):
    assert not is_mutating_tool(name)

  def test_an_unknown_tool_is_a_mutation(self):
    assert is_mutating_tool("some-new-tool")


class TestRecord:
  def test_a_completed_write(self):
    record = _record()
    assert record.status == "completed"
    assert record.error_code is None
    assert record.object_ids == ["agt_1"]
    assert record.duration_ms == 12.35
    assert record.caller_kind == "client"

  def test_arguments_are_fingerprinted_never_kept(self):
    record = _record()
    assert record.arguments_sha256 == fingerprint(
      {"agent_type": "vendor", "name": "Linear"}
    )
    assert "Linear" not in json.dumps(record.__dict__ | {"object_ids": []})

  def test_an_error_result_is_failed_with_its_code(self):
    record = _record(result={"error": "conflict", "message": "exists"})
    assert (record.status, record.error_code) == ("failed", "conflict")
    assert record.object_ids == []

  def test_an_exception_is_failed_with_its_type(self):
    record = _record(result=None, error=ValueError("bad"))
    assert (record.status, record.error_code) == ("failed", "ValueError")

  def test_an_operator_run_is_attributed_to_its_run(self):
    record = _record(caller=McpCaller(operator_type="author", operation_id="op_1"))
    assert record.caller_kind == "operator"
    assert (record.operator_type, record.operation_id) == ("author", "op_1")

  def test_an_external_client_carries_its_credential(self):
    request_token = bind_request_id("req_1")
    principal_token = bind_principal(
      RequestPrincipal(
        user_id="user_1", auth_method="api_key", api_key_prefix="rfsab12"
      )
    )
    try:
      record = _record()
    finally:
      reset_principal(principal_token)
      reset_request_id(request_token)
    assert record.request_id == "req_1"
    assert record.auth_method == "api_key"
    assert record.api_key_prefix == "rfsab12"


class TestObjectIds:
  def test_wrapped_and_argument_ids(self):
    ids = object_ids(
      {"structure_id": "s_7"}, {"success": True, "memory": {"id": "mem_1"}}
    )
    assert ids == ["mem_1", "s_7"]

  def test_duplicates_collapse(self):
    assert object_ids({"id": "a"}, {"id": "a"}) == ["a"]


class TestWriteRecord:
  def test_a_row_lands_in_the_platform_db(self, test_db):
    factory = sessionmaker(bind=test_db.get_bind())
    with patch("robosystems.db.platform.SessionFactory", factory):
      write_record(
        _record(caller=McpCaller(operator_type="author", operation_id="op_9"))
      )

    session = factory()
    try:
      row = (
        session.query(McpMutationAudit)
        .filter(McpMutationAudit.operation_id == "op_9")
        .one()
      )
      assert row.id.startswith("mcpa_")
      assert (row.tool_name, row.status, row.caller_kind) == (
        "create-agent",
        "completed",
        "operator",
      )
      assert row.object_ids == ["agt_1"]
      assert row.org_id is None  # no graph row in this test
    finally:
      session.close()

  def test_a_failed_insert_is_logged_not_raised(self):
    broken = MagicMock(side_effect=RuntimeError("db down"))
    with (
      patch("robosystems.db.platform.SessionFactory", broken),
      patch("robosystems.security.mcp_audit.logger") as log,
    ):
      write_record(_record())
    log.error.assert_called_once()
    assert log.error.call_args.args[0] == "mcp_mutation_audit.unrecorded"
    assert log.error.call_args.kwargs["extra"]["audit"]["tool_name"] == "create-agent"


def _tools(caller: McpCaller | None = None) -> GraphMCPTools:
  client = MagicMock()
  client.graph_id = GRAPH
  client.user_id = "user_1"
  client.audit_caller = caller
  return GraphMCPTools(client, schema_extensions=("roboledger",))


@pytest.mark.asyncio
class TestHook:
  async def test_a_mutating_call_is_recorded(self):
    tools = _tools(McpCaller(operator_type="author", operation_id="op_2"))
    with (
      patch.object(tools, "_dispatch_tool", AsyncMock(return_value={"id": "agt_3"})),
      patch("robosystems.middleware.mcp.tools.manager.write_record") as write,
    ):
      result = await tools.call_tool("create-agent", {"name": "X"}, return_raw=True)

    assert result == {"id": "agt_3"}
    record = write.call_args.args[0]
    assert (record.tool_name, record.status, record.operation_id) == (
      "create-agent",
      "completed",
      "op_2",
    )
    assert record.object_ids == ["agt_3"]

  async def test_a_read_is_not_recorded(self):
    tools = _tools()
    with (
      patch.object(tools, "_dispatch_tool", AsyncMock(return_value=[{"n": 1}])),
      patch("robosystems.middleware.mcp.tools.manager.write_record") as write,
    ):
      await tools.call_tool("read-graph-cypher", {"query": "MATCH (n) RETURN n"})
    write.assert_not_called()

  async def test_a_raising_write_is_recorded_and_still_raises(self):
    tools = _tools()
    with (
      patch.object(tools, "_dispatch_tool", AsyncMock(side_effect=ValueError("x"))),
      patch("robosystems.middleware.mcp.tools.manager.write_record") as write,
      pytest.raises(ValueError),
    ):
      await tools.call_tool("remember", {"text": "t"})
    assert write.call_args.args[0].status == "failed"

  async def test_a_json_rendered_result_is_read_back(self):
    tools = _tools()
    rendered = json.dumps({"error": "conflict", "message": "exists"})
    with (
      patch.object(tools, "_dispatch_tool", AsyncMock(return_value=rendered)),
      patch("robosystems.middleware.mcp.tools.manager.write_record") as write,
    ):
      result = await tools.call_tool("create-agent", {"name": "X"})
    assert result == rendered
    assert write.call_args.args[0].error_code == "conflict"

  async def test_a_failure_to_build_the_record_never_breaks_the_call(self):
    client = MagicMock(spec=["user_id"])  # no graph_id: the record cannot build
    tools = GraphMCPTools(client, schema_extensions=("roboledger",))
    with (
      patch.object(tools, "_dispatch_tool", AsyncMock(return_value={"id": "a"})),
      patch("robosystems.middleware.mcp.tools.manager.logger") as log,
    ):
      result = await tools.call_tool("create-agent", {}, return_raw=True)
    assert result == {"id": "a"}
    assert log.error.call_args.args[0] == "mcp_mutation_audit.unrecorded"
