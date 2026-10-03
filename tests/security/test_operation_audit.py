"""The mutation audit: every mutating call on a graph — REST, an external MCP
client, or an in-app operator — leaves one row, never its arguments, only
their fingerprint; and a failure to record never breaks the call."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.orm import sessionmaker

from robosystems.middleware.mcp.tools.classification import is_mutating_tool
from robosystems.middleware.mcp.tools.manager import GraphMCPTools
from robosystems.middleware.operations import fingerprint_body, log_operation_audit
from robosystems.models.core import OperationMutationAudit
from robosystems.security.operation_audit import (
  AuditCaller,
  build_tool_record,
  object_ids,
  record_api_operation,
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
  return build_tool_record(**kwargs)


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
    assert record.surface == "mcp"

  def test_arguments_are_fingerprinted_never_kept(self):
    record = _record()
    assert record.arguments_fingerprint == fingerprint_body(
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
    record = _record(caller=AuditCaller(operator_type="author", operation_id="op_1"))
    assert record.surface == "operator"
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

  @pytest.mark.parametrize(
    "result",
    [
      {"document_id": "doc_1"},
      {"deleted": True, "report_id": "rpt_1"},
      {"backup_id": "bak_1"},
      {"mapping_id": "map_1", "association_id": "assoc_1"},
      {"connection_id": "conn_1"},
    ],
  )
  def test_any_id_key_is_captured(self, result):
    ids = object_ids({}, result)
    assert ids == [v for k, v in result.items() if k.endswith("_id")]

  def test_context_ids_are_left_to_their_columns(self):
    ids = object_ids({"graph_id": "kg_1", "user_id": "u"}, {"org_id": "o", "id": "x"})
    assert ids == ["x"]

  @pytest.mark.parametrize("tax_id", ["123-45-6789", "123456789", "12-3456789"])
  def test_a_tax_id_is_never_captured(self, tax_id):
    ids = object_ids(
      {"name": "Linear", "tax_id": tax_id},
      {"agent": {"id": "agt_1", "tax_id": tax_id}, "vendor_tax_id": tax_id},
    )
    assert ids == ["agt_1"]

  def test_a_prefixed_tax_id_key_is_never_captured(self):
    assert object_ids({}, {"id": "agt_1", "vendor_tax_id": "GB123456789"}) == ["agt_1"]

  def test_a_bare_number_is_not_an_object_id(self):
    assert object_ids({}, {"id": "agt_1", "account_number_id": "4417"}) == ["agt_1"]

  def test_a_bulk_write_is_capped(self):
    result = {f"item{i}_id": f"id_{i}" for i in range(50)}
    assert len(object_ids({}, result)) == 20


class TestWriteRecord:
  def test_a_row_lands_in_the_platform_db(self, test_db):
    factory = sessionmaker(bind=test_db.get_bind())
    with patch("robosystems.db.platform.SessionFactory", factory):
      write_record(
        _record(caller=AuditCaller(operator_type="author", operation_id="op_9"))
      )

    session = factory()
    try:
      row = (
        session.query(OperationMutationAudit)
        .filter(OperationMutationAudit.operation_id == "op_9")
        .one()
      )
      assert row.id.startswith("oma_")
      assert (row.operation_name, row.status, row.surface) == (
        "create-agent",
        "completed",
        "operator",
      )
      assert row.object_ids == ["agt_1"]
      assert row.org_id is None  # no graph row in this test
    finally:
      session.close()

  def test_a_rest_agent_result_lands_without_its_tax_id(self, test_db):
    from robosystems.security.operation_audit import MutationRecord

    factory = sessionmaker(bind=test_db.get_bind())
    result = {"id": "agt_77", "name": "Linear", "tax_id": "123-45-6789"}
    record = MutationRecord(
      graph_id=GRAPH,
      surface="api",
      operation_name="create-agent",
      status="completed",
      duration_ms=3.0,
      operation_id="op_tax_1",
      object_ids=object_ids({"tax_id": "123-45-6789"}, result),
    )
    with patch("robosystems.db.platform.SessionFactory", factory):
      write_record(record)

    session = factory()
    try:
      row = (
        session.query(OperationMutationAudit)
        .filter(OperationMutationAudit.operation_id == "op_tax_1")
        .one()
      )
      assert row.object_ids == ["agt_77"]
    finally:
      session.close()

  def test_a_failed_insert_is_logged_not_raised(self):
    broken = MagicMock(side_effect=RuntimeError("db down"))
    with (
      patch("robosystems.db.platform.SessionFactory", broken),
      patch("robosystems.security.operation_audit.logger") as log,
    ):
      write_record(_record())
    log.error.assert_called_once()
    assert log.error.call_args.args[0] == "operation_mutation_audit.unrecorded"
    assert (
      log.error.call_args.kwargs["extra"]["audit"]["operation_name"] == "create-agent"
    )


def _tools(caller: AuditCaller | None = None) -> GraphMCPTools:
  client = MagicMock()
  client.graph_id = GRAPH
  client.user_id = "user_1"
  client.audit_caller = caller
  return GraphMCPTools(client, schema_extensions=("roboledger",))


@pytest.mark.asyncio
class TestHook:
  async def test_a_mutating_call_is_recorded(self):
    tools = _tools(AuditCaller(operator_type="author", operation_id="op_2"))
    with (
      patch.object(tools, "_dispatch_tool", AsyncMock(return_value={"id": "agt_3"})),
      patch("robosystems.middleware.mcp.tools.manager.write_record") as write,
    ):
      result = await tools.call_tool("create-agent", {"name": "X"}, return_raw=True)

    assert result == {"id": "agt_3"}
    record = write.call_args.args[0]
    assert (record.operation_name, record.status, record.operation_id) == (
      "create-agent",
      "completed",
      "op_2",
    )
    assert record.object_ids == ["agt_3"]

  async def test_withheld_arguments_never_reach_the_record(self):
    tools = _tools()
    with (
      patch.object(tools, "_dispatch_tool", AsyncMock(return_value={"id": "agt_4"})),
      patch("robosystems.middleware.mcp.tools.manager.write_record") as write,
    ):
      await tools.call_tool(
        "create-agent", {"name": "X", "tax_id": "123-45-6789"}, return_raw=True
      )

    record = write.call_args.args[0]
    assert record.object_ids == ["agt_4"]
    assert record.arguments_fingerprint == fingerprint_body({"name": "X"})

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
    assert log.error.call_args.args[0] == "operation_mutation_audit.unrecorded"


class TestRestSurface:
  def _log(self, **overrides):
    kwargs = {
      "operation_name": "create-agent",
      "operation_id": "op_rest_1",
      "user_id": "user_1",
      "graph_id": GRAPH,
      "duration_ms": 5.0,
      "status": "completed",
      "arguments_fingerprint": "f" * 64,
      "result": {"id": "agt_9", "name": "Linear"},
    }
    kwargs.update(overrides)
    with patch("robosystems.security.operation_audit.record_api_operation") as record:
      log_operation_audit(**kwargs)
    return record

  def test_a_mutating_rest_call_is_queued(self):
    record = self._log()
    kwargs = record.call_args.kwargs
    assert kwargs["operation_name"] == "create-agent"
    assert kwargs["arguments_fingerprint"] == "f" * 64
    assert kwargs["result"] == {"id": "agt_9", "name": "Linear"}

  def test_a_view_operation_is_a_read(self):
    self._log(operation_name="build-fact-grid").assert_not_called()

  def test_the_mcp_surface_is_left_to_the_tool_manager(self):
    self._log(surface="mcp").assert_not_called()

  def test_a_replay_changed_nothing(self):
    self._log(idempotent_replay=True).assert_not_called()

  def test_the_api_row(self):
    with patch("robosystems.security.operation_audit._executor") as executor:
      record_api_operation(
        operation_name="create-agent",
        operation_id="op_rest_2",
        user_id="user_1",
        graph_id=GRAPH,
        duration_ms=5.0,
        status="completed",
        error=None,
        arguments_fingerprint="a" * 64,
        result={"id": "agt_7"},
      )
    fn, row = executor.submit.call_args.args
    assert fn is write_record
    assert (row.surface, row.operation_id, row.object_ids) == (
      "api",
      "op_rest_2",
      ["agt_7"],
    )
    assert row.arguments_fingerprint == "a" * 64

  def test_a_failed_api_call_names_no_objects(self):
    with patch("robosystems.security.operation_audit._executor") as executor:
      record_api_operation(
        operation_name="create-agent",
        operation_id="op_rest_3",
        user_id="user_1",
        graph_id=GRAPH,
        duration_ms=5.0,
        status="failed",
        error="ValueError: bad",
        arguments_fingerprint=None,
        result=None,
      )
    row = executor.submit.call_args.args[1]
    assert (row.status, row.error_code, row.object_ids) == (
      "failed",
      "ValueError: bad",
      [],
    )
