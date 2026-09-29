"""Tool annotations are per-tool claims that directory reviews check."""

from unittest.mock import MagicMock

import pytest

from robosystems.middleware.mcp.tools.annotations import (
  WRITE_TOOL_HINTS,
  tool_annotations,
  tool_hints,
)
from robosystems.middleware.mcp.tools.classification import (
  CYPHER_READ_TOOLS,
  READ_ONLY_MCP_TOOLS,
)
from robosystems.middleware.mcp.tools.registrar import build_tools_for_extension

# Hand-written tools outside READ_ONLY_MCP_TOOLS (registrar tools are
# discovered below).
_HAND_WRITTEN_WRITES = {
  "add-node-table",
  "add-relationship-table",
  "backfill-plan-history",
  "bind-text-block",
  "close-period",
  "create-backup",
  "create-document",
  "create-mapping-association",
  "create-subgraph",
  "delete-document",
  "delete-report",
  "delete-subgraph",
  "forget",
  "materialize",
  "remember",
  "reopen-period",
  "set-write-policy",
  "sync-connection",
  "update-document",
  "update-memory",
  "write-graph-cypher",
}


@pytest.mark.unit
@pytest.mark.parametrize("extension", ["roboledger", "roboinvestor"])
def test_every_registrar_write_has_declared_hints(extension):
  client = MagicMock()
  client.graph_id = "kg1"
  names = set(build_tools_for_extension(extension, client))
  missing = names - WRITE_TOOL_HINTS.keys() - READ_ONLY_MCP_TOOLS
  assert not missing, f"declare hints in annotations.WRITE_TOOL_HINTS: {missing}"


@pytest.mark.unit
def test_every_hand_written_write_has_declared_hints():
  assert not _HAND_WRITTEN_WRITES - WRITE_TOOL_HINTS.keys()


@pytest.mark.unit
def test_hint_table_never_contradicts_the_read_allowlist():
  assert not WRITE_TOOL_HINTS.keys() & (READ_ONLY_MCP_TOOLS | CYPHER_READ_TOOLS)


@pytest.mark.unit
def test_reads_are_read_only_and_idempotent():
  for name in ("get-graph-schema", "read-graph-cypher", "list-agents"):
    assert tool_annotations(name, "T") == {
      "title": "T",
      "readOnlyHint": True,
      "destructiveHint": False,
      "idempotentHint": True,
      "openWorldHint": False,
    }


@pytest.mark.unit
def test_additive_writes_are_not_destructive():
  for name in ("create-agent", "create-document", "remember", "create-report"):
    hints = tool_hints(name)
    assert not hints.read_only and not hints.destructive


@pytest.mark.unit
def test_previews_persist_nothing():
  assert tool_hints("preview-event-block").read_only
  assert tool_hints("preview-reconciling-item").read_only


@pytest.mark.unit
def test_tools_that_reach_quickbooks_are_open_world():
  for name in ("close-period", "execute-event-block", "sync-connection"):
    assert tool_hints(name).open_world


@pytest.mark.unit
def test_unlisted_write_gets_the_cautious_claim():
  assert tool_annotations("not-yet-listed", "T") == {
    "title": "T",
    "readOnlyHint": False,
    "destructiveHint": True,
    "idempotentHint": False,
    "openWorldHint": True,
  }
