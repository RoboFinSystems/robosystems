"""Tests for the per-graph MCP instructions generator.

The generator is a pure function over the resolved tool-name set. The core
invariant — and the reason it's generated rather than hand-authored for entity
graphs — is that it can never reference a tool the graph doesn't expose. These
tests pin that invariant plus the per-category routing.
"""

from __future__ import annotations

from robosystems.middleware.mcp.tools.instructions import build_instructions

# Representative tool sets per graph category.
_CORE = {"read-graph-cypher", "get-graph-schema", "get-example-queries"}
_LEDGER = _CORE | {
  "get-close-playbook",
  "get-fiscal-calendar",
  "get-period-close-status",
  "get-graph-sync-status",
  "get-unmapped-elements",
  "suggest-mapping",
  "list-mapping-structures",
  "get-mapping-summary",
  "live-financial-statement",
  "build-fact-grid",
  "financial-statement-analysis",
  "search-documents",
  "write-graph-cypher",
  "query-graphql",
}
_PORTFOLIO = _CORE | {"create-portfolio-block", "create-security", "query-graphql"}


class TestAuthoredOverride:
  def test_shared_repo_uses_authored_verbatim(self) -> None:
    authored = "Connected to `sec`.\n\nSTART HERE\n- do the thing."
    out = build_instructions(
      graph_id="sec",
      tool_names=_CORE,
      is_shared_repo=True,
      read_only=True,
      authored_override=authored,
    )
    assert out == authored

  def test_blank_override_falls_through_to_generated(self) -> None:
    out = build_instructions(
      graph_id="sec",
      tool_names=_CORE,
      is_shared_repo=True,
      read_only=True,
      authored_override="   ",
    )
    assert out is not None
    assert "READ-ONLY" in out


class TestLedgerGraph:
  def _out(self) -> str:
    out = build_instructions(
      graph_id="kgLEDGER",
      tool_names=_LEDGER,
      is_shared_repo=False,
      read_only=False,
    )
    assert out is not None
    return out

  def test_header_identifies_roboledger(self) -> None:
    assert "RoboLedger entity graph" in self._out()

  def test_close_routes_to_playbook_first(self) -> None:
    out = self._out()
    assert "`get-close-playbook` FIRST" in out
    assert "get-fiscal-calendar" in out

  def test_mapping_section_present(self) -> None:
    assert "`suggest-mapping`" in self._out()

  def test_writes_listed_when_writable(self) -> None:
    assert "`write-graph-cypher`" in self._out()

  def test_tenant_docs_hint_present(self) -> None:
    assert "search-documents" in self._out()


class TestGenericGraph:
  def test_generic_header_and_no_ledger_routing(self) -> None:
    out = build_instructions(
      graph_id="kgPLAIN",
      tool_names=_CORE,
      is_shared_repo=False,
      read_only=False,
    )
    assert out is not None
    assert "custom property graph" in out
    # The invariant: tools not in the set are never named.
    assert "get-close-playbook" not in out
    assert "suggest-mapping" not in out
    assert "write-graph-cypher" not in out


class TestPortfolioGraph:
  def test_header_identifies_roboinvestor(self) -> None:
    out = build_instructions(
      graph_id="kgPORT",
      tool_names=_PORTFOLIO,
      is_shared_repo=False,
      read_only=False,
    )
    assert out is not None
    assert "RoboInvestor entity graph" in out
    assert "`create-security`" in out


class TestSharedRepoGenerated:
  def test_read_only_footer_and_no_writes(self) -> None:
    out = build_instructions(
      graph_id="custrepo",
      tool_names=_CORE | {"write-graph-cypher"},
      is_shared_repo=True,
      read_only=True,
    )
    assert out is not None
    assert "shared read-only data" in out
    # read_only suppresses the write routing even if the tool name leaks in.
    assert "`write-graph-cypher`" not in out


class TestInvariant:
  def test_only_present_tools_are_referenced(self) -> None:
    # A ledger graph missing the sync-status tool must not mention it.
    tools = _LEDGER - {"get-graph-sync-status"}
    out = build_instructions(
      graph_id="kgLEDGER",
      tool_names=tools,
      is_shared_repo=False,
      read_only=False,
    )
    assert out is not None
    assert "get-graph-sync-status" not in out
    # but the orient line still works off the remaining tools
    assert "get-fiscal-calendar" in out
