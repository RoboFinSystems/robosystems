"""Tests for graph-scoped agent filtering."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from robosystems.operations.operators.base import GraphScope, matches_graph_scope

# ── matches_graph_scope ──────────────────────────────────────────────────────


class TestMatchesGraphScope:
  """Test the scope matching function."""

  def test_none_scope_matches_everything(self):
    assert matches_graph_scope(None, "kg123", []) is True
    assert matches_graph_scope(None, "sec", ["roboledger"]) is True

  def test_shared_repo_matches_exact(self):
    scope = GraphScope(shared_repo="sec")
    assert matches_graph_scope(scope, "sec", []) is True

  def test_shared_repo_matches_subgraph(self):
    scope = GraphScope(shared_repo="sec")
    assert matches_graph_scope(scope, "sec_historical", []) is True

  def test_shared_repo_rejects_user_graph(self):
    scope = GraphScope(shared_repo="sec")
    assert matches_graph_scope(scope, "kg123", []) is False

  def test_shared_repo_rejects_wrong_repo(self):
    """A graph belonging to a different shared repo should not match."""
    scope = GraphScope(shared_repo="sec")
    # "industry" would be a different shared repo
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=True,
      ),
      patch(
        "robosystems.config.shared_repositories.resolve_shared_repository_parent",
        return_value="industry",
      ),
    ):
      assert matches_graph_scope(scope, "industry", []) is False

  def test_extension_matches_when_present(self):
    scope = GraphScope(schema_extension="roboledger")
    assert matches_graph_scope(scope, "kg123", ["roboledger"]) is True

  def test_extension_matches_among_multiple(self):
    scope = GraphScope(schema_extension="roboledger")
    assert matches_graph_scope(scope, "kg123", ["roboinvestor", "roboledger"]) is True

  def test_extension_rejects_when_missing(self):
    scope = GraphScope(schema_extension="roboledger")
    assert matches_graph_scope(scope, "kg123", []) is False

  def test_extension_rejects_wrong_extension(self):
    scope = GraphScope(schema_extension="roboledger")
    assert matches_graph_scope(scope, "kg123", ["roboinvestor"]) is False

  def test_both_fields_requires_both(self):
    scope = GraphScope(shared_repo="sec", schema_extension="roboledger")
    # Has extension but not shared repo
    assert matches_graph_scope(scope, "kg123", ["roboledger"]) is False
    # Is shared repo but missing extension
    assert matches_graph_scope(scope, "sec", []) is False
    # Both match
    assert matches_graph_scope(scope, "sec", ["roboledger"]) is True


# ── Orchestrator scope filtering ─────────────────────────────────────────────


class TestOrchestratorScopeFiltering:
  """Test that the orchestrator filters agents by graph scope."""

  def test_get_all_agents_excludes_scoped_agents(self):
    from unittest.mock import MagicMock

    from robosystems.operations.operators.base import (
      Operator,
      OperatorCapability,
      OperatorResult,
      OperatorSpec,
    )
    from robosystems.operations.operators.orchestrator import OperatorOrchestrator

    # Register a scoped and unscoped agent
    class PlatformAgent(Operator):
      spec = OperatorSpec(
        name="Platform",
        description="Works everywhere",
        capabilities=[OperatorCapability.CUSTOM],
      )

      async def run(self, ctx):
        return OperatorResult(content="ok")

    class LedgerAgent(Operator):
      spec = OperatorSpec(
        name="Ledger",
        description="Roboledger only",
        capabilities=[OperatorCapability.FINANCIAL_ANALYSIS],
        graph_scope=GraphScope(schema_extension="roboledger"),
      )

      async def run(self, ctx):
        return OperatorResult(content="ok")

    user = MagicMock()
    user.id = "user1"

    with (
      patch(
        "robosystems.operations.operators.orchestrator.list_operators",
        return_value={
          "platform": {"name": "Platform"},
          "ledger": {"name": "Ledger"},
        },
      ),
      patch(
        "robosystems.operations.operators.orchestrator.get_operator",
        side_effect=lambda t: PlatformAgent() if t == "platform" else LedgerAgent(),
      ),
      patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ),
    ):
      # Graph without roboledger extension
      orch = OperatorOrchestrator("kg_no_ledger", user)
      agents = orch._get_all_operators()
      assert "platform" in agents
      assert "ledger" not in agents

    with (
      patch(
        "robosystems.operations.operators.orchestrator.list_operators",
        return_value={
          "platform": {"name": "Platform"},
          "ledger": {"name": "Ledger"},
        },
      ),
      patch(
        "robosystems.operations.operators.orchestrator.get_operator",
        side_effect=lambda t: PlatformAgent() if t == "platform" else LedgerAgent(),
      ),
      patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=["roboledger"],
      ),
    ):
      # Graph with roboledger extension
      orch = OperatorOrchestrator("kg_with_ledger", user)
      agents = orch._get_all_operators()
      assert "platform" in agents
      assert "ledger" in agents

  @pytest.mark.asyncio
  async def test_specific_agent_raises_for_out_of_scope(self):
    from unittest.mock import MagicMock

    from robosystems.operations.operators.base import (
      Operator,
      OperatorCapability,
      OperatorResult,
      OperatorSpec,
    )
    from robosystems.operations.operators.orchestrator import OperatorOrchestrator

    class ScopedAgent(Operator):
      spec = OperatorSpec(
        name="Scoped",
        description="Scoped",
        capabilities=[OperatorCapability.CUSTOM],
        graph_scope=GraphScope(schema_extension="roboledger"),
      )

      async def run(self, ctx):
        return OperatorResult(content="ok")

    user = MagicMock()
    user.id = "user1"

    with (
      patch(
        "robosystems.operations.operators.orchestrator.get_operator",
        return_value=ScopedAgent(),
      ),
      patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ),
    ):
      orch = OperatorOrchestrator("kg_no_ledger", user)
      with pytest.raises(ValueError, match="not available for graph"):
        await orch._route_to_specific_agent(
          query="test",
          operator_type="scoped",
          mode=OperatorSpec.__dataclass_fields__["supported_modes"].default_factory()[
            0
          ],
          history=None,
          context={},
          stream_callback=None,
        )


# ── list_operators scope metadata ───────────────────────────────────────────────


class TestListAgentsScope:
  """Test that list_operators includes scope information."""

  def test_scope_included_in_list(self):
    from robosystems.operations.operators.base import (
      Operator,
      OperatorCapability,
      OperatorResult,
      OperatorSpec,
    )
    from robosystems.operations.operators.operator_registry import (
      clear_registry,
      list_operators,
      register_operator,
    )

    clear_registry()

    @register_operator("test_unscoped")
    class UnscopedAgent(Operator):
      spec = OperatorSpec(
        name="Unscoped",
        description="No scope",
        capabilities=[OperatorCapability.CUSTOM],
      )

      async def run(self, ctx):
        return OperatorResult(content="ok")

    @register_operator("test_scoped")
    class ScopedAgent(Operator):
      spec = OperatorSpec(
        name="Scoped",
        description="Ledger only",
        capabilities=[OperatorCapability.CUSTOM],
        graph_scope=GraphScope(schema_extension="roboledger"),
      )

      async def run(self, ctx):
        return OperatorResult(content="ok")

    agents = list_operators()

    assert agents["test_unscoped"]["graph_scope"] is None
    assert agents["test_scoped"]["graph_scope"] == {"schema_extension": "roboledger"}

    clear_registry()
