"""Tests for graph-scoped agent filtering."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from robosystems.operations.agents.base import GraphScope, matches_graph_scope

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

    from robosystems.operations.agents.base import (
      Agent,
      AgentCapability,
      AgentResult,
      AgentSpec,
    )
    from robosystems.operations.agents.orchestrator import AgentOrchestrator

    # Register a scoped and unscoped agent
    class PlatformAgent(Agent):
      spec = AgentSpec(
        name="Platform",
        description="Works everywhere",
        capabilities=[AgentCapability.CUSTOM],
      )

      async def run(self, ctx):
        return AgentResult(content="ok")

    class LedgerAgent(Agent):
      spec = AgentSpec(
        name="Ledger",
        description="Roboledger only",
        capabilities=[AgentCapability.FINANCIAL_ANALYSIS],
        graph_scope=GraphScope(schema_extension="roboledger"),
      )

      async def run(self, ctx):
        return AgentResult(content="ok")

    user = MagicMock()
    user.id = "user1"

    with (
      patch(
        "robosystems.operations.agents.orchestrator.list_agents",
        return_value={
          "platform": {"name": "Platform"},
          "ledger": {"name": "Ledger"},
        },
      ),
      patch(
        "robosystems.operations.agents.orchestrator.get_agent",
        side_effect=lambda t: PlatformAgent() if t == "platform" else LedgerAgent(),
      ),
      patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ),
    ):
      # Graph without roboledger extension
      orch = AgentOrchestrator("kg_no_ledger", user)
      agents = orch._get_all_agents()
      assert "platform" in agents
      assert "ledger" not in agents

    with (
      patch(
        "robosystems.operations.agents.orchestrator.list_agents",
        return_value={
          "platform": {"name": "Platform"},
          "ledger": {"name": "Ledger"},
        },
      ),
      patch(
        "robosystems.operations.agents.orchestrator.get_agent",
        side_effect=lambda t: PlatformAgent() if t == "platform" else LedgerAgent(),
      ),
      patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=["roboledger"],
      ),
    ):
      # Graph with roboledger extension
      orch = AgentOrchestrator("kg_with_ledger", user)
      agents = orch._get_all_agents()
      assert "platform" in agents
      assert "ledger" in agents

  @pytest.mark.asyncio
  async def test_specific_agent_raises_for_out_of_scope(self):
    from unittest.mock import MagicMock

    from robosystems.operations.agents.base import (
      Agent,
      AgentCapability,
      AgentResult,
      AgentSpec,
    )
    from robosystems.operations.agents.orchestrator import AgentOrchestrator

    class ScopedAgent(Agent):
      spec = AgentSpec(
        name="Scoped",
        description="Scoped",
        capabilities=[AgentCapability.CUSTOM],
        graph_scope=GraphScope(schema_extension="roboledger"),
      )

      async def run(self, ctx):
        return AgentResult(content="ok")

    user = MagicMock()
    user.id = "user1"

    with (
      patch(
        "robosystems.operations.agents.orchestrator.get_agent",
        return_value=ScopedAgent(),
      ),
      patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ),
    ):
      orch = AgentOrchestrator("kg_no_ledger", user)
      with pytest.raises(ValueError, match="not available for graph"):
        await orch._route_to_specific_agent(
          query="test",
          agent_type="scoped",
          mode=AgentSpec.__dataclass_fields__["supported_modes"].default_factory()[0],
          history=None,
          context={},
          stream_callback=None,
        )


# ── list_agents scope metadata ───────────────────────────────────────────────


class TestListAgentsScope:
  """Test that list_agents includes scope information."""

  def test_scope_included_in_list(self):
    from robosystems.operations.agents.agent_registry import (
      clear_registry,
      list_agents,
      register_agent,
    )
    from robosystems.operations.agents.base import (
      Agent,
      AgentCapability,
      AgentResult,
      AgentSpec,
    )

    clear_registry()

    @register_agent("test_unscoped")
    class UnscopedAgent(Agent):
      spec = AgentSpec(
        name="Unscoped",
        description="No scope",
        capabilities=[AgentCapability.CUSTOM],
      )

      async def run(self, ctx):
        return AgentResult(content="ok")

    @register_agent("test_scoped")
    class ScopedAgent(Agent):
      spec = AgentSpec(
        name="Scoped",
        description="Ledger only",
        capabilities=[AgentCapability.CUSTOM],
        graph_scope=GraphScope(schema_extension="roboledger"),
      )

      async def run(self, ctx):
        return AgentResult(content="ok")

    agents = list_agents()

    assert agents["test_unscoped"]["graph_scope"] is None
    assert agents["test_scoped"]["graph_scope"] == {"schema_extension": "roboledger"}

    clear_registry()
