"""Defense-in-depth tests: shared-repo gating on graph lifecycle ops.

Three handlers previously relied on implicit gates (admin-role check or
subscription-not-found) to reject shared-repo writes. This file exercises
the explicit `is_shared_repository_or_subgraph` guards added afterward so
regressions can't silently open them:

1. `delete-subgraph` — REST
2. `change-tier` — REST
3. `delete-subgraph` MCP tool (`DeleteSubgraphTool.execute`)

Shared repos covered: the SEC master (`sec`) and any subgraph of it
(`sec_historical` is the live case).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.operations import ChangeTierOp, DeleteSubgraphOp
from robosystems.routers.graphs.operations import (
  change_tier_op,
  delete_subgraph_op,
)


class _FakeCache:
  """In-memory idempotency cache matching the real signature."""

  async def reserve(self, *args, **kwargs):
    return True

  async def release(self, *args, **kwargs):
    return None

  def __init__(self) -> None:
    self.store: dict = {}

  async def get(
    self, user_id, graph_id, operation_name, idempotency_key, body_fingerprint
  ):
    return None

  async def put(
    self,
    user_id,
    graph_id,
    operation_name,
    idempotency_key,
    envelope,
    body_fingerprint,
    ttl_seconds=86400,
  ):
    self.store[(user_id, graph_id, operation_name, idempotency_key)] = envelope


def _user() -> MagicMock:
  u = MagicMock()
  u.id = "usr_test"
  return u


# ── REST: delete-subgraph rejects shared-repo parents ─────────────────────


class TestDeleteSubgraphSharedRepoGate:
  @pytest.mark.asyncio
  @pytest.mark.parametrize("graph_id", ["sec", "sec_historical"])
  async def test_rejects_shared_repo_parent(self, graph_id: str) -> None:
    body = DeleteSubgraphOp(subgraph_name="foo")
    with pytest.raises(HTTPException) as exc:
      await delete_subgraph_op(
        body=body,
        graph_id=graph_id,
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=MagicMock(),
      )
    assert exc.value.status_code == 403
    assert "shared repository" in exc.value.detail
    assert graph_id in exc.value.detail


# ── REST: change-tier rejects shared-repo graphs ──────────────────────────


class TestChangeTierSharedRepoGate:
  @pytest.mark.asyncio
  @pytest.mark.parametrize("graph_id", ["sec", "sec_historical"])
  async def test_rejects_shared_repo(self, graph_id: str) -> None:
    body = ChangeTierOp(new_tier="ladybug-large")
    with pytest.raises(HTTPException) as exc:
      await change_tier_op(
        body=body,
        graph_id=graph_id,
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=MagicMock(),
      )
    assert exc.value.status_code == 403
    assert "shared repository" in exc.value.detail
    assert graph_id in exc.value.detail


# ── MCP: DeleteSubgraphTool rejects shared-repo parents ───────────────────


class TestDeleteSubgraphMCPSharedRepoGate:
  @pytest.mark.asyncio
  async def test_rejects_shared_repo_parent(self) -> None:
    """`sec_historical` exists today as a subgraph of the `sec` repo.

    An MCP client rooted at the `sec` parent must not be able to delete it.
    """
    from robosystems.middleware.mcp.tools.graph_tools import DeleteSubgraphTool

    client = MagicMock()
    client.graph_id = "sec"
    client.user = _user()

    tool = DeleteSubgraphTool(client)
    result = await tool.execute({"subgraph_id": "sec_historical"})

    assert result["error"] == "not_allowed_on_shared_repo"
    assert "sec" in result["message"]


# ── MCP: SyncConnectionTool rejects shared repos ──────────────────────────


class TestSyncConnectionMCPSharedRepoGate:
  @pytest.mark.asyncio
  @pytest.mark.parametrize("graph_id", ["sec", "sec_historical"])
  async def test_rejects_shared_repo(self, graph_id: str) -> None:
    """Shared repos sync via their own pipeline; a tenant agent must not
    be able to trigger ingestion against them."""
    from robosystems.middleware.mcp.tools.graph_tools import SyncConnectionTool

    client = MagicMock()
    client.graph_id = graph_id
    client.user = _user()

    result = await SyncConnectionTool(client).execute({})

    assert result["error"] == "not_allowed_on_shared_repo"
    assert graph_id in result["message"]


# ── Navigation still allowed — positive invariants ────────────────────────


class TestSharedRepoNavigationStillAllowed:
  """A SEC user must still be able to get from `sec` to `sec_historical`.

  The write gates above deny a lot on shared repos; navigation is not one of
  them. `list-subgraphs` is now the whole navigation surface — it carries
  each graph's `connector_url` — so if it were gated here, the deeper
  historical filings would be unreachable rather than merely read-only."""

  def test_list_subgraphs_tool_defined_on_shared_repo(self) -> None:
    from robosystems.middleware.mcp.tools.graph_tools import ListSubgraphsTool

    client = MagicMock()
    client.graph_id = "sec"
    client.user = _user()

    defn = ListSubgraphsTool(client).get_tool_definition()
    assert defn["name"] == "list-subgraphs"
    # The address is the point: naming a subgraph without saying where it
    # lives would leave the caller exactly where the old tool pair did.
    assert "connector_url" in defn["description"]
