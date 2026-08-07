"""Carry-over (a): the F1 fail-closed write-role gate is wired into the
hand-written REST lifecycle ops that authenticate on graph membership only.

`require_graph_write_role` itself is unit-tested in
tests/middleware/auth/test_dependencies.py; these assert each call site invokes
it with the right args and short-circuits (fail-closed) before doing any work,
so a read-only `viewer` cannot materialize, create a subgraph, or flip a
connection's outbound write policy.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

OPS = "robosystems.routers.graphs.operations"
MGMT = "robosystems.routers.graphs.connections.management"


def _deny_gate() -> MagicMock:
  return MagicMock(
    side_effect=HTTPException(status_code=403, detail="Write access denied")
  )


@pytest.mark.unit
class TestLifecycleWriteRoleGates:
  def test_materialize_denies_viewer(self):
    from robosystems.routers.graphs.operations import materialize_op

    gate = _deny_gate()
    with patch(f"{OPS}.require_graph_write_role", gate):
      with pytest.raises(HTTPException) as exc:
        asyncio.run(
          materialize_op(
            body=MagicMock(),
            graph_id="kg123",
            user=SimpleNamespace(id="usr_1"),
            idempotency_key=None,
            cache=MagicMock(),
            db=MagicMock(),
          )
        )
    assert exc.value.status_code == 403
    gate.assert_called_once_with("usr_1", "kg123")

  def test_create_subgraph_denies_viewer(self):
    from robosystems.routers.graphs.operations import create_subgraph_op

    gate = _deny_gate()
    with patch(f"{OPS}.require_graph_write_role", gate):
      with pytest.raises(HTTPException) as exc:
        asyncio.run(
          create_subgraph_op(
            body=MagicMock(),
            graph_id="kg123",
            user=SimpleNamespace(id="usr_1"),
            idempotency_key=None,
            cache=MagicMock(),
            db=MagicMock(),
          )
        )
    assert exc.value.status_code == 403
    gate.assert_called_once_with("usr_1", "kg123")

  def test_set_write_policy_denies_viewer(self):
    from robosystems.routers.graphs.connections.management import (
      set_connection_write_policy,
    )

    gate = _deny_gate()
    with patch(f"{MGMT}.require_graph_write_role", gate):
      with pytest.raises(HTTPException) as exc:
        asyncio.run(
          set_connection_write_policy(
            graph_id="kg123",
            connection_id="conn_1",
            request=MagicMock(),
            current_user=SimpleNamespace(id="usr_1"),
            db=MagicMock(),
            _rate_limit=None,
          )
        )
    assert exc.value.status_code == 403
    gate.assert_called_once_with("usr_1", "kg123")


@pytest.mark.unit
class TestMaterializeSubgraphGate:
  """Materialization and direct writes cannot share a database.

  Blue-green rebuilds from DuckDB into `{id}-wip` and renames it over the
  active file, so every direct write made since staging began disappears at
  the swap. A subgraph exists for those direct writes, so the pipeline is
  refused there — before the write-role check, since it is a property of the
  graph rather than of the caller.
  """

  def test_materialize_blocked_on_subgraph(self):
    from robosystems.routers.graphs.operations import materialize_op

    gate = MagicMock()
    with patch(f"{OPS}.require_graph_write_role", gate):
      with pytest.raises(HTTPException) as exc:
        asyncio.run(
          materialize_op(
            body=MagicMock(),
            graph_id="kg19ed34f81c37ba3f31fa_entities",
            user=SimpleNamespace(id="usr_1"),
            idempotency_key=None,
            cache=MagicMock(),
            db=MagicMock(),
          )
        )
    assert exc.value.status_code == 403
    assert "staging" in exc.value.detail.lower()
    gate.assert_not_called()
