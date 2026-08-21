"""The fail-closed write-role gate is wired into the
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
OAUTH = "robosystems.routers.graphs.connections.oauth"
SYNC = "robosystems.routers.graphs.connections.sync"


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
class TestConnectionWriteRoleGates:
  """Connections are a write surface end to end: creating one seeds sync, the
  fiscal calendar and the mapping operator; completing OAuth stores credentials
  and starts a full-rebuild sync; a sync rewrites captured events. All four
  entry points authenticate on membership only, so each must run the shared
  write gate before any work — a viewer must not reach the provider registry,
  the OAuth handler or the sync kernel."""

  def test_create_connection_denies_viewer(self):
    from robosystems.routers.graphs.connections.management import create_connection

    gate = _deny_gate()
    with (
      patch(f"{MGMT}.require_graph_write_role", gate),
      patch(f"{MGMT}.create_robustness_components") as components,
    ):
      with pytest.raises(HTTPException) as exc:
        asyncio.run(
          create_connection(
            graph_id="kg123",
            request=MagicMock(provider="quickbooks"),
            current_user=SimpleNamespace(id="usr_1"),
            db=MagicMock(),
            _rate_limit=None,
          )
        )
    assert exc.value.status_code == 403
    gate.assert_called_once_with("usr_1", "kg123")
    components.assert_not_called()

  def test_init_oauth_denies_viewer(self):
    from robosystems.routers.graphs.connections.oauth import init_oauth

    gate = _deny_gate()
    with (
      patch(f"{OAUTH}.require_graph_write_role", gate),
      patch(f"{OAUTH}.ConnectionService") as service,
    ):
      with pytest.raises(HTTPException) as exc:
        asyncio.run(
          init_oauth(
            graph_id="kg123",
            request=MagicMock(connection_id="conn_1"),
            current_user=SimpleNamespace(id="usr_1"),
            db=MagicMock(),
            _rate_limit=None,
          )
        )
    assert exc.value.status_code == 403
    gate.assert_called_once_with("usr_1", "kg123")
    service.get_connection.assert_not_called()

  def test_oauth_callback_denies_viewer(self):
    from robosystems.routers.graphs.connections.oauth import oauth_callback

    gate = _deny_gate()
    with (
      patch(f"{OAUTH}.require_graph_write_role", gate),
      patch(f"{OAUTH}.ConnectionService") as service,
    ):
      with pytest.raises(HTTPException) as exc:
        asyncio.run(
          oauth_callback(
            provider="quickbooks",
            graph_id="kg123",
            request=MagicMock(error=None),
            current_user=SimpleNamespace(id="usr_1"),
            db=MagicMock(),
            _rate_limit=None,
          )
        )
    assert exc.value.status_code == 403
    gate.assert_called_once_with("usr_1", "kg123")
    service.get_connection.assert_not_called()

  def test_sync_connection_denies_viewer(self):
    from robosystems.routers.graphs.connections.sync import sync_connection

    gate = _deny_gate()
    with (
      patch(f"{SYNC}.require_graph_write_role", gate),
      patch(f"{SYNC}.dispatch_connection_sync") as dispatch,
      patch(f"{SYNC}.idempotent_dispatch") as idem,
    ):
      with pytest.raises(HTTPException) as exc:
        asyncio.run(
          sync_connection(
            graph_id="kg123",
            connection_id="conn_1",
            request=MagicMock(),
            current_user=SimpleNamespace(id="usr_1"),
            db=MagicMock(),
            _rate_limit=None,
            idempotency_key=None,
            cache=MagicMock(),
          )
        )
    assert exc.value.status_code == 403
    gate.assert_called_once_with("usr_1", "kg123")
    dispatch.assert_not_called()
    idem.assert_not_called()


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
