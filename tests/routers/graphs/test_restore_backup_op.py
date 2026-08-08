"""Tests for the restore-backup operation.

Restore is the destructive graph operation — it overwrites a live database with
a snapshot — and until now nothing exercised its route guards. Covers:

- Shared-repo rejection
- Entity-graph rejection (materialize is the correct path there)
- Unresolvable graph rejection: the guard fails closed, not open
- Backup must exist and must belong to the graph in the URL
- Happy path on a generic graph, including the run config handed to Dagster
- Encryption is no longer a precondition (the old "encrypted backups only" gate)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.operations import RestoreBackupOp
from robosystems.routers.graphs.operations import restore_backup_op

GENERIC_GRAPH = "kg1a2b3c4d5e6f7a8b"


class _FakeCache:
  """In-memory idempotency cache matching the real signature."""

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


def _user(user_id: str = "usr_test") -> MagicMock:
  u = MagicMock()
  u.id = user_id
  return u


def _graph(graph_type: str = "generic") -> MagicMock:
  g = MagicMock()
  g.graph_type = graph_type
  return g


def _backup(graph_id: str = GENERIC_GRAPH, encryption_enabled: bool = False):
  b = MagicMock()
  b.graph_id = graph_id
  b.encryption_enabled = encryption_enabled
  return b


async def _call(graph_id: str = GENERIC_GRAPH, backup_id: str = "bk_1", db=None):
  return await restore_backup_op(
    body=RestoreBackupOp(backup_id=backup_id),
    graph_id=graph_id,
    user=_user(),
    idempotency_key=None,
    cache=_FakeCache(),
    db=db if db is not None else MagicMock(),
  )


@pytest.fixture(autouse=True)
def _allow_admin():
  """Admin authorization is covered by its own suite; assume it passes here."""
  with patch("robosystems.routers.graphs.backups.utils.verify_admin_access"):
    yield


class TestRestoreGraphTypeGates:
  @pytest.mark.asyncio
  @pytest.mark.parametrize("graph_id", ["sec", "sec_historical"])
  async def test_rejects_shared_repo(self, graph_id: str) -> None:
    with pytest.raises(HTTPException) as exc:
      await _call(graph_id=graph_id)
    assert exc.value.status_code == 403
    assert "shared repository" in exc.value.detail
    assert graph_id in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.core.Graph.get_by_id")
  async def test_rejects_entity_graph(self, mock_get_graph: MagicMock) -> None:
    mock_get_graph.return_value = _graph("entity")

    with pytest.raises(HTTPException) as exc:
      await _call()
    assert exc.value.status_code == 400
    assert "entity graphs" in exc.value.detail
    assert "materialize" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.core.Graph.get_by_id")
  async def test_fails_closed_when_graph_row_missing(
    self, mock_get_graph: MagicMock
  ) -> None:
    """An unresolvable graph must refuse, not fall through to a restore."""
    mock_get_graph.return_value = None

    with pytest.raises(HTTPException) as exc:
      await _call()
    assert exc.value.status_code == 400
    assert "not found in the registry" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.core.Graph.get_by_id")
  async def test_allows_repository_graph_type(self, mock_get_graph: MagicMock) -> None:
    """`repository` is only blocked when it is a *shared* repo; a private one
    is restorable like any generic graph."""
    mock_get_graph.return_value = _graph("repository")

    with patch("robosystems.models.core.GraphBackup.get_by_id") as mock_backup:
      mock_backup.return_value = _backup()
      with patch(
        "robosystems.worker.client.enqueue_task",
        new=AsyncMock(return_value={"operation_id": "op_1"}),
      ):
        envelope = await _call()

    assert envelope.operation_id == "op_1"


class TestRestoreBackupRecordGates:
  @pytest.mark.asyncio
  @patch("robosystems.models.core.Graph.get_by_id")
  async def test_rejects_missing_backup(self, mock_get_graph: MagicMock) -> None:
    mock_get_graph.return_value = _graph()

    with patch("robosystems.models.core.GraphBackup.get_by_id", return_value=None):
      with pytest.raises(HTTPException) as exc:
        await _call()
    assert exc.value.status_code == 404
    assert "not found" in exc.value.detail.lower()

  @pytest.mark.asyncio
  @patch("robosystems.models.core.Graph.get_by_id")
  async def test_rejects_backup_from_another_graph(
    self, mock_get_graph: MagicMock
  ) -> None:
    """The backup_id is caller-supplied, so ownership must be re-checked
    against the graph in the URL."""
    mock_get_graph.return_value = _graph()

    with patch(
      "robosystems.models.core.GraphBackup.get_by_id",
      return_value=_backup(graph_id="kg_someone_else"),
    ):
      with pytest.raises(HTTPException) as exc:
        await _call()
    assert exc.value.status_code == 403
    assert "does not belong" in exc.value.detail


class TestRestoreHappyPath:
  @pytest.mark.asyncio
  @pytest.mark.parametrize("encryption_enabled", [False, True])
  @patch("robosystems.models.core.Graph.get_by_id")
  async def test_encryption_is_not_a_precondition(
    self, mock_get_graph: MagicMock, encryption_enabled: bool
  ) -> None:
    """Both legacy `encrypted` records and plain ones restore. The old gate
    rejected everything created with the default flag."""
    mock_get_graph.return_value = _graph()

    with patch(
      "robosystems.models.core.GraphBackup.get_by_id",
      return_value=_backup(encryption_enabled=encryption_enabled),
    ):
      with patch(
        "robosystems.worker.client.enqueue_task",
        new=AsyncMock(return_value={"operation_id": "op_2"}),
      ):
        envelope = await _call()

    assert envelope.operation_id == "op_2"
    assert envelope.status == "pending"

  @pytest.mark.asyncio
  @patch("robosystems.models.core.Graph.get_by_id")
  async def test_enqueues_restore_job_with_body_options(
    self, mock_get_graph: MagicMock
  ) -> None:
    mock_get_graph.return_value = _graph()
    enqueue = AsyncMock(return_value={"operation_id": "op_3"})

    with patch("robosystems.models.core.GraphBackup.get_by_id", return_value=_backup()):
      with patch("robosystems.worker.client.enqueue_task", new=enqueue):
        await restore_backup_op(
          body=RestoreBackupOp(
            backup_id="bk_9",
            create_system_backup=False,
            verify_after_restore=False,
          ),
          graph_id=GENERIC_GRAPH,
          user=_user("usr_42"),
          idempotency_key=None,
          cache=_FakeCache(),
          db=MagicMock(),
        )

    kwargs = enqueue.await_args.kwargs
    assert kwargs["params"]["job_name"] == "restore_graph_job"
    assert kwargs["graph_id"] == GENERIC_GRAPH
    assert kwargs["user_id"] == "usr_42"

    ops_config = kwargs["params"]["run_config"]["ops"]["restore_backup"]["config"]
    assert ops_config["backup_id"] == "bk_9"
    assert ops_config["create_system_backup"] is False
    assert ops_config["verify_after_restore"] is False
