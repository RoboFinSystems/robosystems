"""Unit tests for the /query/sql handler (execute_sql).

Kernel-level SQL policy has its own coverage in test_statement_kernel.py; these
exercise the handler wiring — success shape and the shared-repo block reaching
the caller as a 403.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.graphs.tables import SqlStatementRequest
from robosystems.routers.graphs.query import sql as sql_module

MOD = "robosystems.routers.graphs.query.sql"
KERNEL = "robosystems.middleware.graph.statement_kernel"


def _user():
  u = MagicMock()
  u.id = "usr_1"
  return u


async def test_execute_sql_success():
  req = SqlStatementRequest(sql="SELECT 1 AS x")
  client = MagicMock()
  client.query_table = AsyncMock(
    return_value={
      "columns": ["x"],
      "rows": [[1]],
      "row_count": 1,
      "execution_time_ms": 1.0,
    }
  )
  with (
    patch(f"{MOD}.get_universal_repository", new=AsyncMock(return_value=MagicMock())),
    patch(
      "robosystems.graph_api.client.factory.get_graph_client",
      new=AsyncMock(return_value=client),
    ),
  ):
    resp = await sql_module.execute_sql(
      full_request=MagicMock(),
      graph_id="kg1234567890abcdef",
      request=req,
      current_user=_user(),
      _rate_limit=None,
      db=MagicMock(),
    )
  assert resp.columns == ["x"]
  assert resp.rows == [[1]]
  assert resp.row_count == 1
  client.query_table.assert_awaited_once()


async def test_execute_sql_shared_repo_blocked():
  req = SqlStatementRequest(sql="SELECT 1")
  with patch(
    f"{KERNEL}.MultiTenantUtils.is_shared_repository_or_subgraph", return_value=True
  ):
    with pytest.raises(HTTPException) as e:
      await sql_module.execute_sql(
        full_request=MagicMock(),
        graph_id="sec",
        request=req,
        current_user=_user(),
        _rate_limit=None,
        db=MagicMock(),
      )
  assert e.value.status_code == 403
  assert "Shared repositories do not allow direct SQL" in e.value.detail
