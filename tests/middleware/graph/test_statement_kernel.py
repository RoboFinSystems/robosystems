"""Unit tests for StatementKernel — the extracted statement authorization gauntlet.

The four cypher-analyzer predicates are patched per-test so each policy tier is
isolated deterministically (the analyzers have their own coverage). Topology
helpers (subgraph detection, shared-repo, role) are patched to place the graph.
"""

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.graph.statement_kernel import (
  StatementEngine,
  statement_kernel,
)

MOD = "robosystems.middleware.graph.statement_kernel"


def _user():
  u = MagicMock()
  u.id = "usr_1"
  return u


@contextmanager
def _topology(
  *,
  is_write=False,
  is_bulk=False,
  is_admin=False,
  is_ddl=False,
  is_subgraph=False,
  is_shared=False,
  has_write_access=True,
):
  """Patch the analyzers + topology helpers the kernel consults."""
  with (
    patch(f"{MOD}.is_write_operation", return_value=is_write),
    patch(f"{MOD}.is_bulk_operation", return_value=is_bulk),
    patch(f"{MOD}.is_admin_operation", return_value=is_admin),
    patch(f"{MOD}.is_schema_ddl", return_value=is_ddl),
    patch(f"{MOD}.parse_subgraph_id", return_value=(object() if is_subgraph else None)),
    patch(
      f"{MOD}.MultiTenantUtils.is_shared_repository_or_subgraph",
      return_value=is_shared,
    ),
    patch(
      "robosystems.models.core.graph.graph_user.GraphUser.user_has_write_access",
      return_value=has_write_access,
    ),
  ):
    yield


def _authorize(graph_id, statement):
  return statement_kernel.authorize(
    engine=StatementEngine.CYPHER,
    graph_id=graph_id,
    statement=statement,
    user=_user(),
    session=MagicMock(),
  )


def test_read_on_main_graph_ok():
  with _topology(is_write=False):
    auth = _authorize("kg1234567890abcdef", "MATCH (n) RETURN n")
  assert auth.is_write is False
  assert auth.access_type == "read"
  assert auth.is_subgraph is False


def test_write_on_main_graph_blocked():
  with _topology(is_write=True, is_subgraph=False):
    with pytest.raises(HTTPException) as e:
      _authorize("kg1234567890abcdef", "CREATE (n:Foo)")
  assert e.value.status_code == 403
  assert "not allowed on main graphs" in e.value.detail


def test_write_on_subgraph_with_access_ok():
  with _topology(is_write=True, is_subgraph=True, has_write_access=True):
    auth = _authorize("kg1234567890abcdef_dev", "CREATE (n:Foo)")
  assert auth.is_write is True
  assert auth.access_type == "write"
  assert auth.is_subgraph is True


def test_write_on_subgraph_without_role_blocked():
  with _topology(is_write=True, is_subgraph=True, has_write_access=False):
    with pytest.raises(HTTPException) as e:
      _authorize("kg1234567890abcdef_dev", "CREATE (n:Foo)")
  assert e.value.status_code == 403
  assert "read-only (viewer)" in e.value.detail


def test_write_on_shared_subgraph_blocked():
  # shared repo write reaches the shared-repo tier only when it's a subgraph
  with _topology(is_write=True, is_subgraph=True, is_shared=True):
    with pytest.raises(HTTPException) as e:
      _authorize("sec_sub", "CREATE (n:Foo)")
  assert e.value.status_code == 403
  assert "shared repository" in e.value.detail


def test_bulk_operation_blocked():
  with _topology(is_write=False, is_bulk=True):
    with pytest.raises(HTTPException) as e:
      _authorize("kg1234567890abcdef", "COPY foo FROM 'x'")
  assert e.value.status_code == 400
  assert "Bulk operations" in e.value.detail


def test_admin_operation_blocked():
  with _topology(is_write=False, is_admin=True):
    with pytest.raises(HTTPException) as e:
      _authorize("kg1234567890abcdef", "INSTALL httpfs")
  assert e.value.status_code == 403
  assert "Administrative operations" in e.value.detail


def test_schema_ddl_blocked():
  with _topology(is_write=False, is_ddl=True):
    with pytest.raises(HTTPException) as e:
      _authorize("kg1234567890abcdef", "DROP TABLE foo")
  assert e.value.status_code == 403
  assert "Schema DDL" in e.value.detail


def _authorize_sql(graph_id, statement="SELECT 1"):
  return statement_kernel.authorize(
    engine=StatementEngine.SQL,
    graph_id=graph_id,
    statement=statement,
    user=_user(),
    session=MagicMock(),
  )


def test_sql_on_user_graph_ok():
  # SQL is read-only; a non-shared graph is allowed.
  with patch(
    f"{MOD}.MultiTenantUtils.is_shared_repository_or_subgraph", return_value=False
  ):
    auth = _authorize_sql("kg1234567890abcdef")
  assert auth.is_write is False
  assert auth.access_type == "read"


def test_sql_on_shared_repo_blocked():
  # Shared repos have no user columnar tables — SQL is blocked (unlike reads
  # on the Cypher side, which the graph serves).
  with patch(
    f"{MOD}.MultiTenantUtils.is_shared_repository_or_subgraph", return_value=True
  ):
    with pytest.raises(HTTPException) as e:
      _authorize_sql("sec")
  assert e.value.status_code == 403
  assert "Shared repositories do not allow direct SQL" in e.value.detail
