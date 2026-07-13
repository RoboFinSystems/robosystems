"""Tests for graph API table materialization endpoint."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from robosystems.database import get_db_session
from robosystems.graph_api.core.ladybug import get_ladybug_service
from robosystems.graph_api.routers.databases.tables import materialize


@pytest.fixture
def app_client(monkeypatch):
  app = FastAPI()
  app.include_router(materialize.router)

  session = SimpleNamespace(commit=lambda: None)

  app.dependency_overrides[get_db_session] = lambda: session
  return app


def test_materialize_rejects_read_only(monkeypatch, app_client):
  cluster_service = SimpleNamespace(read_only=True)
  app_client.dependency_overrides[get_ladybug_service] = lambda: cluster_service

  client = TestClient(app_client)

  response = client.post(
    "/databases/graph-123/tables/Entity/materialize",
    json={},
  )

  assert response.status_code == 403
  assert "not allowed" in response.json()["detail"]


def test_materialize_endpoint_publishes_busy_counter(monkeypatch, app_client):
  """Endpoint must increment + decrement the busy counter so GHA pre-refresh
  workflows don't cycle the container mid-materialization. Regression: SEC
  prod ran a 60-min materialization without ever flipping
  active_destructive_ops, so a service-refresh fired and killed the run.
  """
  cluster_service = SimpleNamespace(read_only=False)
  app_client.dependency_overrides[get_ladybug_service] = lambda: cluster_service

  # Stub out the real materialization body — we only care about the counter.
  async def fake_impl(**kwargs):
    return materialize.TableMaterializationResponse(
      status="success",
      graph_id=kwargs["graph_id"],
      table_name=kwargs["table_name"],
      rows_ingested=0,
      execution_time_ms=0.0,
    )

  counter_mock = AsyncMock()

  with (
    patch.object(materialize, "_materialize_table_impl", side_effect=fake_impl),
    patch(
      "robosystems.middleware.graph.instance_busy._update_counter_async",
      counter_mock,
    ),
  ):
    client = TestClient(app_client)
    response = client.post(
      "/databases/graph-123/tables/Entity/materialize",
      json={},
    )

  assert response.status_code == 200, response.text
  # Increment on entry, decrement on exit.
  assert counter_mock.await_count == 2
  deltas = [
    call.kwargs.get("delta", call.args[1] if len(call.args) > 1 else None)
    for call in counter_mock.await_args_list
  ]
  assert deltas == [1, -1]
  # All increments tagged with the materialization op_kind.
  kinds = [
    call.kwargs.get("op_kind", call.args[2] if len(call.args) > 2 else None)
    for call in counter_mock.await_args_list
  ]
  assert kinds == [
    materialize.OP_KIND_MATERIALIZATION,
    materialize.OP_KIND_MATERIALIZATION,
  ]


def test_fork_endpoint_publishes_busy_counter(monkeypatch, app_client):
  """fork_from_parent_duckdb runs the same multi-table COPY workflow as
  materialize_table and must mark the instance busy for the same reason —
  otherwise GHA service-refresh can kill a fork mid-run.
  """
  cluster_service = SimpleNamespace(read_only=False)
  app_client.dependency_overrides[get_ladybug_service] = lambda: cluster_service

  async def fake_impl(**kwargs):
    return materialize.ForkFromParentResponse(
      status="success",
      parent_graph_id=kwargs["parent_graph_id"],
      subgraph_id=kwargs["subgraph_id"],
      tables_copied=[],
      total_rows=0,
      execution_time_ms=0.0,
    )

  counter_mock = AsyncMock()

  with (
    patch.object(materialize, "_fork_from_parent_duckdb_impl", side_effect=fake_impl),
    patch(
      "robosystems.middleware.graph.instance_busy._update_counter_async",
      counter_mock,
    ),
  ):
    client = TestClient(app_client)
    response = client.post(
      "/databases/sub-1/tables/sub-1/fork-from/parent-1",
      json={},
    )

  assert response.status_code == 200, response.text
  assert counter_mock.await_count == 2
  deltas = [
    call.kwargs.get("delta", call.args[1] if len(call.args) > 1 else None)
    for call in counter_mock.await_args_list
  ]
  assert deltas == [1, -1]


def test_materialize_endpoint_decrements_counter_on_failure(monkeypatch, app_client):
  """instance_busy is a context manager: counter must decrement even when
  the body raises. Otherwise a single failed materialization would pin the
  counter >0 and block all future refreshes until staleness expires.
  """
  cluster_service = SimpleNamespace(read_only=False)
  app_client.dependency_overrides[get_ladybug_service] = lambda: cluster_service

  async def boom(**kwargs):
    raise RuntimeError("simulated COPY failure")

  counter_mock = AsyncMock()

  with (
    patch.object(materialize, "_materialize_table_impl", side_effect=boom),
    patch(
      "robosystems.middleware.graph.instance_busy._update_counter_async",
      counter_mock,
    ),
  ):
    client = TestClient(app_client)
    with pytest.raises(RuntimeError, match="simulated COPY failure"):
      client.post(
        "/databases/graph-123/tables/Entity/materialize",
        json={},
      )

  assert counter_mock.await_count == 2
  deltas = [
    call.kwargs.get("delta", call.args[1] if len(call.args) > 1 else None)
    for call in counter_mock.await_args_list
  ]
  assert deltas == [1, -1]


# ---------------------------------------------------------------------------
# _build_type_safe_select
# ---------------------------------------------------------------------------


def test_type_safe_select_preserves_order_and_passthrough():
  source = [
    ("src", "VARCHAR"),
    ("dst", "VARCHAR"),
    ("weight", "DOUBLE"),
  ]
  expr = materialize._build_type_safe_select(source)
  assert expr == '"src", "dst", "weight"'


def test_type_safe_select_casts_decimal_to_double():
  """postgres_scan stages Postgres NUMERIC as DuckDB DECIMAL; LadybugDB has no
  DECIMAL type (numeric targets are DOUBLE), so it's cast at the source."""
  source = [
    ("identifier", "VARCHAR"),
    ("amount", "DECIMAL(18,2)"),
  ]
  expr = materialize._build_type_safe_select(source)
  assert '"identifier"' in expr
  assert 'CAST("amount" AS DOUBLE) AS "amount"' in expr


def test_type_safe_select_excludes_and_nulls():
  source = [
    ("identifier", "VARCHAR"),
    ("embedding", "FLOAT[384]"),
    ("file_id", "VARCHAR"),
  ]
  expr = materialize._build_type_safe_select(
    source, exclude_cols={"file_id"}, null_cols={"embedding"}
  )
  assert "file_id" not in expr
  assert 'NULL::FLOAT[384] AS "embedding"' in expr


# ---------------------------------------------------------------------------
# _build_reconciled_select
# ---------------------------------------------------------------------------


def test_build_reconciled_select_emits_target_order():
  """Output SELECT iterates target columns in target order, regardless of source order."""
  target = [
    ("identifier", "STRING"),
    ("category", "STRING"),
    ("type", "STRING"),
    ("confidence", "DOUBLE"),
  ]
  source = ["identifier", "type", "confidence", "category"]
  expr = materialize._build_reconciled_select(target, source, "Classification")

  # Strip everything to a list of "AS <name>" tokens to verify order.
  parts = [p.strip() for p in expr.split(",")]
  ordered_targets = [
    p.split(" AS ")[-1].strip().strip('"') for p in parts if " AS " in p
  ]
  assert ordered_targets == ["identifier", "category", "type", "confidence"]


def test_build_reconciled_select_nulls_missing_target_cols():
  target = [
    ("identifier", "STRING"),
    ("category", "STRING"),  # not in source
    ("type", "STRING"),
  ]
  source = ["identifier", "type"]
  expr = materialize._build_reconciled_select(target, source, "Classification")
  assert "NULL::VARCHAR AS category" in expr
  assert "TRY_CAST(identifier AS VARCHAR)" in expr
  assert "TRY_CAST(type AS VARCHAR)" in expr
