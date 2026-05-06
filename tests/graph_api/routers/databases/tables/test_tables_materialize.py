"""Tests for graph API table materialization endpoint."""

from types import SimpleNamespace

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
    json={"ignore_errors": True},
  )

  assert response.status_code == 403
  assert "not allowed" in response.json()["detail"]


# ---------------------------------------------------------------------------
# _needs_reconciliation
# ---------------------------------------------------------------------------


def test_needs_reconciliation_returns_false_when_target_unknown():
  assert materialize._needs_reconciliation(None, ["identifier", "type"]) is False


def test_needs_reconciliation_false_when_orders_match():
  target = [
    ("identifier", "STRING"),
    ("category", "STRING"),
    ("type", "STRING"),
    ("source", "STRING"),
    ("confidence", "DOUBLE"),
  ]
  source = ["identifier", "category", "type", "source", "confidence"]
  assert materialize._needs_reconciliation(target, source) is False


def test_needs_reconciliation_true_on_set_mismatch():
  target = [
    ("identifier", "STRING"),
    ("category", "STRING"),
    ("type", "STRING"),
  ]
  # Source missing "category" entirely.
  source = ["identifier", "type"]
  assert materialize._needs_reconciliation(target, source) is True


def test_needs_reconciliation_true_on_order_mismatch_only():
  """Same column names, different order — must reconcile.

  Regression: a mid-schema ADD COLUMN (e.g. Classification.category) caused
  DuckDB ALTER TABLE ADD COLUMN to append the new column at the end, while
  the LadybugDB target had it in position 2. Set comparison missed this and
  let positional COPY misalign data into the wrong columns.
  """
  target = [
    ("identifier", "STRING"),
    ("category", "STRING"),
    ("type", "STRING"),
    ("source", "STRING"),
    ("confidence", "DOUBLE"),
  ]
  # DuckDB schema-evolved layout: category appended at end.
  source = ["identifier", "type", "source", "confidence", "category"]
  assert materialize._needs_reconciliation(target, source) is True


def test_needs_reconciliation_ignores_implicit_and_synthetic_cols():
  """file_id (synthetic) and from/to/src/dst (implicit on rel tables) don't count."""
  target = [
    ("prop_a", "STRING"),
    ("prop_b", "STRING"),
  ]
  # Source has from/to and file_id surrounding the props in matching order.
  source = ["from", "to", "prop_a", "prop_b", "file_id"]
  assert materialize._needs_reconciliation(target, source) is False


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
