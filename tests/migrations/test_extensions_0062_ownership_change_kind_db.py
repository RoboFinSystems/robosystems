"""Migration 0062 against a tenant schema: before it, an ``ownership_change``
entry is refused by the kind CHECK; after it, accepted; and its snapshot of
the kinds is the models' own. Runs the migration's per-schema function on
real Postgres."""

from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions.roboledger.entry import ADJUSTMENT_KINDS

pytestmark = pytest.mark.unit

_VERSIONS = (
  Path(__file__).resolve().parents[2] / "migrations" / "extensions" / "versions"
)


def _load(name: str):
  spec = importlib.util.spec_from_file_location(name, _VERSIONS / f"{name}.py")
  assert spec is not None and spec.loader is not None
  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return module


migration = _load("0062_ownership_change_adjustment_kind")


@pytest.fixture()
def tenant():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_m62_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    conn.execute(text(f'SET search_path TO "{schema}"'))
    ExtensionsBase.metadata.create_all(bind=conn)
  try:
    yield engine, schema
  finally:
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _insert(conn, schema: str) -> None:
  conn.execute(
    text(
      f'INSERT INTO "{schema}".entries '
      "(id, entity_id, type, status, posting_date, adjustment_kind, "
      " metadata, version, created_by, created_at, updated_at) "
      "VALUES (:id, 'ent_x', 'adjusting', 'draft', '2026-02-01', "
      "'ownership_change', '{}'::jsonb, 1, 'test', now(), now())"
    ),
    {"id": f"je_{uuid.uuid4().hex[:12]}"},
  )


def test_the_snapshot_is_the_models_kinds():
  assert set(migration._AFTER) == set(ADJUSTMENT_KINDS)


def test_the_kind_is_refused_before_and_accepted_after(tenant):
  engine, schema = tenant
  with engine.begin() as conn:
    migration._check(conn, schema, migration._BEFORE)
  with (
    pytest.raises(IntegrityError, match="ck_entries_adjustment_kind"),
    engine.begin() as conn,
  ):
    _insert(conn, schema)
  with engine.begin() as conn:
    migration._check(conn, schema, migration._AFTER)
    _insert(conn, schema)
