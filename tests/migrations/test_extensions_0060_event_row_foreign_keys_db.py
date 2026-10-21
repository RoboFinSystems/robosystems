"""Migration 0060 against a tenant schema built before the models declared the
event-row foreign keys: a clean schema gets both, validated; one holding
orphans gets them NOT VALID, enforcing new rows; one that has them is left
alone. Runs the migration's own per-schema function on real Postgres."""

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


migration = _load("0060_event_row_foreign_keys")


@pytest.fixture()
def tenant():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_m60_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    conn.execute(text(f'SET search_path TO "{schema}"'))
    ExtensionsBase.metadata.create_all(bind=conn)
    # As every tenant built by create_all before the models declared them.
    for table, _column, name, _target, _on_delete in migration.FOREIGN_KEYS:
      conn.execute(text(f'ALTER TABLE "{schema}".{table} DROP CONSTRAINT "{name}"'))
  try:
    yield engine, schema
  finally:
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _constraints(conn, schema) -> dict[str, bool]:
  """Each event-row foreign key's name and whether it is validated."""
  return dict(
    conn.execute(
      text(
        "SELECT conname, convalidated FROM pg_constraint "
        "WHERE contype = 'f' AND connamespace = to_regnamespace(:schema) "
        "AND conname IN ('fk_entries_triggered_by_event', 'fk_events_agent_id')"
      ),
      {"schema": schema},
    ).all()
  )


def _orphan_entry(conn, schema) -> None:
  conn.execute(
    text(
      f'INSERT INTO "{schema}".entries (id, entity_id, type, posting_date, status, '
      "metadata, version, created_at, updated_at, created_by, triggered_by_event_id) "
      "VALUES ('je_orphan', 'ent_p', 'standard', '2026-07-31', 'draft', '{}', 1, "
      "now(), now(), 'usr_1', 'evt_gone')"
    )
  )


def test_a_clean_tenant_gets_both_keys_validated(tenant):
  engine, schema = tenant
  with engine.begin() as conn:
    migration._add(conn, schema)
    assert _constraints(conn, schema) == {
      "fk_entries_triggered_by_event": True,
      "fk_events_agent_id": True,
    }


def test_orphans_get_the_key_not_valid_and_new_rows_are_held_to_it(tenant, capsys):
  engine, schema = tenant
  with engine.begin() as conn:
    _orphan_entry(conn, schema)
    migration._add(conn, schema)
    assert _constraints(conn, schema) == {
      "fk_entries_triggered_by_event": False,
      "fk_events_agent_id": True,
    }
  assert "1 rows name a missing events row" in capsys.readouterr().out

  with pytest.raises(IntegrityError), engine.begin() as conn:
    conn.execute(
      text(
        f'UPDATE "{schema}".entries SET triggered_by_event_id = :e '
        "WHERE id = 'je_orphan'"
      ),
      {"e": "evt_also_gone"},
    )


def test_a_tenant_that_has_them_is_left_alone(tenant):
  engine, schema = tenant
  with engine.begin() as conn:
    migration._add(conn, schema)
    migration._add(conn, schema)
    assert len(_constraints(conn, schema)) == 2
