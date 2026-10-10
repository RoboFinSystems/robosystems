"""Migration 0043 against a tenant schema as 0042 left it: the column and its
index arrive, statement balances move their document out of metadata, other
events are untouched, and the downgrade puts a later balance's document back
where the earlier code reads it. Runs the migration's own per-schema functions
on real Postgres."""

from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

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


migration = _load("0043_event_document_id")


@pytest.fixture()
def tenant():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_m43_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    conn.execute(text(f'SET search_path TO "{schema}"'))
    ExtensionsBase.metadata.create_all(bind=conn)
    # As 0042 left it.
    conn.execute(text(f'DROP INDEX "{schema}".idx_events_document'))
    conn.execute(text(f'ALTER TABLE "{schema}".events DROP COLUMN document_id'))
  try:
    yield engine, schema
  finally:
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _event(conn, schema, event_id, event_type, metadata_sql):
  category = "reconciliation" if event_type == "balance_observed" else "sales"
  event_class = "support" if event_type == "balance_observed" else "economic"
  conn.execute(
    text(
      f'INSERT INTO "{schema}".events (id, entity_id, event_type, event_category, '
      "event_class, occurred_at, status, source, currency, metadata, payload_drift, "
      "created_at, created_by) VALUES (:id, 'ent_p', :type, :cat, :cls, now(), "
      f"'committed', 'manual', 'USD', {metadata_sql}, false, now(), 'usr_1')"
    ),
    {"id": event_id, "type": event_type, "cat": category, "cls": event_class},
  )


def _column(conn, schema, event_id):
  return conn.execute(
    text(f'SELECT document_id FROM "{schema}".events WHERE id = :id'), {"id": event_id}
  ).scalar()


def test_statement_balances_move_their_document_into_the_column(tenant):
  engine, schema = tenant
  with engine.begin() as conn:
    _event(
      conn, schema, "evt_bal", "balance_observed", """'{"document_id": "doc_stmt"}'"""
    )
    _event(conn, schema, "evt_bal_bare", "balance_observed", "'{}'")
    # Another event's metadata key is its payload, not a citation.
    _event(conn, schema, "evt_inv", "invoice_issued", """'{"document_id": "doc_x"}'""")

    migration._add(conn, schema)

    assert _column(conn, schema, "evt_bal") == "doc_stmt"
    assert _column(conn, schema, "evt_bal_bare") is None
    assert _column(conn, schema, "evt_inv") is None
    index = conn.execute(
      text(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = :s "
        "AND indexname = 'idx_events_document'"
      ),
      {"s": schema},
    ).scalar()
    assert index is not None and "WHERE (document_id IS NOT NULL)" in index
    # Running it again changes nothing.
    migration._add(conn, schema)
    assert _column(conn, schema, "evt_bal") == "doc_stmt"


def test_the_downgrade_puts_a_later_balances_document_back_in_metadata(tenant):
  engine, schema = tenant
  with engine.begin() as conn:
    migration._add(conn, schema)
    _event(
      conn, schema, "evt_new", "balance_observed", '\'{"kind": "statement_ending"}\''
    )
    conn.execute(
      text(
        f"UPDATE \"{schema}\".events SET document_id = 'doc_new' WHERE id = 'evt_new'"
      )
    )

    migration._drop(conn, schema)

    metadata = conn.execute(
      text(f"SELECT metadata FROM \"{schema}\".events WHERE id = 'evt_new'")
    ).scalar()
    assert metadata == {"kind": "statement_ending", "document_id": "doc_new"}
    columns = conn.execute(
      text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = :s AND table_name = 'events'"
      ),
      {"s": schema},
    ).scalars()
    assert "document_id" not in set(columns)
