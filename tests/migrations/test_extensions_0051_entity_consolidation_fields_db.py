"""Migration 0051 against a tenant schema as 0050 left it: the four columns
arrive, every own entity but the group parent takes the method its share
implies, the parent and a linked counterparty stay ``none``, and the
downgrade removes them. Runs the migration's own per-schema functions on
real Postgres."""

from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.operations.roboledger.commands.entity import (
  derived_consolidation_method,
)

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


migration = _load("0051_entity_consolidation_fields")


@pytest.fixture()
def tenant():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_m51_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    conn.execute(text(f'SET search_path TO "{schema}"'))
    ExtensionsBase.metadata.create_all(bind=conn)
    # As 0050 left it.
    for column in migration.COLUMNS:
      conn.execute(text(f'ALTER TABLE "{schema}".entities DROP COLUMN {column}'))
  try:
    yield engine, schema
  finally:
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _entity(conn, schema, entity_id, *, is_parent, source, ownership_pct, seconds):
  conn.execute(
    text(
      f'INSERT INTO "{schema}".entities (id, name, reporting_style_id, status, '
      "is_parent, parent_entity_id, ownership_pct, source, metadata, version, "
      "created_at, updated_at, created_by) VALUES (:id, :id, 'style', 'active', "
      ":is_parent, :parent, :pct, :source, '{}'::jsonb, 1, "
      "now() + make_interval(secs => :seconds), now(), 'usr_1')"
    ),
    {
      "id": entity_id,
      "is_parent": is_parent,
      "parent": None if is_parent else "ent_parent",
      "pct": ownership_pct,
      "source": source,
      "seconds": seconds,
    },
  )


def _row(conn, schema, entity_id):
  return conn.execute(
    text(
      "SELECT consolidation_method, entity_role, joined_on, left_on "
      f'FROM "{schema}".entities WHERE id = :id'
    ),
    {"id": entity_id},
  ).one()


def test_every_member_but_the_parent_takes_the_method_its_share_implies(tenant):
  engine, schema = tenant
  shares = {"ent_whole": 100, "ent_major": 70, "ent_assoc": 40, "ent_stake": 10}
  with engine.begin() as conn:
    _entity(
      conn,
      schema,
      "ent_parent",
      is_parent=True,
      source="native",
      ownership_pct=None,
      seconds=0,
    )
    for i, (entity_id, share) in enumerate(shares.items(), start=1):
      _entity(
        conn,
        schema,
        entity_id,
        is_parent=False,
        source="native",
        ownership_pct=share,
        seconds=i,
      )
    _entity(
      conn,
      schema,
      "ent_unrecorded",
      is_parent=False,
      source="native",
      ownership_pct=None,
      seconds=9,
    )
    _entity(
      conn,
      schema,
      "ent_linked",
      is_parent=False,
      source="linked",
      ownership_pct=None,
      seconds=-5,
    )

    migration._add(conn, schema)

    for entity_id, share in shares.items():
      method, role, joined_on, left_on = _row(conn, schema, entity_id)
      assert method == derived_consolidation_method(share), entity_id
      assert (role, joined_on, left_on) == ("operating", None, None)
    assert _row(conn, schema, "ent_unrecorded")[0] == "full"
    assert _row(conn, schema, "ent_parent")[0] == "none"
    assert _row(conn, schema, "ent_linked")[0] == "none"
    # Running it again changes nothing.
    migration._add(conn, schema)
    assert _row(conn, schema, "ent_assoc")[0] == "equity"


def test_a_schema_with_no_entity_gets_the_columns_and_nothing_else(tenant):
  engine, schema = tenant
  with engine.begin() as conn:
    migration._add(conn, schema)
    columns = {
      row[0]
      for row in conn.execute(
        text(
          "SELECT column_name FROM information_schema.columns "
          "WHERE table_schema = :s AND table_name = 'entities'"
        ),
        {"s": schema},
      )
    }
    assert set(migration.COLUMNS) <= columns

    migration._drop(conn, schema)
    columns = {
      row[0]
      for row in conn.execute(
        text(
          "SELECT column_name FROM information_schema.columns "
          "WHERE table_schema = :s AND table_name = 'entities'"
        ),
        {"s": schema},
      )
    }
    assert not (set(migration.COLUMNS) & columns)


def _chart(conn, schema, taxonomy_id, entity_id, template):
  conn.execute(
    text(
      f'INSERT INTO "{schema}".taxonomies (id, name, taxonomy_type, is_shared, '
      "is_active, is_locked, metadata, created_at, updated_at, created_by) VALUES "
      "(:id, :id, 'chart_of_accounts', false, true, false, "
      "jsonb_build_object('template', :template), now(), now(), 'usr_1')"
    ),
    {"id": taxonomy_id, "template": template},
  )
  conn.execute(
    text(
      f'INSERT INTO "{schema}".entity_taxonomies (id, entity_id, taxonomy_id, '
      "is_primary, basis, created_at, updated_at) VALUES "
      "(:id, :entity, :taxonomy, true, 'chart_of_accounts', now(), now())"
    ),
    {"id": f"et_{taxonomy_id}", "entity": entity_id, "taxonomy": taxonomy_id},
  )


def test_a_consolidation_chart_types_its_entity(tenant):
  """A consolidation entity made before the role existed is known by its
  chart's template; it is typed first, so the method backfill leaves it."""
  engine, schema = tenant
  with engine.begin() as conn:
    _entity(
      conn,
      schema,
      "ent_parent",
      is_parent=True,
      source="native",
      ownership_pct=None,
      seconds=0,
    )
    _entity(
      conn,
      schema,
      "ent_adjust",
      is_parent=False,
      source="native",
      ownership_pct=100,
      seconds=1,
    )
    _entity(
      conn,
      schema,
      "ent_cafe",
      is_parent=False,
      source="native",
      ownership_pct=100,
      seconds=2,
    )
    _chart(conn, schema, "tax_cons", "ent_adjust", "consolidation")
    _chart(conn, schema, "tax_prod", "ent_cafe", "product")

    migration._add(conn, schema)

    assert _row(conn, schema, "ent_adjust")[:2] == ("none", "consolidation")
    assert _row(conn, schema, "ent_cafe")[:2] == ("full", "operating")
