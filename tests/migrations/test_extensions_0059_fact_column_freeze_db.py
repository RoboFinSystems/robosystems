"""Migration 0059 against a tenant schema as 0058 left it: the four columns
arrive; a close-stamped set's facts freeze at the close and keep no claim; a
document set's facts take its exact descriptor; a filed report's facts freeze
at the filing; a report with a bundle learns when it was stamped; a live
report's facts stay live. Runs the migration's own per-schema functions on
real Postgres."""

from __future__ import annotations

import importlib.util
import os
import uuid
from datetime import UTC, datetime
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


migration = _load("0059_fact_column_freeze")

CLOSED_AT = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
FILED_AT = datetime(2026, 8, 1, 12, 0, tzinfo=UTC)
GENERATED_AT = datetime(2026, 7, 15, 8, 0)


@pytest.fixture()
def tenant():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_m59_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    conn.execute(text(f'SET search_path TO "{schema}"'))
    ExtensionsBase.metadata.create_all(bind=conn)
    # As 0058 left it.
    conn.execute(text(f'ALTER TABLE "{schema}".facts DROP COLUMN provenance'))
    conn.execute(text(f'ALTER TABLE "{schema}".facts DROP COLUMN frozen_at'))
    conn.execute(text(f'ALTER TABLE "{schema}".fact_sets DROP COLUMN retracted_at'))
    conn.execute(text(f'ALTER TABLE "{schema}".reports DROP COLUMN bundle_stamped_at'))
  try:
    yield engine, schema
  finally:
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _report(conn, schema, report_id, *, filing_status, filed_at, bundle_url):
  conn.execute(
    text(
      f'INSERT INTO "{schema}".reports (id, name, taxonomy_id, period_type, '
      "comparative, generation_status, ai_generated, metadata, created_at, "
      "updated_at, created_by, filing_status, filed_at, bundle_url, last_generated) "
      "VALUES (:id, 'R', 'tax_1', 'monthly', false, 'published', false, '{}', "
      "'2026-07-01', '2026-07-01', 'usr_1', :status, :filed_at, :bundle, :generated)"
    ),
    {
      "id": report_id,
      "status": filing_status,
      "filed_at": filed_at,
      "bundle": bundle_url,
      "generated": GENERATED_AT,
    },
  )


def _set(
  conn,
  schema,
  set_id,
  *,
  report_id,
  origin,
  factset_type="report",
  created_at=CLOSED_AT,
):
  conn.execute(
    text(
      f'INSERT INTO "{schema}".fact_sets (id, period_start, period_end, '
      "factset_type, entity_id, report_id, metadata, provenance, created_at, "
      "updated_at, created_by) VALUES (:id, '2026-06-01', '2026-06-30', :kind, "
      "'ent_1', :rid, '{}', CAST(:prov AS jsonb), :created, :created, 'usr_1')"
    ),
    {
      "id": set_id,
      "kind": factset_type,
      "rid": report_id,
      "prov": f'{{"origin": "{origin}", "mapping_id": "map_1", "period": "x"}}',
      "created": created_at.replace(tzinfo=None),
    },
  )


def _fact(conn, schema, fact_id, set_id):
  conn.execute(
    text(
      f'INSERT INTO "{schema}".facts (id, element_id, value, period_end, '
      "period_type, unit, entity_id, fact_set_id, fact_scope, created_at, "
      "fact_type, value_type) VALUES (:id, 'el_1', 1.0, '2026-06-30', 'duration', "
      "'USD', 'ent_1', :set_id, 'in_scope', now(), 'Numeric', 'inline')"
    ),
    {"id": fact_id, "set_id": set_id},
  )


def _row(conn, schema, fact_id):
  return conn.execute(
    text(f'SELECT provenance, frozen_at FROM "{schema}".facts WHERE id = :id'),
    {"id": fact_id},
  ).one()


def _columns(conn, schema, table) -> set[str]:
  return set(
    conn.execute(
      text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = :s AND table_name = :t"
      ),
      {"s": schema, "t": table},
    ).scalars()
  )


def test_the_backfill_freezes_what_was_final_and_claims_only_what_is_exact(tenant):
  engine, schema = tenant
  with engine.begin() as conn:
    _set(conn, schema, "fs_canon", report_id=None, origin="pivot")
    _fact(conn, schema, "f_canon", "fs_canon")
    # A standing text-block set: a document set, not a close stamp.
    _set(
      conn,
      schema,
      "fs_doc",
      report_id=None,
      origin="document",
      factset_type="disclosure",
    )
    _fact(conn, schema, "f_doc", "fs_doc")
    _report(
      conn,
      schema,
      "rpt_filed",
      filing_status="filed",
      filed_at=FILED_AT,
      bundle_url=None,
    )
    _set(conn, schema, "fs_filed", report_id="rpt_filed", origin="pivot")
    _fact(conn, schema, "f_filed", "fs_filed")
    _report(
      conn,
      schema,
      "rpt_live",
      filing_status="draft",
      filed_at=None,
      bundle_url="s3://bucket/rpt_live/g1.holon.jsonld",
    )
    _set(conn, schema, "fs_live", report_id="rpt_live", origin="pivot")
    _fact(conn, schema, "f_live", "fs_live")

    migration._expand(conn, schema)

    # A close-stamped set's facts froze at the close; the set's pivot label
    # says nothing about which of its facts were pivoted, so no claim.
    provenance, frozen_at = _row(conn, schema, "f_canon")
    assert provenance is None
    assert frozen_at == CLOSED_AT
    # A document set's descriptor is exact per fact.
    provenance, frozen_at = _row(conn, schema, "f_doc")
    assert provenance["origin"] == "document"
    assert frozen_at is None
    # A filed report's facts froze at the filing.
    provenance, frozen_at = _row(conn, schema, "f_filed")
    assert provenance is None
    assert frozen_at == FILED_AT
    # A live report's facts are live, and its bundle dates from its generation.
    assert _row(conn, schema, "f_live") == (None, None)
    stamped = conn.execute(
      text(f'SELECT bundle_stamped_at FROM "{schema}".reports WHERE id = :id'),
      {"id": "rpt_live"},
    ).scalar()
    assert stamped == GENERATED_AT
    assert "retracted_at" in _columns(conn, schema, "fact_sets")

    # Running it again changes nothing.
    migration._expand(conn, schema)
    assert _row(conn, schema, "f_canon")[1] == CLOSED_AT


def test_the_downgrade_drops_the_four_columns(tenant):
  engine, schema = tenant
  with engine.begin() as conn:
    migration._expand(conn, schema)
    migration._contract(conn, schema)
    assert not {"provenance", "frozen_at"} & _columns(conn, schema, "facts")
    assert "retracted_at" not in _columns(conn, schema, "fact_sets")
    assert "bundle_stamped_at" not in _columns(conn, schema, "reports")
