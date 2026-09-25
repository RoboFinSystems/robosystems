"""Every materialized fact gets its entity edge, report-owned or not.

Runs the materializer's own DuckDB SQL through ``postgres_scan`` against a
throwaway tenant schema in the real extensions database.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.operations.extensions.materialize import (
  _staging_sql,
  build_postgres_connstr,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd06"
ENTITY = "ent_sr1"


def _tenant_tables():
  return [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]


@pytest.fixture(scope="module")
def tenant():
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  engine = create_engine(url)
  try:
    with engine.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    engine.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=_tenant_tables(),
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _seed():
  from robosystems.models.api.fact_provenance import PivotProvenance
  from robosystems.models.extensions.entity import Entity
  from robosystems.models.extensions.roboledger.fact import Fact
  from robosystems.models.extensions.roboledger.report import Report
  from robosystems.operations.roboledger.fact_set import create_fact_set

  with extensions_session(GRAPH) as session:
    session.add(Entity(id=ENTITY, name="Driftline Test Co", created_by="usr_seed"))
    session.add(
      Report(id="rpt_sr1", name="FY", taxonomy_id="tax_sr1", created_by="usr_seed")
    )
    session.flush()
    month_end = date(2026, 7, 31)
    for fact_set_id, report_id in (("fs_close", None), ("fs_report", "rpt_sr1")):
      create_fact_set(
        session,
        id=fact_set_id,
        period_end=month_end,
        factset_type="report",
        entity_id=ENTITY,
        report_id=report_id,
        provenance=PivotProvenance(mapping_id="map_1", period="2026-07-01/2026-07-31"),
        created_by="usr_seed",
      )
      session.flush()
      session.add(
        Fact(
          id=f"fact_{fact_set_id}",
          element_id="el_cash",
          value=100.0,
          period_end=month_end,
          period_type="instant",
          entity_id=ENTITY,
          fact_set_id=fact_set_id,
        )
      )
    session.commit()


def test_close_stamped_facts_get_an_entity_edge(tenant):
  duckdb = pytest.importorskip("duckdb")
  _seed()
  sql = _staging_sql(GRAPH, ENTITY, build_postgres_connstr())["FACT_HAS_ENTITY"]
  con = duckdb.connect()
  try:
    con.execute("INSTALL postgres; LOAD postgres")
    con.execute(sql)
    edges = dict(con.execute("SELECT src, dst FROM FACT_HAS_ENTITY").fetchall())
  finally:
    con.close()

  assert edges == {"fact_fs_close": ENTITY, "fact_fs_report": ENTITY}
