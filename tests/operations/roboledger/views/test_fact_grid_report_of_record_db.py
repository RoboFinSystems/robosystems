"""On a tenant graph, a period's value comes from the report of record.

Three reports carry the same element and period: a filed one, a newer and
more precise draft, and an archived one. The filed report's fact wins; the
draft's precision does not outrank it. Report state is read from the real
extensions database with a throwaway tenant schema.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.operations.roboledger.views.fact_query import (
  _deduplicate_fact_rows,
  _report_standing,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd22"
ENTITY = "ent_sr2"


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


REPORTS = {
  # report_id: (filing_status, last_generated, fact decimals, value)
  "rpt_filed": ("filed", datetime(2026, 3, 1), "-2", 1_200_000.0),
  "rpt_draft": ("draft", datetime(2026, 9, 1), "2", 1_100_000.0),
  "rpt_archived": ("archived", datetime(2026, 1, 1), "-2", 900_000.0),
}


def _seed() -> None:
  from robosystems.models.api.fact_provenance import PivotProvenance
  from robosystems.models.extensions.entity import Entity
  from robosystems.models.extensions.roboledger.fact import Fact
  from robosystems.models.extensions.roboledger.report import Report
  from robosystems.operations.roboledger.fact_set import create_fact_set

  fy_end = date(2025, 12, 31)
  with extensions_session(GRAPH) as session:
    session.add(Entity(id=ENTITY, name="Driftline", created_by="u"))
    for report_id, (status, generated, decimals, value) in REPORTS.items():
      session.add(
        Report(
          id=report_id,
          name=report_id,
          taxonomy_id="tax_sr2",
          filing_status=status,
          last_generated=generated,
          created_by="u",
        )
      )
      session.flush()
      create_fact_set(
        session,
        id=f"fs_{report_id}",
        period_end=fy_end,
        factset_type="report",
        entity_id=ENTITY,
        report_id=report_id,
        provenance=PivotProvenance(mapping_id="map_1", period="2025-01-01/2025-12-31"),
        created_by="u",
      )
      session.flush()
      session.add(
        Fact(
          id=f"fact_{report_id}",
          element_id="el_revenue",
          value=value,
          decimals=decimals,
          period_start=date(2025, 1, 1),
          period_end=fy_end,
          period_type="duration",
          entity_id=ENTITY,
          fact_set_id=f"fs_{report_id}",
        )
      )
    session.commit()


def test_the_filed_report_wins_over_a_newer_more_precise_draft(tenant):
  _seed()
  # As the graph returns them: the draft first, so row order would pick it.
  rows = [
    {
      "fact_id": f"fact_{report_id}",
      "element_id": "rs-gaap:Revenues",
      "period_start": "2025-01-01",
      "period_end": "2025-12-31",
      "entity_name": "Driftline",
      "decimals": decimals,
      "value": value,
    }
    for report_id, (_s, _g, decimals, value) in sorted(
      REPORTS.items(), key=lambda item: item[0] != "rpt_draft"
    )
  ]
  standing = _report_standing(GRAPH, [row["fact_id"] for row in rows])

  (only,) = _deduplicate_fact_rows(rows, standing)
  assert only["value"] == 1_200_000.0
