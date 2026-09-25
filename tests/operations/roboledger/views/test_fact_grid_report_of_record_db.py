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
  _tenant_fact_context,
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
  standing, _ = _tenant_fact_context(GRAPH, [row["fact_id"] for row in rows])

  (only,) = _deduplicate_fact_rows(rows, standing)
  assert only["value"] == 1_200_000.0


SCHEDULES = {
  # fact_set_id: (period_end, amount)
  "fs_sched_a": (date(2026, 7, 31), 400.0),
  "fs_sched_b": (date(2026, 7, 31), 1_071.42),
  "fs_sched_a_2031": (date(2031, 1, 31), 400.0),
}


def _seed_schedules() -> None:
  """Two depreciation schedules on one account, one of them projected years out."""
  from robosystems.models.api.fact_provenance import ScheduleProvenance
  from robosystems.models.extensions.roboledger.fact import Fact
  from robosystems.operations.roboledger.fact_set import create_fact_set

  with extensions_session(GRAPH) as session:
    for fact_set_id, (period_end, amount) in SCHEDULES.items():
      create_fact_set(
        session,
        id=fact_set_id,
        period_start=period_end.replace(day=1),
        period_end=period_end,
        factset_type="schedule",
        entity_id=ENTITY,
        provenance=ScheduleProvenance(
          structure_id=f"str_{fact_set_id}", method="straight_line"
        ),
        created_by="u",
      )
      session.flush()
      session.add(
        Fact(
          id=f"fact_{fact_set_id}",
          element_id="el_depreciation",
          value=amount,
          decimals="2",
          period_start=period_end.replace(day=1),
          period_end=period_end,
          period_type="duration",
          entity_id=ENTITY,
          fact_set_id=fact_set_id,
        )
      )
    session.commit()


async def test_a_schedule_amount_is_never_the_accounts_value(tenant):
  from unittest.mock import AsyncMock, patch

  from robosystems.operations.roboledger.views.fact_query import query_fact_grid

  _seed_schedules()
  rows = [
    {
      "fact_id": f"fact_{fact_set_id}",
      "element_id": "coa:7000",
      "period_start": period_end.replace(day=1).isoformat(),
      "period_end": period_end.isoformat(),
      "entity_name": "Driftline",
      "decimals": "2",
      "value": amount,
    }
    for fact_set_id, (period_end, amount) in SCHEDULES.items()
  ]
  repository = AsyncMock()
  repository.execute_query.return_value = rows
  with patch(
    "robosystems.operations.roboledger.views.fact_query.get_graph_repository",
    AsyncMock(return_value=repository),
  ):
    facts, truncated = await query_fact_grid(GRAPH, elements=["coa:7000"])

  assert facts == []
  assert truncated is False
