"""Metric operands bind the requested window, not a newer shorter one."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.extensions.roboledger.fact import Fact
from robosystems.operations.information_block.metrics import _bind_operand
from robosystems.operations.roboledger.fact_set import create_fact_set

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd10"
ENTITY = "ent_bind"
REVENUE = "elem_revenue"
YEAR_END = date(2025, 12, 31)


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
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=tables,
      )
    with extensions_session(GRAPH) as session:
      now = datetime.now(UTC)
      # The year's report first, December's monthly report after it.
      for created, start, value in (
        (now - timedelta(days=5), date(2025, 1, 1), 1_200_000.0),
        (now, date(2025, 12, 1), 100_000.0),
      ):
        fact_set = create_fact_set(
          session,
          period_end=YEAR_END,
          period_start=start,
          factset_type="report",
          entity_id=ENTITY,
          provenance={
            "origin": "pivot",
            "mapping_id": "m",
            "period": f"{start}/{YEAR_END}",
          },
          created_by="test",
        )
        fact_set.created_at = created
        session.flush()
        session.add(
          Fact(
            element_id=REVENUE,
            entity_id=ENTITY,
            fact_set_id=fact_set.id,
            period_start=start,
            period_end=YEAR_END,
            period_type="duration",
            value=value,
            fact_scope="in_scope",
          )
        )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.mark.parametrize(
  ("requested_start", "expected"),
  [
    (date(2025, 1, 1), 1_200_000.0),
    (None, 1_200_000.0),
    (date(2025, 12, 1), 100_000.0),
  ],
)
def test_the_requested_window_binds(tenant, requested_start, expected):
  with extensions_session(GRAPH) as session:
    bound = _bind_operand(
      session,
      element_id=REVENUE,
      entity_id=ENTITY,
      period_end=YEAR_END,
      period_start=requested_start,
    )
  assert bound is not None and bound.value == expected


MONTHLY_ONLY = "ent_monthly_only"


def test_a_window_with_no_fact_of_its_own_binds_nothing(tenant):
  """Only December's set exists: the year and the quarter ending with it are
  different numbers, so neither binds December."""
  with extensions_session(GRAPH) as session:
    fact_set = create_fact_set(
      session,
      period_end=YEAR_END,
      period_start=date(2025, 12, 1),
      factset_type="report",
      entity_id=MONTHLY_ONLY,
      provenance={"origin": "pivot", "mapping_id": "m", "period": "2025-12"},
      created_by="test",
    )
    session.flush()
    session.add(
      Fact(
        element_id=REVENUE,
        entity_id=MONTHLY_ONLY,
        fact_set_id=fact_set.id,
        period_start=date(2025, 12, 1),
        period_end=YEAR_END,
        period_type="duration",
        value=100_000.0,
        fact_scope="in_scope",
      )
    )
    session.flush()
    for requested_start in (date(2025, 1, 1), date(2025, 10, 1)):
      assert (
        _bind_operand(
          session,
          element_id=REVENUE,
          entity_id=MONTHLY_ONLY,
          period_end=YEAR_END,
          period_start=requested_start,
        )
        is None
      )
    session.rollback()
