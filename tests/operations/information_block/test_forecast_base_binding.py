"""Regression tests for the forecast base-month FactSet binder.

``_actual_set_at`` seeds the forecast's prior values, the BS roll's
month-zero state, and the mapping-provenance lookup. It must prefer the
canonical close-time stamp (report_id IS NULL) over publication
snapshots — the same contract as the statement envelope loaders — or a
Report published later for the base month flips the forecast base onto
a frozen snapshot that can diverge from the statement rendered beside it.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import FactSet, Structure, Taxonomy
from robosystems.operations.information_block.forecast_compute import _actual_set_at

pytestmark = pytest.mark.unit

PERIOD_START = date(2026, 6, 1)
PERIOD_END = date(2026, 6, 30)


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_fcbind_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))

  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


@pytest.fixture()
def statement_structure(ext_session):
  taxonomy = Taxonomy(name="Style", taxonomy_type="reporting_extension")
  ext_session.add(taxonomy)
  ext_session.flush()
  structure = Structure(
    name="Income Statement", block_type="income_statement", taxonomy_id=taxonomy.id
  )
  ext_session.add(structure)
  ext_session.flush()
  return structure


def _add_set(
  session,
  structure_id: str,
  *,
  report_id: str | None,
  created_offset_minutes: int,
) -> FactSet:
  fact_set = FactSet(
    structure_id=structure_id,
    period_start=PERIOD_START,
    period_end=PERIOD_END,
    factset_type="report",
    entity_id="ent_1",
    report_id=report_id,
  )
  fact_set.provenance = {"origin": "pivot", "mapping_id": "m", "period": "2026-06"}
  fact_set.created_at = datetime(2026, 7, 1, tzinfo=UTC) + timedelta(
    minutes=created_offset_minutes
  )
  session.add(fact_set)
  session.flush()
  return fact_set


class TestActualSetAt:
  def test_canonical_beats_later_publication_snapshot(
    self, ext_session, statement_structure
  ):
    canonical = _add_set(
      ext_session, statement_structure.id, report_id=None, created_offset_minutes=0
    )
    _add_set(
      ext_session,
      statement_structure.id,
      report_id="rep_1",
      created_offset_minutes=60,
    )
    ext_session.commit()

    found = _actual_set_at(
      ext_session, statement_structure.id, "ent_1", PERIOD_START, PERIOD_END
    )

    assert found is not None
    assert found.id == canonical.id

  def test_newest_wins_among_publications_when_no_canonical(
    self, ext_session, statement_structure
  ):
    _add_set(
      ext_session,
      statement_structure.id,
      report_id="rep_1",
      created_offset_minutes=0,
    )
    newer = _add_set(
      ext_session,
      statement_structure.id,
      report_id="rep_2",
      created_offset_minutes=60,
    )
    ext_session.commit()

    found = _actual_set_at(
      ext_session, statement_structure.id, "ent_1", PERIOD_START, PERIOD_END
    )

    assert found is not None
    assert found.id == newer.id

  def test_newest_canonical_wins_among_canonicals(
    self, ext_session, statement_structure
  ):
    _add_set(
      ext_session, statement_structure.id, report_id=None, created_offset_minutes=0
    )
    newer = _add_set(
      ext_session, statement_structure.id, report_id=None, created_offset_minutes=60
    )
    ext_session.commit()

    found = _actual_set_at(
      ext_session, statement_structure.id, "ent_1", PERIOD_START, PERIOD_END
    )

    assert found is not None
    assert found.id == newer.id
