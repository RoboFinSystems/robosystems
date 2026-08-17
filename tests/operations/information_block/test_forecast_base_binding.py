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


class TestCanonicalOnly:
  """`canonical_only` asks a different question from "which set is better".

  The publication fallback is right when seeding from a month already known
  to be closed — a report published later must not beat the close stamp,
  but it will do in a pinch. It is wrong when the question *is* whether the
  month is closed, which is what the forecast walk's moving anchor asks.
  `create_report` consults no fiscal calendar, so a report generated for an
  open or future month leaves a perfectly valid publication snapshot; and
  reopening a month retracts only the canonical set
  (`statement_sets._canonical_set_ids_in_window`), deliberately leaving the
  snapshot behind. Close-stamping is the only thing that means closed.

  These run against a real schema because the callers stub `_actual_set_at`
  out entirely — a harness that implements the distinction itself proves
  the call sites pass the flag, not that the flag does anything.
  """

  def test_a_publication_snapshot_alone_is_not_canonical(
    self, ext_session, statement_structure
  ):
    _add_set(
      ext_session, statement_structure.id, report_id="rep_1", created_offset_minutes=0
    )
    ext_session.commit()

    assert (
      _actual_set_at(
        ext_session,
        statement_structure.id,
        "ent_1",
        PERIOD_START,
        PERIOD_END,
        canonical_only=True,
      )
      is None
    )

  def test_the_publication_fallback_survives_by_default(
    self, ext_session, statement_structure
  ):
    """The control — the same row, and the default read still finds it.

    Seeding the walk from an explicitly authored base month keeps working
    on a tenant whose base carries only a published report.
    """
    published = _add_set(
      ext_session, statement_structure.id, report_id="rep_1", created_offset_minutes=0
    )
    ext_session.commit()

    found = _actual_set_at(
      ext_session, statement_structure.id, "ent_1", PERIOD_START, PERIOD_END
    )

    assert found is not None
    assert found.id == published.id

  def test_a_canonical_set_is_found(self, ext_session, statement_structure):
    canonical = _add_set(
      ext_session, statement_structure.id, report_id=None, created_offset_minutes=0
    )
    _add_set(
      ext_session, statement_structure.id, report_id="rep_1", created_offset_minutes=60
    )
    ext_session.commit()

    found = _actual_set_at(
      ext_session,
      statement_structure.id,
      "ent_1",
      PERIOD_START,
      PERIOD_END,
      canonical_only=True,
    )

    assert found is not None
    assert found.id == canonical.id


class TestNewestActualMonthIsCanonical:
  """The anchor scan's upper bound answers the same question, so it takes
  the same filter — otherwise a report published for a future month sets a
  bound past the seam and the scan starts by probing months nobody closed.
  """

  def test_a_publication_only_month_does_not_raise_the_bound(
    self, ext_session, statement_structure
  ):
    from robosystems.operations.information_block.forecast_compute import (
      _newest_actual_month,
    )

    _add_set(
      ext_session, statement_structure.id, report_id=None, created_offset_minutes=0
    )
    # A report generated for a month three ahead of the close.
    later = _add_set(
      ext_session,
      statement_structure.id,
      report_id="rep_future",
      created_offset_minutes=60,
    )
    later.period_start = date(2026, 9, 1)
    later.period_end = date(2026, 9, 30)
    ext_session.commit()

    assert (
      _newest_actual_month(ext_session, statement_structure.id, "ent_1") == "2026-06"
    )

  def test_no_canonical_sets_at_all_is_none(self, ext_session, statement_structure):
    from robosystems.operations.information_block.forecast_compute import (
      _newest_actual_month,
    )

    _add_set(
      ext_session, statement_structure.id, report_id="rep_1", created_offset_minutes=0
    )
    ext_session.commit()

    assert _newest_actual_month(ext_session, statement_structure.id, "ent_1") is None
