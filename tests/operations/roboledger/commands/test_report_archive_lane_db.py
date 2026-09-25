"""DB-backed proof of the report archive lane.

A filed report is a record: archiving hides it from the default list and is
reversible; deleting is only for reports that were never filed. Runs against
the real extensions database with a throwaway tenant schema.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.extensions.reports import ReportLifecycle
from robosystems.operations.roboledger.commands.reports import (
  InvalidFilingTransitionError,
  ReportNotFiledError,
  delete_report,
  transition_filing_status,
)
from robosystems.operations.roboledger.reads.reports import list_reports

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd05"
AUTHOR = "usr_author"


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


@pytest.fixture(autouse=True)
def reports(tenant):
  """One report in each filing state, all generated."""
  from robosystems.models.extensions import Taxonomy
  from robosystems.models.extensions.roboledger.report import Report

  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM reports"))
    session.execute(text("DELETE FROM taxonomies"))
    session.add(
      Taxonomy(
        id="tax_rpt_0001",
        name="Reporting",
        taxonomy_type="reporting_standard",
        created_by="usr_seed",
      )
    )
    session.flush()
    for report_id, filing_status in (
      ("rpt_draft", "draft"),
      ("rpt_review", "under_review"),
      ("rpt_filed", "filed"),
      ("rpt_archived", "archived"),
    ):
      session.add(
        Report(
          id=report_id,
          taxonomy_id="tax_rpt_0001",
          name=report_id,
          period_start=date(2025, 1, 1),
          period_end=date(2025, 12, 31),
          filing_status=filing_status,
          generation_status="published",
          created_by=AUTHOR,
        )
      )
  yield


def _listed(lifecycle: ReportLifecycle | None = None) -> set[str]:
  with extensions_session(GRAPH) as session:
    response = (
      list_reports(session)
      if lifecycle is None
      else list_reports(session, lifecycle=lifecycle)
    )
  return {r.id for r in response.reports}


def _filing_status(report_id: str) -> str | None:
  from robosystems.models.extensions.roboledger.report import Report

  with extensions_session(GRAPH) as session:
    row = session.get(Report, report_id)
    return None if row is None else row.filing_status


class TestListLifecycle:
  def test_default_list_hides_archived_reports(self):
    assert _listed() == {"rpt_draft", "rpt_review", "rpt_filed"}

  def test_archived_filter_returns_only_archived_reports(self):
    assert _listed(ReportLifecycle.ARCHIVED) == {"rpt_archived"}

  def test_all_filter_returns_every_report(self):
    assert _listed(ReportLifecycle.ALL) == {
      "rpt_draft",
      "rpt_review",
      "rpt_filed",
      "rpt_archived",
    }


class TestArchiveIsReversible:
  def test_archived_report_can_be_unarchived_to_filed(self):
    with extensions_session(GRAPH) as session:
      result = transition_filing_status(
        session, "rpt_archived", "filed", acting_user_id=AUTHOR
      )
    assert result.filing_status == "filed"
    assert _filing_status("rpt_archived") == "filed"
    assert "rpt_archived" in _listed()

  def test_unarchive_is_not_a_back_door_to_filing_a_draft(self):
    with extensions_session(GRAPH) as session:
      with pytest.raises(InvalidFilingTransitionError):
        transition_filing_status(session, "rpt_draft", "filed", acting_user_id=AUTHOR)
    assert _filing_status("rpt_draft") == "draft"

  def test_a_draft_cannot_be_archived(self):
    with extensions_session(GRAPH) as session:
      with pytest.raises(InvalidFilingTransitionError):
        transition_filing_status(
          session, "rpt_draft", "archived", acting_user_id=AUTHOR
        )


class TestDeleteIsForUnfiledReports:
  def test_a_generated_draft_can_be_deleted(self):
    with extensions_session(GRAPH) as session:
      assert delete_report(session, "rpt_draft", AUTHOR) is True
    assert _filing_status("rpt_draft") is None

  def test_a_report_under_review_can_be_deleted(self):
    with extensions_session(GRAPH) as session:
      assert delete_report(session, "rpt_review", AUTHOR) is True
    assert _filing_status("rpt_review") is None

  @pytest.mark.parametrize("report_id", ["rpt_filed", "rpt_archived"])
  def test_a_filed_or_archived_report_cannot_be_deleted(self, report_id):
    with extensions_session(GRAPH) as session:
      with pytest.raises(ReportNotFiledError):
        delete_report(session, report_id, AUTHOR)
    assert _filing_status(report_id) is not None
