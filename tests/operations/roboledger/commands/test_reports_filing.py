"""Tests for the filing lifecycle commands — file_report + transition_filing_status."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.commands.reports import (
  InvalidFilingTransitionError,
  ReportNotFoundError,
  file_report,
  transition_filing_status,
)


def _make_report_def(
  *,
  filing_status: str = "draft",
  filed_at: datetime | None = None,
  filed_by: str | None = None,
) -> MagicMock:
  """A MagicMock that mimics a Report ORM row well enough for these tests."""
  r = MagicMock()
  r.id = "rpt_01"
  r.name = "Q1 2026 Statements"
  r.taxonomy_id = "tax_usgaap_reporting"
  r.generation_status = "complete"
  r.period_type = "quarterly"
  r.period_start = date(2026, 1, 1)
  r.period_end = date(2026, 3, 31)
  r.comparative = True
  r.periods = None
  r.mapping_id = "map_01"
  r.ai_generated = False
  r.created_at = datetime(2026, 4, 1, tzinfo=UTC)
  r.last_generated = datetime(2026, 4, 1, tzinfo=UTC)
  r.filing_status = filing_status
  r.filed_at = filed_at
  r.filed_by = filed_by
  r.supersedes_id = None
  r.superseded_by_id = None
  r.source_graph_id = None
  r.source_report_id = None
  r.shared_at = None
  return r


def _patch_response_helpers():
  """Patch the helpers report_to_response calls so we don't need real DB data.

  ``file_report`` / ``transition_filing_status`` import these helpers at
  call time (lazy import to break a module-level cycle), so the patch
  has to land on the source module — patching the importer's namespace
  is too late.
  """
  return patch.multiple(
    "robosystems.operations.roboledger.reads.reports",
    load_structures=MagicMock(return_value=[]),
    resolve_entity_name=MagicMock(return_value=None),
    report_to_response=MagicMock(side_effect=lambda r, _s, _e: r),
  )


# ── file_report ────────────────────────────────────────────────────────────


def test_file_report_transitions_draft_to_filed() -> None:
  session = MagicMock()
  session.get.return_value = _make_report_def(filing_status="draft")

  with _patch_response_helpers():
    result = file_report(session, "rpt_01", filed_by="user_01")

  assert result.filing_status == "filed"
  assert result.filed_by == "user_01"
  assert result.filed_at is not None
  session.flush.assert_called_once()


def test_file_report_transitions_under_review_to_filed() -> None:
  session = MagicMock()
  session.get.return_value = _make_report_def(filing_status="under_review")

  with _patch_response_helpers():
    result = file_report(session, "rpt_01", filed_by="user_01")

  assert result.filing_status == "filed"


def test_file_report_rejects_already_filed() -> None:
  session = MagicMock()
  session.get.return_value = _make_report_def(
    filing_status="filed",
    filed_at=datetime(2026, 4, 10, tzinfo=UTC),
    filed_by="user_old",
  )

  with pytest.raises(InvalidFilingTransitionError) as exc:
    file_report(session, "rpt_01", filed_by="user_01")

  assert "filed" in str(exc.value)


def test_file_report_rejects_archived() -> None:
  session = MagicMock()
  session.get.return_value = _make_report_def(filing_status="archived")

  with pytest.raises(InvalidFilingTransitionError):
    file_report(session, "rpt_01", filed_by="user_01")


def test_file_report_raises_when_report_missing() -> None:
  session = MagicMock()
  session.get.return_value = None

  with pytest.raises(ReportNotFoundError):
    file_report(session, "rpt_missing", filed_by="user_01")


# ── transition_filing_status ──────────────────────────────────────────────


def test_transition_draft_to_under_review() -> None:
  session = MagicMock()
  session.get.return_value = _make_report_def(filing_status="draft")

  with _patch_response_helpers():
    result = transition_filing_status(session, "rpt_01", "under_review")

  assert result.filing_status == "under_review"


def test_transition_under_review_back_to_draft() -> None:
  session = MagicMock()
  session.get.return_value = _make_report_def(filing_status="under_review")

  with _patch_response_helpers():
    result = transition_filing_status(session, "rpt_01", "draft")

  assert result.filing_status == "draft"


def test_transition_filed_to_archived() -> None:
  session = MagicMock()
  session.get.return_value = _make_report_def(
    filing_status="filed",
    filed_at=datetime(2026, 4, 10, tzinfo=UTC),
    filed_by="user_01",
  )

  with _patch_response_helpers():
    result = transition_filing_status(session, "rpt_01", "archived")

  assert result.filing_status == "archived"


def test_transition_rejects_filing_via_generic_path() -> None:
  """``filed`` must be reached via :func:`file_report` so audit fields land."""
  session = MagicMock()
  session.get.return_value = _make_report_def(filing_status="under_review")

  with pytest.raises(InvalidFilingTransitionError):
    transition_filing_status(session, "rpt_01", "filed")


def test_transition_rejects_archive_from_draft() -> None:
  """Archive is only legal from filed — direct draft → archived is illegal."""
  session = MagicMock()
  session.get.return_value = _make_report_def(filing_status="draft")

  with pytest.raises(InvalidFilingTransitionError):
    transition_filing_status(session, "rpt_01", "archived")


def test_transition_raises_when_report_missing() -> None:
  session = MagicMock()
  session.get.return_value = None

  with pytest.raises(ReportNotFoundError):
    transition_filing_status(session, "rpt_missing", "under_review")
