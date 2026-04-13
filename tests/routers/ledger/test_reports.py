"""Unit tests for ledger report endpoints."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
)
from robosystems.routers.ledger.reports import (
  create_report,
  delete_report,
  get_report,
  get_statement,
  list_reports,
  share_report,
)

MODULE = "robosystems.routers.ledger.reports"
GRAPH_ID = "kg01234567890abcdef"


@pytest.fixture(autouse=True)
def _mock_resolve_entity_name():
  with patch(f"{MODULE}._resolve_entity_name", return_value="Test Entity"):
    yield


def _make_user():
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _make_report_def(**overrides):
  """Create a mock Report."""
  rd = MagicMock()
  rd.id = overrides.get("id", "rpt_01ABC")
  rd.name = overrides.get("name", "Q1 Financial Statements")
  rd.taxonomy_id = overrides.get("taxonomy_id", "tax_usgaap_reporting")
  rd.generation_status = overrides.get("generation_status", "published")
  rd.period_type = overrides.get("period_type", "quarterly")
  rd.comparative = overrides.get("comparative", True)
  rd.mapping_id = overrides.get("mapping_id", "struct_coa_mapping")
  rd.period_start = overrides.get("period_start", date(2026, 1, 1))
  rd.period_end = overrides.get("period_end", date(2026, 3, 31))
  rd.ai_generated = overrides.get("ai_generated", False)
  rd.ai_intent = None
  rd.ai_workspace_id = None
  rd.ai_confidence = None
  rd.created_at = overrides.get("created_at", datetime(2026, 3, 29, tzinfo=UTC))
  rd.last_generated = overrides.get("last_generated", datetime(2026, 3, 29, tzinfo=UTC))
  rd.source_graph_id = overrides.get("source_graph_id")
  rd.source_report_id = overrides.get("source_report_id")
  rd.shared_at = overrides.get("shared_at")
  rd.created_by = "usr_test123"
  return rd


def _make_report_facts():
  """Create mock ReportFacts."""
  from robosystems.operations.roboledger.reports.fact_grid import (
    PeriodSpec,
    ReportFact,
    ReportFacts,
  )

  return ReportFacts(
    facts=[
      ReportFact(
        element_id="elem_gaap_revenues",
        element_qname="us-gaap:Revenues",
        element_name="Revenue",
        classification="revenue",
        balance_type="credit",
        value=500000.0,
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        period_type="duration",
      ),
    ],
    periods=[
      PeriodSpec(start=date(2026, 1, 1), end=date(2026, 3, 31), label="Current"),
      PeriodSpec(start=date(2025, 10, 3), end=date(2025, 12, 31), label="Prior"),
    ],
    unmapped_count=0,
    taxonomy_id="tax_usgaap_reporting",
    mapping_id="struct_coa_mapping",
  )


def _make_structures():
  from robosystems.models.api.extensions.reports import StructureSummary

  return [
    StructureSummary(
      id="struct_income_statement",
      name="US GAAP Income Statement",
      structure_type="income_statement",
    ),
    StructureSummary(
      id="struct_balance_sheet",
      name="US GAAP Balance Sheet",
      structure_type="balance_sheet",
    ),
  ]


def _mock_session_context(mock_session):
  """Create a patched extensions_session context manager."""
  ctx = MagicMock()
  ctx.__enter__ = MagicMock(return_value=mock_session)
  ctx.__exit__ = MagicMock(return_value=False)
  return ctx


class TestCreateReport:
  @pytest.mark.asyncio
  async def test_creates_report_successfully(self):
    mock_session = MagicMock()
    mock_session.flush = MagicMock()
    mock_session.commit = MagicMock()

    # Mock taxonomy lookup
    mock_tax_result = MagicMock()
    mock_tax_result.fetchone.return_value = MagicMock(id="tax_usgaap_reporting")

    mock_rd = _make_report_def(generation_status="generating")
    report_facts = _make_report_facts()

    # Mock entity lookup
    mock_entity_result = MagicMock()
    mock_entity_result.fetchone.return_value = MagicMock(id="entity_123")

    mock_session.execute.side_effect = [
      mock_tax_result,  # taxonomy exists check
      MagicMock(),  # DELETE from facts
    ]

    with (
      patch(f"{MODULE}.extensions_session") as mock_ext,
      patch(f"{MODULE}.generate_report_facts", return_value=report_facts),
      patch(f"{MODULE}.Report", return_value=mock_rd),
      patch(f"{MODULE}._get_entity_id", return_value="entity_123"),
      patch(f"{MODULE}._load_structures", return_value=_make_structures()),
    ):
      mock_ext.return_value = _mock_session_context(mock_session)

      body = CreateReportRequest(
        name="Q1 Financial Statements",
        mapping_id="struct_coa_mapping",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
      )

      result = await create_report(
        graph_id=GRAPH_ID,
        body=body,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.taxonomy_id == "tax_usgaap_reporting"
    assert len(result.structures) == 2
    mock_session.add.assert_called()
    mock_session.commit.assert_called_once()

  @pytest.mark.asyncio
  async def test_rejects_end_before_start(self):
    body = CreateReportRequest(
      name="Bad Dates",
      mapping_id="struct_1",
      period_start=date(2026, 6, 30),
      period_end=date(2026, 1, 1),
    )

    with pytest.raises(HTTPException) as exc_info:
      await create_report(
        graph_id=GRAPH_ID,
        body=body,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert exc_info.value.status_code == 422


class TestListReports:
  @pytest.mark.asyncio
  async def test_lists_reports(self):
    mock_session = MagicMock()
    rd1 = _make_report_def(id="rpt_1", name="Report 1")
    rd2 = _make_report_def(id="rpt_2", name="Report 2")

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [rd1, rd2]
    mock_session.execute.return_value = mock_result

    with (
      patch(f"{MODULE}.extensions_session") as mock_ext,
      patch(f"{MODULE}._load_structures", return_value=_make_structures()),
    ):
      mock_ext.return_value = _mock_session_context(mock_session)

      result = await list_reports(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.reports) == 2
    assert result.reports[0].id == "rpt_1"
    assert result.reports[1].id == "rpt_2"

  @pytest.mark.asyncio
  async def test_empty_list(self):
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = []
    mock_session.execute.return_value = mock_result

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      result = await list_reports(
        graph_id=GRAPH_ID,
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.reports) == 0


class TestGetReport:
  @pytest.mark.asyncio
  async def test_gets_report_with_structures(self):
    mock_session = MagicMock()
    rd = _make_report_def()
    mock_session.get.return_value = rd

    with (
      patch(f"{MODULE}.extensions_session") as mock_ext,
      patch(f"{MODULE}._load_structures", return_value=_make_structures()),
    ):
      mock_ext.return_value = _mock_session_context(mock_session)

      result = await get_report(
        graph_id=GRAPH_ID,
        report_id="rpt_01ABC",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert result.id == "rpt_01ABC"
    assert result.taxonomy_id == "tax_usgaap_reporting"
    assert len(result.structures) == 2

  @pytest.mark.asyncio
  async def test_report_not_found(self):
    mock_session = MagicMock()
    mock_session.get.return_value = None

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      with pytest.raises(HTTPException) as exc_info:
        await get_report(
          graph_id=GRAPH_ID,
          report_id="rpt_nonexistent",
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404


class TestGetStatement:
  @pytest.mark.asyncio
  async def test_invalid_structure_type(self):
    with pytest.raises(HTTPException) as exc_info:
      await get_statement(
        graph_id=GRAPH_ID,
        report_id="rpt_01ABC",
        structure_type="invalid_type",
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert exc_info.value.status_code == 422
    assert "Invalid structure_type" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_report_not_found(self):
    mock_session = MagicMock()
    mock_session.get.return_value = None

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      with pytest.raises(HTTPException) as exc_info:
        await get_statement(
          graph_id=GRAPH_ID,
          report_id="rpt_nonexistent",
          structure_type="income_statement",
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404


class TestDeleteReport:
  @pytest.mark.asyncio
  async def test_deletes_report(self):
    mock_session = MagicMock()
    rd = _make_report_def()
    mock_session.get.return_value = rd

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      await delete_report(
        graph_id=GRAPH_ID,
        report_id="rpt_01ABC",
        current_user=_make_user(),
        _rate_limit=None,
      )

    mock_session.delete.assert_called_once_with(rd)
    mock_session.commit.assert_called_once()
    # Should also have executed DELETE FROM facts
    mock_session.execute.assert_called()

  @pytest.mark.asyncio
  async def test_delete_not_found(self):
    mock_session = MagicMock()
    mock_session.get.return_value = None

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      with pytest.raises(HTTPException) as exc_info:
        await delete_report(
          graph_id=GRAPH_ID,
          report_id="rpt_nonexistent",
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404


class TestShareReport:
  def _mock_session_with_publish_list(self, mock_session, target_graph_ids=None):
    """Set up mock session to return a publish list and its members."""
    from robosystems.models.extensions import PublishList, PublishListMember

    mock_list = MagicMock(spec=PublishList)
    mock_list.id = "plist_01TEST"
    mock_list.name = "Test List"

    members = []
    for gid in target_graph_ids or []:
      m = MagicMock(spec=PublishListMember)
      m.target_graph_id = gid
      members.append(m)

    # Chain: first execute returns publish list, second returns members
    mock_result_list = MagicMock()
    mock_result_list.scalar_one_or_none.return_value = mock_list

    mock_result_members = MagicMock()
    mock_result_members.scalars.return_value.all.return_value = members

    mock_session.execute.side_effect = [mock_result_list, mock_result_members]

  @pytest.mark.asyncio
  async def test_rejects_unpublished_report(self):
    mock_session = MagicMock()
    rd = _make_report_def(generation_status="generating")
    mock_session.get.return_value = rd

    self._mock_session_with_publish_list(mock_session, ["kg_target_123"])

    from robosystems.models.api.extensions.reports import ShareReportRequest

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      with pytest.raises(HTTPException) as exc_info:
        await share_report(
          graph_id=GRAPH_ID,
          report_id="rpt_01ABC",
          body=ShareReportRequest(publish_list_id="plist_01TEST"),
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 422
    assert "published" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_report_not_found_for_share(self):
    mock_session = MagicMock()
    mock_session.get.return_value = None

    self._mock_session_with_publish_list(mock_session, ["kg_target_123"])

    from robosystems.models.api.extensions.reports import ShareReportRequest

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      with pytest.raises(HTTPException) as exc_info:
        await share_report(
          graph_id=GRAPH_ID,
          report_id="rpt_nonexistent",
          body=ShareReportRequest(publish_list_id="plist_01TEST"),
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404

  @pytest.mark.asyncio
  async def test_publish_list_not_found(self):
    mock_session = MagicMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_session.execute.return_value = mock_result

    from robosystems.models.api.extensions.reports import ShareReportRequest

    with patch(f"{MODULE}.extensions_session") as mock_ext:
      mock_ext.return_value = _mock_session_context(mock_session)

      with pytest.raises(HTTPException) as exc_info:
        await share_report(
          graph_id=GRAPH_ID,
          report_id="rpt_01ABC",
          body=ShareReportRequest(publish_list_id="plist_nonexistent"),
          current_user=_make_user(),
          _rate_limit=None,
        )

    assert exc_info.value.status_code == 404
    assert "Publish list" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_share_returns_error_for_invalid_target(self):
    """Target graph without roboledger schema should return error result."""
    mock_session = MagicMock()
    rd = _make_report_def(generation_status="published")
    mock_session.get.return_value = rd

    self._mock_session_with_publish_list(mock_session, ["kg_no_ledger"])

    # After publish list queries, session.execute is used for fact query
    mock_fact_result = MagicMock()
    mock_fact_result.fetchall.return_value = []
    # Reset side_effect so subsequent calls work
    original_side_effect = mock_session.execute.side_effect
    call_count = [0]
    results = list(original_side_effect) if original_side_effect else []

    def execute_handler(*args, **kwargs):
      idx = call_count[0]
      call_count[0] += 1
      if idx < len(results):
        return results[idx]
      return mock_fact_result

    mock_session.execute.side_effect = execute_handler

    from robosystems.models.api.extensions.reports import (
      ShareReportRequest,
      ShareResultItem,
    )

    with (
      patch(f"{MODULE}.extensions_session") as mock_ext,
      patch(
        f"{MODULE}._share_to_target",
        return_value=ShareResultItem(
          target_graph_id="kg_no_ledger",
          status="error",
          error="Target graph does not have 'roboledger' schema extension.",
          fact_count=0,
        ),
      ),
    ):
      mock_ext.return_value = _mock_session_context(mock_session)

      result = await share_report(
        graph_id=GRAPH_ID,
        report_id="rpt_01ABC",
        body=ShareReportRequest(publish_list_id="plist_01TEST"),
        current_user=_make_user(),
        _rate_limit=None,
      )

    assert len(result.results) == 1
    assert result.results[0].status == "error"
    assert "roboledger" in result.results[0].error
