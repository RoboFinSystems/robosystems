"""Tests for the report MCP tools: delete-report and get-report-bundle."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.report_tools import (
  DeleteReportTool,
  GetReportBundleTool,
)
from robosystems.models.api.extensions.reports import ReportBundleDownloadResponse
from robosystems.operations.roboledger.commands.reports import (
  NotAuthorizedError,
  ReportHasActiveSharesError,
  ReportNotFiledError,
)
from robosystems.operations.roboledger.reads.reports import (
  BundleSigningError,
  ReportBundleNotAvailableError,
)
from robosystems.routers.graphs.mcp.execute import READ_ONLY_MCP_TOOLS

MODULE = "robosystems.middleware.mcp.tools.report_tools"


@pytest.fixture
def client():
  c = MagicMock()
  c.graph_id = "kgtest123"
  c.user_id = "user_01"
  return c


@contextmanager
def _env(delete_side_effect=None, delete_return=True, access_error=None):
  session = MagicMock()

  @contextmanager
  def _session(_graph_id):
    yield session

  with (
    patch(f"{MODULE}._check_graph_access", return_value=access_error),
    patch(f"{MODULE}.user_is_graph_admin", return_value=False),
    patch(f"{MODULE}.extensions_session", _session),
    patch(
      f"{MODULE}.delete_report",
      side_effect=delete_side_effect,
      return_value=delete_return,
    ) as delete,
    patch(f"{MODULE}.mark_graph_stale") as stale,
    patch(f"{MODULE}.delete_report_artifacts") as artifacts,
  ):
    yield delete, stale, artifacts


def test_definition_names_the_filed_lock(client):
  defn = DeleteReportTool(client).get_tool_definition()
  assert defn["name"] == "delete-report"
  assert defn["inputSchema"]["required"] == ["report_id"]
  assert "filed" in defn["description"]
  assert "regenerate-report" in defn["description"]


@pytest.mark.asyncio
async def test_deletes_then_marks_stale_and_removes_artifacts(client):
  with _env() as (delete, stale, artifacts):
    result = await DeleteReportTool(client).execute({"report_id": "rpt_01"})

  assert result == {"deleted": True, "report_id": "rpt_01"}
  delete.assert_called_once()
  assert delete.call_args.args[1:] == ("rpt_01", "user_01")
  stale.assert_called_once_with("kgtest123", "report_deleted")
  artifacts.assert_called_once_with("kgtest123", ["rpt_01"])


@pytest.mark.asyncio
async def test_a_filed_report_is_refused_and_nothing_else_runs(client):
  refusal = ReportNotFiledError("Report 'rpt_01' is 'filed' and cannot be deleted.")
  with _env(delete_side_effect=refusal) as (_delete, stale, artifacts):
    result = await DeleteReportTool(client).execute({"report_id": "rpt_01"})

  assert result["error"] == "not_allowed"
  assert "filed" in result["message"]
  stale.assert_not_called()
  artifacts.assert_not_called()


@pytest.mark.asyncio
async def test_missing_report_is_not_found(client):
  with _env(delete_return=False) as (_delete, stale, artifacts):
    result = await DeleteReportTool(client).execute({"report_id": "rpt_x"})

  assert result["error"] == "not_found"
  stale.assert_not_called()
  artifacts.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
  ("exc", "code"),
  [
    (NotAuthorizedError("no"), "access_denied"),
    (ReportHasActiveSharesError("rpt_01", ["kg_other"]), "conflict"),
  ],
)
async def test_domain_refusals_map_to_error_codes(client, exc, code):
  with _env(delete_side_effect=exc) as (_delete, _stale, artifacts):
    result = await DeleteReportTool(client).execute({"report_id": "rpt_01"})

  assert result["error"] == code
  artifacts.assert_not_called()


@pytest.mark.asyncio
async def test_no_write_access_stops_before_the_delete(client):
  denied = {"error": "access_denied", "message": "read-only"}
  with _env(access_error=denied) as (delete, _stale, _artifacts):
    result = await DeleteReportTool(client).execute({"report_id": "rpt_01"})

  assert result == denied
  delete.assert_not_called()


@pytest.mark.asyncio
async def test_report_id_is_required(client):
  with _env() as (delete, _stale, _artifacts):
    result = await DeleteReportTool(client).execute({})

  assert result["error"] == "invalid_input"
  delete.assert_not_called()


# ── get-report-bundle ────────────────────────────────────────────────────


@contextmanager
def _bundle_env(side_effect=None, response=None, access_error=None):
  @contextmanager
  def _session(_graph_id):
    yield MagicMock()

  with (
    patch(f"{MODULE}._check_graph_access", return_value=access_error) as access,
    patch(f"{MODULE}.extensions_session", _session),
    patch(
      f"{MODULE}.reads_reports.get_report_download_url",
      side_effect=side_effect,
      return_value=response,
    ) as signed,
  ):
    yield signed, access


def _download(fmt="tavi"):
  return ReportBundleDownloadResponse(
    download_url="https://bucket.s3.amazonaws.com/r/g1.tavi.json?X-Amz-Signature=x",
    expires_at=datetime(2026, 9, 25, 23, 10, tzinfo=UTC),
    content_type="application/json",
    format=fmt,
    generation_count=1,
  )


def test_bundle_definition_warns_the_link_is_a_credential(client):
  defn = GetReportBundleTool(client).get_tool_definition()
  assert defn["name"] == "get-report-bundle"
  assert defn["inputSchema"]["required"] == ["report_id"]
  assert defn["inputSchema"]["properties"]["format"]["enum"] == [
    "tavi",
    "holon-jsonld",
    "xbrl-2.1",
  ]
  assert "Never place the link" in defn["description"]


def test_bundle_is_a_viewer_read():
  assert "get-report-bundle" in READ_ONLY_MCP_TOOLS


@pytest.mark.asyncio
async def test_bundle_defaults_to_tavi_on_a_read_check(client):
  with _bundle_env(response=_download()) as (signed, access):
    result = await GetReportBundleTool(client).execute({"report_id": "rpt_01"})

  access.assert_called_once_with("kgtest123")
  assert signed.call_args.args[1:] == ("kgtest123", "rpt_01")
  assert signed.call_args.kwargs == {"flavor": "tavi"}
  assert result["report_id"] == "rpt_01"
  assert result["format"] == "tavi"
  assert result["download_url"].startswith("https://")
  assert result["expires_at"] == "2026-09-25T23:10:00Z"


@pytest.mark.asyncio
async def test_bundle_passes_the_format_through(client):
  with _bundle_env(response=_download("holon-jsonld")) as (signed, _access):
    await GetReportBundleTool(client).execute(
      {"report_id": "rpt_01", "format": "holon-jsonld"}
    )

  assert signed.call_args.kwargs == {"flavor": "holon-jsonld"}


@pytest.mark.asyncio
async def test_bundle_refuses_an_unknown_format_before_signing(client):
  with _bundle_env(response=_download()) as (signed, _access):
    result = await GetReportBundleTool(client).execute(
      {"report_id": "rpt_01", "format": "pdf"}
    )

  assert result["error"] == "invalid_input"
  signed.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
  ("kwargs", "code"),
  [
    ({"response": None}, "not_found"),
    (
      {"side_effect": ReportBundleNotAvailableError("not published")},
      "not_available",
    ),
    ({"side_effect": BundleSigningError("boto")}, "command_failed"),
  ],
)
async def test_bundle_failures_map_to_error_codes(client, kwargs, code):
  with _bundle_env(**kwargs):
    result = await GetReportBundleTool(client).execute({"report_id": "rpt_01"})

  assert result["error"] == code
  assert "boto" not in result["message"]


@pytest.mark.asyncio
async def test_bundle_without_access_never_signs(client):
  denied = {"error": "access_denied", "message": "no"}
  with _bundle_env(response=_download(), access_error=denied) as (signed, _):
    result = await GetReportBundleTool(client).execute({"report_id": "rpt_01"})

  assert result == denied
  signed.assert_not_called()
