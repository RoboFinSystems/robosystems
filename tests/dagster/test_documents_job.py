"""Tests for the search-index rebuild job."""

from unittest.mock import MagicMock, patch

import pytest
from dagster import JobDefinition, build_op_context


class TestRebuildDocumentsJobDefinition:
  """Structure and registration of the rebuild job."""

  @pytest.mark.unit
  def test_job_has_one_op(self):
    from robosystems.dagster.jobs.documents import rebuild_documents_job

    assert isinstance(rebuild_documents_job, JobDefinition)
    op_names = {op_def.name for op_def in rebuild_documents_job.all_node_defs}
    assert op_names == {"rebuild_documents"}

  @pytest.mark.unit
  def test_job_has_correct_tags(self):
    from robosystems.dagster.jobs.documents import rebuild_documents_job

    assert rebuild_documents_job.tags.get("dagster/priority") == "1"
    assert rebuild_documents_job.tags.get("dagster/max_retries") == "3"

  @pytest.mark.unit
  def test_job_registered_in_definitions(self):
    from robosystems.dagster.definitions import all_jobs
    from robosystems.dagster.jobs.documents import rebuild_documents_job

    assert rebuild_documents_job in all_jobs


def _row(doc_id: str) -> MagicMock:
  doc = MagicMock()
  doc.id = doc_id
  return doc


def _run(graph_id=None, rows_by_graph=None, failing=()):
  """Run the op against fakes; return (result, service, graph_ids mock)."""
  from robosystems.dagster.jobs.documents import (
    RebuildDocumentsConfig,
    rebuild_documents,
  )

  rows_by_graph = rows_by_graph or {}
  session = MagicMock()
  db = MagicMock()
  db.get_session.return_value.__enter__ = lambda *_: session
  db.get_session.return_value.__exit__ = lambda *_: False

  service = MagicMock()

  def _resync(doc):
    if doc.id in failing:
      raise RuntimeError("Document produced no indexable sections")
    response = MagicMock()
    response.sections_indexed = 2
    return response

  service.resync_document.side_effect = _resync

  with (
    patch(
      "robosystems.models.core.Document.graph_ids",
      return_value=sorted(rows_by_graph),
    ) as graph_ids,
    patch(
      "robosystems.models.core.Document.get_by_graph",
      side_effect=lambda gid, _session: rows_by_graph.get(gid, []),
    ),
    patch(
      "robosystems.operations.document_service.DocumentService",
      return_value=service,
    ),
  ):
    result = rebuild_documents(
      build_op_context(), RebuildDocumentsConfig(graph_id=graph_id), db
    )
  return result, service, graph_ids


class TestRebuildDocumentsOp:
  """The op walks PostgreSQL rows through the upload path, per graph."""

  @pytest.mark.unit
  def test_every_graph_with_rows_is_rebuilt_when_no_graph_is_given(self):
    result, service, graph_ids = _run(
      rows_by_graph={
        "kg_a": [_row("doc_1"), _row("doc_2")],
        "kg_b": [_row("doc_3")],
      }
    )

    graph_ids.assert_called_once()
    assert service.resync_document.call_count == 3
    assert result == {"graphs": 2, "documents": 3, "sections": 6, "failed": 0}

  @pytest.mark.unit
  def test_one_graph_skips_enumeration(self):
    result, service, graph_ids = _run(
      graph_id="kg_a",
      rows_by_graph={"kg_a": [_row("doc_1"), _row("doc_2")], "kg_b": [_row("x")]},
    )

    graph_ids.assert_not_called()
    assert service.resync_document.call_count == 2
    assert result["graphs"] == 1
    assert result["documents"] == 2

  @pytest.mark.unit
  def test_a_failing_row_is_counted_and_the_rest_still_index(self):
    """One bad document must not strand the rest of the tenant's search —
    the row is still in PostgreSQL, so the recovery is a re-run."""
    result, service, _ = _run(
      graph_id="kg_a",
      rows_by_graph={"kg_a": [_row("doc_1"), _row("doc_bad"), _row("doc_3")]},
      failing={"doc_bad"},
    )

    assert service.resync_document.call_count == 3
    assert result == {"graphs": 1, "documents": 2, "sections": 4, "failed": 1}
