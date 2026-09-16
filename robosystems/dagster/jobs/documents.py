"""Rebuild user graphs' uploaded documents into the search index.

PostgreSQL holds every uploaded document as the source of truth; the OpenSearch
index is derived from it. This job is the way back after the index is recreated
(``just admin <env> search recreate-index`` — a mapping change, which cannot be
done in place) or otherwise lost: walk every ``Document`` row through the same
section → embed → index path an upload takes.

SEC content is not rebuilt here; that is the SEC text-index pipeline.
"""

from typing import Any

from dagster import Config, OpExecutionContext, job, op

from robosystems.dagster.resources import DatabaseResource


class RebuildDocumentsConfig(Config):
  """One graph, or — left unset — every graph that has documents."""

  graph_id: str | None = None


@op
def rebuild_documents(
  context: OpExecutionContext,
  config: RebuildDocumentsConfig,
  db: DatabaseResource,
) -> dict[str, Any]:
  """Re-index every document row, graph by graph, continuing past failures.

  A document that fails stays in PostgreSQL, so the recovery is a re-run for
  its graph; one bad row must not strand every other tenant's search.
  ``get_by_graph`` loads a graph's rows whole — fine at today's volume; page
  with ``yield_per`` if a tenant ever holds thousands.
  """
  from robosystems.models.core import Document
  from robosystems.operations.document_service import DocumentService

  totals = {"graphs": 0, "documents": 0, "sections": 0, "failed": 0}

  with db.get_session() as session:
    graph_ids = [config.graph_id] if config.graph_id else Document.graph_ids(session)
    service = DocumentService(session)

    for graph_id in graph_ids:
      indexed = sections = failed = 0
      for doc in Document.get_by_graph(graph_id, session):
        doc_id = doc.id
        try:
          response = service.resync_document(doc)
        except Exception as e:
          failed += 1
          context.log.error(f"{graph_id}: document {doc_id} failed to index: {e}")
          continue
        indexed += 1
        sections += response.sections_indexed

      context.log.info(
        f"{graph_id}: {indexed} documents, {sections} sections indexed"
        + (f", {failed} failed" if failed else "")
      )
      totals["graphs"] += 1
      totals["documents"] += indexed
      totals["sections"] += sections
      totals["failed"] += failed

  if totals["failed"]:
    context.log.warning(
      f"{totals['failed']} documents failed to index; re-run their graphs"
    )
  context.log.info(
    f"Rebuilt {totals['documents']} documents ({totals['sections']} sections) "
    f"across {totals['graphs']} graphs"
  )
  return totals


@job(
  tags={"dagster/priority": "1", "dagster/max_retries": 3},
  description=(
    "Rebuild user graphs' uploaded documents into the search index from PostgreSQL"
  ),
)
def rebuild_documents_job():
  """Manually triggered: after ``search recreate-index``, or to repair one graph."""
  rebuild_documents()
