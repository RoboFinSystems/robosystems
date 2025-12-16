"""
Celery task for XBRL graph ingestion using DuckDB-based pattern.

This task uses the XBRLDuckDBGraphProcessor to ingest XBRL data via
DuckDB staging tables and the Graph API, as an alternative to the
proven COPY-based consolidation approach.
"""

from celery import shared_task
from typing import Dict, Any

from robosystems.logger import logger
from robosystems.adapters.sec.processors import XBRLDuckDBGraphProcessor


@shared_task(
  name="sec_xbrl.ingest_via_duckdb",
  bind=True,
  max_retries=3,
  default_retry_delay=300,
)
def ingest_via_duckdb(
  self,
  rebuild: bool = True,
  year: int = None,
) -> Dict[str, Any]:
  """
  Ingest SEC data using DuckDB-based pattern (DuckDB staging tables → Ingest).

  IMPORTANT: This approach always rebuilds the graph from scratch (default) because
  DuckDB staging tables discover and load ALL processed files from S3, not just new ones.
  This is different from the COPY-based approach which works incrementally with
  consolidated files.

  Args:
      rebuild: Whether to rebuild graph database from scratch (default: True).
               Should remain True to avoid duplicate key errors.
      year: Optional year filter for processing. If provided, only files
            from that year will be included in the rebuild.

  Returns:
      Processing results with statistics
  """
  import traceback

  caller_stack = "".join(traceback.format_stack()[:-1])
  logger.info(
    f"Starting SEC DuckDB-based ingestion task (year={year or 'all'}) "
    f"- Task ID: {self.request.id}"
  )
  logger.debug(f"Ingestion task dispatch stack trace:\n{caller_stack}")

  try:
    processor = XBRLDuckDBGraphProcessor(graph_id="sec")

    # Run async processor in sync context
    import asyncio

    result = asyncio.run(
      processor.process_files(
        rebuild=rebuild,
        year=year,
      )
    )

    return result

  except Exception as e:
    logger.error(f"SEC DuckDB-based ingestion task failed: {e}", exc_info=True)
    raise self.retry(exc=e)
