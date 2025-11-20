"""Table operations Celery tasks."""

from .duckdb_staging import stage_file_in_duckdb
from .graph_ingestion import ingest_file_to_graph

__all__ = ["stage_file_in_duckdb", "ingest_file_to_graph"]
