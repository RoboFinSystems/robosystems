"""Table operations Celery tasks."""

from .duckdb_staging import stage_file_in_duckdb
from .graph_materialization import materialize_file_to_graph

__all__ = ["stage_file_in_duckdb", "materialize_file_to_graph"]
