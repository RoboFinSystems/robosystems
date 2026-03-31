"""Shared constants for the background worker system.

Used by both the consumer loop and the Dagster reaper sensor.
"""

# Per-task-type timeouts in seconds. Agent tasks can take 2+ minutes
# for complex mapping operations. Shorter tasks get tighter limits.
TASK_TIMEOUTS: dict[str, int] = {
  "agent": 300,  # 5 minutes
  "graph_creation": 60,  # 1 minute
  "subgraph_creation": 60,  # 1 minute
  "repository_provisioning": 60,  # 1 minute
  "graph_materialization": 120,  # 2 minutes
  "dagster_job_monitor": 3600,  # 1 hour (backup/restore can be long)
  "file_staging": 60,  # 1 minute
  "document_indexing": 120,  # 2 minutes
}
DEFAULT_TASK_TIMEOUT = 120  # 2 minutes

# Maximum retry attempts before a task is moved to the DLQ
MAX_RETRIES = 3
