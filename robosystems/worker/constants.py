"""Shared constants for the background worker system.

Used by both the consumer loop and the Dagster reaper sensor.
"""

# Per-task-type timeouts in seconds. Agent tasks can take 2+ minutes
# for complex mapping operations — the mapping agent makes one AI
# call per CoA element during rs-gaap refinement, so a 100-element
# CoA at ~3-4s/call lands around 6-7 minutes. Match the agent's own
# ``ExecutionProfile.max_time=600`` so the worker doesn't kill jobs
# the agent thinks are still in budget.
TASK_TIMEOUTS: dict[str, int] = {
  "agent": 600,  # 10 minutes
  "extensions_materialize": 1800,  # 30 minutes
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
