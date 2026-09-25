"""Shared constants for the background worker system.

Used by both the consumer loop and the Dagster reaper sensor.
"""

# Per-task-type timeouts (seconds). Keys must correspond exactly to
# TASK_REGISTRY in both directions (an unknown task type silently gets the
# default); tests/worker/test_task_timeout_coverage.py enforces it.
#
# "operator" matches the operator's own ExecutionProfile max of 600s. The
# creation budgets derive from the Graph API calls they make at worst-case
# retry (~127s each on GraphClientConfig defaults; two calls for a graph,
# three for a subgraph, the last a fork scaling with parent data); the same
# test pins them against the client config.
TASK_TIMEOUTS: dict[str, int] = {
  "operator": 600,
  "extensions_materialize": 1800,
  "graph_creation": 600,
  "subgraph_creation": 900,
  # At least one Graph API chunk (CHUNK_TIMEOUT, 600s) and at most half the
  # materialize lock's TTL; the same test pins both.
  "graph_materialization": 1800,
  "dagster_job_monitor": 3600,  # backup/restore can be long
  # Drain (120s) + reattach (300s) precede verification; under 420s would kill
  # the migration with the volume detached.
  "graph_tier_upgrade": 900,
  # Publishes one QuickBooks entry per draft, and the Intuit SDK has no
  # per-request timeout. This value is also run_blocking's join grace and the
  # close fence wait (fence_wait_ms).
  "period_close": 600,
}
DEFAULT_TASK_TIMEOUT = 120

# Attempts before a task moves to the DLQ
MAX_RETRIES = 3

# A live worker refreshes its heartbeat key; the reaper never takes a task from
# a worker that is still beating, since that worker removes its own tasks.
WORKER_HEARTBEAT_INTERVAL = 30
WORKER_HEARTBEAT_TTL = 90


def worker_heartbeat_key(worker_id: str) -> str:
  return f"worker:alive:{worker_id}"
