"""Shared constants for the background worker system.

Used by both the consumer loop and the Dagster reaper sensor.
"""

# Per-task-type timeouts in seconds. Keys must match the ``task_type`` the
# enqueuer sends; an unrecognized key silently falls back to
# ``DEFAULT_TASK_TIMEOUT``, which is how a task whose own internal waits far
# exceed two minutes can be killed mid-flight without anything saying so.
#
# This mapping must stay in exact correspondence with ``TASK_REGISTRY`` —
# every registered task type has an entry here, and every entry here names a
# registered task type. ``tests/worker/test_task_timeout_coverage.py`` asserts
# both directions. The second direction is not pedantry: a task-type rename
# leaves a dead key here *and* a registered task silently on the default, and
# checking only for missing keys catches half of it. The "agent" -> "operator"
# rename is the recorded precedent.
#
# The "operator" budget matches the operator's own
# ``ExecutionProfile.max_time=600`` so the worker doesn't kill jobs the
# operator thinks are still in budget — the MappingOperator makes one AI call
# per CoA element during rs-gaap refinement, so a 100-element CoA at ~3-4s per
# call lands around 6-7 minutes.
TASK_TIMEOUTS: dict[str, int] = {
  "operator": 600,  # 10 minutes
  "extensions_materialize": 1800,  # 30 minutes
  "graph_creation": 60,  # 1 minute
  "subgraph_creation": 60,  # 1 minute
  "graph_materialization": 120,  # 2 minutes
  "dagster_job_monitor": 3600,  # 1 hour (backup/restore can be long)
  # Drain (120s) + reattach (300s) are awaited before health verification even
  # starts, so anything under 420s kills the migration around the volume
  # detach — with the graph's volume unattached and its subscription left
  # mid-flight. 900 covers those waits plus snapshot, ASG capacity, and
  # verification, with headroom.
  "graph_tier_upgrade": 900,  # 15 minutes
}
DEFAULT_TASK_TIMEOUT = 120  # 2 minutes

# Maximum retry attempts before a task is moved to the DLQ
MAX_RETRIES = 3
