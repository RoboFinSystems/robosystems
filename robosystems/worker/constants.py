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
#
# The two creation budgets are derived from the Graph API calls they make, not
# from how long a run usually takes. One call's worst case is the client's own
# ``timeout`` across ``max_retries + 1`` attempts plus exponential backoff —
# ~127s on ``GraphClientConfig`` defaults. Graph creation makes two such calls
# (create_database, install_schema) before provisioning the extensions tenant
# schema and copying the taxonomy library into it; subgraph creation makes
# three, and its third is the fork, whose duration scales with the parent's
# data. Both previously sat at 60s — below a *single* call's retry budget — so
# a slow-but-succeeding run was killed mid-flight, and until the rollback was
# fixed that killed run stranded its allocation.
# ``tests/worker/test_task_timeout_coverage.py`` pins both against the client
# config, so raising a retry budget without raising these fails there.
TASK_TIMEOUTS: dict[str, int] = {
  "operator": 600,  # 10 minutes
  "extensions_materialize": 1800,  # 30 minutes
  "graph_creation": 600,  # 10 minutes
  "subgraph_creation": 900,  # 15 minutes (the fork copies parent data)
  "graph_materialization": 120,  # 2 minutes
  "dagster_job_monitor": 3600,  # 1 hour (backup/restore can be long)
  # Drain (120s) + reattach (300s) are awaited before health verification even
  # starts, so anything under 420s kills the migration around the volume
  # detach — with the graph's volume unattached and its subscription left
  # mid-flight. 900 covers those waits plus snapshot, ASG capacity, and
  # verification, with headroom.
  "graph_tier_upgrade": 900,  # 15 minutes
  # The close publishes one QuickBooks entry per in-period draft, and the
  # Intuit SDK sets no per-request timeout — a single call that hits the
  # retry ladder backs off ~31s on its own. A 34-entry month runs ~30s;
  # a heavier one, or one slow call, runs much longer. The budget cannot
  # cancel the thread the close runs on; ``BaseTask.run_blocking`` waits one
  # more budget for it to land and reports what it produced, and only past
  # that grace is the close abandoned as still running. So this number is
  # doing three jobs — the budget, the grace, and (as ``fence_wait_ms``) how
  # long a worker close waits behind another close of the same period — and
  # matching the operator's 600s keeps the ordinary overrun inside the
  # first of them.
  "period_close": 600,  # 10 minutes
}
DEFAULT_TASK_TIMEOUT = 120  # 2 minutes

# Maximum retry attempts before a task is moved to the DLQ
MAX_RETRIES = 3
