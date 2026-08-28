# Background Task Worker

Always-on ECS service that runs long-running, user-initiated operations off the request path, with SSE progress streaming and tenant-isolated execution.

Use the worker when a user is waiting and wants progress or cancellation. Use a [Dagster job](../dagster/README.md) for scheduled or sensor-triggered platform work. Use FastAPI `BackgroundTasks` only for fire-and-forget work short enough that losing it on a restart is acceptable.

## How it works

```
API route calls enqueue_task()
  → Creates an SSE operation (PENDING) on Valkey DB 3
  → RPUSH the task JSON onto "worker:tasks" on Valkey DB 6
  → Returns 202 with _links for stream/status/cancel

Worker consumer loop
  → BLMOVE atomically moves the task from "worker:tasks" to a per-worker
    inflight list
  → Looks up the handler in the task registry
  → Marks the operation RUNNING (until then /status reports it PENDING with
    its queue position), marks itself protected from ECS scale-in, then
    executes with progress streaming through OperationManager
  → Removes the task from inflight on success
  → Disposes DB connection pools before the next task
```

The AI Operator endpoints (`POST /v1/graphs/{graph_id}/operator[/{operator_type}]`) are the main producer: every operator run is queued here, and the API answers 202 with the operation links (or waits a bounded time under `?mode=sync`). Nothing AI-driven runs in the API process.

RPUSH plus BLMOVE gives FIFO ordering. The inflight list is the reliability half: if a worker crashes mid-task, the task stays there until the `worker_inflight_reaper_sensor` in the Dagster daemon finds it and requeues it. A task that stays stale past its attempt budget is moved to `worker:dlq` with a `dlq_reason`; inspect and drain it with `just admin <env> worker dlq list|retry|clear`.

## Running

```bash
uv run python -m robosystems.worker
```

In Docker the worker is a separate container in the `robosystems` profile.

## Enqueueing

```python
from robosystems.worker.client import enqueue_task

response = await enqueue_task(
    task_type="operator",
    graph_id="kg...",
    user_id="user_...",
    params={"operator_type": "mapping", "mapping_id": "struct_..."},
)
# response includes operation_id, status, _links.stream / .status / .cancel
```

A `task_type` + `graph_id` + `user_id` + `params` combination is deduplicated within `DEDUP_TTL` seconds and returns the existing operation instead of enqueueing twice. Deduplication is skipped when `graph_id` is `None` (graph creation, for instance).

## Registered task types

| `task_type` | Handler | Purpose |
| ----------- | ------- | ------- |
| `operator` | `operations/operators/adapters/worker_task.py` | Dispatches to an AI Operator via `params["operator_type"]` |
| `graph_creation` | `operations/graph/tasks/graph_creation.py` | Provision a new graph |
| `graph_materialization` | `operations/graph/tasks/graph_materialization.py` | Materialize a graph |
| `extensions_materialize` | `operations/graph/tasks/extensions_materialize.py` | OLTP→OLAP materialization for a tenant |
| `graph_tier_upgrade` | `operations/graph/tasks/graph_tier_upgrade.py` | Move a graph to another tier |
| `dagster_job_monitor` | `worker/tasks/dagster_monitoring.py` | Submit a Dagster job and relay its status to SSE |

Per-task-type timeouts live in `constants.py`, shared by the consumer and the reaper sensor. An unrecognized `task_type` falls back to `DEFAULT_TASK_TIMEOUT`.

## Adding a task

Non-operator tasks extend `BaseTask` and register with `@register_task`:

```python
from typing import Any
from robosystems.worker.tasks.base import BaseTask
from robosystems.worker.tasks import register_task

@register_task("my_task")
class MyTask(BaseTask):
    async def execute(self) -> dict[str, Any]:
        await self.report_progress("Starting...", percent=0)
        # do work
        await self.report_progress("Done", percent=100)
        return {"result": "ok"}
```

Registration happens through side-effect imports at module load — add the import to `worker/__init__.py`. Add a timeout for the new `task_type` in `constants.py`.

AI Operator work does not need a new task class: `OperatorWorkerTask` already handles `task_type="operator"` and dispatches on `params["operator_type"]`.

## Pausing for input

A task that reaches a decision only a human should make stops there rather than guessing:

```python
async def execute(self) -> dict[str, Any]:
    if self.resume is None:
        drafts = await self.run_blocking(self._draft_entries)
        await self.pause_for_input(
            "Post 34 drafted entries and close 2026-07?",
            checkpoint={"drafts": [d.id for d in drafts]},
            details={"period": "2026-07", "count": len(drafts)},
        )
    # Re-entered through the resume: pick up from the checkpoint
    if self.resume["input"].get("approved"):
        return await self.run_blocking(self._post, self.resume["checkpoint"]["drafts"])
    return {"posted": 0, "declined": True}
```

`pause_for_input` records the prompt, the checkpoint and this task's queue payload on the operation (status `awaiting_input`), then raises `TaskPaused` so `execute` unwinds without a result — the consumer neither completes nor fails it. `/v1/operations/{id}/status` shows the prompt and a `resume` link; `POST /v1/operations/{id}/resume` with `{"input": {...}}` puts the same operation back on the queue with `params["resume"] = {"checkpoint", "input"}`, which the task reads back through `self.resume`. A paused operation's stream replays and closes; reconnect after the resume to follow the rest. Cancelling a paused operation works like cancelling a running one.

## Layout

| Module | Contents |
| ------ | -------- |
| `__init__.py` | Side-effect imports that trigger `@register_task`, plus `load_adapter_tasks()` |
| `__main__.py` | Entry point — `asyncio.run(consumer.run())` |
| `consumer.py` | BLMOVE loop, dispatch, operation lifecycle, signal handling |
| `client.py` | `enqueue_task()` — used by API routes |
| `constants.py` | Per-task-type timeouts, shared with the reaper sensor |
| `cleanup.py` | Disposes SQLAlchemy connection pools between tasks |
| `metrics.py` | Daemon thread publishing `QueueDepth`, `InFlight`, `Backlog` (queued + in flight — what the scaling alarms read) and `DLQDepth` to CloudWatch, independent of the consume loop |
| `task_protection.py` | ECS scale-in protection while a task is in flight |
| `tasks/__init__.py` | `TASK_REGISTRY`, `@register_task`, `get_task_handler()` |
| `tasks/base.py` | `BaseTask` ABC with progress, cancellation and pause-for-input helpers |
| `tasks/dagster_monitoring.py` | `DagsterJobMonitorTask` |

## Tenant isolation

`cleanup.py` disposes every SQLAlchemy connection pool between tasks. Correctly written tasks already use `extensions_session()` and close platform sessions in `finally` blocks, but this catches what slips through: a pooled connection carrying a leaked `search_path` into the next task would read another tenant's schema.

## Scaling

The ECS service scales on `Backlog` — queued plus in-flight tasks — published once a minute by every worker's metrics thread. Queue depth alone counted only the waiting tasks, so one worker busy on a ten-minute close with two questions queued behind it never crossed the scale-out threshold; with backlog that reads 3 and adds a worker. Scale-in waits for a backlog of zero for five minutes, so a worker mid-task is never the one removed (and is protected regardless, below).

## Scale-in protection

While processing a task the worker marks itself protected from ECS scale-in and clears protection when idle — otherwise a busy worker chosen for termination gets SIGKILLed at the StopTimeout and its task is only requeued after the full task timeout elapses. Protection calls are best-effort: failures are logged, never raised, and outside ECS the manager disables itself entirely.

## SSE progress

Tasks stream progress through `OperationManager` on Valkey DB 3. Clients connect to `/v1/operations/{operation_id}/stream`.

## Valkey databases

| DB | Purpose |
| -- | ------- |
| 3 | SSE operations (`OperationManager`) |
| 6 | Worker task queue (`worker:tasks`), per-worker inflight lists, and `worker:dlq` |

Never hardcode these numbers — see `config/valkey_registry.py` for the full allocation.
