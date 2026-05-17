# Background Task Worker

Always-on ECS service that processes long-running operations from a Valkey queue. Provides SSE progress streaming, Dagster observability, and tenant-isolated execution.

## How It Works

```
API route calls enqueue_task()
  → Creates SSE operation (PENDING) on Valkey DB 3
  → Pushes task JSON to worker queue on Valkey DB 6
  → Returns 202 with _links for stream/status/cancel

Worker consumer (BRPOP loop)
  → Picks up task from queue
  → Looks up handler via task registry
  → Executes with progress streaming via OperationManager
  → Reports completion to Dagster
  → Cleans up DB connections between tasks
```

## Running

```bash
uv run python -m robosystems.worker
```

In Docker, the worker runs as a separate container in the `robosystems` profile.

## Enqueueing Tasks

```python
from robosystems.worker.client import enqueue_task

response = await enqueue_task(
    task_type="operator",
    graph_id="kg...",
    user_id="user_...",
    params={"operator_type": "mapping", "mapping_id": "struct_..."},
)
# response includes operation_id, _links.stream, _links.status
```

## Task Registration

Non-operator tasks extend `BaseTask` and register with `@register_task`:

```python
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

Operator tasks use the unified Operator system instead — `OperatorWorkerTask` at `operations/operators/adapters/worker_task.py` handles `task_type="operator"` and dispatches to the appropriate operator via `params["operator_type"]`.

## Module Layout

```
worker/
    __init__.py       # Imports task modules to trigger @register_task decorators
    __main__.py       # Entry point: asyncio.run(consumer.run())
    consumer.py       # BRPOP loop, task dispatch, lifecycle management
    client.py         # enqueue_task() — used by API routes
    cleanup.py        # DB connection pool disposal between tasks (tenant isolation)
    dagster.py        # Fire-and-forget AssetMaterialization reporting
    tasks/
        __init__.py   # Task registry: @register_task, get_task_handler()
        base.py       # BaseTask ABC with progress/cancellation helpers
```

## Tenant Isolation

`cleanup.py` disposes all SQLAlchemy connection pools between tasks. This prevents `search_path` leaking between tenants — a lesson from a prior Celery incident where a leaked session caused cross-tenant writes.

## SSE Progress

Tasks stream progress to clients via the existing `OperationManager` (Valkey DB 3). Clients connect to `/v1/operations/{task_id}/stream` for real-time updates.

## Valkey Databases

| DB | Purpose |
|---|---|
| 3 | SSE operations (OperationManager) |
| 6 | Worker task queue (BRPOP) |

See `config/valkey_registry.py` for the full allocation.
