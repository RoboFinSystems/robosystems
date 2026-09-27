# Robustness Middleware

Resilience and operational-visibility primitives used across the graph
endpoints: a per-graph/per-operation circuit breaker, hierarchical timeout
coordination, structured operation logging, and a lightweight operation-metric
shim.

**These are plain helper classes, not decorators or ASGI middleware.** You
construct them at module scope and call them explicitly inside the handler.
There is no `@retry`, `@timeout`, `@fallback`, or `@throttle` in this package,
no retry-policy classes, and no fallback handlers. Retries and backoff, where
needed, live with the relevant client or adapter; this package breaks, times
out, logs, and tracks status.

```python
from robosystems.middleware.robustness import (
    CircuitBreakerManager,
    TimeoutCoordinator,
    OperationStatus,
    OperationType,
    get_operation_logger,
    record_operation_metric,
)
```

## Circuit breaker (`circuit_breaker.py`)

`CircuitBreakerManager` tracks state per `(graph_id, operation)` key and blocks
requests to a graph/operation that has been failing repeatedly, giving it room
to recover.

```python
CircuitBreakerManager(
    failure_threshold: int | None = None,   # default: TuningConfig.get_circuit_breaker_threshold() → 5
    recovery_timeout: int | None = None,    # default: TuningConfig.get_circuit_breaker_timeout() → 60s
    half_open_max_calls: int = 1,           # probes admitted once the recovery window passes
)
```

Omitted arguments resolve through `TuningConfig`, which is SSM-tunable at
runtime (`circuits/THRESHOLD`, `circuits/TIMEOUT`) — you can widen a breaker in
production without a redeploy.

**Construct it once, at module scope.** State lives in the instance, in
process memory; a breaker built per request starts at zero every time and can
never trip.

States (`CircuitState.state`):

- **closed** — everything is admitted.
- **open** — nothing is admitted until `recovery_timeout` has passed since the
  last failure.
- **half_open** — `half_open_max_calls` probes are admitted; everyone else still
  gets the 503. A probe's success closes and forgets the circuit; its failure
  reopens it for a full recovery window. Probes that never report back are
  re-armed after another window, so a lost probe cannot wedge the circuit.

Methods:

- `check_circuit(graph_id, operation) -> bool` — call before the operation;
  raises `CircuitOpenError` (an `HTTPException(503)` with `Retry-After`) if the
  circuit is not admitting.
- `record_success(graph_id, operation)` — closes the circuit and forgets it.
  Only circuits with a recorded failure are stored, so a caller-supplied
  operation name cannot grow the map, and a healthy operation has no entry.
- `record_failure(graph_id, operation, error=None)` — increments the failure
  count and opens the circuit at the threshold. **Pass `error`**: only
  infrastructure failures count. A `GraphClientError` (bad Cypher), an
  `HTTPException` below 500, and the breaker's own `CircuitOpenError` are
  ignored — the last so that polling an open circuit cannot hold it open.
- `get_circuit_status(...)` / `get_all_circuit_status()` — status snapshots for
  monitoring.

```python
from robosystems.middleware.robustness import CircuitBreakerManager

circuit_breaker = CircuitBreakerManager()

circuit_breaker.check_circuit(graph_id, "analytics_metrics")  # raises 503 if open

try:
    result = await run_operation()
    circuit_breaker.record_success(graph_id, "analytics_metrics")
except Exception as e:
    circuit_breaker.record_failure(graph_id, "analytics_metrics", error=e)
    raise
```

## Timeout coordinator (`timeout_coordinator.py`)

`TimeoutCoordinator` holds per-operation `TimeoutConfiguration`s with a
decreasing budget across four layers — `endpoint_timeout > queue_timeout >
tool_timeout > instance_timeout` — so each layer leaves headroom for the one
above it and an inner timeout fires before its outer wrapper gives up. Getting
that ordering backwards produces the confusing failure where the client sees a
generic gateway timeout instead of the specific inner error;
`validate_timeout_hierarchy()` exists to catch it.

Defaults are defined for `cypher_query`, `read-graph-cypher`,
`get-graph-schema`, `get-graph-info`, and a `default` fallback.

Methods:

- `get_timeout_config(tool_name) -> TimeoutConfiguration`
- `get_endpoint_timeout(tool_name)` / `get_queue_timeout(...)` /
  `get_tool_timeout(...)` / `get_instance_timeout(...)`
- `validate_timeout_hierarchy(tool_name) -> bool`
- `get_timeout_summary(tool_name) -> dict`
- `calculate_timeout(operation_type, complexity_factors=None) -> float` —
  maps an operation type to a config and scales the endpoint timeout by
  complexity (row `limit`, `has_search`, `fields_count`), capped at 3x.

```python
from robosystems.middleware.robustness import TimeoutCoordinator

timeout_coordinator = TimeoutCoordinator()

operation_timeout = timeout_coordinator.calculate_timeout(
    operation_type="analytics_query",
    complexity_factors={"limit": 5000, "has_search": True},
)

metrics = await asyncio.wait_for(
    collect_metrics(graph_id),
    timeout=operation_timeout,
)
```

## Operation logger (`operation_logging.py`)

`OperationLogger` provides structured operation logging with correlation IDs,
slow-operation detection, and an audit trail. Get the shared singleton via
`get_operation_logger()`.

Entries are `OperationLogEntry` dataclasses categorized by
`OperationLogEventType` — `OPERATION_START`, `OPERATION_SUCCESS`,
`OPERATION_FAILURE`, `CIRCUIT_BREAKER_OPEN`, `EXTERNAL_SERVICE_CALL`,
`CREDIT_OPERATION`, `PERFORMANCE_ALERT`, and others.

**The buffer is per-process and in-memory** — bounded and thread-safe
(`max_log_entries`, default 10,000), so `get_recent_logs()` on a multi-task
deployment returns only what that task saw, and everything is lost on restart.
Entries are also written through the standard `logger`, which is what actually
reaches CloudWatch. Use the buffer for live debugging, not as a durable record.

Methods:

- `log_operation_start(...) -> operation_id`
- `log_operation_success(operation_id, result_metadata=None)`
- `log_operation_failure(operation_id, error, error_metadata=None)`
- `operation_context(operation, endpoint, ...)` — context manager that logs
  start, then success or failure automatically
- `log_external_service_call(...)`, `log_circuit_breaker_event(...)`,
  `log_resource_event(...)`, `log_credit_operation(...)`
- `get_recent_logs(endpoint=None, graph_id=None, event_type=None, ...)` —
  query the in-memory buffer

```python
from robosystems.middleware.robustness import get_operation_logger

operation_logger = get_operation_logger()

# Explicit start/finish
op_id = operation_logger.log_operation_start(
    operation="analytics_query",
    endpoint="/v1/graphs/{graph_id}/metrics",
    graph_id=graph_id,
    user_id=user_id,
)
try:
    result = await run_operation()
    operation_logger.log_operation_success(op_id)
except Exception as e:
    operation_logger.log_operation_failure(op_id, e)
    raise

# Or the context manager equivalent
with operation_logger.operation_context(
    "entity_create", endpoint, graph_id=graph_id, user_id=user_id
) as op_id:
    result = await some_operation()  # success/failure logged automatically
```

## Operation metrics (`operation_metrics.py`)

**This module does not emit metrics.** The real metrics pipeline — request
duration, errors, business events, query queue, SSE — is
[`../otel/`](../otel/README.md). Two things live here:

1. **Circuit-breaker status tracking** — `CircuitBreakerMetrics` (via
   `get_operation_metrics_collector()`) stores the latest circuit state per
   `(graph_id, operation)`. `CircuitBreakerManager` updates it internally; you
   normally don't call it directly.
2. **`record_operation_metric(...)`** — a logging shim. It logs a warning for
   any non-`SUCCESS` status or any operation slower than 10 s, and is otherwise
   a no-op. If you need a metric on a dashboard, use the OTel helpers instead;
   calling this and then looking for a Prometheus series is the mistake.

`OperationType` and `OperationStatus` are enums for the operation category and
outcome (`OperationType.ANALYTICS_QUERY`; `OperationStatus.SUCCESS` /
`FAILURE` / `TIMEOUT` / `CIRCUIT_OPEN` / `INSUFFICIENT_CREDITS`).

```python
from robosystems.middleware.robustness import (
    record_operation_metric,
    OperationType,
    OperationStatus,
)

record_operation_metric(
    operation_type=OperationType.ANALYTICS_QUERY,
    status=OperationStatus.SUCCESS,
    duration_ms=operation_duration_ms,
    endpoint="/v1/graphs/{graph_id}/metrics",
    graph_id=graph_id,
    user_id=user_id,
    operation_name="get_graph_metrics",
)
```

## Putting it together

The graph routers (`routers/graphs/usage.py`, `health.py`, `limits.py`,
`info.py`, `query/sql.py`, `mcp/handlers.py`) construct these helpers at module
scope and wire them together inside the handler: check the circuit, compute a
timeout, run under `asyncio.wait_for`, then record success or failure on the
breaker and emit logs.

```python
circuit_breaker = CircuitBreakerManager()
timeout_coordinator = TimeoutCoordinator()
operation_logger = get_operation_logger()

async def handler(graph_id, user_id):
    start = time.time()
    try:
        circuit_breaker.check_circuit(graph_id, "analytics_metrics")
        timeout = timeout_coordinator.calculate_timeout("analytics_query")
        result = await asyncio.wait_for(run_operation(graph_id), timeout=timeout)
        circuit_breaker.record_success(graph_id, "analytics_metrics")
        record_operation_metric(
            operation_type=OperationType.ANALYTICS_QUERY,
            status=OperationStatus.SUCCESS,
            duration_ms=(time.time() - start) * 1000,
            endpoint="/v1/graphs/{graph_id}/metrics",
            graph_id=graph_id,
            user_id=user_id,
        )
        return result
    except Exception as e:
        circuit_breaker.record_failure(graph_id, "analytics_metrics", error=e)
        raise
```

## Configuration

Only the circuit breaker is runtime-tunable. Its defaults come from
`TuningConfig`, backed by SSM, so they change without a redeploy:

| Setting              | Accessor                          | SSM key              | Default |
| -------------------- | --------------------------------- | -------------------- | ------- |
| Failure threshold    | `get_circuit_breaker_threshold()` | `circuits/THRESHOLD` | 5       |
| Recovery timeout (s) | `get_circuit_breaker_timeout()`   | `circuits/TIMEOUT`   | 60      |

```bash
just ssm-get prod tuning/circuits/THRESHOLD
just ssm-set prod tuning/circuits/THRESHOLD 8
```

Timeout budgets are defined in code, per operation type, inside
`TimeoutCoordinator`. The operation logger's thresholds
(`slow_operation_threshold_ms`, `max_log_entries`) are constructor arguments on
`OperationLogger`. Changing either means a code change.

## Related

- [`../otel/README.md`](../otel/README.md) — the metrics and tracing surface
- [`../graph/README.md`](../graph/README.md) — the routers that use these helpers
