# Robustness Middleware

This middleware provides a small set of resilience and operational-visibility
primitives used across the graph endpoints: a per-graph/per-operation circuit
breaker, hierarchical timeout coordination, structured operation logging, and a
lightweight operation-metric shim. These are plain helper classes and functions
— they are constructed and called explicitly inside endpoint handlers, not
applied as decorators.

## Overview

The robustness middleware:

- Tracks failures per `(graph_id, operation)` and short-circuits requests to a
  failing graph (circuit breaker)
- Provides coordinated, layered timeout budgets so endpoint, queue, tool, and
  instance timeouts don't conflict
- Emits structured, in-memory operation logs for debugging and audit
- Records notable operation outcomes (failures / slow operations), delegating
  the primary metrics pipeline to OpenTelemetry (`middleware/otel/metrics.py`)

## Architecture

```
robustness/
├── __init__.py              # Public exports
├── circuit_breaker.py       # CircuitBreakerManager + CircuitState
├── timeout_coordinator.py   # TimeoutCoordinator + TimeoutConfiguration
├── operation_logging.py     # OperationLogger + get_operation_logger()
└── operation_metrics.py     # record_operation_metric + OperationType/OperationStatus
```

Public exports (`__init__.py`):

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

## Key Components

### 1. Circuit Breaker (`circuit_breaker.py`)

`CircuitBreakerManager` tracks state per `(graph_id, operation)` key and blocks
requests to a graph/operation that has been failing repeatedly, allowing it to
recover.

**State (`CircuitState` dataclass):** `failure_count`, `last_failure_time`,
`is_open`, `last_success_time`. There is no explicit "half-open" object — once
`recovery_timeout` elapses, the next `check_circuit` call resets the circuit and
lets a request through (a one-shot recovery probe).

**Constructor:**

```python
CircuitBreakerManager(
    failure_threshold: int | None = None,   # default: TuningConfig.get_circuit_breaker_threshold()
    recovery_timeout: int | None = None,     # default: TuningConfig.get_circuit_breaker_timeout()
    half_open_max_calls: int = 3,
)
```

When `failure_threshold` / `recovery_timeout` are omitted they are read from
`TuningConfig` (SSM-tunable at runtime via `circuits/THRESHOLD` and
`circuits/TIMEOUT`).

**Key methods:**

- `check_circuit(graph_id, operation) -> bool` — call before the operation;
  raises `HTTPException(503)` with a `Retry-After` header if the circuit is open.
- `record_success(graph_id, operation)` — resets the failure count and closes
  the circuit.
- `record_failure(graph_id, operation, error=None)` — increments the failure
  count and opens the circuit at the threshold. **Client errors are ignored:**
  if `error` is a `GraphClientError` (e.g. bad Cypher syntax) it does not count
  toward tripping the breaker.
- `get_circuit_status(...)` / `get_all_circuit_status()` — status snapshots for
  monitoring.

**Usage (the pattern used in the graph routers):**

```python
from robosystems.middleware.robustness import CircuitBreakerManager

circuit_breaker = CircuitBreakerManager()

# Before the operation — raises HTTP 503 if the circuit is open
circuit_breaker.check_circuit(graph_id, "analytics_metrics")

try:
    result = await run_operation()
    circuit_breaker.record_success(graph_id, "analytics_metrics")
except Exception as e:
    circuit_breaker.record_failure(graph_id, "analytics_metrics", error=e)
    raise
```

### 2. Timeout Coordinator (`timeout_coordinator.py`)

`TimeoutCoordinator` holds per-operation `TimeoutConfiguration`s with a
decreasing budget across four layers (`endpoint_timeout > queue_timeout >
tool_timeout > instance_timeout`) so each layer leaves headroom for the one
above it. Defaults are defined for `cypher_query`, `read-graph-cypher`,
`get-graph-schema`, `get-graph-info`, and a `default` fallback.

**Key methods:**

- `get_timeout_config(tool_name) -> TimeoutConfiguration`
- `get_endpoint_timeout(tool_name)` / `get_queue_timeout(...)` /
  `get_tool_timeout(...)` / `get_instance_timeout(...)`
- `validate_timeout_hierarchy(tool_name) -> bool`
- `get_timeout_summary(tool_name) -> dict`
- `calculate_timeout(operation_type, complexity_factors=None) -> float` —
  maps an operation type to a config and scales the endpoint timeout by
  complexity (row `limit`, `has_search`, `fields_count`), capped at 3x.

**Usage:**

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

### 3. Operation Logger (`operation_logging.py`)

`OperationLogger` provides structured, in-memory operation logging with
correlation IDs, slow-operation detection, and an audit trail. Retrieve the
shared singleton via `get_operation_logger()`.

Log entries are `OperationLogEntry` dataclasses categorized by
`OperationLogEventType` (e.g. `OPERATION_START`, `OPERATION_SUCCESS`,
`OPERATION_FAILURE`, `CIRCUIT_BREAKER_OPEN`, `EXTERNAL_SERVICE_CALL`,
`CREDIT_OPERATION`, `PERFORMANCE_ALERT`). Entries are kept in a bounded,
thread-safe in-memory buffer (`max_log_entries`, default 10,000) and also
written through the standard `logger`.

**Key methods:**

- `log_operation_start(...) -> operation_id`
- `log_operation_success(operation_id, result_metadata=None)`
- `log_operation_failure(operation_id, error, error_metadata=None)`
- `operation_context(operation, endpoint, ...)` — context manager that logs
  start, then success or failure automatically
- `log_external_service_call(...)`, `log_circuit_breaker_event(...)`,
  `log_resource_event(...)`, `log_credit_operation(...)`
- `get_recent_logs(endpoint=None, graph_id=None, event_type=None, ...)` —
  query the in-memory buffer

**Usage:**

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

### 4. Operation Metrics (`operation_metrics.py`)

A lightweight shim. The primary metrics pipeline (request duration, errors,
business events, query queue, SSE) lives in `middleware/otel/metrics.py`. This
module is retained for two purposes:

1. **Circuit-breaker status tracking** — `CircuitBreakerMetrics` (accessed via
   `get_operation_metrics_collector()`) stores the latest circuit state per
   `(graph_id, operation)`. `CircuitBreakerManager` updates it internally; you
   normally don't call it directly.
2. **`record_operation_metric(...)`** — a backward-compatible logging shim. It
   logs a warning for any non-`SUCCESS` status or any operation slower than
   10s; otherwise it is a no-op. It does **not** emit OTel metrics.

`OperationType` and `OperationStatus` are enums describing the operation
category and outcome (e.g. `OperationType.ANALYTICS_QUERY`,
`OperationStatus.SUCCESS` / `FAILURE` / `TIMEOUT` / `CIRCUIT_OPEN` /
`INSUFFICIENT_CREDITS`).

**Usage:**

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

## Putting It Together

The graph routers (`routers/graphs/usage.py`, `health.py`, `limits.py`,
`info.py`, `query/sql.py`, `mcp/handlers.py`) construct these helpers at
module scope and wire them together inside the handler: check the circuit,
compute a timeout, run under `asyncio.wait_for`, then record success/failure on
the breaker and emit logs/metrics.

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

The circuit breaker reads its defaults from `TuningConfig` (SSM-tunable at
runtime, no redeploy required):

| Setting              | TuningConfig accessor               | SSM key             |
| -------------------- | ----------------------------------- | ------------------- |
| Failure threshold    | `get_circuit_breaker_threshold()`   | `circuits/THRESHOLD`|
| Recovery timeout (s) | `get_circuit_breaker_timeout()`     | `circuits/TIMEOUT`  |

Timeout budgets are defined in code in `TimeoutCoordinator` (per operation
type); the operation logger's thresholds (`slow_operation_threshold_ms`,
`max_log_entries`) are constructor arguments on `OperationLogger`.

## Notes

- These helpers are constructed explicitly in handlers; there are no
  decorators (`@retry` / `@timeout` / `@fallback` / `@throttle`), no retry-policy
  classes, and no fallback handlers in this module.
- Retries/backoff, if needed, live with the relevant client/adapter; this
  module focuses on breaking, timing out, logging, and status tracking.
- The primary metrics surface is OpenTelemetry — see
  `middleware/otel/README.md`.
