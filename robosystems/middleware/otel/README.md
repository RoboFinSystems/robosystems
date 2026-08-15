# OpenTelemetry Middleware

Instrumentation for the API: OTLP metrics (and optionally traces) exported to a
collector, plus a set of helpers for recording request, auth, error, business,
and query-queue metrics from application code. `setup.py` wires the OTel SDK to
FastAPI; `metrics.py` holds every instrument the platform defines.

In production the API task runs an ADOT collector sidecar that remote-writes to
Amazon Managed Prometheus. **Only metrics are exported.** Span export is behind
`OTEL_TRACES_ENABLED` (off), and the deployed collector defines no traces
pipeline, so nothing lands in a traces backend today.

## Turning it on

`OTEL_ENABLED` gates everything. It is `false` unless explicitly set (env var,
or the `OTEL_ENABLED` SSM parameter). When it is false, `setup_telemetry()`
logs and returns — no instruments, no exporters, no overhead.

| Variable                      | Default                 | Effect                                                        |
| ----------------------------- | ----------------------- | ------------------------------------------------------------- |
| `OTEL_ENABLED`                | `false`                 | Master switch. Nothing is instrumented while false.            |
| `OTEL_TRACES_ENABLED`         | `false`                 | Span export. Off until the collector carries a traces pipeline. |
| `OTEL_SERVICE_NAME`           | `robosystems`           | `service.name` resource attribute.                             |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | Port `4317` is rewritten to `4318`; the exporters are HTTP.    |
| `OTEL_RESOURCE_ATTRIBUTES`    | `""`                    | `key=value,key=value`; merged over the built-in attributes.    |
| `OTEL_CONSOLE_EXPORT`         | `false`                 | Dev only — dumps spans to stdout.                              |

**`OTEL_TRACES_ENABLED` is a real switch as of 2026-08-14** (env var or the
`OTEL_TRACES_ENABLED` SSM parameter; seeded `false` by `bin/setup/aws.sh`).
Before that it was read via `getattr(env, ...)` against an attribute
`config/env.py` never defined, so it read as configurable and was permanently
off. It now gates only the OTLP *span* exporter; metrics never depended on it.
Turning it on is not enough on its own — the deployed ADOT collector has no
traces pipeline and there is no traces backend yet, so spans would be exported
to a collector that drops them. The plan for both is
`local/RoboSystems/specs/security/observability-tracing.md`.

A tracer provider and span processors are still installed whenever
`OTEL_ENABLED` is true, so spans are produced and redacted — they just have
nowhere to go, except in development, where `OTEL_CONSOLE_EXPORT=true` still
attaches the console exporter.

The service version is read from the installed `robosystems` package metadata,
not from an env var. `service.instance.id` is set to the hostname (the container
ID on Fargate) — see "Gotchas" for why that matters.

Local stack:

```bash
docker compose --profile observability up -d
# Grafana        http://localhost:4000
# Prometheus     http://localhost:9090
# OTel collector http://localhost:8889/metrics
```

The dev path is otherwise a no-op: with the default `localhost:4318` endpoint
and `ENVIRONMENT=dev`, the exporters are skipped even if `OTEL_ENABLED=true`,
so run the `observability` profile to see anything.

## Setup

```python
from robosystems.middleware.otel import setup_telemetry, shutdown_telemetry

setup_telemetry(app)   # at startup; takes only the FastAPI app
shutdown_telemetry()   # at shutdown; flushes providers
```

`setup_telemetry` instruments FastAPI (with `excluded_urls="status,health"`),
`requests`, and `psycopg2`, then installs the metric views and the span
processors (the redaction processor always; an exporter only in the dev console
case, per above). Every step is wrapped in `try/except`: a missing or
unreachable collector degrades to no telemetry rather than a failed boot.

## Recording metrics

Three entry points, all from `robosystems.middleware.otel.metrics`.

**Decorator** — for FastAPI routes. Note the first parameter is
`endpoint_name`, and always pass `method` explicitly:

```python
@router.post("/login")
@endpoint_metrics_decorator(
    "/v1/auth/login",
    method="POST",
    business_event_type="user_login",
)
async def login_endpoint(request: LoginRequest):
    return await do_login(request)
```

Without `method`, the decorator falls back to scanning the function signature
for a `fastapi.Request` and labels the metric `"UNKNOWN"` when it finds none —
which silently breaks any dashboard filtering by method. If the handler returns
an object with a truthy `idempotent_replay` attribute, the business-event
counter is suppressed for that call (request count and latency still fire), so
"how many times did this actually execute" stays meaningful.

**Context manager** — for finer-grained or non-route code:

```python
with endpoint_metrics_context("/v1/auth/login", "POST", user_id=user_id) as ctx:
    result = await some_operation()
    ctx.record_business_event("user_authenticated", {"success": True})
```

**Direct calls** — `record_request_metrics`, `record_auth_metrics`,
`record_error_metrics`, `record_query_queue_metrics`. The last one dispatches on
a `metric_type` string (`"submission"`, `"wait_time"`, `"execution"`,
`"concurrent_update"`) with the remaining fields passed as keyword arguments:

```python
record_query_queue_metrics(
    metric_type="execution",
    graph_id=graph_id,
    user_id=user_id,
    execution_time_seconds=elapsed,
    status="completed",
)
```

`record_request_metrics` drops `GET /v1/status` and `GET /status` outright — the
load-balancer health check would otherwise dominate the series.

## Instruments

All instrument names are prefixed `robosystems_`. The full set is defined in
`EndpointMetrics._ensure_instruments()` in `metrics.py`; the families are:

| Family        | Instruments                                                                                                  |
| ------------- | ------------------------------------------------------------------------------------------------------------ |
| Requests      | `api_requests_total`, `api_request_duration_seconds`, `api_errors_total`                                       |
| Auth          | `auth_attempts_total`, `auth_failures_total`                                                                   |
| Business      | `business_events_total`                                                                                        |
| Graph content | `graph_nodes_total`, `graph_relationships_total`, `graph_size_bytes`                                           |
| Query queue   | `query_queue_size`, `query_submissions_total`, `query_queue_rejections_total`, `query_wait_time_seconds`, `query_execution_time_seconds`, `query_concurrent_executions`, `query_completions_total`, `query_user_limit_rejections_total` |
| SSE           | `sse_connections_active`, `sse_connections_{opened,closed,rejected}_total`, `sse_events_{emitted,failed}_total`, `sse_redis_circuit_breaker_opens_total`, `sse_connection_queue_overflows_total` |
| Graph API     | `graph_api_requests_total`, `graph_api_errors_total`, `graph_api_duration_seconds`                              |
| Other         | `rate_limit_rejections_total`, `credits_consumed_total`, `db_pool_connections`                                  |

Histogram buckets are set through `get_metric_views()`: `API_DURATION_BUCKETS`
(5ms–10s, fifteen buckets) for request duration, `QUERY_DURATION_BUCKETS`
(10ms–60s) for queue wait and execution, `GRAPH_API_DURATION_BUCKETS`
(5ms–300s, covering fast reads through ingestion and backup) for Graph API
proxy calls.

## Gotchas

**Cardinality is a cost line, not a style preference.** Amazon Managed
Prometheus bills by sample. Two guards already exist in code and should not be
removed casually:

- `get_metric_views()` restricts the auto-generated `http.server.*` instruments
  to a server-controlled attribute allowlist. FastAPIInstrumentor otherwise
  records the client-supplied `Host` header, and internet scanners hitting the
  public ALB with junk hostnames mint ~48 new series apiece.
- `_sanitize_endpoint()` normalizes endpoint labels. Never put a UUID,
  timestamp, or raw user input in a label.

**`service.instance.id` must stay unique per process.** It defaults to the
hostname. If every ECS task exports its cumulative counters into one shared
series, `rate()` reads the interleaved per-task samples as constant counter
resets and inflates volume metrics by a large factor. Setting
`service.instance.id` in `OTEL_RESOURCE_ATTRIBUTES` overrides the default —
only do that with something genuinely per-process.

**Query strings are redacted at span start.** `QueryParamRedactionSpanProcessor`
rewrites `http.url`, `url.full`, `http.target`, and `url.query` through the same
redaction list as request logging (`middleware/logging.py`). OTel's built-in
`redact_url` covers only cloud-signature parameters, not `token` or `api_key`,
so credential-bearing query strings would otherwise reach the tracing backend.
Add new credential parameter names to `redact_sensitive_query_params`, not here.

**Health checks are filtered twice** — once in the app (`excluded_urls`, and the
skip inside `record_request_metrics`) and once in the deployed collector (a
`filter/healthcheck` processor dropping `/v1/status`, `/status`, `/health`).
Both layers exist because the auto-instrumentation and the manual helpers
produce different datapoints.

**Decorator order matters.** `@endpoint_metrics_decorator` goes *below*
`@router.post(...)`, so the router registers the wrapped function.

## Deployment

Local collector, Prometheus, and Grafana configs live in `config/`
(`otel-collector-config.yaml`, `prometheus.yaml`, `datasources/`,
`dashboards/`). The production topology is three pieces:

- `cloudformation/prometheus.yaml` — the Managed Prometheus workspace.
- `cloudformation/api.yaml` — the `adot-collector` sidecar on the API task,
  conditional on the observability stack existing. Its config is inlined as
  `AOT_CONFIG_CONTENT`: OTLP in on 4317/4318, a health-check filter, batching,
  and `prometheusremotewrite` with sigv4 auth. Metrics only.
- `cloudformation/grafana.yaml` — the Managed Grafana workspace, deployed by
  `.github/workflows/deploy-grafana.yml`.

## Debugging

```python
import logging
logging.getLogger("opentelemetry").setLevel(logging.DEBUG)
```

Metrics not appearing usually means one of: `OTEL_ENABLED` is false; the
endpoint is still `localhost:4318` in a dev environment (exporters skipped); or
the collector is unreachable and the failure was swallowed by the graceful
degradation path — check the API logs for `Failed to configure OTLP` around
startup. Traces missing is expected, not a misconfiguration: nothing can turn
the OTLP span exporter on today (see "Turning it on").
