# OpenTelemetry Middleware

This middleware provides comprehensive observability through distributed tracing and metrics collection using OpenTelemetry and AWS Distro for OpenTelemetry (ADOT).

## Overview

The OpenTelemetry middleware:

- Collects and exports metrics and traces to Amazon Managed Prometheus and AWS X-Ray
- Provides automatic instrumentation for FastAPI, and database operations
- Enables custom business metrics and event tracking
- Supports both local development and production environments
- Integrates with Amazon Managed Grafana for visualization

## Architecture

```
otel/
├── __init__.py              # Module exports
├── setup.py                 # OpenTelemetry initialization
├── metrics.py               # Centralized metrics utilities
├── config/                  # Configuration files
│   ├── otel-collector-config.yaml   # Local OTEL Collector config
│   ├── adot-collector-config.yaml   # AWS ADOT Collector config
│   ├── prometheus.yaml              # Prometheus scrape config
│   ├── datasources/                 # Grafana datasources
│   └── dashboards/                  # Grafana dashboards
└── README.md                # This documentation
```

## Key Components

### 1. Setup (`setup.py`)

Initializes OpenTelemetry providers and auto-instrumentation.

**Features:**

- **Environment-Aware**: Automatically enables in staging/prod
- **OTLP Exporters**: Sends metrics and traces to collectors
- **Auto-Instrumentation**: FastAPI, requests, psycopg2
- **Graceful Degradation**: Continues if collectors unavailable

**Usage:**

```python
from robosystems.middleware.otel.setup import setup_opentelemetry

# Called during application startup
setup_opentelemetry(
    service_name="robosystems-api",
    service_version="1.0.0"
)
```

### 2. Metrics Collection (`metrics.py`)

Provides utilities for consistent metrics collection.

**Core Classes:**

- `EndpointMetrics`: Singleton managing metric instruments
- `endpoint_metrics_decorator`: Automatic metrics decorator
- `endpoint_metrics_context`: Context manager for metrics

**Key Functions:**

- `record_request_metrics()`: HTTP request metrics
- `record_auth_metrics()`: Authentication metrics
- `record_error_metrics()`: Error tracking
- `record_query_queue_metrics()`: Query queue metrics

## Metrics Reference

### Request Metrics

```
robosystems_api_requests_total{endpoint, method, status_code, status_class, user_authenticated}
  - Type: Counter
  - Description: Total API requests

robosystems_api_request_duration_seconds{endpoint, method, status_code, status_class, user_authenticated}
  - Type: Histogram
  - Description: Request duration in seconds
  - Buckets: 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
```

### Authentication Metrics

```
robosystems_auth_attempts_total{endpoint, method, auth_type}
  - Type: Counter
  - Auth Types: email_password_login, jwt_token, api_key, sso_token_exchange

robosystems_auth_failures_total{endpoint, method, auth_type, failure_reason}
  - Type: Counter
  - Failure Reasons: user_not_found_or_inactive, invalid_password, invalid_token
```

### Error Metrics

```
robosystems_api_errors_total{endpoint, method, error_type, error_code, user_authenticated}
  - Type: Counter
  - Description: API errors by type and code
```

### Business Event Metrics

```
robosystems_business_events_total{endpoint, method, event_type, event_*, user_authenticated}
  - Type: Counter
  - Event Types: user_registered, user_login, entity_created, graph_created
```

### Query Queue Metrics

```
robosystems_query_queue_size{priority}
  - Type: Gauge
  - Description: Current queue size

robosystems_query_wait_time_seconds{graph_id, user_id, priority}
  - Type: Histogram
  - Description: Queue wait time
  - Buckets: 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0

robosystems_query_execution_time_seconds{graph_id, user_id, status, error_type}
  - Type: Histogram
  - Description: Query execution time
```

## Usage Patterns

### 1. Using the Decorator (Recommended)

```python
from robosystems.middleware.otel.metrics import endpoint_metrics_decorator

@router.post("/api/endpoint")
@endpoint_metrics_decorator(
    endpoint="/v1/api/endpoint",
    business_event_type="custom_event"  # Optional
)
async def my_endpoint(request: MyRequest):
    # Metrics are automatically collected
    result = await process_request(request)
    return result
```

### 2. Using Context Manager

```python
from robosystems.middleware.otel.metrics import endpoint_metrics_context

async def my_endpoint(request: MyRequest):
    with endpoint_metrics_context(
        endpoint="/v1/api/endpoint",
        method="POST",
        user_id=request.user_id
    ) as ctx:
        result = await some_operation()

        # Record custom business events
        ctx.record_business_event(
            event_type="operation_completed",
            event_data={"items_processed": 42}
        )

        return result
```

### 3. Authentication Metrics

```python
from robosystems.middleware.otel.metrics import record_auth_metrics

@router.post("/login")
async def login(credentials: LoginRequest):
    # Record auth attempt
    record_auth_metrics(
        endpoint="/v1/auth/login",
        method="POST",
        auth_type="email_password_login",
        success=False
    )

    try:
        user = await authenticate_user(credentials)

        # Record successful auth
        record_auth_metrics(
            endpoint="/v1/auth/login",
            method="POST",
            auth_type="email_password_login",
            success=True,
            user_id=user.id
        )

        return {"token": create_jwt_token(user)}

    except InvalidCredentialsError:
        # Record failed auth with reason
        record_auth_metrics(
            endpoint="/v1/auth/login",
            method="POST",
            auth_type="email_password_login",
            success=False,
            failure_reason="invalid_password"
        )
        raise
```

### 4. Query Queue Metrics

```python
from robosystems.middleware.otel.metrics import record_query_queue_metrics

# Record submission
record_query_queue_metrics(
    metric_type="submission",
    graph_id=graph_id,
    user_id=user_id,
    priority=priority,
    success=True,
)

# Record wait time
record_query_queue_metrics(
    metric_type="wait_time",
    graph_id=query.graph_id,
    user_id=query.user_id,
    priority=query.priority,
    wait_time_seconds=wait_time,
)

# Record execution
record_query_queue_metrics(
    metric_type="execution",
    graph_id=query.graph_id,
    user_id=query.user_id,
    execution_time_seconds=execution_time,
    status="completed",
    error_type=None,
)
```

## Configuration

### Environment Variables

```bash
# Core OTEL Configuration
OTEL_SERVICE_NAME=robosystems-api-prod
OTEL_SERVICE_VERSION=1.0.0
OTEL_RESOURCE_ATTRIBUTES=deployment.environment=prod,service.namespace=robosystems
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf

# Environment Control
ENVIRONMENT=prod                      # prod/staging enables OTEL
OTEL_FORCE_ENABLE=false              # Force enable in dev

# Auto-instrumentation
OTEL_PYTHON_LOGGING_AUTO_INSTRUMENTATION_ENABLED=true
OTEL_PYTHON_FASTAPI_EXCLUDED_URLS=/health,/metrics

# AWS Integration (Production)
AWS_PROMETHEUS_ENDPOINT=https://aps-workspaces.us-east-1.amazonaws.com/workspaces/ws-xxxx/
AWS_REGION=us-east-1

# Performance Tuning
OTEL_BSP_MAX_QUEUE_SIZE=2048
OTEL_BSP_MAX_EXPORT_BATCH_SIZE=512
OTEL_BSP_EXPORT_TIMEOUT_MILLIS=30000
OTEL_METRIC_EXPORT_INTERVAL_MILLIS=60000
```

### ADOT Collector Configuration

The ADOT collector runs as a sidecar container in ECS:

```yaml
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 1s
    send_batch_size: 50
  resource:
    attributes:
      - key: service.name
        value: ${OTEL_SERVICE_NAME}
        action: upsert

exporters:
  prometheusremotewrite:
    endpoint: ${AWS_PROMETHEUS_ENDPOINT}api/v1/remote_write
    auth:
      authenticator: sigv4auth
  awsxray:
    region: ${AWS_REGION}

service:
  pipelines:
    metrics:
      receivers: [otlp]
      processors: [batch, resource]
      exporters: [prometheusremotewrite]
    traces:
      receivers: [otlp]
      processors: [batch, resource]
      exporters: [awsxray]
```

## Deployment

### Local Development

```bash
# Start observability stack
docker compose --profile observability up -d

# Enable OTEL in development
export OTEL_FORCE_ENABLE=true
export OTEL_SERVICE_NAME=robosystems-api-dev

# Access monitoring tools
# - Grafana: http://localhost:4000 (admin/password)
# - Prometheus: http://localhost:9090
# - OTEL Collector: http://localhost:8889/metrics
```

### Production Deployment

1. **Infrastructure**: Deploy via CloudFormation

   ```bash
   gh workflow run deploy-observability.yml
   ```

2. **ECS Integration**: ADOT collector included as sidecar

3. **Access Grafana**: https://grafana.robosystems.ai (AWS SSO)

## Monitoring & Dashboards

### Key Metrics to Monitor

1. **API Health**

   - Request rate by endpoint
   - Error rate (4xx and 5xx)
   - P50/P90/P99 latency
   - Authentication success rate

2. **Business Metrics**

   - User registrations per hour
   - Active users
   - API key usage
   - Resource creation rates

3. **Infrastructure**

   - ECS task CPU/memory
   - Database connections
   - Cache hit rates
   - Queue lengths

4. **Query Queue**
   - Queue size and utilization
   - Wait time percentiles
   - Execution time by status
   - Per-user patterns

### Grafana Queries

```promql
# Request Rate
sum(rate(robosystems_api_requests_total[5m])) by (endpoint)

# Error Rate
sum(rate(robosystems_api_requests_total{status_class=~"4xx|5xx"}[5m])) by (endpoint)
/ sum(rate(robosystems_api_requests_total[5m])) by (endpoint)

# P95 Latency
histogram_quantile(0.95,
  sum(rate(robosystems_api_request_duration_seconds_bucket[5m])) by (endpoint, le)
)

# Authentication Success Rate
sum(rate(robosystems_auth_attempts_total[5m])) by (auth_type)
- sum(rate(robosystems_auth_failures_total[5m])) by (auth_type)
```

### Alerts Configuration

```yaml
groups:
  - name: robosystems_api
    rules:
      - alert: HighErrorRate
        expr: |
          sum(rate(robosystems_api_requests_total{status_class="5xx"}[5m])) by (endpoint)
          / sum(rate(robosystems_api_requests_total[5m])) by (endpoint)
          > 0.05
        for: 5m
        labels:
          severity: critical

      - alert: HighLatency
        expr: |
          histogram_quantile(0.95,
            sum(rate(robosystems_api_request_duration_seconds_bucket[5m])) by (endpoint, le)
          ) > 5
        for: 5m
        labels:
          severity: warning
```

## Best Practices

1. **Use Decorators**: For standard request metrics
2. **Record Business Events**: For important operations
3. **Include User Context**: When available
4. **Consistent Naming**: For endpoints and events
5. **Avoid High Cardinality**: No UUIDs or timestamps in labels
6. **Test Locally**: Use docker compose before deploying
7. **Monitor Performance**: Track overhead in production
8. **Set Up Alerts**: For critical business metrics

## Troubleshooting

### Common Issues

1. **Metrics Not Appearing**

   - Check ENVIRONMENT is staging/prod
   - Verify collector connectivity
   - Review logs for errors

2. **Missing Labels**

   - Ensure all parameters passed
   - Check user context included

3. **High Memory Usage**

   - Adjust batch processor settings
   - Reduce queue sizes

4. **Decorator Not Working**
   - Must be after router decorator
   - Check proper import

### Debug Mode

```python
import logging
logging.getLogger("opentelemetry").setLevel(logging.DEBUG)
```

### Performance Impact

- Request metrics: < 0.1ms per request
- Business events: < 0.05ms per event
- Memory: ~50MB for collector
- CPU: < 1% under normal load

## Security Considerations

1. **Data Privacy**: Don't include PII in metrics
2. **Label Cardinality**: Avoid unbounded labels
3. **Access Control**: Grafana uses AWS SSO
4. **Network Security**: ADOT uses IAM authentication
5. **Metric Retention**: 90 days in Prometheus
