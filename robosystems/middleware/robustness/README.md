# Robustness Middleware

This middleware provides circuit breakers and resilience patterns to ensure system stability and graceful degradation under failure conditions.

## Overview

The robustness middleware:
- Implements circuit breaker pattern for external services
- Provides automatic retry with exponential backoff
- Handles timeout and failure scenarios gracefully
- Tracks service health and performance metrics
- Enables graceful degradation when services fail

## Architecture

```
robustness/
├── __init__.py              # Module exports
├── circuit_breaker.py       # Circuit breaker implementation
├── decorators.py            # Decorator-based resilience patterns
├── retry_policies.py        # Retry strategies and policies
├── health_tracker.py        # Service health monitoring
└── fallback_handlers.py     # Fallback response strategies
```

## Key Components

### 1. Circuit Breaker (`circuit_breaker.py`)

Prevents cascading failures by breaking connections to failing services.

**States:**
- **CLOSED**: Normal operation, requests pass through
- **OPEN**: Service failing, requests blocked
- **HALF_OPEN**: Testing if service recovered

**Configuration:**
```python
class CircuitBreakerConfig:
    failure_threshold: int = 5         # Failures before opening
    success_threshold: int = 2         # Successes to close
    timeout: float = 60.0             # Seconds before half-open
    expected_exception: type = Exception
```

**Usage:**
```python
from robosystems.middleware.robustness import CircuitBreaker

# Create circuit breaker for external service
qb_breaker = CircuitBreaker(
    name="quickbooks_api",
    failure_threshold=5,
    timeout=60
)

# Use in service calls
@qb_breaker
async def call_quickbooks_api():
    response = await client.get("/api/v1/entity")
    return response.json()

# Check circuit state
if qb_breaker.state == CircuitState.OPEN:
    logger.warning("QuickBooks API circuit is open")
```

### 2. Resilience Decorators (`decorators.py`)

Decorator-based patterns for common resilience needs.

**Available Decorators:**

#### @retry
Automatic retry with configurable policies:
```python
@retry(
    max_attempts=3,
    backoff_factor=2.0,
    exceptions=(HTTPError, ConnectionError)
)
async def fetch_data():
    return await external_api.get_data()
```

#### @timeout
Enforce operation timeouts:
```python
@timeout(seconds=30)
async def long_operation():
    return await process_large_dataset()
```

#### @fallback
Provide fallback responses on failure:
```python
@fallback(default_value={"status": "unavailable"})
async def get_service_status():
    return await health_check_api()
```

#### @throttle
Rate limit operations:
```python
@throttle(max_calls=100, period=60)  # 100 calls per minute
async def api_endpoint():
    return await process_request()
```

### 3. Retry Policies (`retry_policies.py`)

Sophisticated retry strategies for different scenarios.

**Built-in Policies:**

#### Exponential Backoff
```python
policy = ExponentialBackoff(
    initial_delay=1.0,
    max_delay=60.0,
    factor=2.0,
    jitter=True
)

retrier = Retrier(policy=policy, max_attempts=5)
result = await retrier.execute(operation)
```

#### Linear Backoff
```python
policy = LinearBackoff(
    delay=5.0,
    max_attempts=3
)
```

#### Custom Policy
```python
class CustomRetryPolicy(RetryPolicy):
    def get_delay(self, attempt: int) -> float:
        # Custom logic
        return min(attempt * 2, 30)

    def should_retry(self, exception: Exception) -> bool:
        # Retry only specific errors
        return isinstance(exception, RecoverableError)
```

### 4. Health Tracker (`health_tracker.py`)

Monitors service health and performance.

**Features:**
- **Health Checks**: Periodic service health validation
- **Metric Collection**: Response times, error rates
- **Alerting**: Triggers alerts on degradation
- **Status Reporting**: Provides health dashboards

**Usage:**
```python
tracker = HealthTracker()

# Register service
tracker.register_service(
    name="graph_api",
    health_check=lambda: check_kuzu_health(),
    interval=30  # Check every 30 seconds
)

# Get service status
status = tracker.get_status("graph_api")
print(f"Service health: {status.health_score}%")
print(f"Average latency: {status.avg_latency}ms")
```

### 5. Fallback Handlers (`fallback_handlers.py`)

Strategies for handling failures gracefully.

**Available Strategies:**

#### Cache Fallback
Return cached data when service fails:
```python
@cache_fallback(ttl=300)
async def get_user_data(user_id: str):
    return await user_service.get_user(user_id)
```

#### Default Value Fallback
Return default values on failure:
```python
@default_fallback(value={"balance": 0, "status": "unknown"})
async def get_account_balance(account_id: str):
    return await banking_api.get_balance(account_id)
```

#### Degraded Service Fallback
Provide limited functionality:
```python
@degraded_fallback
async def complex_analysis(data):
    try:
        return await ml_service.analyze(data)
    except ServiceUnavailable:
        # Return simple analysis instead
        return basic_analysis(data)
```

## Integration Patterns

### 1. With External APIs

```python
from robosystems.middleware.robustness import (
    CircuitBreaker, retry, timeout
)

class ExternalAPIClient:
    def __init__(self):
        self.breaker = CircuitBreaker(
            name="external_api",
            failure_threshold=5
        )

    @retry(max_attempts=3)
    @timeout(seconds=10)
    async def call_api(self, endpoint: str):
        async with self.breaker:
            response = await httpx.get(f"https://api.example.com{endpoint}")
            response.raise_for_status()
            return response.json()
```

### 2. With Database Operations

```python
@retry(
    max_attempts=3,
    exceptions=(OperationalError, TimeoutError),
    backoff_factor=1.5
)
async def execute_critical_query(query: str):
    async with get_db_connection() as conn:
        return await conn.execute(query)
```

### 3. With Message Queues

```python
class ResilientQueue:
    @circuit_breaker(failure_threshold=10)
    @retry(max_attempts=5)
    async def publish_message(self, message: dict):
        await self.queue.publish(message)

    @fallback(default_value=[])
    async def consume_messages(self, count: int = 10):
        return await self.queue.consume(count)
```

## Configuration

Environment variables:
```bash
# Circuit Breaker Defaults
CIRCUIT_BREAKER_FAILURE_THRESHOLD=5
CIRCUIT_BREAKER_SUCCESS_THRESHOLD=2
CIRCUIT_BREAKER_TIMEOUT=60
CIRCUIT_BREAKER_HALF_OPEN_REQUESTS=3

# Retry Defaults
RETRY_MAX_ATTEMPTS=3
RETRY_INITIAL_DELAY=1.0
RETRY_MAX_DELAY=60.0
RETRY_BACKOFF_FACTOR=2.0

# Timeout Defaults
DEFAULT_OPERATION_TIMEOUT=30
DEFAULT_API_TIMEOUT=10
DEFAULT_QUERY_TIMEOUT=60

# Health Check Configuration
HEALTH_CHECK_INTERVAL=30
HEALTH_CHECK_TIMEOUT=5
HEALTH_CHECK_FAILURE_THRESHOLD=3
```

## Monitoring & Metrics

### Circuit Breaker Metrics
- State transitions (closed → open → half-open)
- Failure rates and counts
- Success rates after recovery
- Time spent in each state

### Retry Metrics
- Retry attempts per operation
- Success rate after retries
- Total retry delay added
- Retry exhaustion events

### Health Metrics
- Service availability percentage
- Average response times
- Error rates by type
- Health check success rates

## Best Practices

### 1. Choose Appropriate Thresholds
- Set failure thresholds based on service SLAs
- Consider traffic patterns when setting timeouts
- Balance between stability and responsiveness

### 2. Implement Proper Fallbacks
- Always have a fallback strategy
- Ensure fallbacks don't cascade failures
- Test fallback paths regularly

### 3. Monitor Circuit States
- Alert on circuit breaker openings
- Track patterns of failures
- Review and adjust thresholds

### 4. Use Timeouts Wisely
- Set timeouts slightly above p99 latency
- Consider downstream timeout budgets
- Implement timeout hierarchies

### 5. Test Failure Scenarios
- Regularly test circuit breakers
- Simulate service failures
- Verify fallback behaviors

## Common Patterns

### 1. Bulkhead Pattern
Isolate resources to prevent total failure:
```python
# Separate circuit breakers for different operations
read_breaker = CircuitBreaker(name="db_read")
write_breaker = CircuitBreaker(name="db_write")
```

### 2. Timeout Budget
Allocate timeout budgets across service calls:
```python
async def complex_operation(timeout_budget=30):
    # Allocate timeouts
    db_timeout = timeout_budget * 0.3
    api_timeout = timeout_budget * 0.5
    processing_timeout = timeout_budget * 0.2

    data = await timeout(db_timeout)(fetch_from_db)()
    enriched = await timeout(api_timeout)(enrich_via_api)(data)
    return await timeout(processing_timeout)(process)(enriched)
```

### 3. Adaptive Retry
Adjust retry strategy based on error types:
```python
class AdaptiveRetry:
    def should_retry(self, error: Exception, attempt: int) -> bool:
        if isinstance(error, RateLimitError):
            # Longer delay for rate limits
            self.delay = 60
            return attempt < 2
        elif isinstance(error, NetworkError):
            # Standard exponential backoff
            self.delay = 2 ** attempt
            return attempt < 5
        return False
```

## Troubleshooting

### Circuit Breaker Issues

1. **Breaker Opens Too Frequently**
   - Increase failure threshold
   - Review timeout settings
   - Check service health

2. **Breaker Doesn't Close**
   - Verify success threshold
   - Check half-open test requests
   - Review service recovery

### Retry Issues

1. **Too Many Retries**
   - Reduce max attempts
   - Implement circuit breakers
   - Check if errors are recoverable

2. **Retry Storms**
   - Add jitter to delays
   - Implement backoff
   - Use circuit breakers

### Performance Issues

1. **High Latency**
   - Review timeout settings
   - Check retry delays
   - Monitor circuit breaker states

2. **Resource Exhaustion**
   - Implement bulkheads
   - Add rate limiting
   - Review connection pools
