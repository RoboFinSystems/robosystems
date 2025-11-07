# Credits Middleware

This middleware implements the credit-based billing system exclusively for AI operations (Anthropic/OpenAI API calls) on the RoboSystems platform.

## Overview

The credits middleware:

- Tracks credit consumption ONLY for AI operations (Anthropic/OpenAI API calls)
- Handles token-based billing using actual API usage
- Enforces credit limits for AI operations
- Provides caching for high-performance credit checks
- All database operations (queries, imports, backups) are included

## Architecture

```
credits/
├── __init__.py              # Module exports
└── cache.py                 # Redis/Valkey-based caching
```

## Business Model

### Credit-Based Pricing

The platform uses a simplified credit model focused exclusively on AI operations:

1. Users receive monthly AI credit allocations based on their tier
2. ONLY AI operations (Anthropic/OpenAI) consume credits based on actual token usage
3. All database operations are included (queries, imports, backups, etc.)
4. Storage is billed separately in USD (not credits) for overages
5. AI operations are blocked when credits are exhausted

### Subscription Tiers (AI Credits)

```
Standard:    10,000 credits/month (~100 AI agent calls)
Enterprise:  50,000 credits/month (~500 AI agent calls)
Premium:     200,000 credits/month (~2000 AI agent calls)
```

## Key Components

### 1. Credit Cache (`cache.py`)

High-performance caching layer using Redis/Valkey.

**Features:**

- **Balance Caching**: 5-minute TTL for credit balances
- **Operation Cost Caching**: 1-hour TTL for operation costs
- **Summary Caching**: 10-minute TTL for usage summaries
- **Atomic Operations**: Thread-safe credit updates

**Cache Keys:**

```
graph_credit:{graph_id}              # Graph credit balance
shared_credit:{user_id}:{repository} # Shared repository credits
credit_summary:{graph_id}            # Usage summary
op_cost:{operation_type}             # Operation costs
```

**Usage:**

```python
cache = CreditCache()

# Cache balance
cache.cache_graph_credit_balance(
    graph_id="kg1a2b3c",
    balance=Decimal("50000"),
    multiplier=Decimal("1.0"),
    graph_tier="standard"
)

# Get cached balance
balance = cache.get_cached_graph_credit_balance("kg1a2b3c")

# Invalidate on consumption
cache.invalidate_graph_credit_balance("kg1a2b3c")
```


## Operation Costs

### AI Token-Based Costs

```python
# AI operations use actual token consumption
AI_TOKEN_COSTS = {
    # Anthropic Claude models (per 1K tokens)
    "claude-3-opus": {
        "input": 15,    # 15 credits per 1K input tokens
        "output": 75,   # 75 credits per 1K output tokens
    },
    "claude-3-sonnet": {
        "input": 3,     # 3 credits per 1K input tokens
        "output": 15,   # 15 credits per 1K output tokens
    },
    
    # OpenAI models (per 1K tokens)
    "gpt-4": {
        "input": 30,    # 30 credits per 1K input tokens
        "output": 60,   # 60 credits per 1K output tokens
    },
}

# Storage costs
STORAGE_COSTS = {
    "per_gb_over_limit": 100,  # 100 credits per GB over monthly limit
}

# All database operations are included
INCLUDED_OPERATIONS = [
    "query", "analytics", "backup", "sync", "import",
    "mcp_call", "api_call", "connection_sync"
]
```

### Simplified Billing Model

```python
# No multipliers in the simplified model
# AI operations use actual token counts
# Storage has fixed per-GB pricing per tier
# All database operations are included
```

## Configuration

Environment variables:

```bash
# Cache Configuration
CREDIT_BALANCE_CACHE_TTL=300         # Balance cache TTL (seconds)
CREDIT_SUMMARY_CACHE_TTL=600         # Summary cache TTL
CREDIT_OPERATION_COST_CACHE_TTL=3600 # Operation cost cache TTL

# Redis/Valkey Configuration
VALKEY_URL=redis://localhost:6379    # Redis connection URL

# Credit Thresholds
CREDIT_LOW_BALANCE_THRESHOLD=0.2     # Alert at 20% remaining
CREDIT_CRITICAL_THRESHOLD=0.05       # Critical at 5% remaining
```

## Integration

### 1. With AI Endpoints (Token-Based Consumption)

```python
from robosystems.operations.graph import CreditService

router = APIRouter()

@router.post("/v1/graphs/{graph_id}/agent")
async def agent_endpoint(
    graph_id: str,
    request: AgentRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    # Initialize agent
    agent = FinancialAgent(graph_id, db)
    
    # Execute agent (tracks tokens internally)
    response = await agent.process(request)
    
    # Agent automatically consumes credits based on actual tokens
    # No decorator needed - consumption happens post-operation
    return response
```

### 2. For Database Operations (Included)

```python
@router.post("/v1/graphs/{graph_id}/query")
async def execute_query(
    graph_id: str,
    query: QueryRequest,
    current_user: User = Depends(get_current_user),
    repo: Repository = Depends(get_graph_repository)
):
    # No credit decorator needed - queries are included
    return await repo.execute_query(query.cypher)
```

### 3. Manual AI Credit Consumption

```python
from robosystems.operations.graph import CreditService

credit_service = CreditService(session)

# After AI operation completes
usage = anthropic_response.usage
credit_service.consume_ai_tokens(
    graph_id="kg1a2b3c",
    input_tokens=usage.input_tokens,
    output_tokens=usage.output_tokens,
    model="claude-3-opus",
    operation_type="agent_call",
    user_id="user_456"
)
```

### 4. Storage Overage Handling

```python
# Storage overages are billed separately, not through credits
# Each tier includes storage:
# - Standard: 100 GB included, $1.00/GB overage
# - Enterprise: 500 GB included, $0.50/GB overage  
# - Premium: 2 TB included, $0.25/GB overage

# Monthly storage billing (handled by billing system, not credits)
storage_gb = calculate_storage_usage(graph_id)
limit_gb = get_storage_limit_for_tier(tier)

if storage_gb > limit_gb:
    overage_gb = storage_gb - limit_gb
    overage_cost_usd = overage_gb * overage_rate_for_tier(tier)
    # Billed directly in USD, not credits
```

## Monitoring

### Key Metrics

1. **Credit Consumption Rate**

   - Credits consumed per minute
   - By operation type
   - By user/graph

2. **Balance Levels**

   - Graphs approaching limits
   - Average utilization percentage
   - Credit exhaustion events

3. **Cache Performance**

   - Hit/miss ratios
   - Cache latency
   - Invalidation frequency

4. **Reservation Metrics**
   - Active reservations
   - Timeout rates
   - Average reservation duration

### Alerts

Configure alerts for:

- Credit balance < 20% (warning)
- Credit balance < 5% (critical)
- Credit exhaustion events
- High reservation timeout rate
- Cache connection failures

## Best Practices

1. **Use Caching**: Always check cache before database
2. **Batch Operations**: Use batch endpoints for better rates
3. **Monitor Usage**: Track credit consumption patterns
4. **Set Budgets**: Configure spending limits per graph
5. **Plan Capacity**: Ensure sufficient credits for operations

## Troubleshooting

### Common Issues

1. **"Insufficient credits" errors**

   - Check current balance
   - Review recent consumption
   - Consider upgrading tier

2. **High credit consumption**

   - Review operation frequency
   - Optimize expensive queries
   - Use batch operations

3. **Cache misses**

   - Verify Redis connectivity
   - Check TTL configuration
   - Monitor invalidation patterns

4. **Reservation timeouts**
   - Increase timeout for long operations
   - Optimize operation performance
   - Check for deadlocks

## Security Considerations

1. **Credit Fraud Prevention**

   - Validate all consumption requests
   - Log all transactions with metadata
   - Monitor for unusual patterns

2. **Rate Limiting**

   - Credit-based rate limiting
   - Prevent credit exhaustion attacks
   - Enforce fair usage policies

3. **Audit Trail**
   - Complete transaction history
   - User attribution for all operations
   - Immutable audit logs
