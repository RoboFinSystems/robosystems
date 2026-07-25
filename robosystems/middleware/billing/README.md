# Credits Middleware

This middleware implements the credit-based billing system exclusively for AI operations (Anthropic Claude calls via AWS Bedrock) on the RoboSystems platform.

## Overview

The credits middleware:

- Tracks credit consumption ONLY for AI operations (Anthropic Claude calls via AWS Bedrock)
- Handles token-based billing using actual API usage
- Enforces credit limits for AI operations
- Provides caching for high-performance credit checks
- All database operations (queries, imports, backups) are included

## Architecture

```
credits/
├── __init__.py              # Module exports
└── cache.py                 # In-memory TTL-based caching
```

## Business Model

### Credit-Based Pricing

The platform uses a simplified credit model focused exclusively on AI operations:

1. Users receive monthly AI credit allocations based on their tier
2. ONLY AI operations (Anthropic via Bedrock) consume credits based on actual token usage
3. All database operations are included (queries, imports, backups, etc.)
4. Storage is included per tier up to a limit; overages are not billed today (no credit or USD metering path exists in code)
5. AI operations are blocked when credits are exhausted

### Subscription Tiers (AI Credits)

```
Standard: 8,000 credits/month (~200 AI agent calls)
Large:    32,000 credits/month (~800 AI agent calls)
XLarge:   100,000 credits/month (~2,600 AI agent calls)
```

## Key Components

### 1. Credit Cache (`cache.py`)

High-performance in-memory caching layer with TTL-based expiration.

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
# AI operations use actual token consumption.
# Single Claude 4 Sonnet tier (config key `anthropic_claude_4_sonnet`
# in config/billing/ai.py). This dict is illustrative — the
# authoritative rates live in AIBillingConfig.TOKEN_PRICING.
AI_TOKEN_COSTS = {
    # Anthropic Claude 4 Sonnet (per 1K tokens)
    "anthropic_claude_4_sonnet": {
        "input": 3,     # 3 credits per 1K input tokens
        "output": 15,   # 15 credits per 1K output tokens
    },
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
# AI operations use actual token counts (minimum charge of 1 credit applied)
# Storage is included per tier up to a limit (not metered to a charge today)
# All database operations are included
```

## Configuration

Environment variables:

```bash
# Cache Configuration
CREDIT_BALANCE_CACHE_TTL=300         # Balance cache TTL (seconds)
CREDIT_SUMMARY_CACHE_TTL=600         # Summary cache TTL
CREDIT_OPERATION_COST_CACHE_TTL=3600 # Operation cost cache TTL

# Credit Thresholds
CREDIT_LOW_BALANCE_THRESHOLD=0.2     # Alert at 20% remaining
CREDIT_CRITICAL_THRESHOLD=0.05       # Critical at 5% remaining
```

## Integration

### 1. With AI Endpoints (Token-Based Consumption)

```python
from robosystems.operations.graph import CreditService

router = APIRouter()

@router.post("/v1/graphs/{graph_id}/operator")
async def operator_endpoint(
    graph_id: str,
    request: OperatorRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db_session)
):
    # The operator orchestrator selects and runs the AI Operator.
    orchestrator = OperatorOrchestrator(graph_id, db)
    response = await orchestrator.execute(request)

    # The operator consumes credits post-operation based on actual tokens
    # (CreditService.consume_ai_tokens). No decorator needed.
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
    model="us.anthropic.claude-sonnet-4-6",  # Bedrock model id
    operation_description="operator query",
    user_id="user_456",
)
```

`consume_ai_tokens` takes `operation_description` (a free-text label for the
transaction), not an `operation_type`. The `model` argument accepts the Bedrock
profile id (e.g. `us.anthropic.claude-sonnet-4-6`) and is mapped internally to
the pricing key `anthropic_claude_4_sonnet` in `AIBillingConfig.TOKEN_PRICING`.

### 4. Storage Handling

Storage is **included with each tier up to a per-tier limit** and is **not
billed as an overage today** — not in credits and not in USD. There is no live
storage metering-to-billing path in the code: the only storage logic is
limit enforcement.

- `CreditService.check_storage_limit()` reports current usage against the
  graph's `storage_limit_gb` (with an optional admin override). It returns
  status/recommendations; it does **not** consume credits or compute a charge.
- There is no storage pricing constant or metering code in the billing path —
  storage credit metering is not implemented.
- `consume_ai_tokens` is the only credit-consuming entry point; its inline note
  reads "Storage is included in each tier (no metering/overage)."

The "all database operations are free" guarantee is unaffected: only AI token
usage consumes credits. If usage-based storage billing is added later, it would
introduce a new consumption path here; until then, exceeding the limit is a
gating/limit concern, not a billed event.

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
