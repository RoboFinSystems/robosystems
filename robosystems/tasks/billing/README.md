# Billing Tasks

Automated billing tasks for the RoboSystems platform, handling credit allocation, storage billing, and usage tracking.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    BILLING TASK SYSTEM                      │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  HOURLY (usage_collector.py)                                │
│  └─► Collect storage snapshots with breakdown               │
│      └─► GraphUsageTracking table                           │
│                                                             │
│  DAILY (storage_billing.py)                                 │
│  └─► Average yesterday's snapshots                          │
│      └─► Calculate storage overage (above included limit)   │
│          └─► Consume credits (10 credits/GB/day)            │
│              └─► Allow negative balances                    │
│                                                             │
│  MONTHLY (monthly_credit_reset.py)                          │
│  └─► Identify negative balances (overages)                  │
│      └─► Generate overage invoices                          │
│          └─► Allocate fresh monthly credits                 │
│              └─► Clean up old transactions                  │
│                                                             │
│  SHARED REPOSITORIES (shared_credit_allocation.py)          │
│  └─► Query BillingSubscription (source of truth)            │
│      └─► Create/sync UserRepository & UserRepositoryCredits │
│          └─► Allocate credits for active subscriptions      │
│              └─► Deactivate credits for canceled            │
│                                                             │
│  HEALTH CHECKS (credit_allocation.py)                       │
│  └─► Weekly monitoring of credit system health              │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## BillingSubscription Integration

### Unified Subscription Management

The billing system uses **`BillingSubscription`** as the **single source of truth** for all subscriptions:

#### Graph Subscriptions

- **Resource Type:** `"graph"`
- **Resource ID:** Graph ID (e.g., `"kg1a2b3c"`)
- **Billing:** Per-graph (owned by graph creator)
- **Credit Allocation:** Via `monthly_credit_reset.py`

#### Repository Subscriptions

- **Resource Type:** `"repository"`
- **Resource ID:** Repository name (e.g., `"sec"`, `"industry"`)
- **Billing:** Per-user (each user has own subscription)
- **Credit Allocation:** Via `shared_credit_allocation.py`

### Synchronization Flow

```
BillingSubscription (source of truth)
    ↓
    ├─► GRAPH CREDITS
    │   └─► GraphCredits (one per graph)
    │       └─► GraphCreditTransaction (audit trail)
    │
    └─► REPOSITORY CREDITS
        └─► UserRepository (get or create)
            └─► UserRepositoryCredits (get or create)
                └─► CreditTransaction (audit trail)
```

**Key Points:**

- Subscription status controls credit allocation
- Credit pools created automatically if subscription exists
- Canceled subscriptions → deactivated credit pools
- Ensures credits and subscriptions never go out of sync

## Task Files

### Core Billing Tasks

#### `usage_collector.py`

**Schedule:** Hourly at minute :05
**Purpose:** Collect detailed storage metrics from all graphs

**Process:**

1. Query all user graphs
2. Calculate storage using `StorageCalculator`:
   - Files: S3 user uploads
   - Tables: S3 CSV/Parquet imports
   - Graphs: EBS Kuzu database
   - Subgraphs: EBS subgraph data
3. Get instance metadata from Graph API
4. Record snapshot in `GraphUsageTracking`
5. Clean up snapshots older than 1 year

**Key Functions:**

- `graph_usage_collector()` - Main hourly task
- `collect_graph_metrics()` - Get metadata from Graph API
- `get_user_graphs_with_details()` - Query all graphs

---

#### `storage_billing.py`

**Schedule:** Daily at 2:00 AM
**Purpose:** Bill for storage overages above included limits

**Storage Model:**

- Standard: 100 GB included
- Large: 500 GB included
- XLarge: 2 TB included
- Overage rate: 10 credits/GB/day

**Process:**

1. Calculate average storage from yesterday's snapshots
2. Determine overage (total - included)
3. Consume credits via `consume_storage_credits()`
4. Allow negative balances (storage is mandatory)

**Key Functions:**

- `daily_storage_billing()` - Main daily task
- `get_graphs_with_storage_usage()` - Find graphs with snapshots
- `calculate_daily_average_storage()` - Average snapshots
- `monthly_storage_summary()` - Monthly analytics (2nd of month)

---

#### `monthly_credit_reset.py`

**Schedule:** 1st of month at configured hour
**Purpose:** Process overages and allocate fresh monthly credits

**Process:**

1. Identify graphs with negative balances
2. Generate overage invoices (~$0.005 per credit)
3. Call `bulk_allocate_monthly_credits()` for all graphs
4. Clean up old transactions (12 month retention)

**Key Functions:**

- `monthly_credit_reset()` - Main monthly task
- `get_graphs_with_negative_balance()` - Find overages
- `process_overage_invoice()` - Create invoice records
- `cleanup_old_transactions()` - Database cleanup
- `generate_monthly_usage_report()` - Monthly analytics (2nd of month)

---

#### `shared_credit_allocation.py`

**Schedule:** 1st of month at configured hour
**Purpose:** Allocate credits for shared repository subscriptions

**BillingSubscription Integration:**

- **Source of Truth:** Uses `BillingSubscription` (resource_type="repository")
- **Synchronization:** Creates/syncs `UserRepository` and `UserRepositoryCredits`
- **Status Enforcement:** Only allocates if subscription status is ACTIVE
- **Cleanup:** Deactivates credits for canceled subscriptions

**Repositories:**

- SEC (Securities and Exchange Commission data)
- Industry (industry benchmarks)
- Economic (economic indicators)
- Market (market data)

**Process:**

1. Query active `BillingSubscription` (resource_type="repository")
2. For each subscription:
   - Get or create `UserRepository`
   - Get or create `UserRepositoryCredits`
   - Reactivate if subscription active but credits inactive
3. Allocate monthly credits with rollover logic
4. Deactivate `UserRepositoryCredits` for canceled subscriptions

**Key Functions:**

- `allocate_monthly_shared_credits()` - Main allocation task with BillingSubscription integration
- `deactivate_canceled_subscription_credits()` - Sync credit status with subscriptions
- `check_credit_allocation_health()` - Weekly health check

---

#### `credit_allocation.py`

**Schedule:** Weekly health checks
**Purpose:** Monitor graph credit system health

**Functions:**

- `check_graph_credit_health()` - Weekly monitoring (Monday 4 AM)
- `allocate_graph_credits_for_user()` - Utility for specific users

**Note:** Monthly allocation moved to `monthly_credit_reset.py`

---

## Credit System

### AI Credits

- Agent calls: 100 credits (Anthropic/OpenAI API)
- All database operations: FREE (included with instance)

### Storage Credits

- Storage overage: 10 credits/GB/day
- Only charges for storage ABOVE included limit
- Negative balances allowed (mandatory charges)

### Monthly Allocations by Tier

- Standard: 10,000 credits/month
- Large: 50,000 credits/month
- XLarge: 200,000 credits/month

---

## Data Models

### GraphUsageTracking

Records hourly storage snapshots with breakdown:

- `storage_gb` - Total storage
- `files_storage_gb` - S3 user files
- `tables_storage_gb` - S3 table imports
- `graphs_storage_gb` - EBS database
- `subgraphs_storage_gb` - EBS subgraphs
- `event_type` - STORAGE_SNAPSHOT
- `billing_year/month/day` - For daily aggregation

### GraphCredits

Credit pool for each graph:

- `current_balance` - Available credits
- `monthly_allocation` - Tier-based allocation
- `last_allocation_date` - Last allocation timestamp
- `graph_tier` - Subscription tier

### GraphCreditTransaction

Audit trail for all credit operations:

- `transaction_type` - ALLOCATION, CONSUMPTION, BONUS
- `amount` - Credits (positive or negative)
- `description` - Human-readable description
- `metadata` - JSON with operation details

---

## Schedule Summary

| Task                              | Frequency | Time                 | Purpose                            |
| --------------------------------- | --------- | -------------------- | ---------------------------------- |
| `collect-storage-usage`           | Hourly    | :05                  | Snapshot storage metrics           |
| `daily-storage-billing`           | Daily     | 2:00 AM              | Bill for storage overages          |
| `monthly-credit-reset`            | Monthly   | 1st, configured hour | Process overages, allocate credits |
| `monthly-usage-report`            | Monthly   | 2nd, 6:00 AM         | Generate analytics report          |
| `allocate-monthly-shared-credits` | Monthly   | 1st, configured hour | Shared repo credits                |
| `monthly-storage-summary`         | Monthly   | 2nd, 5:00 AM         | Storage analytics                  |
| `check-graph-credit-health`       | Weekly    | Monday, 4:00 AM      | Health monitoring                  |
| `check-credit-allocation-health`  | Weekly    | Monday, 3:00 AM      | Health monitoring                  |

---

## Key Services

### StorageCalculator (`operations/graph/storage_service.py`)

Calculates actual storage from multiple sources:

- S3: Queries database tables
- EBS: Filesystem walk (TODO: production EBS API)

### CreditService (`operations/graph/credit_service.py`)

Manages all credit operations:

- `consume_storage_credits()` - Overage-based billing
- `bulk_allocate_monthly_credits()` - Monthly allocation
- `consume_ai_tokens()` - AI operation billing

---

## Environment Variables

```bash
ENVIRONMENT=dev|staging|prod  # Tasks only run in non-dev
CREDIT_ALLOCATION_HOUR=0      # Hour for monthly tasks (default: midnight)
CREDIT_ALLOCATION_DAY=1       # Day of month for allocation
```

---

## Testing

```bash
# Run billing tests
just test -k "billing or storage or credit"

# Check task registration
uv run python -c "from robosystems.tasks.schedule import BEAT_SCHEDULE; print(BEAT_SCHEDULE.keys())"

# Verify imports
uv run python -c "from robosystems.tasks.billing.monthly_credit_reset import monthly_credit_reset"
```

---

## Monitoring

### Health Checks

- Weekly credit health checks detect:
  - Overdue allocations (>35 days)
  - Low balances (<10% of allocation)
  - Zero allocations (configuration errors)

### Logs

All tasks log at INFO level:

- Task start/completion
- Processing statistics
- Error details with graphs affected

### Metrics

Tasks return detailed result dicts:

- Graphs processed
- Credits consumed/allocated
- Overage invoices generated
- Processing errors

---

## Migration Notes

### From Old System

If migrating from the old `allocate_monthly_graph_credits` task:

1. The function has been removed from `credit_allocation.py`
2. Use `monthly_credit_reset` instead (includes overage processing)
3. Update any manual task triggers to use new task name

### Database

- Migration `7a35e59fc400` adds storage breakdown fields
- Existing `GraphUsageTracking` records still work
- New snapshots include detailed breakdown
