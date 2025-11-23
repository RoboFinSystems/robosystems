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

## Payment-First Provisioning

### Overview

The platform uses a **payment-first checkout flow** where users add payment methods before resources are provisioned. This prevents unpaid resource creation and ensures billing is set up before infrastructure costs are incurred.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│               PAYMENT-FIRST FLOW                            │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. USER INITIATES CHECKOUT                                 │
│     └─► POST /v1/billing/checkout/graph or /repository      │
│         └─► Create BillingSubscription (status=pending)     │
│             └─► Create Stripe Checkout Session              │
│                 └─► Redirect to Stripe payment page         │
│                                                             │
│  2. USER COMPLETES PAYMENT                                  │
│     └─► Stripe Checkout Session                             │
│         └─► Add payment method (card)                       │
│             └─► Stripe webhook: checkout.session.completed  │
│                 └─► Update subscription (status=provision)  │
│                     └─► Trigger Celery task                 │
│                                                             │
│  3. RESOURCE PROVISIONING                                   │
│     └─► Celery Worker                                       │
│         ├─► provision_graph_task                            │
│         │   └─► Create graph database                       │
│         │       └─► Activate subscription                   │
│         │           └─► Status: active                      │
│         │                                                   │
│         └─► provision_repository_access_task                │
│             └─► Allocate credits                            │
│                 └─► Grant access                            │
│                     └─► Generate invoice                    │
│                         └─► Activate subscription           │
│                             └─► Status: active              │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Stripe Webhook Setup

#### 1. Configure Webhook Endpoint in Stripe Dashboard

1. Navigate to **Developers** → **Webhooks** in Stripe Dashboard
2. Click **Add endpoint**
3. Set endpoint URL:
   - **Staging**: `https://staging-api.robosystems.com/admin/v1/webhooks/stripe`
   - **Production**: `https://api.robosystems.com/admin/v1/webhooks/stripe`
4. Select events to listen for:
   - `checkout.session.completed` - Payment method collected
   - `invoice.payment_succeeded` - Payment successful
   - `invoice.payment_failed` - Payment failed
   - `customer.subscription.updated` - Subscription changes
   - `customer.subscription.deleted` - Subscription canceled
5. Save and copy the **Signing Secret** (starts with `whsec_`)

#### 2. Set Environment Variables

Add to `.env` and AWS Secrets Manager:

```bash
STRIPE_SECRET_KEY=sk_test_...      # Test: sk_test_*, Prod: sk_live_*
STRIPE_WEBHOOK_SECRET=whsec_...    # From Stripe Dashboard
STRIPE_PUBLISHABLE_KEY=pk_test_... # For frontend
BILLING_ENABLED=true
```

**IMPORTANT**: Environment validation will fail on startup if:

- `BILLING_ENABLED=true` and Stripe keys are missing
- Test key (`sk_test_`) used in production environment
- Invalid key format (must start with `sk_live_` or `sk_test_`)
- Invalid webhook secret format (must start with `whsec_`)

#### 3. Verify Webhook Security

The webhook endpoint (`/admin/v1/webhooks/stripe`) does NOT use `@require_admin` authentication because Stripe webhooks cannot provide admin API keys. Instead, security is enforced through **Stripe webhook signature verification**:

```python
payload = await request.body()
signature = request.headers.get("stripe-signature")

provider = get_payment_provider("stripe")
event = provider.verify_webhook(payload, signature)
```

This verifies:

- Webhook originated from Stripe
- Payload hasn't been tampered with
- Timestamp is recent (prevents replay attacks)

#### 4. Test Webhook Integration

Use Stripe CLI for local testing:

```bash
# Install Stripe CLI
brew install stripe/stripe-cli/stripe

# Login to Stripe
stripe login

# Forward webhooks to local dev server
stripe listen --forward-to localhost:8000/admin/v1/webhooks/stripe

# Trigger test events
stripe trigger checkout.session.completed
stripe trigger invoice.payment_succeeded
stripe trigger customer.subscription.updated
```

#### 5. Monitor Webhooks

In Stripe Dashboard → **Developers** → **Webhooks**:

- View event delivery logs
- Retry failed deliveries
- Check response codes
- Inspect payloads

### Webhook Event Handlers

#### `checkout.session.completed`

**Purpose**: User completed checkout and added payment method

**Handler**: `handle_checkout_completed()`

**Actions**:

1. Find subscription by session ID
2. Mark customer as having payment method
3. Update subscription status to `provisioning`
4. Trigger provisioning task (`provision_graph_task` or `provision_repository_access_task`)

#### `invoice.payment_succeeded`

**Purpose**: Payment processed successfully

**Handler**: `handle_payment_succeeded()`

**Actions**:

1. Find subscription by Stripe subscription ID
2. Mark customer as having payment method
3. Trigger provisioning if status is `pending_payment` or `provisioning`

#### `invoice.payment_failed`

**Purpose**: Payment attempt failed

**Handler**: `handle_payment_failed()`

**Actions**:

1. Find subscription by Stripe subscription ID
2. Update subscription status to `unpaid`
3. Store error message in subscription metadata

#### `customer.subscription.updated`

**Purpose**: Subscription status changed in Stripe

**Handler**: `handle_subscription_updated()`

**Actions**:

1. Map Stripe status to internal status:
   - `active` → `active`
   - `past_due` → `past_due`
   - `unpaid` → `unpaid`
   - `canceled` → `canceled`
   - `incomplete` → `pending_payment`
   - `trialing` → `active`
2. Update subscription status in database

#### `customer.subscription.deleted`

**Purpose**: Subscription canceled in Stripe

**Handler**: `handle_subscription_deleted()`

**Actions**:

1. Find subscription by Stripe subscription ID
2. Cancel subscription immediately
3. Mark status as `canceled`

### Provisioning Tasks

#### `provision_graph_task`

**File**: `tasks/graph_operations/provision_graph.py`

**Triggered By**: Webhook after payment confirmation

**Process**:

1. Query subscription (must be in `provisioning` status)
2. Create graph database via `GenericGraphServiceSync`
3. Set subscription `resource_id` to graph ID
4. Activate subscription (status → `active`)
5. Commit transaction

**Retry Logic**:

- Max retries: 3
- Initial delay: 60 seconds
- Exponential backoff enabled
- Max backoff: 600 seconds (10 minutes)
- Retries on: `ConnectionError`, `TimeoutError`, `OperationalError`

**Cleanup on Failure**:

- After all retries exhausted, delete partially created graph
- Update subscription status to `failed`
- Store error in subscription metadata

#### `provision_repository_access_task`

**File**: `tasks/billing/provision_repository.py`

**Triggered By**: Webhook after payment confirmation

**Process**:

1. Query subscription (must be in `provisioning` status)
2. Allocate monthly credits via `RepositorySubscriptionService`
3. Grant repository access
4. Set subscription `resource_id` to repository name
5. Activate subscription (status → `active`)
6. Create audit log entry
7. Generate subscription invoice
8. Commit transaction

**Retry Logic**:

- Max retries: 3
- Initial delay: 60 seconds
- Exponential backoff enabled
- Max backoff: 600 seconds (10 minutes)
- Retries on: `ConnectionError`, `TimeoutError`, `OperationalError`

**Cleanup on Failure**:

- After all retries exhausted, revoke repository access
- Update subscription status to `failed`
- Store error in subscription metadata

### Payment Provider Abstraction

The platform uses an abstract `PaymentProvider` interface for extensibility:

**Current Provider**: Stripe (`StripePaymentProvider`)

**Future Providers**: Crossmint (crypto payments)

**Key Methods**:

- `create_customer()` - Create customer in payment system
- `create_checkout_session()` - Generate payment page URL
- `verify_webhook()` - Verify webhook signature
- `list_payment_methods()` - Get customer payment methods
- `list_invoices()` - Get billing history
- `get_upcoming_invoice()` - Preview next charge

**Price Auto-Creation**:

The Stripe provider automatically creates Stripe products and prices from billing config:

1. Check Redis cache for existing price ID (24-hour TTL)
2. If not cached, search Stripe for product by metadata
3. If not found, create product and price from `BillingConfig`
4. Cache price ID for future requests
5. Use distributed locks to prevent race conditions

**API Version Pinning**:

The Stripe provider pins to API version `2024-11-20.acacia` to prevent unexpected breaking changes during Stripe API updates.

### Error Handling

#### Transaction Management

All provisioning tasks use explicit transaction management:

**Success Path**:

- Complete all operations
- `session.commit()` at end

**Failure Path**:

- `session.rollback()` to undo partial changes
- `session.refresh(subscription)` to reload from database
- Update subscription status to `failed` in separate transaction
- Nested error handling prevents cascading failures

#### Webhook Failures

If webhook processing fails:

1. Return 500 status to Stripe
2. Stripe automatically retries with exponential backoff
3. Webhook appears in Stripe Dashboard with error details
4. Manual retry available through Stripe Dashboard

#### Provisioning Failures

If provisioning fails after retries exhausted:

1. Cleanup partial resources (delete graph, revoke access)
2. Update subscription status to `failed`
3. Store detailed error message in subscription metadata
4. User sees error in subscription status API

### Testing

#### Unit Tests

```bash
# Test webhook handlers
just test tests/routers/admin/test_webhooks.py

# Test provisioning tasks
just test tests/tasks/billing/test_provision_repository.py
just test tests/tasks/graph_operations/test_provision_graph.py

# Test payment provider
just test tests/operations/billing/test_payment_provider.py
```

#### Integration Testing

Use Stripe test mode:

1. Set `STRIPE_SECRET_KEY=sk_test_...`
2. Use Stripe test cards (e.g., `4242 4242 4242 4242`)
3. Trigger test webhooks via Stripe CLI
4. Verify provisioning completes
5. Check subscription status in database

### Monitoring

#### Key Metrics

- **Webhook Success Rate**: Monitor 200 responses vs 4xx/5xx
- **Provisioning Success Rate**: Track `active` vs `failed` subscriptions
- **Provisioning Duration**: Time from webhook to activation
- **Retry Frequency**: How often tasks retry before success/failure
- **Cleanup Actions**: Frequency of partial resource cleanup

#### Logs

All provisioning operations log at INFO level:

- Webhook events received and processed
- Provisioning task start/completion
- Resource creation (graph ID, credits allocated)
- Error details with subscription ID and user ID

#### Alerts

Configure alerts for:

- Webhook failures (non-200 responses)
- Provisioning failures after all retries
- Cleanup operations (indicates problems)
- Unusual provisioning duration (>5 minutes)

### Security Considerations

1. **Webhook Verification**: All webhooks verified via Stripe signature
2. **No Admin Auth**: Webhooks bypass admin middleware (Stripe can't provide API keys)
3. **HTTPS Required**: Webhooks only work over HTTPS (enforced by Stripe)
4. **API Version Pinning**: Prevents unexpected breaking changes
5. **Environment Validation**: Startup checks prevent production misconfig
6. **Test Key Detection**: Prevents test keys in production environment

---

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
   - Graphs: EBS LadybugDB database
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
