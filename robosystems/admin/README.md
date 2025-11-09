# RoboSystems Admin CLI

Remote administration tool for managing subscriptions, customers, invoices, credits, graphs, users, and infrastructure operations via the admin API.

## Quick Start

The admin CLI is available through the `just admin` wrapper command:

```bash
just admin dev <command> <args>
```

## Security & IP Whitelisting

**IMPORTANT**: The Admin API is protected by IP whitelisting at the infrastructure level (Application Load Balancer).

### Access Control

- **Admin API** (`/admin/v1/*`): Restricted to whitelisted IP addresses only
- **Regular API** (`/v1/*`): Open to internet (protected by JWT/API keys)

### IP Whitelist Configuration

Access is controlled via GitHub variables for bastion SSH and Admin API:

**Bastion SSH Access** (`BASTION_ALLOWED_CIDR`):
- Single CIDR block for SSH access to bastion host
- Typically your office IP or corporate VPN endpoint

**Admin API Access** (`ADMIN_ALLOWED_CIDRS`):
- Comma-separated CIDR blocks for Admin API endpoints
- Can specify multiple IPs/networks

**Format:**
```bash
# Bastion (single CIDR)
203.0.113.42/32

# Admin API (single or multiple CIDRs)
203.0.113.42/32
# OR
203.0.113.0/24,198.51.100.0/24,10.0.1.0/24
```

**Setting the variables:**
```bash
# Bastion SSH access (single CIDR)
gh variable set BASTION_ALLOWED_CIDR --body "YOUR.IP.ADDRESS/32"

# Admin API access (can be single or comma-separated)
gh variable set ADMIN_ALLOWED_CIDRS --body "YOUR.IP.ADDRESS/32"
# OR for multiple IPs
gh variable set ADMIN_ALLOWED_CIDRS --body "203.0.113.0/24,198.51.100.0/24"
```

**Note:** These are stored as variables (not secrets) since IP addresses aren't truly secret, and visibility helps with troubleshooting.

### Security Features

- ✅ **Infrastructure-level blocking**: ALB denies requests before reaching application
- ✅ **Defense in depth**: IP whitelist + API key authentication
- ✅ **Fail-secure**: Returns 403 Forbidden for unauthorized IPs
- ✅ **Audit logging**: All admin operations logged via CloudWatch and audit trail

## Environment Targeting

The admin CLI can target three environments:

| Environment | API URL | Authentication | IP Restriction |
|------------|---------|----------------|----------------|
| `dev` | http://localhost:8000 | `ADMIN_API_KEY` from `.env.local` | None (localhost) |
| `staging` | https://api.staging.robosystems.ai | AWS Secrets Manager | Whitelisted IPs only |
| `prod` | https://api.robosystems.ai | AWS Secrets Manager | Whitelisted IPs only |

**Examples:**
```bash
just admin dev customers list        # Local development
just admin staging customers list    # Staging environment (requires whitelisted IP)
just admin prod stats                # Production environment (requires whitelisted IP)
```

## Available Commands

### Statistics

View system-wide subscription and customer statistics.

```bash
just admin dev stats
```

**Output includes:**
- Total subscriptions by status, plan, and billing interval
- Monthly recurring revenue (MRR)
- Customer counts and payment method stats

---

### Customer Management

#### List Customers

Display all customers with their billing settings.

```bash
just admin dev customers list
```

**Output columns:**
- User ID
- Name
- Email
- Payment Method (Yes/No)
- Invoice Billing (Yes/No)
- Payment Terms
- Billing Email

#### Update Customer

Modify customer billing settings, including enabling invoice billing for enterprise customers.

```bash
# Enable invoice billing for enterprise customer
just admin dev customers update USER_ID \
  --invoice-billing \
  --billing-email "ap@enterprise.com" \
  --billing-contact-name "Jane Smith - Accounts Payable" \
  --payment-terms "net_30"

# Disable invoice billing (revert to Stripe)
just admin dev customers update USER_ID --no-invoice-billing

# Update only billing email
just admin dev customers update USER_ID --billing-email "new@email.com"
```

**Options:**
- `--invoice-billing` / `--no-invoice-billing` - Enable/disable invoice billing
- `--billing-email TEXT` - Billing email address
- `--billing-contact-name TEXT` - Billing contact name
- `--payment-terms TEXT` - Payment terms (e.g., `net_30`, `net_60`, `net_90`)

---

### Subscription Management

#### List Subscriptions

Display all graph subscriptions with optional filters.

```bash
# List all subscriptions
just admin dev subscriptions list

# Filter by status
just admin dev subscriptions list --status active
just admin dev subscriptions list --status pending_payment

# Filter by tier
just admin dev subscriptions list --tier kuzu-standard
just admin dev subscriptions list --tier kuzu-enterprise

# Filter by customer email
just admin dev subscriptions list --email user@example.com

# Include canceled subscriptions
just admin dev subscriptions list --include-canceled

# Limit results
just admin dev subscriptions list --limit 50
```

**Available statuses:**
- `pending_payment` - Awaiting payment
- `provisioning` - Resources being created
- `active` - Fully active subscription
- `past_due` - Payment overdue
- `unpaid` - Payment failed
- `canceled` - Subscription canceled
- `failed` - Provisioning failed

**Available tiers:**
- `kuzu-standard` - Standard shared infrastructure
- `kuzu-large` - Dedicated large instance
- `kuzu-xlarge` - Dedicated extra-large instance
- `kuzu-enterprise` - Custom enterprise tier

#### Get Subscription

Retrieve detailed information about a specific subscription.

```bash
just admin dev subscriptions get SUBSCRIPTION_ID
```

**Output includes:**
- Subscription ID and status
- User ID and resource information
- Plan details and pricing
- Billing period dates
- Stripe subscription ID (if applicable)
- Provider information

#### Create Subscription

Create a new subscription for a user (admin-initiated).

```bash
just admin dev subscriptions create USER_ID \
  --resource-type graph \
  --resource-id GRAPH_ID \
  --plan-name kuzu-standard
```

#### Update Subscription

Modify an existing subscription.

```bash
just admin dev subscriptions update SUBSCRIPTION_ID \
  --status active \
  --plan-name kuzu-large
```

#### Subscription Audit Log

View the audit trail for a subscription.

```bash
# View all events
just admin dev subscriptions audit SUBSCRIPTION_ID

# Filter by event type
just admin dev subscriptions audit SUBSCRIPTION_ID --event-type SUBSCRIPTION_ACTIVATED

# Limit results
just admin dev subscriptions audit SUBSCRIPTION_ID --limit 20
```

**Common event types:**
- `SUBSCRIPTION_CREATED`
- `SUBSCRIPTION_ACTIVATED`
- `SUBSCRIPTION_CANCELED`
- `INVOICE_GENERATED`
- `PAYMENT_SUCCEEDED`
- `PAYMENT_FAILED`

---

### Invoice Management

#### List Invoices

Display all invoices with optional filters.

```bash
# List all invoices
just admin dev invoices list

# Filter by status
just admin dev invoices list --status open
just admin dev invoices list --status paid

# Filter by user
just admin dev invoices list --user-id USER_ID

# Limit results
just admin dev invoices list --limit 50
```

**Available statuses:**
- `open` - Invoice pending payment
- `paid` - Invoice paid
- `void` - Invoice voided
- `uncollectible` - Marked as uncollectible

#### Get Invoice

Retrieve detailed information about a specific invoice.

```bash
just admin dev invoices get INVOICE_ID
```

**Output includes:**
- Invoice number and status
- User ID and billing information
- Line items with amounts
- Payment details (if paid)
- Due date and payment terms
- Stripe invoice ID (if applicable)

#### Mark Invoice Paid

Manually mark an invoice as paid (for non-Stripe payments like wire transfers).

```bash
just admin dev invoices mark-paid INVOICE_ID \
  --payment-method "wire_transfer" \
  --payment-reference "REF-123456"
```

**Use cases:**
- Wire transfers
- ACH payments
- Check payments
- Other non-Stripe payment methods

**Note:** Stripe invoices are automatically marked paid via webhooks.

---

### Credit Management

#### List Credit Pools

Display all graph credit pools with optional filters.

```bash
# List all credit pools
just admin dev credits list

# Filter by user email
just admin dev credits list --user-email user@example.com

# Filter by tier
just admin dev credits list --tier kuzu-standard

# Show only low balance pools (< 10% remaining)
just admin dev credits list --low-balance

# Limit results
just admin dev credits list --limit 50
```

**Output columns:**
- Graph ID
- User ID
- Tier
- Current Balance
- Monthly Allocation
- Credit Multiplier

#### Get Credit Pool

Retrieve detailed information about a specific credit pool.

```bash
just admin dev credits get GRAPH_ID
```

**Output includes:**
- Graph ID and user ID
- Tier configuration
- Current balance and monthly allocation
- Credit multiplier
- Storage limit override (if any)

#### Add Bonus Credits

Grant bonus credits to a graph (one-time allocation).

```bash
just admin dev credits bonus GRAPH_ID \
  --amount 10000 \
  --description "Customer retention - Q1 bonus"
```

**Use cases:**
- Customer retention incentives
- Service recovery compensation
- Promotional credits
- Special project allocations

#### Credit Analytics

View system-wide credit usage analytics.

```bash
# Overall analytics
just admin dev credits analytics

# Filter by tier
just admin dev credits analytics --tier kuzu-enterprise
```

**Output includes:**
- Total pools and allocations
- Total consumed this month
- Top consumers
- Breakdown by tier

#### Credit Health Check

Monitor credit system health and identify issues.

```bash
just admin dev credits health
```

**Output includes:**
- System status (healthy/warning/critical)
- Pools with negative balances
- Pools with low balances
- Total pools with issues

---

### Graph Management

#### List Graphs

Display all graphs with optional filters.

```bash
# List all graphs
just admin dev graphs list

# Filter by owner email
just admin dev graphs list --user-email user@example.com

# Filter by tier
just admin dev graphs list --tier kuzu-large

# Filter by backend
just admin dev graphs list --backend kuzu

# Limit results
just admin dev graphs list --limit 50
```

**Output columns:**
- Graph ID
- Name
- Tier
- Backend
- Status
- Storage

#### Get Graph

Retrieve detailed information about a specific graph.

```bash
just admin dev graphs get GRAPH_ID
```

**Output includes:**
- Graph ID, name, description
- Owner and organization
- Tier and backend configuration
- Storage usage and limits
- Subgraph count and limits

#### Graph Analytics

View cross-graph analytics and statistics.

```bash
# Overall analytics
just admin dev graphs analytics

# Filter by tier
just admin dev graphs analytics --tier kuzu-standard
```

**Output includes:**
- Total graphs
- Breakdown by tier, backend, status
- Total storage usage
- Largest graphs

---

### User Management

#### List Users

Display all users with optional filters.

```bash
# List all users
just admin dev users list

# Filter by email (partial match)
just admin dev users list --email example.com

# Show only verified users
just admin dev users list --verified-only

# Limit results
just admin dev users list --limit 50
```

**Output columns:**
- User ID
- Email
- Name
- Verified (Yes/No)
- Org Role
- Created Date

#### Get User

Retrieve detailed information about a specific user.

```bash
just admin dev users get USER_ID
```

**Output includes:**
- User ID, email, name
- Email verification status
- Organization ID and role
- Creation date and last login

#### User Graphs

List all graphs owned by a user.

```bash
just admin dev users graphs USER_ID
```

**Output includes:**
- Graph ID and name
- Access level (owner/admin/write/read)
- Created date

#### User Activity

View recent user activity and operations.

```bash
# Recent activity (last 7 days)
just admin dev users activity USER_ID

# Extended period
just admin dev users activity USER_ID --days 30

# Limit results
just admin dev users activity USER_ID --limit 50
```

**Output includes:**
- Recent API calls
- Graph operations
- Login activity
- Subscription changes

---

### Remote Infrastructure Operations

The admin CLI can execute infrastructure operations on the bastion host via AWS Systems Manager (SSM).

#### Database Migrations

Execute Alembic database migrations remotely on staging/production.

```bash
# Run pending migrations
just admin staging migrations up
just admin prod migrations up

# Rollback last migration
just admin staging migrations down
just admin prod migrations down

# Show current migration version
just admin staging migrations current
just admin prod migrations current
```

**Environment behavior:**
- `dev`: Runs locally via subprocess
- `staging/prod`: Executes via SSM on bastion host

**Security:**
- Requires AWS IAM authentication
- Requires admin API key from Secrets Manager
- All operations logged to CloudWatch

#### SEC Operations

Manage SEC filings database operations remotely.

```bash
# Load company data
just admin prod sec load --ticker NVDA
just admin prod sec load --ticker AAPL --year 2024

# Check SEC database health
just admin prod sec health

# Plan batch import
just admin prod sec plan --start-year 2020 --end-year 2024

# Execute pipeline phase
just admin prod sec phase --phase download
just admin prod sec phase --phase process --resume

# Check pipeline status
just admin prod sec status
```

**Available phases:**
- `download` - Download XBRL filings from SEC
- `process` - Parse and validate filings
- `consolidate` - Prepare for bulk import
- `ingest` - Load into graph database

**Environment behavior:**
- `dev`: Runs locally against local SEC database
- `staging/prod`: Executes via SSM on bastion host

---

## Common Use Cases

### Enable Enterprise Billing

Convert a customer from Stripe billing to invoice billing with net terms:

```bash
# 1. Update customer to enable invoice billing
just admin dev customers update USER_ID \
  --invoice-billing \
  --billing-email "ap@enterprise.com" \
  --billing-contact-name "Jane Smith" \
  --payment-terms "net_30"

# 2. Verify the change
just admin dev customers list
```

**How it works:**
- Existing Stripe subscriptions continue using Stripe billing
- New subscriptions will generate our invoices instead of Stripe checkout
- Customer can provision resources immediately without credit card

### Monitor Subscription Status

Check the status of a newly created subscription:

```bash
# 1. List subscriptions for a user
just admin dev subscriptions list --email user@example.com

# 2. Get detailed status
just admin dev subscriptions get SUBSCRIPTION_ID

# 3. Check audit log for issues
just admin dev subscriptions audit SUBSCRIPTION_ID
```

### Handle Failed Payments

When a payment fails and needs manual resolution:

```bash
# 1. Find the subscription
just admin dev subscriptions list --status unpaid

# 2. Check related invoices
just admin dev invoices list --user-id USER_ID --status open

# 3. After receiving payment via wire transfer
just admin dev invoices mark-paid INVOICE_ID \
  --payment-method "wire_transfer" \
  --payment-reference "WIRE-20250107-123"

# 4. Update subscription status if needed
just admin dev subscriptions update SUBSCRIPTION_ID --status active
```

### Review System Health

Get comprehensive overview of the system:

```bash
# Billing statistics
just admin dev stats

# Credit system health
just admin dev credits health

# Graph analytics
just admin dev graphs analytics

# Recent invoices
just admin dev invoices list --limit 20

# Subscriptions needing attention
just admin dev subscriptions list --status pending_payment
just admin dev subscriptions list --status unpaid

# Low balance credit pools
just admin dev credits list --low-balance
```

### Monitor Credit Usage

Track and manage credit consumption:

```bash
# 1. Check overall credit health
just admin prod credits health

# 2. View top consumers
just admin prod credits analytics

# 3. Identify low balance pools
just admin prod credits list --low-balance

# 4. Grant bonus credits if needed
just admin prod credits bonus GRAPH_ID \
  --amount 5000 \
  --description "Q1 performance bonus"
```

### Remote Database Migrations

Execute database migrations on production:

```bash
# 1. Check current migration version
just admin prod migrations current

# 2. Run pending migrations
just admin prod migrations up

# 3. Verify migration success
just admin prod migrations current
```

**Note:** Migrations execute remotely on the bastion host via SSM.

## Authentication

### Development (`dev`)

Uses `ADMIN_API_KEY` from `.env.local` file. This is automatically loaded by the justfile wrapper.

**Setup:**
Ensure your `.env.local` contains:
```bash
ADMIN_API_KEY=dev-admin-key-for-testing-only
```

### Staging/Production (`staging`, `prod`)

Uses AWS Secrets Manager for authentication. Requires AWS CLI configured with the `robosystems` profile.

**Setup:**
```bash
# Configure AWS CLI profile
aws configure --profile robosystems

# Test connection
just admin staging stats
just admin prod stats
```

The admin key is automatically retrieved from:
- Staging: `robosystems/staging/admin`
- Production: `robosystems/prod/admin`

## Direct CLI Usage

While the justfile wrapper is recommended, you can also use the CLI directly:

```bash
# Development
UV_ENV_FILE=.env.local uv run python -m robosystems.admin.cli -e dev customers list

# Staging/Production
uv run python -m robosystems.admin.cli -e staging customers list
```

## Troubleshooting

### Authentication Errors

**Development:**
```bash
# Check .env.local has ADMIN_API_KEY
grep ADMIN_API_KEY .env.local

# Ensure API is running
just logs api
```

**Staging/Production:**
```bash
# Verify AWS credentials
aws sts get-caller-identity --profile robosystems

# Check secret exists
aws secretsmanager get-secret-value \
  --secret-id robosystems/staging/admin \
  --profile robosystems \
  --region us-east-1
```

### Connection Errors

```bash
# Development - ensure API is running
docker compose ps
just restart api

# Staging/Production - check API status
curl https://api.staging.robosystems.ai/health
curl https://api.robosystems.ai/health
```

### Invalid User/Subscription IDs

User IDs and Subscription IDs are UUIDs in specific formats:

```bash
# User ID format: user_R_<base64>
user_R_Bq7hZ4tyEVk8qa6u_UmQ

# Subscription ID format: sub_<uuid>
sub_123e4567-e89b-12d3-a456-426614174000
```

List entities to find the correct ID:
```bash
just admin dev customers list
just admin dev subscriptions list
```

## Security Notes

### Access Control

- **IP Whitelisting**: Admin API is restricted to whitelisted IP addresses at ALB level
- **API Key Authentication**: Requires valid admin API key from AWS Secrets Manager
- **Defense in Depth**: Both IP and API key validation required for staging/production

### Best Practices

- Never commit `.env.local` to git
- Rotate production admin keys regularly via AWS Secrets Manager
- Update IP whitelist when office/VPN IP changes
- Audit admin actions via subscription audit logs and CloudWatch
- Use staging environment for testing administrative operations
- Test migrations on staging before running on production

### IP Whitelist Management

The IP whitelists are managed via GitHub variables (not secrets, for visibility):

```bash
# Update bastion SSH whitelist (single CIDR)
gh variable set BASTION_ALLOWED_CIDR --body "203.0.113.0/24"

# Update Admin API whitelist (can be comma-separated)
gh variable set ADMIN_ALLOWED_CIDRS --body "203.0.113.0/24,198.51.100.0/24"

# View current values
gh variable list | grep ALLOWED

# After updating, redeploy stacks for changes to take effect:
# - Bastion stack: gh workflow run staging.yml (or prod.yml)
# - API stack: gh workflow run staging.yml (or prod.yml)
```

### Remote Operations Security

Remote bastion operations (migrations, SEC) use AWS Systems Manager:
- Requires valid AWS IAM credentials
- Commands logged to CloudWatch Logs
- Instance auto-starts if stopped
- No SSH key required (uses SSM Session Manager)

## Related Documentation

- [Billing Configuration](/robosystems/config/billing/core.py) - Subscription plans and pricing
- [Customer Model](/robosystems/models/billing/customer.py) - Customer data model
- [Subscription Model](/robosystems/models/billing/subscription.py) - Subscription lifecycle
- [Invoice Model](/robosystems/models/billing/invoice.py) - Invoice management
