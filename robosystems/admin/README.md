# RoboSystems Admin CLI

Remote administration tool for managing subscriptions, customers, and invoices via the admin API.

## Quick Start

The admin CLI is available through the `just admin` wrapper command:

```bash
just admin dev <command> <args>
```

## Environment Targeting

The admin CLI can target three environments:

| Environment | API URL | Authentication |
|------------|---------|----------------|
| `dev` | http://localhost:8000 | `ADMIN_API_KEY` from `.env.local` |
| `staging` | https://api.staging.robosystems.ai | AWS Secrets Manager |
| `prod` | https://api.robosystems.ai | AWS Secrets Manager |

**Examples:**
```bash
just admin dev customers list        # Local development
just admin staging customers list    # Staging environment
just admin prod stats                # Production environment
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

Get overview of the billing system:

```bash
# System statistics
just admin dev stats

# Recent invoices
just admin dev invoices list --limit 20

# Subscriptions needing attention
just admin dev subscriptions list --status pending_payment
just admin dev subscriptions list --status unpaid
```

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

- Admin API key grants full administrative access
- Never commit `.env.local` to git
- Rotate production admin keys regularly
- Audit admin actions via subscription audit logs
- Use staging environment for testing administrative operations

## Related Documentation

- [Billing Configuration](/robosystems/config/billing/core.py) - Subscription plans and pricing
- [Customer Model](/robosystems/models/billing/customer.py) - Customer data model
- [Subscription Model](/robosystems/models/billing/subscription.py) - Subscription lifecycle
- [Invoice Model](/robosystems/models/billing/invoice.py) - Invoice management
