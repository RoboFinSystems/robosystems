# Admin CLI

Remote administration tool for managing the RoboSystems platform. Covers subscriptions, invoices, credits, graphs, users, organizations, cache, and database migrations.

## How It Works

The admin CLI talks to the Admin API (`/admin/v1/*`) which is **not accessible from the public internet** — it's blocked at the ALB. Access requires an SSM tunnel through the bastion host to the ECS Service Discovery endpoint.

```
Local machine ──SSM tunnel──> Bastion ──> ECS Service Discovery ──> Admin API
```

Two layers of auth:
1. **IAM** — controls who can establish the SSM tunnel (who can be an admin)
2. **API key** — controls what operations they can perform

## Setup

### Local Development

No tunnel needed. The CLI talks directly to `localhost:8000` (Docker).

```bash
# Ensure .env.local has ADMIN_API_KEY
just admin dev stats
```

### Staging / Production

Start an SSM tunnel in one terminal, run commands in another:

```bash
# Terminal 1: start tunnel
./bin/tools/tunnels.sh prod api-internal

# Terminal 2: run commands
just admin prod stats
just admin staging subscriptions list
```

The admin API key is automatically retrieved from AWS Secrets Manager (`robosystems/{env}/admin`).

## Command Reference

All commands use the format: `just admin <env> <group> <command> [options]`

### stats

System-wide overview of subscriptions, customers, and MRR.

```bash
just admin dev stats
```

### subscriptions

```bash
just admin dev subscriptions list                          # List all
just admin dev subscriptions list --status active          # Filter by status
just admin dev subscriptions list --tier ladybug-large     # Filter by tier
just admin dev subscriptions list --email user@example.com # Filter by email
just admin dev subscriptions list --include-canceled       # Include canceled
just admin dev subscriptions get SUBSCRIPTION_ID           # Get details
just admin dev subscriptions create USER_ID --resource-type graph --resource-id GRAPH_ID --plan-name ladybug-standard
just admin dev subscriptions update SUBSCRIPTION_ID --status active --plan-name ladybug-large
just admin dev subscriptions audit SUBSCRIPTION_ID         # Audit log
just admin dev subscriptions audit SUBSCRIPTION_ID --event-type PAYMENT_FAILED
```

Statuses: `pending_payment`, `provisioning`, `active`, `past_due`, `unpaid`, `canceled`, `failed`

### invoices

```bash
just admin dev invoices list                               # List all
just admin dev invoices list --status open                 # Filter by status
just admin dev invoices list --user-id USER_ID             # Filter by user
just admin dev invoices get INVOICE_ID                     # Get details
just admin dev invoices mark-paid INVOICE_ID \
  --payment-method "wire_transfer" \
  --payment-reference "REF-123456"                         # Manual payment
```

Statuses: `open`, `paid`, `void`, `uncollectible`

### credits

Graph credit pools (AI operation billing):

```bash
just admin dev credits list                                # List all pools
just admin dev credits list --low-balance                  # Low balance only
just admin dev credits list --user-email user@example.com  # Filter by email
just admin dev credits get GRAPH_ID                        # Get pool details
just admin dev credits bonus GRAPH_ID --amount 10000 --description "Q1 bonus"
just admin dev credits analytics                           # Usage analytics
just admin dev credits analytics --tier ladybug-large      # Filter by tier
just admin dev credits health                              # System health check
```

Repository credit pools (shared repo access):

```bash
just admin dev credits repos list                          # List all repo pools
just admin dev credits repos list --repository-type sec    # Filter by repo
just admin dev credits repos get USER_REPOSITORY_ID        # Get pool details
just admin dev credits repos bonus USER_REPOSITORY_ID --amount 500 --description "bonus"
```

### graphs

```bash
just admin dev graphs list                                 # List all
just admin dev graphs list --user-email user@example.com   # Filter by owner
just admin dev graphs list --tier ladybug-large            # Filter by tier
just admin dev graphs list --backend ladybug               # Filter by backend
just admin dev graphs get GRAPH_ID                         # Get details
just admin dev graphs analytics                            # Cross-graph stats
```

### users

```bash
just admin dev users list                                  # List all
just admin dev users list --email example.com              # Filter by email
just admin dev users list --verified-only                  # Verified only
just admin dev users get USER_ID                           # Get details
just admin dev users graphs USER_ID                        # User's graphs
just admin dev users activity USER_ID                      # Recent activity
just admin dev users activity USER_ID --days 30            # Extended period
```

### orgs

```bash
just admin dev orgs list                                   # List all
just admin dev orgs get ORG_ID                             # Get details
just admin dev orgs update ORG_ID --name "New Name"        # Update org
```

### cache

Valkey cache inspection and management:

```bash
just admin dev cache info                                  # Overview of all databases
just admin dev cache info auth                             # Single database detail
just admin dev cache keys auth --pattern "apikey:*"        # List matching keys
just admin dev cache flush auth                            # Flush single database
just admin dev cache flush all -y                          # Flush all (skip confirm)
just admin dev cache delete-keys auth --pattern "old:*"    # Delete matching keys
```

Database names: `auth`, `rate-limits`, `credits`, `billing`, `sse`, `locks`, `graph-routing`, `task-state`

### migrations

Database migrations via Alembic. Runs locally in dev, remotely on the bastion host via SSM in staging/prod.

```bash
just admin dev migrations up                               # Apply pending
just admin dev migrations down                             # Rollback one
just admin dev migrations current                          # Show version
```

## Common Workflows

### Enable Invoice Billing for Enterprise Customer

```bash
# Update org to enable invoice billing
just admin dev orgs update ORG_ID \
  --invoice-billing \
  --billing-email "ap@enterprise.com" \
  --payment-terms "net_30"

# Verify
just admin dev orgs get ORG_ID
```

### Handle Failed Payment

```bash
# Find unpaid subscriptions
just admin dev subscriptions list --status unpaid

# Check invoices
just admin dev invoices list --user-id USER_ID --status open

# Mark paid after receiving wire transfer
just admin dev invoices mark-paid INVOICE_ID \
  --payment-method "wire_transfer" \
  --payment-reference "WIRE-20250107-123"

# Reactivate subscription
just admin dev subscriptions update SUBSCRIPTION_ID --status active
```

### Monitor Credit Health

```bash
just admin prod credits health                             # System health
just admin prod credits analytics                          # Top consumers
just admin prod credits list --low-balance                 # At-risk pools
just admin prod credits bonus GRAPH_ID --amount 5000 --description "retention"
```

### Production Migration

```bash
just admin prod migrations current                         # Check version
just admin prod migrations up                              # Apply migrations
just admin prod migrations current                         # Verify
```

### Emergency Cache Flush

After deploying changes that alter cache key formats or database numbering:

```bash
just admin prod cache info                                 # Check state
just admin prod cache flush auth -y                        # Flush specific DB
just admin prod cache flush all -y                         # Nuclear option
```

## Troubleshooting

**"Connection refused"** — API isn't running. Dev: `just restart`. Staging/prod: check SSM tunnel is active.

**"403 Forbidden"** — Admin API key is wrong or missing. Dev: check `ADMIN_API_KEY` in `.env.local`. Staging/prod: check AWS Secrets Manager.

**"SSM agent not responding"** — Bastion may be stopped. The tunnel script auto-starts it, but SSM agent takes ~30s to initialize.

**Finding IDs** — User IDs look like `user_R_Bq7hZ4tyEVk8qa6u_UmQ`, subscription IDs like `sub_123e4567-...`. Use `list` commands to find them.
