# Admin CLI

Remote administration for the platform: subscriptions, invoices, credits, graphs, users, organizations, SCIM provisioning, cache, instances, search indices, worker queues, and database migrations.

## How it works

The CLI talks to the Admin API (`/admin/v1/*`), which is **blocked at the ALB** and not reachable from the public internet. Staging and production access requires an SSM tunnel through the bastion host to the ECS Service Discovery endpoint.

```
Local machine ──SSM tunnel──> Bastion ──> ECS Service Discovery ──> Admin API
```

Two layers of authorization: **IAM** controls who can open the tunnel (who can be an admin), and the **admin API key** controls what they can do once connected.

## Setup

### Local development

No tunnel needed — the CLI talks to `localhost:8000` directly. Make sure `.env.local` has `ADMIN_API_KEY`.

```bash
just admin dev stats
```

### Staging / production

Open a tunnel in one terminal, run commands in another:

```bash
# Terminal 1
just tunnel prod api-internal

# Terminal 2
just admin prod stats
just admin staging subscriptions list
```

The admin API key is retrieved automatically from AWS Secrets Manager (`robosystems/{env}/admin`).

## Commands

Every command follows `just admin <env> <group> <command> [options]`, where `<env>` is `dev`, `staging`, or `prod`.

### stats

System-wide overview of subscriptions, customers, and MRR.

```bash
just admin dev stats
```

### subscriptions

```bash
just admin dev subscriptions list                          # List all
just admin dev subscriptions list --status active
just admin dev subscriptions list --resource-type graph
just admin dev subscriptions list --email user@example.com
just admin dev subscriptions list --include-canceled
just admin dev subscriptions get SUBSCRIPTION_ID
just admin dev subscriptions create --org-id ORG_ID --resource-id GRAPH_ID --plan-name ladybug-standard
just admin dev subscriptions update SUBSCRIPTION_ID --status active --plan-name ladybug-large
just admin dev subscriptions audit SUBSCRIPTION_ID
just admin dev subscriptions audit SUBSCRIPTION_ID --event-type PAYMENT_FAILED
```

Statuses: `pending_payment`, `provisioning`, `active`, `past_due`, `unpaid`, `canceled`, `failed`

### invoices

```bash
just admin dev invoices list
just admin dev invoices list --status open
just admin dev invoices list --user-id USER_ID
just admin dev invoices get INVOICE_ID
just admin dev invoices mark-paid INVOICE_ID \
  --payment-method "wire_transfer" \
  --payment-reference "REF-123456"
```

Statuses: `open`, `paid`, `void`, `uncollectible`

### credits

Graph credit pools (AI operation billing):

```bash
just admin dev credits list
just admin dev credits list --low-balance
just admin dev credits list --user-email user@example.com
just admin dev credits get GRAPH_ID
just admin dev credits bonus GRAPH_ID --amount 10000 --description "Q1 bonus"
just admin dev credits reset GRAPH_ID --reason "..."   # forfeits balance, refills to allocation
just admin dev credits analytics
just admin dev credits analytics --tier ladybug-large
just admin dev credits health
```

Repository credit pools (shared repository access):

```bash
just admin dev credits repos list
just admin dev credits repos list --repository-type sec
just admin dev credits repos get USER_REPOSITORY_ID
just admin dev credits repos bonus USER_REPOSITORY_ID --amount 500 --description "bonus"
```

### graphs

```bash
just admin dev graphs list
just admin dev graphs list --user-email user@example.com
just admin dev graphs list --tier ladybug-large
just admin dev graphs list --backend ladybug
just admin dev graphs get GRAPH_ID
just admin dev graphs analytics
just admin dev graphs deprovision GRAPH_ID --skip-backup   # tears down infrastructure
```

### users

```bash
just admin dev users list
just admin dev users list --email example.com
just admin dev users list --verified-only
just admin dev users get USER_ID
just admin dev users graphs USER_ID
just admin dev users activity USER_ID
just admin dev users deactivate USER_OR_EMAIL
just admin dev users activate USER_OR_EMAIL
just admin dev users delete USER_OR_EMAIL --dry-run
```

`users deactivate` is the support-plane response short of deletion; it takes a user ID or an email, as `activate` and `delete` do.

`users delete` takes a user ID or an email. It frees the email address but retains billing and audit history, and refuses while the user's org still has live graphs, subscriptions in force, or active repository access.

### orgs

```bash
just admin dev orgs list
just admin dev orgs get ORG_ID
just admin dev orgs update ORG_ID \
  --invoice-billing \
  --billing-email "ap@example.com" \
  --billing-contact-name "Accounts Payable" \
  --payment-terms "net_30" \
  --max-graphs 25
```

### scim

SCIM provisioning for dedicated tenants. `bootstrap` creates-or-reuses the
enterprise org and mints the bearer token the customer's IdP presents — the raw
token prints once and is never recoverable, so paste it into the IdP connector
immediately. Pass either `--org-id` (attach to an existing org) or `--org-name`
(create a new `ENTERPRISE` org).

```bash
just admin prod scim bootstrap --org-name "Acme Inc"
just admin prod scim bootstrap --org-id ORG_ID --token-name scim-provisioning --expires-in-days 365
just admin prod scim revoke-token TOKEN_ID
```

### cache

Valkey inspection and management. Database names are the kebab-case form of the `ValkeyDatabase` enum members in `config/valkey_registry.py`: `auth`, `rate-limits`, `graph-routing`, `sse`, `locks`, `mcp-cache`, `worker-queue`, `operation-idempotency`.

```bash
just admin dev cache info                                  # Overview of all databases
just admin dev cache info auth                             # Single database detail
just admin dev cache keys auth --pattern "apikey:*"
just admin dev cache flush auth
just admin dev cache flush all -y                          # Skip confirmation
just admin dev cache delete-keys auth --pattern "old:*"
```

### instances

LadybugDB fleet inspection and scaling.

```bash
just admin prod instances list
just admin prod instances info INSTANCE_ID
just admin prod instances scale TIER DESIRED --min 1 --max 4   # also syncs GHA variables
just admin prod instances cleanup
```

### search

OpenSearch index inspection and maintenance. `--graph-id` defaults to `sec`.

```bash
just admin prod search count --graph-id GRAPH_ID
just admin prod search query "revenue recognition" --graph-id GRAPH_ID --size 10
just admin prod search delete SOURCE_TYPE --graph-id GRAPH_ID --before 2025-01-01
just admin prod search force-merge          # compact segments after a delete
```

### worker

Background worker queue inspection and dead-letter handling.

```bash
just admin prod worker status
just admin prod worker dlq list
just admin prod worker dlq retry
just admin prod worker dlq clear
```

### migrations

Alembic migrations — run locally in dev, remotely on the bastion host via SSM in staging and production.

```bash
just admin dev migrations up
just admin dev migrations down
just admin dev migrations current
```

## Workflows

### Handle a failed payment

```bash
just admin prod subscriptions list --status unpaid
just admin prod invoices list --user-id USER_ID --status open
just admin prod invoices mark-paid INVOICE_ID \
  --payment-method "wire_transfer" \
  --payment-reference "WIRE-20250107-123"
just admin prod subscriptions update SUBSCRIPTION_ID --status active
```

### Monitor credit health

```bash
just admin prod credits health          # System health
just admin prod credits analytics       # Top consumers
just admin prod credits list --low-balance
just admin prod credits bonus GRAPH_ID --amount 5000 --description "retention"
```

### Cache flush after a key-format change

```bash
just admin prod cache info
just admin prod cache flush auth -y
just admin prod cache flush all -y      # Nuclear option
```

## Troubleshooting

**"Connection refused"** — the API isn't reachable. In dev, `just restart`. In staging/prod, check that the SSM tunnel is still up.

**"403 Forbidden"** — the admin API key is wrong or missing. In dev, check `ADMIN_API_KEY` in `.env.local`. In staging/prod, check AWS Secrets Manager.

**"SSM agent not responding"** — the bastion may be stopped. The tunnel script starts it automatically, but the SSM agent takes roughly 30 seconds to initialize.

**Finding IDs** — user IDs look like `user_R_Bq7hZ4tyEVk8qa6u_UmQ`, subscription IDs like `sub_123e4567-…`. Use the `list` command in the relevant group.
