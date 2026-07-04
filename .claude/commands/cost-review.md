Run a read-only AWS cost & usage review, then produce a cost report and a prioritized savings plan ranked by dollar impact. Pairs with the `cost-analysis-cur-athena` (the $ side) and `prometheus-usage-amp-cli` (the usage-driver side) runbooks for the account-specific query detail.

## Scope & guardrails

- **Read-only.** `describe-*` / `list-*` / `get-*`, Cost Explorer (`ce`), and Athena reads only; never `get-secret-value`. Any change (rightsizing, deleting an idle resource, buying a Savings Plan) needs explicit user confirmation, and destructive actions are the user's to run.
- **Output is business-confidential — never commit it.** This skill (the *methodology*) is public-safe; its *output* is your **unit economics and margins** (effective cost net of SP/RI is literally the pricing number). Write reports to a git-ignored path (`local/`, scratchpad) or a private Artifact — never to the repo, and don't paste cost figures anywhere shareable.
- **Effective cost, not headline cost.** Cost Explorer's `AmortizedCost`/`NetAmortizedCost` *net out promotional credits*, hiding structural cost. For unit economics, compute **effective cost net of SP/RI discounts but BEFORE credits** — via the CUR in Athena (see the runbook). CE is fine for a quick total; the CUR is the source of truth for structure.

## Phase 1 — Cost overview & anomalies

```bash
aws ce get-cost-and-usage --time-period Start=<mo-start>,End=<mo-end> --granularity MONTHLY \
  --metrics UnblendedCost --group-by Type=DIMENSION,Key=SERVICE --region us-east-1
aws ce get-anomalies --date-interval StartDate=<start>,EndDate=<end> --region us-east-1
```
Then the **effective-cost breakdown by component × environment** from the CUR via Athena — port the canonical queries from the `cost-analysis-cur-athena` runbook (it holds the exact catalog/table/partitions).

## Phase 2 — Rightsizing

`aws compute-optimizer get-*-recommendations` for ECS/Fargate services, RDS, EC2 (LadybugDB tiers), and ElastiCache — look for over-provisioned CPU/memory and downsizing candidates. Cross-check against admission-control/headroom needs before acting.

## Phase 3 — Idle & orphaned (fast wins)

- Unattached EBS volumes, old/orphaned snapshots, stopped-but-billing instances, idle load balancers, unassociated EIPs, orphaned ENIs.
- **Stale ECR images** (ties to the lifecycle policy — `describe-images`, check the lifecycle rules); **orphaned S3 buckets** (a public/retained bucket with no references is cost *and* security — see `/security-audit`).
- Old RDS manual snapshots, unused KMS keys.

## Phase 4 — Commitment coverage

`aws ce get-savings-plans-coverage` / `get-reservation-coverage` and `get-savings-plans-purchase-recommendation`. Check **Fargate Spot** weighting (Spot-preferred here) and **ARM/Graviton** adoption (already the default — flag any x86 stragglers).

## Phase 5 — Storage, logs & transfer

- **S3**: storage class distribution, Intelligent-Tiering, lifecycle rules, old-version bloat.
- **CloudWatch Logs**: per-log-group retention + ingested volume (a common silent cost).
- **Data transfer**: NAT Gateway data-processing vs VPC endpoints (`VPC_ENDPOINT_MODE`), CloudFront egress.

## Phase 6 — Usage drivers (the "why is it expensive" side)

- **Amazon Managed Prometheus** active-series cardinality + ingestion — the top cost driver for observability. The `aws` CLI can't run PromQL; use the SigV4 signing helpers + canonical cardinality queries in the `prometheus-usage-amp-cli` runbook (top metrics by series, label drill-down, deploy-sawtooth trend).
- High-cardinality metrics/labels, log volume per service, per-tier graph compute.

## Phase 7 — Tagging & allocation

Untagged/mis-tagged resources (break cost attribution), and confirm the Cost & Usage Report (the `RoboSystemsCUR` stack) is delivering.

## Output — report + savings plan

1. **Cost report** — effective cost by component × environment, trend, and the top cost drivers (deduped: a few services usually dominate).
2. **Prioritized savings plan** — ranked by **estimated $/month saved × effort**, each item with the concrete action and any risk (e.g. rightsizing vs headroom). Separate *free wins* (delete idle, fix retention, prune images) from *commitment decisions* (Savings Plans) from *architecture* (endpoint mode, tiering).

Keep the framing on **structural/effective cost** (unit economics), not credit-masked totals. Offer to render a private cost dashboard Artifact if useful.
