# AWS Grafana Dashboard Exports

Reference exports from Amazon Managed Grafana with templated datasource UIDs.

## Files

| File | Description |
|------|-------------|
| `prod.json` | Production environment monitoring |
| `staging.json` | Staging environment monitoring |
| `cur.json` | Cost and Usage Report dashboard |

## Template Variables

These dashboards use Grafana's `${DS_*}` variable syntax for datasources:

| Variable | Type | Description |
|----------|------|-------------|
| `${DS_CLOUDWATCH}` | CloudWatch | AWS CloudWatch datasource |
| `${DS_PROMETHEUS}` | Prometheus | Amazon Managed Prometheus |
| `${DS_ATHENA}` | Athena | AWS Athena (CUR dashboard only) |

When importing, Grafana will prompt you to map these to your actual datasources.

## Usage

1. Open Amazon Managed Grafana workspace
2. Go to Dashboards > Import
3. Upload or paste the JSON
4. Map datasources when prompted:
   - Select your CloudWatch datasource for `${DS_CLOUDWATCH}`
   - Select your Prometheus datasource for `${DS_PROMETHEUS}`
   - Select your Athena datasource for `${DS_ATHENA}` (CUR only)

## Datasources Required

- **CloudWatch**: AWS CloudWatch (usually auto-configured)
- **Prometheus**: Amazon Managed Prometheus workspace
- **Athena**: For CUR dashboard - requires CUR with Athena integration (see below)

## CUR Setup (Cost and Usage Reports)

The `cur.json` dashboard requires AWS Cost and Usage Reports configured with Athena integration.
This is a one-time setup done via AWS Console (not managed via CloudFormation).

### Setup Steps

1. Go to **AWS Billing Console** > **Cost & Usage Reports**
2. Create a new report with these settings:
   - Report name: `RoboSystemsCostAndUsage`
   - S3 bucket: `robosystems-cur`
   - Report path prefix: `cur`
   - Enable **Athena integration** (creates Glue database automatically)
3. AWS generates a CloudFormation template - run it to create:
   - Glue database: `athenacurcfn_robo_systems_cost_and_usage`
   - Glue crawler for automatic table updates
   - Lambda triggers for S3 notifications
4. Configure Athena datasource in Grafana pointing to the Glue database

### Required Tags for Cost Allocation

Ensure AWS resources are tagged for the dashboard filters:
- `user:component` - Component identifier (e.g., `api`, `worker`, `ladybug`)
- `user:environment` - Environment name (e.g., `prod`, `staging`)

## Updating

When exporting updated dashboards from Grafana:

1. Open dashboard > Settings (gear icon) > JSON Model
2. Copy JSON and save to this directory
3. Replace hardcoded datasource UIDs with template variables:
   - CloudWatch UID → `${DS_CLOUDWATCH}`
   - Prometheus UID → `${DS_PROMETHEUS}`
   - Athena UID → `${DS_ATHENA}`
4. Set root `id` and `uid` to `null`
