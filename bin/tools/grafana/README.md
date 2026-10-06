# Grafana Dashboard Templates

Portable Grafana dashboard exports with templated datasource UIDs for easy import into any Grafana instance.

## Files

| File | Description |
|------|-------------|
| `ops.json` | Platform observability (API, ECS, RDS, Valkey, Prometheus metrics) |
| `logs.json` | CloudWatch logs (API, Dagster, Graph API) |
| `cur.json` | AWS Cost and Usage Report dashboard |

## Template Variables

Dashboards use Grafana template variables for datasources and environment selection:

| Variable | Type | Used In | Description |
|----------|------|---------|-------------|
| `${prometheus}` | Prometheus | ops | Amazon Managed Prometheus |
| `${cloudwatch}` | CloudWatch | ops | AWS CloudWatch metrics |
| `${datasource}` | CloudWatch | logs | CloudWatch datasource |
| `${athena}` | Athena | cur | AWS Athena for CUR data |
| `${env}` | Custom | ops, logs | Environment selector (`prod`, `staging`) |
| `${cur_table}` | Constant | cur | Athena table the CUR is crawled into (hidden; set once after import) |
| `${granularity}` | Custom | cur | Bucket size for the cost time series: daily, weekly or monthly |

The datasource variables appear as dropdowns at the top of each dashboard; pick your
datasource there after import. The filter variables (`environment`, `component`, ...) treat
**All** as "no filter", so a value missing from the dropdown is never silently excluded.

## Usage

1. Open Grafana workspace
2. Go to Dashboards > Import
3. Upload or paste the JSON
4. Map datasources when prompted

## Datasources Required

- **Prometheus**: Amazon Managed Prometheus workspace
- **CloudWatch**: AWS CloudWatch (usually auto-configured)
- **Athena**: For CUR dashboard - requires CUR with Athena integration

## CUR Setup (Cost and Usage Reports)

The `cur.json` dashboard requires AWS Cost and Usage Reports configured with Athena integration.

### Setup Steps

1. Go to **AWS Billing Console** > **Cost & Usage Reports**
2. Create a new report with these settings:
   - Enable **Athena integration** (creates Glue database automatically)
3. AWS generates a CloudFormation template - run it to create:
   - Glue database
   - Glue crawler for automatic table updates
   - Lambda triggers for S3 notifications
4. Configure the Athena datasource in Grafana with the Glue database, catalog, region and
   workgroup as its defaults; every panel queries through those defaults
5. After importing the dashboard, set the `cur_table` constant (Dashboard settings >
   Variables) to the crawled table name, e.g. `robosystemscostandusage`

### Required Tags for Cost Allocation

Ensure AWS resources are tagged for the dashboard filters:
- `user:component` - Component identifier (e.g., `api`, `worker`, `ladybug`)
- `user:environment` - Environment name (e.g., `prod`, `staging`)

Lines without a tag are not dropped: untagged spend is attributed to the AWS product that
billed it (component) or shown as `untagged` (environment).

### Cost Methodology

Every panel reports structural, effective cost: Savings Plan and Reserved Instance
discounts applied, commitment fees counted only for their unused portion, promotional
credits and refunds excluded. The overview adds a **Net After Credits** tile for what was
actually billed. RI fees are excluded because they book once per month and would spike the
first day of a running month. The same CASE expression, from AWS's CUR query library, is
used by all panels, so every breakdown sums to the Period Cost tile.

## Updating Dashboards

When exporting updated dashboards from Grafana:

1. Open dashboard > Settings (gear icon) > JSON Model
2. Copy JSON and save to this directory
