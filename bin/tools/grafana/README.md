# Grafana Dashboard Templates

Portable Grafana dashboard exports with templated datasource UIDs for easy import into any Grafana instance.

## Files

| File | Description |
|------|-------------|
| `ops.json` | Platform observability (API, ECS, RDS, Valkey, Prometheus metrics) |
| `logs.json` | CloudWatch logs: ingest per log group, errors and security events by service, API failures and slow requests, Graph API query latency, live streams |
| `cur.json` | AWS Cost and Usage Report dashboard |

## Template Variables

Dashboards use Grafana template variables for datasources and environment selection:

| Variable | Type | Used In | Description |
|----------|------|---------|-------------|
| `${prometheus}` | Prometheus | ops | Amazon Managed Prometheus |
| `${cloudwatch}` | CloudWatch | ops, logs | AWS CloudWatch metrics and Logs Insights |
| `${athena}` | Athena | cur | AWS Athena for CUR data |
| `${env}` | Custom | ops, logs | Environment selector (`prod`, `staging`) |
| `${level}` | Custom | logs | `ERROR`, `WARNING`, `INFO` or All, applied to the log panels |
| `${search}` | Textbox | logs | Regular expression matched against each line in the log panels |
| `${log_groups}`, `${log_group_names}` | Query | logs | Hidden: the log groups under `/robosystems/${env}/`, as ARNs for Logs Insights and as names for the ingest metrics |
| `${rds_instance}`, `${api_alb}`, … | Query | ops | Hidden: resource lookups scoped by `${env}` and `${graph_tier}` (RDS instance, API load balancer, ECS clusters and services, OpenSearch domain, graph ASGs); the panels and alarm annotations read their dimensions from these |
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
billed it (component) or shown as `shared` (environment).

## Alarm Annotations (ops)

`ops.json` carries CloudWatch alarm state changes as dashboard annotations, so an alarm
transition draws a marker on the panels that graph the metric it watches. They need only the
CloudWatch datasource; its role must allow `cloudwatch:DescribeAlarms`,
`DescribeAlarmsForMetric` and `DescribeAlarmHistory` (the Grafana stack grants these).

- Alarms with dimensions (RDS, OpenSearch, graph writers, the API load balancer, the graph
  fleet) are matched exactly: namespace, metric, dimensions, statistic and period must equal
  the alarm's definition, with the dimension values coming from the hidden lookup variables.
- Dimension-less alarms in custom namespaces (Worker, Dagster) use prefix matching on the alarm
  name, filtered by the `${env}`-scoped namespace and statistic. The SNS action prefix keeps
  autoscaling alarms, which rest in ALARM by design, out of the set. Grafana never substitutes
  variables in the name prefix itself, so a prefix must not embed the environment.
- The annotation toggles are hidden to keep the header clean; enable, disable or show them under
  Dashboard settings, Annotations. CloudWatch keeps 30 days of alarm history, and each transition
  renders twice (the state change and the notification action).
- Alarms built on metric math (the volume disk alarms) and the security alarms, whose names put
  the environment before the prefix, have no annotation.

## Logs Setup

The `logs.json` dashboard needs only the CloudWatch datasource. The CloudFormation templates
create one log group per service under `/robosystems/${Environment}/` (`api`, `worker`,
`dagster`, `graph-api`, plus `audit-firehose` and `bastion-host`); the dashboard discovers
whatever exists under that prefix, so a stack that is not deployed shows an empty panel
rather than an error.

- The error, security, API and Graph API panels parse the structured JSON lines the services
  write (`level`, `component`, `action`, `status_code`, `duration_ms`, `user_id`,
  `request_id`) and fall back to the level word for text lines such as Dagster's own output.
- Every Logs Insights panel scans the selected time range on each load, and CloudWatch bills
  that per GB scanned. The dashboard defaults to 24 hours; widen the range deliberately.
- **Audit Delivery Failures** counts subscription-filter deliveries that failed or were
  throttled. The API group's filter forwards security and operation audit records to the
  audit Firehose, so anything above zero means audit records were not delivered.

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
3. Clear the cached selections of every query variable so account resource names and the
   account id stay out of the template; they are re-resolved on dashboard load:

   ```bash
   jq '.id = null
       | (.templating.list[] | select(.type == "query") | .current) = {selected: false, text: "", value: ""}
       | (.templating.list[] | select(.type == "query") | .options) = []' ops.json > ops.tmp && mv ops.tmp ops.json
   ```
