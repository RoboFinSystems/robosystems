# Grafana Dashboard Templates

This directory contains generalized Grafana dashboard templates that can be customized for different deployments.

## Dashboard Files

- **cur.json** - Cost and Usage Report (CUR) dashboard for AWS cost analysis
- **nonprod.json** - Non-production environment monitoring dashboard
- **prod.json** - Production environment monitoring dashboard

## Template Variables

The dashboards use the following template variables that should be replaced during deployment:

### System Variables
- `${SYSTEM_NAME}` - The system name in proper case (e.g., "RoboSystems")
- `${SYSTEM_NAME_LOWER}` - The system name in lowercase (e.g., "robosystems")
- `${ENVIRONMENT}` - The environment name (e.g., "prod", "staging", "dev")
- `${AWS_REGION}` - AWS region (e.g., "us-east-1")

### Datasource UIDs
- `${DS_ATHENA}` - UID for the AWS Athena datasource
- `${DS_PROMETHEUS}` - UID for the Prometheus/AMP datasource
- `${DS_CLOUDWATCH}` - UID for the CloudWatch datasource
- `${DS_DEFAULT}` - UID for any other datasource type

### AWS Athena Specific
- `${ATHENA_CUR_DATABASE}` - Athena database name for Cost and Usage Reports
- `${CUR_TABLE_NAME}` - Table name containing CUR data

## Usage

1. Replace all template variables with your actual values
2. Import the dashboard JSON into Grafana
3. Verify datasource connections
4. Update any query filters or time ranges as needed

## Dashboard Descriptions

### Cost and Usage Dashboard (cur.json)
Monitors AWS costs broken down by:
- Daily cost trends
- Cost by component (using resource tags)
- Cost by environment
- Cost by AWS service
- Support for filtering by environment and component tags

### Non-Production Monitoring (nonprod.json)
Monitors staging and development environments:
- Kuzu database metrics
- API performance metrics
- Worker queue metrics
- Error rates and alerts

### Production Monitoring (prod.json)
Production-specific monitoring with:
- Enhanced Kuzu database metrics
- API latency and throughput
- Worker performance and backlogs
- Critical error tracking
- Auto-scaling metrics

## Required Tags

For proper cost allocation, ensure your AWS resources are tagged with:
- `user:component` - Component identifier (e.g., "api", "worker", "kuzu")
- `user:environment` - Environment name (e.g., "prod", "staging", "dev")

## Notes

- Dashboards are configured for `browser` timezone by default
- All panels support time range selection via Grafana's time picker
- Variables are multi-select enabled where appropriate
- Queries include cost optimization logic for Savings Plans and Reserved Instances