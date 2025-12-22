# Dagster Orchestration

This directory contains the Dagster-based orchestration system for all scheduled and event-driven tasks.

## Directory Structure

```
dagster/
├── README.md              # This file
├── __init__.py            # Module exports
├── definitions.py         # Main Dagster entry point
├── resources/             # Shared infrastructure resources
│   ├── database.py        # PostgreSQL resource
│   ├── storage.py         # S3 resource
│   └── graph.py           # LadybugDB graph resource
├── jobs/                  # Job definitions
│   ├── billing.py         # Credit allocation, storage billing
│   └── infrastructure.py  # Auth cleanup, health checks
├── sensors/               # Event-driven triggers
│   └── provisioning.py    # Subscription provisioning sensor
└── assets/                # Data pipeline assets (Phase 2-4)
    └── __init__.py        # SEC, QuickBooks, Plaid assets
```

## Quick Start

### Local Development

```bash
# Start Dagster development server
uv run dagster dev -m robosystems.dagster

# Access UI at http://localhost:3000
```

### Running Jobs Manually

```bash
# Run a specific job
uv run dagster job execute -m robosystems.dagster -j monthly_credit_allocation_job

# Run with config
uv run dagster job execute -m robosystems.dagster -j daily_storage_billing_job \
  -c '{"ops": {"bill_storage_credits": {"config": {"target_date": "2025-12-15"}}}}'
```

## Jobs Overview

### Billing Jobs

| Job                             | Schedule               | Description                                   |
| ------------------------------- | ---------------------- | --------------------------------------------- |
| `monthly_credit_allocation_job` | 1st of month, midnight | Process overages and allocate monthly credits |
| `daily_storage_billing_job`     | Daily at 2 AM          | Bill storage usage credits                    |
| `hourly_usage_collection_job`   | Every hour at :05      | Collect storage snapshots                     |
| `monthly_usage_report_job`      | 2nd of month, 6 AM     | Generate usage reports                        |

### Infrastructure Jobs

| Job                       | Schedule        | Description                 |
| ------------------------- | --------------- | --------------------------- |
| `hourly_auth_cleanup_job` | Every hour      | Clean up expired API keys   |
| `weekly_health_check_job` | Mondays at 3 AM | Credit system health checks |

## Resources

Resources provide shared infrastructure to jobs and assets:

```python
from robosystems.dagster.resources import DatabaseResource, S3Resource, GraphResource

@op
def my_op(context, db: DatabaseResource, s3: S3Resource):
    with db.get_session() as session:
        # Database operations
        pass

    s3.upload_file(file_obj, "path/to/file.parquet")
```

## Sensors

Sensors watch for conditions and trigger jobs:

- **`pending_subscription_sensor`**: Watches for subscriptions in "provisioning" status and triggers graph creation

## Custom Data Sources (Fork-Friendly)

When forking RoboSystems, add custom data pipelines in the `custom_*` namespace:

1. Create adapter: `adapters/custom_myservice/` (client + processors)
2. Create assets: `dagster/assets/custom_myservice.py`
3. Register in `definitions.py`

The `custom_*` namespace ensures upstream updates never conflict with your additions. See [Adapters README](../adapters/README.md#fork-friendly-custom-adapters) for details.

## Related Documentation

- [Dagster Documentation](https://docs.dagster.io/) - Official Dagster docs
- [Adapters README](../adapters/README.md) - External service integrations
