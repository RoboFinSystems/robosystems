"""
RoboSystems Celery Tasks.

This module imports all task modules to ensure they are registered with Celery.
Tasks are organized by functional domain:

- graph_operations: Graph database SSE operations (create, backup)
- table_operations: DuckDB staging and materialization
- agents: Real-time AI agent analysis

Note: Billing, infrastructure, data_sync, provisioning, and SEC XBRL tasks
have been migrated to Dagster. See robosystems/dagster/ for:
- Scheduled jobs (billing, health checks)
- Sensor-triggered jobs (provisioning)
- Data pipeline assets (Plaid, QuickBooks, SEC)
"""

# Import all task modules to register them with Celery
from . import graph_operations  # noqa
from . import table_operations  # noqa
from . import agents  # noqa
