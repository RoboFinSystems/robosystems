"""
RoboSystems Celery Tasks.

This module imports all task modules to ensure they are registered with Celery.
Tasks are organized by functional domain:

- data_sync: External API synchronization (SEC, QuickBooks, Plaid)
- billing: Credit management and usage billing
- graph_operations: Graph database operations
- infrastructure: Infrastructure monitoring and maintenance

Note: Monitoring tasks have been migrated to Lambda functions (worker-monitor)
Note: Processing tasks have been removed - agent tasks now in operations.agents
"""

# Import all task modules to register them with Celery
from . import data_sync  # noqa
from . import billing  # noqa
from . import graph_operations  # noqa
from . import infrastructure  # noqa
