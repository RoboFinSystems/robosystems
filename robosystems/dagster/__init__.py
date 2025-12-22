"""RoboSystems Dagster orchestration.

This module provides Dagster-based data orchestration for:
- Billing and infrastructure scheduled tasks
- SEC EDGAR data pipeline
- QuickBooks data sync
- Plaid banking integration

The Dagster setup replaces background tasks for all orchestration needs, providing:
- Better observability through the Dagster UI
- Asset-based data lineage tracking
- Declarative scheduling and sensors
- Unified monitoring for all pipeline activity
"""

from robosystems.dagster.definitions import defs

__all__ = ["defs"]
