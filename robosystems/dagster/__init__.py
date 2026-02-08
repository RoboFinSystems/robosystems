"""RoboSystems Dagster orchestration.

This module provides Dagster-based data orchestration for:
- Billing and infrastructure scheduled tasks
- SEC EDGAR data pipeline
- User graph operations

The Dagster setup replaces background tasks for all orchestration needs, providing:
- Better observability through the Dagster UI
- Asset-based data lineage tracking
- Declarative scheduling and sensors
- Unified monitoring for all pipeline activity

Note: `defs` is lazily imported to avoid circular imports.
The adapter pipeline (adapters/sec/pipeline/) imports dagster.resources,
which would trigger this __init__.py. Lazy loading breaks the cycle.
"""


def __getattr__(name: str):
  if name == "defs":
    from robosystems.dagster.definitions import defs

    return defs
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["defs"]
