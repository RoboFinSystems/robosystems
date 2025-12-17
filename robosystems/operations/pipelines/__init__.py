"""
Pipeline Operations - Migrated to Dagster

Data pipelines have been migrated to Dagster for improved orchestration,
observability, and scalability.

See:
- robosystems/dagster/assets/sec.py - SEC XBRL pipeline
- robosystems/dagster/jobs/sec.py - SEC jobs and schedules

For local development:
  just sec-load NVDA 2025    # Load company via Dagster pipeline
"""

__all__: list[str] = []
