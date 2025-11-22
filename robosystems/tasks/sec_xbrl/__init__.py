"""
SEC XBRL Processing and Ingestion Pipeline

This package contains all SEC-specific tasks for the XBRL processing pipeline:
- Entity discovery and filing collection
- XBRL processing (parallel)
- LadybugDB database ingestion (direct from processed files)

The pipeline follows this architecture:
1. Entity discovery and filing collection
2. XBRL processing (parallel) → processed/year={year}/
3. LadybugDB ingestion (direct from processed files using wildcards)
"""

from .ingestion import ingest_sec_data
from .maintenance import reset_sec_database, full_reset_for_year
from .orchestration import (
  plan_phased_processing,
  start_phase,
  get_phase_status,
  download_company_filings,
  process_company_filings,
  smart_retry_failed_companies,
  get_pipeline_metrics,
  cleanup_phase_connections,
  handle_phase_completion,
)

__all__ = [
  # Ingestion
  "ingest_sec_data",
  # Maintenance
  "reset_sec_database",
  "full_reset_for_year",
  # Orchestration
  "plan_phased_processing",
  "start_phase",
  "get_phase_status",
  "download_company_filings",
  "process_company_filings",
  "smart_retry_failed_companies",
  "get_pipeline_metrics",
  "cleanup_phase_connections",
  "handle_phase_completion",
]
