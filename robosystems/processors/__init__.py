"""
Processors layer for data transformation and business logic processing.

This module contains specialized processors that handle complex data transformations,
calculations, and business logic for the RoboSystems platform.

Key processors:
- XBRLGraphProcessor: SEC XBRL filing processing and graph transformation
- SchemaProcessor: DataFrame schema adaptation and validation
- SchemaIngestionProcessor: Schema-driven ingestion configuration generation
- TrialBalanceProcessor: Financial trial balance calculations
- ScheduleProcessor: Financial schedule generation and analysis
- QBTransactionsProcessor: QuickBooks transaction processing and normalization
"""

# Core processors
from .xbrl_graph import XBRLGraphProcessor
from .schema_processor import SchemaProcessor
from .schema_ingestion import (
  SchemaIngestionProcessor,
  SchemaIngestConfig,
  IngestTableInfo,
  create_roboledger_ingestion_processor,
  create_custom_ingestion_processor,
)
from .trial_balance import TrialBalanceProcessor
from .schedule import ScheduleProcessor
from .qb_transactions import QBTransactionsProcessor

__all__ = [
  "XBRLGraphProcessor",
  "SchemaProcessor",
  "SchemaIngestionProcessor",
  "SchemaIngestConfig",
  "IngestTableInfo",
  "create_roboledger_ingestion_processor",
  "create_custom_ingestion_processor",
  "TrialBalanceProcessor",
  "ScheduleProcessor",
  "QBTransactionsProcessor",
]
