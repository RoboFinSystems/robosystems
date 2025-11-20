"""
Processors layer for data transformation and business logic processing.

This module contains specialized processors that handle complex data transformations,
calculations, and business logic for the RoboSystems platform.

Key processors:
- XBRLGraphProcessor: SEC XBRL filing processing and graph transformation
- XBRLSchemaAdapter: DataFrame schema adaptation and validation for XBRL
- XBRLSchemaConfigGenerator: Schema-driven ingestion configuration for XBRL
- QBTransactionsProcessor: QuickBooks transaction processing and normalization
"""

# Core processors
from .xbrl_graph import XBRLGraphProcessor
from .xbrl.schema_adapter import XBRLSchemaAdapter
from .xbrl.schema_config_generator import (
  XBRLSchemaConfigGenerator,
  SchemaIngestConfig,
  IngestTableInfo,
  create_roboledger_ingestion_processor,
  create_custom_ingestion_processor,
)
from .qb_transactions import QBTransactionsProcessor

__all__ = [
  "XBRLGraphProcessor",
  "XBRLSchemaAdapter",
  "XBRLSchemaConfigGenerator",
  "SchemaIngestConfig",
  "IngestTableInfo",
  "create_roboledger_ingestion_processor",
  "create_custom_ingestion_processor",
  "QBTransactionsProcessor",
]
