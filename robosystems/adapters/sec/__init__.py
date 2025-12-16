"""SEC EDGAR adapter for XBRL financial data extraction."""

from robosystems.adapters.sec.client import SECClient, SEC_BASE_URL, enable_test_mode
from robosystems.adapters.sec.arelle import ArelleClient
from robosystems.adapters.sec.processors import (
  XBRLGraphProcessor,
  XBRL_GRAPH_PROCESSOR_VERSION,
  XBRLSchemaAdapter,
  XBRLSchemaConfigGenerator,
  SchemaIngestConfig,
  IngestTableInfo,
  create_roboledger_ingestion_processor,
  create_custom_ingestion_processor,
  XBRLDuckDBGraphProcessor,
)

__all__ = [
  # Client
  "SECClient",
  "SEC_BASE_URL",
  "enable_test_mode",
  "ArelleClient",
  # Processors
  "XBRLGraphProcessor",
  "XBRL_GRAPH_PROCESSOR_VERSION",
  "XBRLSchemaAdapter",
  "XBRLSchemaConfigGenerator",
  "SchemaIngestConfig",
  "IngestTableInfo",
  "create_roboledger_ingestion_processor",
  "create_custom_ingestion_processor",
  "XBRLDuckDBGraphProcessor",
]
