"""SEC EDGAR adapter for XBRL financial data extraction."""

from robosystems.adapters.sec.client import SEC_BASE_URL, SECClient, enable_test_mode
from robosystems.adapters.sec.client.arelle import ArelleClient
from robosystems.adapters.sec.processors import (
  XBRL_GRAPH_PROCESSOR_VERSION,
  IngestTableInfo,
  SchemaIngestConfig,
  XBRLDuckDBGraphProcessor,
  XBRLGraphProcessor,
  XBRLSchemaAdapter,
  XBRLSchemaConfigGenerator,
  create_custom_ingestion_processor,
  create_roboledger_ingestion_processor,
)

__all__ = [
  "SEC_BASE_URL",
  "XBRL_GRAPH_PROCESSOR_VERSION",
  "ArelleClient",
  "IngestTableInfo",
  # Client
  "SECClient",
  "SchemaIngestConfig",
  "XBRLDuckDBGraphProcessor",
  # Processors
  "XBRLGraphProcessor",
  "XBRLSchemaAdapter",
  "XBRLSchemaConfigGenerator",
  "create_custom_ingestion_processor",
  "create_roboledger_ingestion_processor",
  "enable_test_mode",
]
