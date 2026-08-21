"""QuickBooks adapter for accounting data integration.

This adapter provides:
- QBClient: OAuth-authenticated API client for QuickBooks Online
- dbt project (dbt/) for QB data transformation to RoboLedger schema

Data pipeline architecture:
  1. Extract: QBClient fetches accounts, journal entries, company info
  2. Transform: dbt-duckdb models transform raw QB data to RoboLedger schema
  3. Load: DuckDB tables inserted into the extensions PostgreSQL tenant schema
"""

from robosystems.adapters.quickbooks.client import QBClient

__all__ = [
  "QBClient",
]
