"""RoboLedger graph-backed analytical views (XBRL hypercubes).

This package is the **graph-side** read layer for the roboledger schema —
sibling to `reads/` (which queries the OLTP extensions database).

The XBRL hypercube schema (`Fact`, `Element`, `Period`, `Unit`, `Entity`,
`Report` nodes in LadybugDB) is owned by roboledger and shared by:

- **Roboledger tenant graphs**: data materialized from extensions
  PostgreSQL → DuckDB → LadybugDB
- **SEC shared repository**: public XBRL filings ingested via Arelle

Both backends use the same Cypher reads, so this package serves either
graph_id transparently. It is graph-database analytical code, NOT
extensions OLTP code, and lives under roboledger because the schema is
roboledger's even when the data inside is SEC's.

**Use this for:** cross-period and cross-entity analytical queries
(fact grids, hypercube slices). Reads return fact records or `FactGrid`
Pydantic models — scoped and deduplicated, never pivoted; arranging facts
into cells is the consumer's job.

**Don't use this for:** transactional lookups against tenant ledgers
(account balances, draft entries, period close gates) — those live in
`reads/` against the extensions OLTP database.
"""

from robosystems.operations.roboledger.views.fact_grid_builder import (
  FactGridBuilder,
  summarize_by_element,
)
from robosystems.operations.roboledger.views.fact_query import query_fact_grid
from robosystems.operations.roboledger.views.financial_statement_query import (
  deduplicate_facts,
  query_financial_statement,
)

__all__ = [
  "FactGridBuilder",
  "deduplicate_facts",
  "query_fact_grid",
  "query_financial_statement",
  "summarize_by_element",
]
