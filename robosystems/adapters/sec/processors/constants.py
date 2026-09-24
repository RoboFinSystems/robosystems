"""Constants shared by SEC processing, consolidation and ingestion."""

# Node tables shared across filings, deduplicated during consolidation. Their
# ids are content-derived UUID5s, so equal content means equal identifier.
SHARED_NODE_TABLES = frozenset(
  {
    "nodes/Element",
    "nodes/Label",
    "nodes/Reference",
    "nodes/Unit",
    "nodes/Period",
  }
)

QUARTER_END_DAYS = {
  1: "-03-31",
  2: "-06-30",
  3: "-09-30",
  4: "-12-31",
}
