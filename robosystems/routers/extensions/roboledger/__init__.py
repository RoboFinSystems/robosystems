"""RoboLedger extension surface under `/extensions/roboledger/{graph_id}/operations/`.

- `operations/`: command writes, one module per OpenAPI tag.
- `views.py`: graph-backed analytical reads, mounted on `FACT_GRID_ENABLED`
  (the SEC shared repository uses the same schema).
- `reads.py`: the OLTP-backed analytical read.

Typed reads live on GraphQL at `/extensions/{graph_id}/graphql`.
"""
