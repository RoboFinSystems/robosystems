"""RoboLedger extension surface.

Everything mounts under `/extensions/roboledger/{graph_id}/operations/` —
command writes and analytical reads alike, since both are dispatcher
operations carrying an `OperationEnvelope`, an idempotency key and an audit
record. Three routers divide it:

- **`operations/`** — the command writes, one module per OpenAPI tag
  (setup, taxonomy, information blocks, ledger & events, close, reports,
  distribution). Delegates to `operations/roboledger/commands/*`.
- **`views.py`** — graph-backed analytical reads over the LadybugDB XBRL
  hypercube and over published reports read whole. Mounted on
  `FACT_GRID_ENABLED` rather than `ROBOLEDGER_ENABLED`, because the schema
  is roboledger's but the SEC shared repository uses it too.
- **`reads.py`** — the OLTP-backed analytical read, sharing views' tag.

`_common.py` holds what all three need: the operation context builder, the
dispatcher, the schema-missing 404, and the registrar factory.

Simple typed reads (entity, accounts, trial balance, etc.) are NOT here —
they are on the GraphQL surface at `/extensions/{graph_id}/graphql`.
"""
