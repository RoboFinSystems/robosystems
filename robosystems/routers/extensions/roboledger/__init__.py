"""RoboLedger extension surface.

Hosts two sub-surfaces under `/extensions/roboledger/{graph_id}/`:

- **`/operations/{op_name}`** — command writes (close-period,
  create-information-block, create-event-block, etc.). Each operation runs
  through `execute_operation` and returns an `OperationEnvelope`.
  Implementations live in `operations.py` and delegate to
  `operations/roboledger/commands/*`.

- **`/views`** — analytical graph-backed reads (XBRL hypercube fact grids).
  Queries the LadybugDB graph schema, not extensions PostgreSQL. Lives here
  because the schema is roboledger's. Implementation in `views.py` delegates
  to `operations/roboledger/views/`.

Simple typed reads (entity, accounts, trial balance, etc.) are NOT here —
they're on the GraphQL surface at `/extensions/{graph_id}/graphql`.
"""
