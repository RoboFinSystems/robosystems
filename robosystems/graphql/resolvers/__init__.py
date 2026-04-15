"""Per-domain resolver classes for the extensions GraphQL schema.

Each module exposes a `@strawberry.type` class (e.g., `LedgerQuery`,
`InvestorQuery`) that defines the read-surface fields for that domain.
`graphql/schema.py` composes the top-level `Query` root from all of
them via inheritance.

Resolvers stay thin — every field body should be ~3 lines:
1. Authenticate + graph-access check (`require_user` + `check_graph_access`)
2. Open an `extensions_session(graph_id)`
3. Call into `operations/{domain}/reads/*.py`

No business logic lives here. Error translation: `None` returns become
`null` in the response; `ValueError`/`ProgrammingError` from a missing
schema returns `null` rather than a GraphQL error (mirroring REST 404).
"""
