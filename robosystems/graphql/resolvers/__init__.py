"""Per-domain resolver classes for the extensions GraphQL schema.

Each module exposes a `@strawberry.type` class (e.g., `LedgerQuery`,
`InvestorQuery`) that defines the read-surface fields for that domain.
`graphql/schema.py` composes the top-level `Query` root from all of
them via inheritance.

Resolvers stay thin — a field body is typically three lines:
1. `open_extensions_session(info, ...)` (or `open_library_session`), which
   runs the auth and extension gates
2. Call into `operations/{domain}/reads/*.py`
3. Wrap the Pydantic result in its Strawberry type

No business logic lives here. Error translation: `None` returns become
`null` in the response; `ValueError`/`ProgrammingError` from a missing
schema returns `null` rather than a GraphQL error (mirroring REST 404).
"""
