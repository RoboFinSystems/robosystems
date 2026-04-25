"""Operations for the taxonomy library.

The taxonomy library is the public schema's shared reference material
(FAC + us-gaap + rs-gaap). Tenants pull copies during
provisioning but never modify library rows.

This module exposes reads (no writes — library mutation goes through
admin CLI + migrations only) following the same pure-function pattern
as `operations/roboledger/` and `operations/roboinvestor/`:

- `reads/` — session-in, Pydantic-out query functions. Callable from
  REST routers, GraphQL resolvers, and MCP tools without duplication.
"""
