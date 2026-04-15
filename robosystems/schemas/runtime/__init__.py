"""Schema runtime — builders, validators, parsers, and managers.

This subpackage contains the runtime-facing helpers that consume the
declarative schema layer (`schemas/base.py`, `schemas/extensions/`,
`schemas/models.py`, `schemas/loader.py`) to do work: build and compile
schemas, validate property operations, parse Cypher DDL back to
metadata, and handle user-supplied custom schemas.

The declaration-vs-runtime split is intentional:
- `schemas/` top level — ontology source of truth (pure data, no side effects)
- `schemas/runtime/` — runtime behavior that consumes the declarations
- `schemas/extensions/` — per-extension declarations

See `schemas/README.md` for the two invariants governing this layout.
"""
