"""Schema runtime — builders, validators, parsers, and managers.

The runtime-facing helpers that consume the declarative schema layer
(`schemas/base.py`, `schemas/extensions/`, `schemas/models.py`,
`schemas/loader.py`): compile schemas, validate property operations, parse
Cypher DDL back to metadata, and handle user-supplied custom schemas.

The declaration-vs-runtime split is intentional — the top level holds ontology
declarations only (pure data, no side effects), and everything that acts on
them lives here. See `schemas/README.md` for the two invariants governing this
layout.
"""
