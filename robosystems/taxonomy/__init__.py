"""Taxonomy subsystem — the public library and its supporting runtime.

The taxonomy library is the shared reference material that every entity
graph consults. Canonical content lives in the `public` schema of the
extensions database; each tenant schema holds a read-only copy copied
in at provision time. Tenants author their Chart of Accounts and
mapping associations locally; they never modify library rows.

The library is organized into three tiers:

- `packages/` — atomic taxonomy units (FAC concepts, rs-gaap concepts,
  type-subtype, etc.). Each subdirectory is a versioned JSON-LD package.
- `bridges/` — cross-namespace equivalence taxonomies (FAC ↔ rs-gaap,
  Disclosure names ↔ underlying text-block elements, future SEC export
  bridges). Same loader, same schema; different role.
- `frameworks/` — named compositions that pin specific package + bridge
  versions into one bundle. `Graph.taxonomy_pin` references a framework
  by `name@version`.

This module is the single source of truth for library content and its
supporting runtime:

- `model.py` — Pydantic `TaxonomyPackage` and its component specs
- `loaders/` — JSON-LD → TaxonomyPackage (via rdflib) + filesystem
  discovery for the three-tier layout
- `pins.py` — polymorphic `resolve_pin()` that turns a Graph's
  `taxonomy_pin` JSONB into the flat `{standard: version}` dict the
  tenant writer consumes
- `writers/` — TaxonomyPackage → SQL INSERTs into `public.*` with UUID5
  IDs; tenant writer copies the pinned subset into per-graph schemas
- `seed.py` — legacy Python-dict seed (retained for compatibility
  during the JSON-LD migration).

See `packages/README.md`, `bridges/README.md`, and
`frameworks/README.md` for details on each tier.
"""
