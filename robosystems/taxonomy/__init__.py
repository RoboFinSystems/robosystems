"""Taxonomy subsystem — the public library and its supporting runtime.

The taxonomy library is the shared reference material that every entity
graph consults. Canonical content lives in the `public` schema of the
extensions database; each tenant schema holds a read-only copy copied
in at provision time. Tenants author their Chart of Accounts and
mapping associations locally; they never modify library rows.

This module is the single source of truth for library content and its
supporting runtime:

- `model.py` — Pydantic `TaxonomyPackage` and its component specs
- `loaders/` — JSON-LD → TaxonomyPackage (via rdflib)
- `writers/` — TaxonomyPackage → SQL INSERTs into `public.*` with UUID5 IDs
- `seeds/` — RoboSystems canonical JSON-LD taxonomy forks, committed to
  git and edited in place (see `seeds/README.md`). Upstream XBRL is not
  tracked; `scripts/import_upstream_seeds.py` is kept as an archaeological
  one-shot for re-bootstrapping a seed from source.
- `seed.py` — legacy Python-dict seed (retained for the initial migration
  downgrade path; retired when Phase 1 lands).
"""
