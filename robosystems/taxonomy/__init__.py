"""Taxonomy subsystem — the Python runtime around the library content.

The taxonomy library (JSON-LD content + framework manifests) lives at
the repo-root `/taxonomy/` directory, peer to `/robosystems/`. This
package is the Python code that walks it, parses it, and writes its
contents into per-tenant schemas at provision time.

## Lifecycle

```
disk (/taxonomy/)
    │
    │  discovery.py        walks frameworks/{name}/v*.json + packages/* + bridges/*
    ▼
filesystem paths
    │
    │  loader.py           parses each taxonomy.jsonld → TaxonomyPackage
    ▼
TaxonomyPackage (Pydantic, see model.py)
    │
    │  migration 0002      INSERTs into public.* tables (frameworks-wide library)
    ▼
public schema (extensions DB)
    │
    │  pins.py             resolves Graph.taxonomy_pin → flat {standard: version}
    │  writer.py           copies the pinned subset into per-tenant schema
    ▼
tenant schema (per-graph subset)
```

## Modules

- `model.py` — Pydantic `TaxonomyPackage` and component specs (the
  in-memory shape of taxonomy content)
- `discovery.py` — filesystem walking, manifest reading, dependency
  expansion (`load_framework_manifest`, `expand_framework_to_pin`,
  `list_framework_seed_paths`, `framework_root`)
- `loader.py` — JSON-LD parser via rdflib → `TaxonomyPackage`
  (`load_taxonomy_package`)
- `pins.py` — polymorphic `resolve_pin()` that turns a Graph's
  `taxonomy_pin` JSONB into the flat `{standard: version}` dict the
  writer consumes
- `writer.py` — copies the pinned library subset into a per-graph
  schema at provision time (`copy_library_into_tenant`)
- `seed.py` — legacy Python-dict seeder (retained for migration 0001
  backward compatibility)

The library content itself is documented in `/taxonomy/README.md` and
`/taxonomy/frameworks/README.md`.
"""

from __future__ import annotations

from robosystems.taxonomy.discovery import (
  FRAMEWORKS_DIR,
  bridge_path,
  expand_framework_to_pin,
  framework_root,
  list_bridges,
  list_framework_manifests,
  list_framework_seed_paths,
  list_packages,
  load_framework_manifest,
  package_path,
)
from robosystems.taxonomy.loader import load_taxonomy_package

__all__ = [
  "FRAMEWORKS_DIR",
  "bridge_path",
  "expand_framework_to_pin",
  "framework_root",
  "list_bridges",
  "list_framework_manifests",
  "list_framework_seed_paths",
  "list_packages",
  "load_framework_manifest",
  "load_taxonomy_package",
  "package_path",
]
