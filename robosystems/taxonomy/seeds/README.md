# Taxonomy Seeds — RoboSystems Canonical Forks

This directory holds the canonical JSON-LD taxonomy artifacts that seed
the `public` schema of the extensions database. Every tenant graph
receives a copy at provision time and consults it as shared reference
material.

## Fork model

The seeds here are **RoboSystems canonical forks**. A handful were
originally bootstrapped from upstream XBRL (mostly Charlie Hoffman's
Seattle Method publications + FASB us-gaap 2017); the rest were authored
natively as part of the taxonomy library build-out.

Whichever origin they had, all seeds are now maintained **directly in
JSON-LD** inside this directory:

- Edits happen in the `taxonomy.jsonld` file for the relevant standard.
- Upstream XBRL is **not** tracked. We do not re-import, rebase, or
  reconcile against upstream on any schedule.
- Migrations read these files verbatim from git — there is no network
  fetch in the ingest path.

Each seed declares its provenance in its top-level JSON-LD metadata:

```json
{
  "standard": "sfac6",
  "forked_from": {
    "author": "Charlie Hoffman (Seattle Method)",
    "url": "http://xbrlsite.com/seattlemethod/golden/sfac6/sfac6-entryPoint.xsd",
    "format": "XBRL taxonomy (entry point + linkbases)"
  },
  "forked_at": "2026-04-19",
  "upstream_tracking": "frozen"
}
```

…or, for seeds we authored natively:

```json
{
  "standard": "fac-calculations",
  "origin": "native",
  "created_at": "2026-04-19"
}
```

## Layout

```
seeds/
├── sfac6/v1/               forked — SFAC 6 elements (19 concepts)
├── fac/v1/                 forked — FAC fundamental concepts (~177 concepts)
├── rs-gaap/v1/             forked — RoboSystems canonical us-gaap (~2,000 concepts)
├── type-subtype/v1/        forked — rs-gaap classification linkbase
├── fac-to-rs-gaap/v1/      native — FAC → rs-gaap equivalence arcs
├── fac-calculations/v1/    native — FAC BS/IS/CF accounting identities (summationOf arcs)
├── fac-presentation/v1/    native — FAC multi-variant presentation hierarchies
└── rs-gaap-presentation/v1/ native — rs-gaap presentation hierarchies
```

## Editing seeds

The JSON-LD is the source of truth. Edit it directly.

For structural, non-trivial edits, the recommended path is to:

1. Write a small script under `robosystems/scripts/` that computes the
   desired change from the current seed (and any referenced seeds).
2. Run it once; verify the diff; commit both the script and the updated
   seed.

Examples of scripts that follow this pattern:

- `robosystems/scripts/curate_fac_axes.py`
- `robosystems/scripts/build_fac_calculations_seed.py`
- `robosystems/scripts/build_fac_presentation_seed.py`
- `robosystems/scripts/build_rs_gaap_seed.py`

After editing, re-run the migration against a fresh extensions database
to confirm the seeds still load cleanly:

```bash
just migrate-down extensions -1    # rewind the taxonomy-library migration
just migrate-up extensions         # re-apply; reloads all seeds
```

## Re-bootstrapping from upstream

If you ever genuinely need to re-derive a forked seed from its upstream
XBRL source (to audit drift, compare against a newer upstream, or adopt
an intentional upgrade), use the archaeological one-shot:

```bash
uv run python -m robosystems.scripts.import_upstream_seeds --only sfac6
```

This will overwrite the target seed in place. Do it on a branch, diff
against `main`, and decide element-by-element what to carry forward.

## Adding a new taxonomy

1. Create `seeds/<name>/v1/taxonomy.jsonld` with top-level metadata
   (`standard`, `version`, `taxonomy_type`, `namespace_uri`,
   `description`, and either `forked_from`/`forked_at`/`upstream_tracking`
   or `origin`/`created_at`).
2. The migration (`migrations/extensions/versions/0002_taxonomy_library.py`)
   auto-discovers all `v*/taxonomy.jsonld` files under `seeds/` — no
   explicit list to update.
3. If the new taxonomy introduces a new `source` value for elements or
   a new `association_type` for associations, widen the corresponding
   CHECK constraints in the migration.
