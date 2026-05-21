# rs-gaap v1 — Packages

This directory holds the **packages owned by the `rs-gaap@v1` framework** —
the curated us-gaap leaves, the presentation/calc/disclosure machinery
that lives on top of them, and the Reporting Styles surface for
vertical/filer-profile flavoring.

Packages owned by the upstream **`fac@v1`** framework (Charlie Hoffman's
universal accounting-concept substrate) live next door at
`../../../fac/packages/`. They are inherited via this framework's
`depends_on: [{framework: fac, version: v1}]` and load first.

Each package here is a versioned, self-contained JSON-LD unit;
migration 0002 walks the framework manifest to load them in dependency
+ ordinal order at extensions DB provisioning time.

For cross-namespace equivalence taxonomies (bridges) see `../bridges/`.
For framework composition rules and manifest schema see
`../../../README.md`.

## Fork model

These packages are **RoboSystems canonical forks**. A handful were
originally bootstrapped from upstream XBRL (Charlie Hoffman's Seattle
Method publications + FASB us-gaap 2017); the rest were authored
natively as part of the taxonomy library build-out.

Whichever origin they had, all packages are now maintained **directly
in JSON-LD** inside this directory:

- Edits happen in the `taxonomy.jsonld` file for the relevant standard.
- Upstream XBRL is **not** tracked. We do not re-import, rebase, or
  reconcile against upstream on any schedule.
- Migrations read these files verbatim from git — there is no network
  fetch in the ingest path.

Each package declares its provenance in its top-level JSON-LD metadata:

```json
{
  "standard": "rs-gaap",
  "forked_from": {
    "author": "FASB us-gaap 2017",
    "url": "https://xbrl.fasb.org/us-gaap/2017/",
    "format": "XBRL taxonomy"
  },
  "forked_at": "2026-04-19",
  "upstream_tracking": "frozen"
}
```

…or, for packages we authored natively:

```json
{
  "standard": "rs-gaap-calculations",
  "origin": "native",
  "created_at": "2026-04-19"
}
```

## Layout

```
packages/
├── rs-gaap/v1/                       forked  — RoboSystems canonical us-gaap (~2,000 leaves)
├── rs-gaap-traits/v1/                native  — per-element trait bindings for rs-gaap leaves;
│                                              seeds `element_traits` junction (binds rs-gaap
│                                              elements to fac-traits axes)
├── rs-gaap-hierarchy/v1/             forked  — rs-gaap class hierarchy
├── rs-gaap-presentation/v1/          native  — rs-gaap presentation hierarchies
├── rs-gaap-calculations/v1/          native  — rs-gaap calc DAG (composes with fac-calculations)
├── rs-gaap-type-subtype/v1/          forked  — rs-gaap classification linkbase (general-special arcs + ASC citations)
├── rs-gaap-disclosures/v1/           native  — named Disclosures (~30)
├── rs-gaap-disclosure-mechanics/v1/  native  — DM rules per Disclosure
├── rs-gaap-reporting-checklist/v1/   native  — DR rules per report type
└── rs-gaap-reporting-styles/v1/      native  — vertical / filer-profile composition surface
                                                (Default, Small Private, Banking, Insurance,
                                                Mining, Cannabis, …)
```

The trait vocabulary (`fac-traits/v1`, 99 traits across 25 categories)
lives in the upstream `fac` framework and is inherited here via
`depends_on`. `rs-gaap-traits/v1/` is the rs-gaap-specific binding —
it declares which trait values apply to which rs-gaap leaves. Future
peer frameworks (rs-ifrs, rs-call-report, rs-statutory) each ship
their own `*-traits/v1` binding package, all targeting the same
fac-traits vocabulary.

## Notes on specific packages

**`rs-gaap-type-subtype`** is Charlie's classification linkbase pattern.
Theoretically reusable, but currently scoped to rs-gaap; if/when
another framework needs the same pattern, promote to `fac@v1` or
extract to its own framework.

**`rs-gaap-reporting-styles`** is THE vertical-flavor surface. New
industries (Mining, Cannabis, Cooperative, B-Corp, etc.) are added
here as new Reporting Style rows, **not** as new frameworks. The
framework boundary is regulatory regime (GAAP, IFRS, call report,
statutory, tax); the Reporting Style boundary is filer profile
within a regime.

**SFAC 6** doesn't have its own package — its content (Assets,
Liabilities, Equity, Revenues, Expenses, etc.) is encoded as the
`elementsOfFinancialStatements` trait category inside
`fac-traits/v1` (in the `fac` framework) and attached to rs-gaap
concepts via `rs-gaap-traits/v1`'s trait-assignment arcs.
This keeps SFAC 6 categorization queryable per-element without giving
it a separate concept namespace, and lets every rs-* framework
inherit the same axes.

## Editing packages

The JSON-LD is the source of truth. Edit it directly. We're past the
bootstrap phase — packages are now curated and crafted by hand, not
regenerated from canned scripts.

After editing, re-run migrations against a fresh extensions database
to confirm the packages still load cleanly:

```bash
just reset-local                  # full reset; reloads all packages + bridges
# — or, narrower —
just migrate-down extensions -1   # rewind the taxonomy-library migration
just migrate-up   extensions      # re-apply
```

## Adding a new package

1. Create `packages/<name>/v1/taxonomy.jsonld` with top-level metadata
   (`standard`, `version`, `taxonomy_type`, `namespace_uri`,
   `description`, and either `forked_from`/`forked_at`/`upstream_tracking`
   or `origin`/`created_at`).
2. Add an entry to this framework's manifest at `../manifest.json`
   under `packages[]` with an `ordinal` (load order) and `is_required`
   flag.
3. If the new package introduces a new `source` value for elements or
   a new `association_type` for associations, widen the corresponding
   CHECK constraints in migration `0002_taxonomy_library.py`.

The migration auto-walks the framework manifest, so no explicit
Python list to update beyond the manifest itself.
