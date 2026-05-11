# Taxonomy Packages — RoboSystems Canonical Forks

This directory holds the **packages** tier of the three-tier taxonomy
library. Each subdirectory is a versioned, self-contained JSON-LD
package; the migration loads them at extensions DB provisioning time
in the order specified by a Framework manifest (see `../frameworks/`).

Packages are **atomic units**. Each one declares qnames in its own
namespace and is loadable independently. The composition of which
packages a tenant gets is decided by the framework they're pinned to.

For cross-namespace equivalence taxonomies (bridges) see
`../bridges/`. For named compositions of packages + bridges see
`../frameworks/`.

## Fork model

The packages here are **RoboSystems canonical forks**. A handful were
originally bootstrapped from upstream XBRL (mostly Charlie Hoffman's
Seattle Method publications + FASB us-gaap 2017); the rest were
authored natively as part of the taxonomy library build-out.

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
  "standard": "fac",
  "forked_from": {
    "author": "Charlie Hoffman (Seattle Method)",
    "url": "http://xbrlsite.com/seattlemethod/golden/fac/fac-entryPoint.xsd",
    "format": "XBRL taxonomy (entry point + linkbases)"
  },
  "forked_at": "2026-04-19",
  "upstream_tracking": "frozen"
}
```

…or, for packages we authored natively:

```json
{
  "standard": "fac-calculations",
  "origin": "native",
  "created_at": "2026-04-19"
}
```

## Layout

```
packages/
├── fac/v1/                            forked  — FAC fundamental concepts (~177)
├── fac-presentation/v1/               native  — FAC multi-variant presentation hierarchies
├── fac-calculations/v1/               native  — FAC BS/IS/CF accounting identities
├── fac-rules/v1/                      forked  — Seattle Method verification rules
├── rs-gaap/v1/                        forked  — RoboSystems canonical us-gaap (~2,000)
├── rs-gaap-presentation/v1/           native  — rs-gaap presentation hierarchies
├── rs-gaap-calculations/v1/           native  — rs-gaap calc DAG (composes with fac-calculations)
├── rs-gaap-hierarchy/v1/              forked  — rs-gaap class hierarchy
├── us-gaap-metamodel/v1/              forked  — FASB metamodel trait vocabulary (99 traits, 25 categories — incl. SFAC 6 as `elementsOfFinancialStatements` traits)
├── rs-gaap-to-metamodel/v1/           native  — trait assignments (rs-gaap ↔ FASB metamodel)
├── type-subtype/v1/                   forked  — rs-gaap classification linkbase
├── rs-gaap-disclosures/v1/            native  — named Disclosures (~30)        ★ Phase C
├── rs-gaap-disclosure-mechanics/v1/   native  — DM rules per Disclosure        ★ Phase C
├── rs-gaap-reporting-checklist/v1/    native  — DR rules per report type       ★ Phase C
└── rs-gaap-reporting-styles/v1/       native  — composition styles             ★ Phase C
```

Note: SFAC 6 doesn't have its own package — its content (Assets, Liabilities,
Equity, Revenues, Expenses, etc.) is encoded as the `elementsOfFinancialStatements`
trait category inside `us-gaap-metamodel/v1` and attached to concepts via
`element_traits`.  This keeps the SFAC 6 categorization queryable per-element
without giving it a separate concept namespace.

Cross-namespace equivalence taxonomies (e.g. `fac-to-rs-gaap`) live
under `../bridges/`, not here.

## Editing packages

The JSON-LD is the source of truth. Edit it directly. We're past the
bootstrap phase — packages are now curated and crafted by hand, not
regenerated from canned scripts.

After editing, re-run the migration against a fresh extensions database
to confirm the packages still load cleanly:

```bash
just migrate-down extensions -1    # rewind the taxonomy-library migration
just migrate-up extensions         # re-apply; reloads all packages + bridges
```

## Adding a new package

1. Create `packages/<name>/v1/taxonomy.jsonld` with top-level metadata
   (`standard`, `version`, `taxonomy_type`, `namespace_uri`,
   `description`, and either `forked_from`/`forked_at`/`upstream_tracking`
   or `origin`/`created_at`).
2. Add an entry to the relevant Framework manifest in `../frameworks/`
   (e.g. `frameworks/rs-gaap-base/v1.json`) under `packages[]` with an
   `ordinal` (load order) and `is_required` flag.
3. If the new package introduces a new `source` value for elements or
   a new `association_type` for associations, widen the corresponding
   CHECK constraints in the migration `0002_taxonomy_library.py`.

The migration auto-walks the framework manifest, so no explicit Python
list to update beyond the framework manifest itself.
