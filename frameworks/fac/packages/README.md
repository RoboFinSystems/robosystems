# fac v1 — Packages

This directory holds the **packages owned by the `fac@v1` framework** —
Charlie Hoffman's universal Fundamental Accounting Concepts substrate
(Assets, Liabilities, Equity, Revenues, Expenses, Income, Cash Flow)
plus its presentation hierarchies, calculation identities, and
verification rules.

`fac@v1` is the **dependency root** for every accounting framework in
this library. `rs-gaap` depends on it today; future peer frameworks
like `rs-ifrs`, `rs-call-report`, `rs-statutory`, `rs-irs`, `rs-ferc`
will all depend on it. It has no own bridges — bridges from
`fac` into other namespaces (like `fac-to-rs-gaap`) live with the
framework that authored the mapping (in that case, `rs-gaap`).

For framework composition rules and manifest schema see
`../../../README.md`. For the dependency graph and how `depends_on`
resolution walks dependencies first, see
`robosystems/taxonomy/discovery.py`.

## Fork model

These packages were originally bootstrapped from Charlie Hoffman's
Seattle Method publications and are now maintained **directly in
JSON-LD** inside this directory:

- Edits happen in the `taxonomy.jsonld` file for the relevant standard.
- Upstream XBRL is **not** tracked. We do not re-import, rebase, or
  reconcile against Seattle Method publications on any schedule.
- Migrations read these files verbatim from git — there is no network
  fetch in the ingest path.

Each package declares its provenance in its top-level JSON-LD metadata.

## Layout

```
packages/
├── fac-traits/v1/           forked  — universal trait vocabulary (24 axes, 99 traits,
│                                      25 categories — elementsOfFinancialStatements,
│                                      liquidity, activityType, recordedValue,
│                                      flowClassification, …); seeds the `traits` table
├── fac/v1/                  forked  — FAC fundamental concepts (~177 elements)
├── fac-presentation/v1/     native  — FAC multi-variant presentation hierarchies
├── fac-calculations/v1/     native  — FAC BS/IS/CF accounting identities
└── fac-rules/v1/            forked  — Seattle Method verification rules (14 rules,
                                       3 categories, 5 patterns)
```

## Notes on `fac-traits`

The trait vocabulary was originally forked from FASB's us-gaap 2026
metamodel publication, but the axes themselves are not us-gaap-specific
— `elementsOfFinancialStatements`, `liquidity`, `activityType`,
`recordedValue`, `realizationStatus`, `flowClassification`, etc.
describe how *any* accounting element is classified, regardless of
regulatory regime. Hoisted into `fac` so every dependent rs-* framework
(rs-gaap today; rs-ifrs / rs-call-report / rs-statutory tomorrow)
inherits the same axes via `depends_on` and no rs-* framework has to
re-author or duplicate the trait catalog.

Per-element trait assignments live in each framework's
`*-traits/v1/` package (today: `rs-gaap-traits/v1/`).
A future `rs-ifrs-traits/v1/` would bind ifrs elements to the
same axes; jurisdiction-specific enum values would extend the trait
catalog if the existing 95 members don't cover them.

## Editing packages

The JSON-LD is the source of truth. Edit it directly. After editing,
re-run migrations against a fresh extensions database to confirm the
packages still load cleanly:

```bash
just reset-local
```

A change to `fac@v1` packages affects every framework that depends on
it (today: `rs-gaap`; tomorrow: every other `rs-*` framework). Bump
`fac` to `v2` and update each dependent framework's `depends_on` only
when an edit is genuinely breaking; small additive edits can stay in
`v1` since dependents pin a specific version.

## Adding a new package

`fac` is meant to stay small and universal. Before adding a new
package here, ask: is this concept *fundamental* to accounting in
general (true for every regulatory regime, every jurisdiction), or
is it specific to one regime?

- Universal: add here, every dependent framework gets it for free.
- Regime-specific: add to the dependent framework's own packages
  directory (e.g. `frameworks/rs-gaap/packages/`).

If the addition is genuinely universal:

1. Create `packages/<name>/v1/taxonomy.jsonld` with top-level metadata
   (`standard`, `version`, `taxonomy_type`, `namespace_uri`,
   `description`, and either `forked_from`/`forked_at`/`upstream_tracking`
   or `origin`/`created_at`).
2. Add an entry to this framework's manifest at `../manifest.json`
   under `packages[]` with an `ordinal` (load order) and `is_required`
   flag.

The migration auto-walks the framework manifest, so no explicit
Python list to update beyond the manifest itself.
