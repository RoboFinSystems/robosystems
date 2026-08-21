# fac v1 — Packages

This directory holds the **packages owned by the `fac@v1` framework** —
Charlie Hoffman's universal Fundamental Accounting Concepts substrate
(Assets, Liabilities, Equity, Revenues, Expenses, Income, Cash Flow)
plus its presentation hierarchies and calculation identities.

`fac` is deliberately **framework-neutral**: it carries no codification
references (FASB ASC, IAS/IFRS, IRC) and no validation rules that target a
specific framework's concepts. Those are regime-specific and live with the
dependent framework — e.g. the cross-tree consistency rules that target
`rs-gaap:` subtotals were moved to `rs-gaap-rules` (see
`frameworks/rs-gaap/packages/`).

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
├── fac-traits/v1/           forked  — universal trait vocabulary (26 categories:
│                                      24 FASB axes + flowClassification + recurrence,
│                                      99 traits — elementsOfFinancialStatements,
│                                      liquidity, activityType, recordedValue,
│                                      flowClassification, recurrence, …); seeds the `traits` table
├── fac/v1/                  forked  — FAC fundamental concepts (~175 elements)
├── fac-presentation/v1/     native  — FAC multi-variant presentation hierarchies
└── fac-calculations/v1/     native  — FAC BS/IS/CF accounting identities
```

Verification rules are **not** a fac package: the L1 cross-tree consistency
identities target `rs-gaap:` subtotals, so they live in the rs-gaap framework
(`rs-gaap-rules`, 3 rules) alongside the L2 `rs-gaap-rollup-rules`.

## Tenant copy: only the vocabulary, not the substrate

Of fac's packages, **only `fac-traits` is copied into each tenant schema**
(`tenant_copy: true`). It must be — every `element_traits` row (rs-gaap
bindings *and* the CoA accounts' EFS + liquidity traits) FKs into the `traits`
table it seeds.

`fac` (concepts), `fac-presentation`, `fac-calculations`, and the
`fac-to-rs-gaap` bridge are all **`tenant_copy: false`** — seeded into the
public library but *not* copied per-tenant. They're the dormant cross-framework
substrate: nothing in a tenant's live render or CoA→rs-gaap mapping path reads
them (the reporting tier is rs-gaap-canonical; the FAC "summary view" that would
project rs-gaap detail back through the bridge isn't built yet). Keeping them
public-only means they stay evolvable — a tenant library is immutable once
provisioned, so anything copied in is frozen, whereas a public-only package can
be re-curated and resync-added later. Promote one by flipping its
`tenant_copy` back to `true` and re-syncing.

## Notes on `fac-traits`

The trait vocabulary was originally forked from FASB's us-gaap 2026
metamodel publication, but the axes themselves are not us-gaap-specific
— `elementsOfFinancialStatements`, `liquidity`, `activityType`,
`recordedValue`, `realizationStatus`, `flowClassification`, etc.
describe how *any* accounting element is classified, regardless of
regulatory regime. They live in `fac` so every dependent rs-* framework
inherits the same axes via `depends_on` and no rs-* framework has to
re-author or duplicate the trait catalog.

Per-element trait assignments live in each framework's
`*-traits/v1/` package (today: `rs-gaap-traits/v1/`).
A future `rs-ifrs-traits/v1/` would bind ifrs elements to the
same axes; jurisdiction-specific enum values would extend the trait
catalog if the existing 99 members don't cover them.

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
2. Add an entry to this framework's manifest at `../v1.json`
   under `packages[]` with an `ordinal` (load order) and `is_required`
   flag.

The migration auto-walks the framework manifest, so no explicit
Python list to update beyond the manifest itself.
