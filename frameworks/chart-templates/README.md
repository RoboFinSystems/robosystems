# Chart templates — stencils, not a framework

This directory holds the **shipped chart-of-accounts templates** that
`initialize-chart-of-accounts` instantiates for a company that starts
fresh and keeps its books natively (no QuickBooks sync). It sits beside the
frameworks because it is authored JSON-LD content maintained with the
same tooling and vocabulary, but it is **not a framework and not a
package** — the `ontology/` precedent:

- **No manifest.** There is no `v*.json` here, so framework discovery
  (`robosystems/taxonomy/discovery.py`) never sees it, and no migration
  seeds it. Nothing in this directory reaches the `public` library.
- **Never copied into a tenant.** A framework package is referenced by
  identity — seeded, copied at provision as immutable `library-seeder`
  rows, resynced, versioned. A template is the opposite: it is **forked
  once**. The initialize operation reads the file, mints tenant-owned
  `coa:<code>` elements (`source='native'`, `created_by=<user>`), creates
  the mapping structure and arcs, and the tenant owns the result from
  then on. A later edit to a template never touches a chart already
  instantiated from it.
- **One-time.** Once a graph holds an active `chart_of_accounts`
  taxonomy — QuickBooks-synced, authored through the taxonomy block, or
  initialized here — the operation refuses (409). A chart is never
  replaced; customization is `update-taxonomy-block`.

Design record: `local/RoboSystems/specs/taxonomy/chart-templates-as-data.md`.
Doctrine: `specs/adapters/mercury-adapter.md` §8.1.

## Layout

```
chart-templates/
├── README.md
├── saas/v1/
│   ├── chart.jsonld               # the accounts — framework-free
│   └── mappings/rs-gaap.jsonld    # CoA → rs-gaap, with the legal-form variants
├── services/v1/{chart.jsonld, mappings/rs-gaap.jsonld}
└── product/v1/{chart.jsonld, mappings/rs-gaap.jsonld}
```

A template is two kinds of file, deliberately separate, because **a chart
maps into many frameworks** (one ledger, many filings: rs-gaap for the
book, a future `rs-irs` for tax, `rs-call-report` for a bank):

### `chart.jsonld` — the accounts

Framework-free. What it does anchor to is the substrate: `hasTrait` is a
`fac-traits` IRI (`trait:elementsOfFinancialStatements/asset` …), the
vocabulary every framework depends on. Each account carries `code`,
`label`, `hasTrait`, `subClassification`, `balance` (`debit` | `credit`)
and an optional `documentation`. No namespace is declared for the
accounts — the `coa:` qname is minted at instantiate time, never in the
file. `key` must equal the directory name; `ordinal` orders the catalogue
for pickers.

### `mappings/<framework>.jsonld` — one file per framework

`framework` must equal the file stem and names the target framework
(which is also the `Element.source` of that framework's concepts in a
tenant's library copy — how the operation decides the set applies).
`arcs` are the form-independent rows (`from` = an account code, `to` = a
concept IRI in the framework's namespace). `variants` swap rows by the
framework's Reporting Style family — for rs-gaap the two equity accounts
by legal form (`corporation` / `partnership` / `llc`); `defaultVariant`
names the fallback. A framework with no such split ships one variant or
none. Adding tax mappings to a template is a new `rs-irs.jsonld` beside
the rs-gaap one, with no edit to the accounts.

## How the operation applies a template

`initialize-chart-of-accounts(template, entity_type?, name?)` creates the
chart through the TaxonomyBlock envelope, then **for each mapping set
whose framework is present in the tenant's library copy** creates a
`coa_mapping` structure (`structureName`) and its associations, resolving
targets by qname. Misses are reported as `unresolved`, never fatal. Today
every tenant carries `rs-gaap`; when a graph's pin goes plural, a fresh
chart arrives mapped into every framework it carries, with no change to
the operation.

## Adding or editing a template

1. `mkdir <key>/v1/mappings`; write `chart.jsonld` (set `key`, `ordinal`,
   `displayName`, `description`, `accounts`) and at least
   `mappings/rs-gaap.jsonld`.
2. Every `to` must resolve in its framework's package —
   `tests/operations/taxonomy_block/test_chart_templates.py` checks each
   target against `frameworks/rs-gaap/packages/rs-gaap/v1/taxonomy.jsonld`,
   so a library rename fails a test instead of surfacing as `unresolved`
   on a customer's first day.
3. No seeding, no migration, no reset: the registry
   (`robosystems/operations/taxonomy_block/chart_templates/__init__.py`)
   reads the files at import time. A running container needs a restart
   (`just restart`) to pick up an edit; `frameworks/` is baked into the
   image, so a deploy carries it.
4. The `saas` and `services` demos read their chart and mappings from
   these files; `product` is the coffee-roaster demo's chart with generic
   names, pinned structurally by the same test.
