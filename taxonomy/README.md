# RoboSystems Taxonomy Library

The taxonomy library is the **authoritative source** for the JSON-LD
artifacts that every tenant graph copies into its per-tenant schema at
provision time. It lives at the repo root (peer to `robosystems/`) to
signal that it's a deliverable in its own right — the Python loader
code under `robosystems/taxonomy/` is just the seam that ingests it.

## The six-layer pattern

Every reporting domain in this library — accounting GAAP today, IFRS /
call report / tax / energy reporting in the future — shares the same
shape:

| Layer | Role | Examples |
| --- | --- | --- |
| **Upstream authority** | Publishes a taxonomy nobody edits but everyone files against | FASB us-gaap, IASB IFRS, NAIC statutory, FFIEC call report, FERC Form 1, EIA-861, EPA TRI |
| **`rs-` curation** | Our authored, edited, render-target version of the upstream | `rs-gaap` (today); future `rs-ifrs`, `rs-call-report`, `rs-irs`, `rs-ferc`, `rs-statutory`, `rs-tri` |
| **Framework manifest** | Pins packages + bridges + Styles into one composable deliverable | `frameworks/rs-gaap/v1/manifest.json` |
| **Reporting Styles** | Vertical / filer-profile variants **within** a framework | Default, Small Private Company, Banking, Insurance, Mining, Cannabis (defined inside `rs-gaap-reporting-styles/v1/`) |
| **Bridges** | Equivalences between namespaces (curation ↔ upstream for filing, or curation ↔ peer) | `fac-to-rs-gaap`, future `rs-gaap-to-us-gaap` (SEC export), `rs-gaap-to-ifrs` |
| **Tenant CoA → curation mapping** | Per-graph: a customer's chart of accounts → rs-* leaves | LINE_ITEM_RELATES_TO_ELEMENT + Associations (lives in the tenant schema, not here) |

The unit doesn't have to be currency. XBRL already handles non-monetary
units (MWh, barrels, tons CO₂e, headcount), and our Fact model
inherits that — energy and emissions frameworks slot into the same
pattern without special-casing.

## Top-level layout

```
taxonomy/
├── README.md                       (this file — six-layer overview)
└── frameworks/
    ├── README.md                   (manifest schema, composition, dependency rules)
    │
    ├── fac/v1/                     universal accounting substrate
    │   ├── manifest.json
    │   └── packages/
    │       ├── fac-metamodel/v1/   universal trait vocabulary (24 axes)
    │       ├── fac/v1/             FAC concepts (Assets, Liabilities, Equity, …)
    │       ├── fac-presentation/v1/
    │       ├── fac-calculations/v1/
    │       └── fac-rules/v1/       Seattle Method verification rules
    │
    └── rs-gaap/v1/                 US GAAP curation; depends_on fac@v1
        ├── manifest.json
        ├── packages/
        │   ├── README.md
        │   ├── rs-gaap/v1/         curated us-gaap leaves
        │   ├── rs-gaap-presentation/v1/
        │   ├── rs-gaap-calculations/v1/
        │   ├── rs-gaap-disclosures/v1/
        │   ├── rs-gaap-disclosure-mechanics/v1/
        │   ├── rs-gaap-hierarchy/v1/
        │   ├── rs-gaap-reporting-checklist/v1/
        │   ├── rs-gaap-reporting-styles/v1/    ★ vertical / filer-profile surface
        │   ├── rs-gaap-to-metamodel/v1/        trait assignments for rs-gaap leaves
        │   └── type-subtype/v1/                Charlie's classification linkbase
        └── bridges/
            ├── README.md
            ├── fac-to-rs-gaap/v1/
            └── rs-gaap-disclosures-to-rs-gaap-textblocks/v1/
```

Every future framework (`rs-ifrs`, `rs-call-report`, `rs-irs`,
`rs-ferc`, `rs-statutory`, `rs-tri`, …) is a sibling under
`frameworks/` with the same internal shape and its own `depends_on`
declaration. Atoms with byte-identical content (like `fac/v1`) live in
a single canonical framework that others depend on, not duplicated
copies.

## One ledger, many filings — the load-bearing premise

Every U.S. business has multiple simultaneous reporting frameworks
projecting from one ledger:

| Entity profile | Active frameworks |
| --- | --- |
| Public bank | `rs-gaap` (10-K) + `rs-call-report` (FFIEC 031/041) + `rs-irs` (1120) |
| Public insurer | `rs-gaap` (10-K) + `rs-statutory` (NAIC) + `rs-irs` (1120) |
| Regulated utility | `rs-gaap` (10-K) + `rs-ferc` (Form 1) + `rs-irs` |
| Private LLC | `rs-gaap` (book) + `rs-irs` (1065/1120) |
| Non-public corp | `rs-gaap` (book) + `rs-irs` (1120) |

Schedule M-1 / M-3 is the federally-mandated, line-by-line
reconciliation between book and tax — it exists *because* book and tax
are intentionally different frameworks projecting from the same
ledger. The same dynamic applies to call-report-to-GAAP, stat-to-GAAP,
FERC-to-GAAP.

Today the library ships one framework (`rs-gaap`). The directory
structure and `depends_on` machinery support adding peer frameworks
without restructure. The mature `Graph.taxonomy_pin` shape (plural,
role-tagged: `[{framework, role: "book"|"regulatory"|"tax"}, ...]`) is
a model-layer evolution, not a library-layer one.

## Why this lives at the top level

The taxonomy library is roughly half the bytes of this repo (after
including the rs-gaap-* hierarchies + bridges + future frameworks).
Burying it inside `robosystems/` would understate its standing — it's
the authoritative artifact, version-able and publishable, that the
Python code happens to load. Top-level placement preserves the
optionality of one day publishing it as a standalone deliverable
without restructuring.

## Where the code lives

The Python ingest code stays at `robosystems/taxonomy/`:

- `robosystems/taxonomy/loaders/discovery.py` — filesystem walking,
  manifest reading, dependency expansion
- `robosystems/taxonomy/loaders/jsonld_loader.py` — JSON-LD parser,
  produces Pydantic `TaxonomyPackage`
- `robosystems/taxonomy/writers/tenant_writer.py` — copies the library
  into a tenant schema at provision time
- `robosystems/taxonomy/pins.py` — resolves `Graph.taxonomy_pin` to
  a flat `{standard: version}` dict (walks `depends_on`)
- `robosystems/taxonomy/model.py` — Pydantic models for the parser
- `robosystems/taxonomy/seed.py` — legacy Python-dict seeder (kept for
  the old migration 0001 codepath)

The `TAXONOMY_ROOT` constant in `discovery.py` resolves to this
directory from the loader's package path; tests can override with a
synthetic root.
