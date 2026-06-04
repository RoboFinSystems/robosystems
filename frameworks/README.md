# Reporting Frameworks — the taxonomy library

This directory is the **authoritative source** for the JSON-LD taxonomy
artifacts that every tenant graph copies into its per-tenant schema at
provision time. Each child directory is a **framework** (fac, rs-gaap,
…); the Python code that discovers, parses, writes, and pins this
content lives at `robosystems/taxonomy/`.

The directory name matches the DB tables it seeds: `frameworks`,
`framework_packages`, `framework_bridges`.

## What a framework is

A **framework** is a named, versioned, addressable bundle that pins
specific `(package, version)` and `(bridge, version)` tuples and
optionally `depends_on` other frameworks. The deliverable that says
"`rs-gaap` v1 = these N packages + these M bridges + everything in
`fac` v1, at these versions, in this load order."

A tenant graph pins a framework via:

```jsonc
Graph.taxonomy_pin = {"framework": "rs-gaap@v1"}
```

The resolver in `robosystems.taxonomy.pins.resolve_pin` expands this
into a flat `{standard: version}` dict, walking `depends_on` to include
atoms owned by dependency frameworks. The tenant pin stays single;
dependency expansion is invisible to the tenant.

## The six-layer pattern

Every reporting domain in this library — accounting GAAP today, IFRS /
call report / tax / energy reporting in the future — shares the same
shape:

| Layer | Role | Examples |
| --- | --- | --- |
| **Upstream authority** | Publishes a taxonomy nobody edits but everyone files against | FASB us-gaap, IASB IFRS, NAIC statutory, FFIEC call report, FERC Form 1, EIA-861, EPA TRI |
| **`rs-` curation** | Our authored, edited, render-target version of the upstream | `rs-gaap` (today); future `rs-ifrs`, `rs-call-report`, `rs-irs`, `rs-ferc`, `rs-statutory`, `rs-tri` |
| **Framework manifest** | Pins packages + bridges + Styles into one composable deliverable | `rs-gaap/v1.json` |
| **Reporting Styles** | Vertical / filer-profile variants **within** a framework | Default, Small Private Company, Banking, Insurance, Mining, Cannabis (defined inside `rs-gaap-reporting-styles/v1/`) |
| **Bridges** | Equivalences between namespaces (curation ↔ upstream for filing, or curation ↔ peer) | `fac-to-rs-gaap`, future `rs-gaap-to-us-gaap` (SEC export), `rs-gaap-to-ifrs` |
| **Tenant CoA → curation mapping** | Per-graph: a customer's chart of accounts → rs-* leaves | LINE_ITEM_RELATES_TO_ELEMENT + Associations (lives in the tenant schema, not here) |

The unit doesn't have to be currency. XBRL already handles non-monetary
units (MWh, barrels, tons CO₂e, headcount), and our Fact model inherits
that — energy and emissions frameworks slot into the same pattern
without special-casing.

## Framework vs. Reporting Style — don't conflate them

| | Framework | Reporting Style |
| --- | --- | --- |
| **What it is** | A self-contained taxonomy stack with its own concept namespace, presentation, calc, and rules | A vertical / filer-profile composition of named Disclosures *within* a framework |
| **Cardinality** | A few (one per regulatory regime: GAAP, IFRS, call report, tax, FERC, …) | Many per framework (Default, Small Private, Banking, Insurance, Mining, Cannabis, …) |
| **Lives at** | `{name}/{version}.json` (+ shared `packages/` and `bridges/` siblings) | Rows inside the `*-reporting-styles/v1/` package of a framework |
| **Pinned by** | Graph (entity → framework via `Graph.taxonomy_pin`) | Entity within a graph (provisioning-time selection) |
| **Example** | `rs-gaap@v1` for US GAAP filers | `Banking Style` for an entity in `rs-gaap@v1` |

A bank that files a 10-K and a call report is pinned to **two
frameworks** (`rs-gaap` + `rs-call-report`) and within each one picks
the **Reporting Style** for its filer profile (Banking Style in rs-gaap).

## Current layout

```
frameworks/
├── README.md
│
├── fac/                        universal accounting substrate
│   ├── v1.json                 framework manifest (flat; v2.json can sit alongside)
│   └── packages/
│       ├── fac-traits/v1/      universal trait vocabulary (24 axes; seeds `traits`)
│       ├── fac/v1/             FAC concepts (Assets, Liabilities, Equity, …)
│       ├── fac-presentation/v1/
│       ├── fac-calculations/v1/
│       └── fac-rules/v1/       Seattle Method verification rules
│
└── rs-gaap/                    US GAAP curation; depends_on fac@v1
    ├── v1.json                 framework manifest
    ├── packages/
    │   ├── README.md
    │   ├── rs-gaap/v1/                    curated us-gaap leaves
    │   ├── rs-gaap-traits/v1/             per-element trait bindings (seeds `element_traits`)
    │   ├── rs-gaap-hierarchy/v1/
    │   ├── rs-gaap-presentation/v1/
    │   ├── rs-gaap-calculations/v1/
    │   ├── rs-gaap-type-subtype/v1/       general-special (type/subtype) arcs
    │   ├── rs-gaap-references/v1/         ASC citation reference linkbase (attach-by-qname)
    │   ├── rs-gaap-labels/v1/             supplementary + total-role label linkbase (attach-by-qname)
    │   ├── rs-gaap-disclosures/v1/
    │   ├── rs-gaap-disclosure-mechanics/v1/
    │   ├── rs-gaap-reporting-checklist/v1/
    │   └── rs-gaap-reporting-styles/v1/   ★ vertical / filer-profile surface
    ├── bridges/
    │   ├── README.md
    │   ├── fac-to-rs-gaap/v1/
    │   └── rs-gaap-disclosures-to-rs-gaap-textblocks/v1/
    └── tenant-exclude/
        └── v1.json                 per-tenant copy curation (policy, NOT a package)
```

### `tenant-exclude/` — per-tenant copy curation (a policy, not a package)

`tenant-exclude/v1.json` is **framework-level policy**, the same tier as the
manifest — *not* a package. Packages are JSON-LD taxonomy content **seeded into
the DB** and listed in the manifest's `packages[]`; this artifact is a flat
qname list **consumed by the copy path** (`taxonomy/writer.py`) at provision
time to decide which rs-gaap concepts a *tenant* receives. The full rs-gaap
catalog (~2155 concepts) stays in the **public** library (it backs the future
SEC us-gaap bridge); a tenant gets the curated subset (~1568) with the
clear-cut breadth omitted — XBRL dimension members/domains, general-special-
disconnected concepts, and industry verticals that belong in peer frameworks.
Generated + regenerated by
`robosystems/taxonomy/scripts/generate_tenant_exclude.py` (run against a seeded
library DB); the keep-critical subtraction it applies guarantees no rollup-
or rule-critical concept is ever excluded. Promotion is reversible: drop a
qname and re-sync. Deliberately **not** under `packages/` so the seeder never
mistakes a policy file for seedable taxonomy content.

Framework version lives in the manifest filename (`v1.json`), not in a
subdirectory. A future `rs-gaap@v2` ships as `rs-gaap/v2.json` alongside
`v1.json` and reuses the shared `packages/` + `bridges/` directories
(pinning whichever package versions it needs). The only `v1` segment in
a leaf path identifies the *package* version.

Every future framework (`rs-ifrs`, `rs-call-report`, `rs-irs`,
`rs-ferc`, `rs-statutory`, `rs-tri`, …) is a sibling here with the same
internal shape and its own `depends_on`. Atoms with byte-identical
content (like `fac/v1`) live in a single canonical framework that others
depend on, not duplicated copies.

## Manifest shape

A framework manifest lives at `{name}/{version}.json`:

```json
{
  "framework": "rs-gaap",
  "version": "v1",
  "title": "RoboSystems rs-gaap Reporting Framework",
  "description": "...",
  "framework_type": "reporting",
  "depends_on": [
    {"framework": "fac", "version": "v1"}
  ],
  "packages": [
    {"standard": "rs-gaap",                  "version": "v1", "ordinal": 0, "is_required": true},
    {"standard": "rs-gaap-traits",           "version": "v1", "ordinal": 1, "is_required": true},
    {"standard": "rs-gaap-presentation",     "version": "v1", "ordinal": 3, "is_required": true},
    {"standard": "rs-gaap-reporting-styles", "version": "v1", "ordinal": 9, "is_required": true}
  ],
  "bridges": [
    {"bridge": "fac-to-rs-gaap", "version": "v1", "ordinal": 0, "is_required": true}
  ]
}
```

Fields:

- **`framework`** / **`version`** — the addressable identifier
  (`framework@version`).
- **`framework_type`** — `reporting` (a complete reporting framework
  like rs-gaap, fac, or future rs-ifrs / rs-call-report); other types
  reserved for future use.
- **`depends_on[]`** — frameworks this one builds on. Resolved
  depth-first before this framework's own packages load. Cycles raise
  `ValueError`. Today only `rs-gaap → fac`; future regulatory frameworks
  like `rs-call-report` will depend on both `fac` and `rs-gaap` (since
  call reports map from GAAP numbers with regulatory adjustments).
- **`packages[]`** — atomic units owned by this framework. Each entry is
  a `(standard, version, ordinal, is_required)` tuple.
  - `ordinal` orders the load: dependencies first, then this framework's
    packages by ordinal, then its bridges by ordinal. Within the
    migration's three-phase loader (elements, arcs, rules) ordering is
    mostly irrelevant since all phases run over all entries before the
    next phase starts; ordinal is for display and human review.
  - `is_required: false` lets the migration skip an entry whose
    `taxonomy.jsonld` doesn't exist on disk yet.
  - `tenant_copy` (default `true`) is an **orthogonal** axis to
    `is_required`. It governs the *per-tenant copy*, not the public seed:
    a present `taxonomy.jsonld` is always seeded into the public library
    (the seeder keys off file presence + `is_required`, not this flag),
    but `tenant_copy: false` omits the package from
    `expand_framework_to_pin`, so `writer.copy_library_into_tenant`
    **does not copy it into per-tenant schemas**. Use it for content that
    should stay canonical in the library but is dormant / parked at MVP —
    keeping it out of every immutable tenant schema (and off the COGS
    line) without losing the definition. Promote later by flipping it back
    to `true` and running `operations/taxonomy_block/resync.py` (the copy
    gates each association on both element endpoints existing locally, so
    a cross-package arc into a `tenant_copy: false` package self-skips
    rather than dangling a NOT NULL FK).
- **`bridges[]`** — equivalence taxonomies owned by this framework
  (typically bridges *out of* a dependency framework's namespace into
  this one's). Same tuple shape as packages.

## Framework resolution

`Graph.taxonomy_pin` (JSONB) controls which framework a tenant gets.
The resolver accepts four polymorphic shapes:

```jsonc
// 1. null → fall back to DEFAULT_FRAMEWORK (rs-gaap@v1)
null

// 2. framework reference
{"framework": "rs-gaap@v1"}

// 3. framework reference with per-package overrides
{"framework": "rs-gaap@v1", "overrides": {"rs-gaap": "v2"}}

// 4. legacy direct pin (backward compat for pre-framework graphs)
{"fac": "v1", "rs-gaap": "v1", ...}
```

Cases 1–3 expand the framework manifest (and walk `depends_on`) into a
flat `{standard: version}` dict. Case 4 is returned as-is.

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
are intentionally different frameworks projecting from the same ledger.
The same dynamic applies to call-report-to-GAAP, stat-to-GAAP,
FERC-to-GAAP.

Today the library ships two frameworks (fac + rs-gaap). The directory
structure and `depends_on` machinery support adding peer frameworks
without restructure. The mature `Graph.taxonomy_pin` shape (plural,
role-tagged: `[{framework, role: "book"|"regulatory"|"tax"}, ...]`) is a
model-layer evolution, not a library-layer one.

## Adding a new framework

1. Create `<name>/v1.json` matching the schema above. Declare
   `depends_on` for any prior frameworks whose atoms you reuse.
2. Add the framework's own packages under
   `<name>/packages/<standard>/<version>/taxonomy.jsonld`.
3. Add bridges owned by this framework (typically bridging *into* one of
   your dependencies' namespaces) under
   `<name>/bridges/<bridge>/<version>/taxonomy.jsonld`.
4. Run a fresh `just reset-local` — migration 0007 walks every
   `frameworks/*/v*.json` and inserts `frameworks` / `framework_packages`
   / `framework_bridges` rows automatically. No Python list to maintain.
5. Tenants opt in via `Graph.taxonomy_pin = {"framework": "<name>@v1"}`.

## Versioning

Frameworks are versioned independently of their constituent packages and
their dependencies. A framework version bump is appropriate when:

- Adding or removing a package, bridge, or `depends_on` entry.
- Bumping a `depends_on` to a different dependency version.
- Changing load order in a way that affects content.
- Changing the framework's identity (purpose, target use case).

Adding new optional packages (marked `is_required: false`) does NOT
require a framework version bump as long as existing tenants get the
same expansion they did before.

## Where the code lives

The Python runtime stays at `robosystems/taxonomy/` as a flat module
(single-word filenames, read top-to-bottom as the lifecycle):

- `model.py` — Pydantic `TaxonomyPackage` and component specs
- `discovery.py` — filesystem walking, manifest reading, dependency
  expansion (the `FRAMEWORKS_DIR` constant points at this directory)
- `loader.py` — JSON-LD parser via rdflib → `TaxonomyPackage`
- `writer.py` — copies the pinned library subset into a tenant schema
- `pins.py` — resolves `Graph.taxonomy_pin` to a flat `{standard:
  version}` dict (walks `depends_on`)
- `seed.py` — legacy Python-dict seeder (kept for migration 0001)
