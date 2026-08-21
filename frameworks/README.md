# Reporting Frameworks — the taxonomy library

This directory is the **authoritative source** for the JSON-LD taxonomy
artifacts that every tenant graph copies into its per-tenant schema at
provision time. Each child directory carrying a top-level `v*.json`
manifest is a **framework** (cm, fac, rs-gaap today) — the sibling
`ontology/` holds the canonical RDF ontology + SHACL shapes and is
deliberately _not_ a framework. The Python code that discovers, parses,
writes, and pins this content lives at `robosystems/taxonomy/`.

The directory name matches the DB tables it seeds: `frameworks`,
`framework_packages`, `framework_bridges`.

## What a framework is

A **framework** is a named, versioned, addressable bundle that pins
specific `(package, version)` and `(bridge, version)` tuples and
optionally `depends_on` other frameworks. The deliverable that says
"`rs-gaap` v1 = these N packages + these M bridges + everything in
`fac` v1 and `cm` v1, at these versions, in this load order."

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

| Layer                             | Role                                                                                 | Examples                                                                                                   |
| --------------------------------- | ------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| **Upstream authority**            | Publishes a taxonomy nobody edits but everyone files against                         | FASB us-gaap, IASB IFRS, NAIC statutory, FFIEC call report, FERC Form 1, EIA-861, EPA TRI                  |
| **`rs-` curation**                | Our authored, edited, render-target version of the upstream                          | `rs-gaap` (today); future `rs-ifrs`, `rs-call-report`, `rs-irs`, `rs-ferc`, `rs-statutory`, `rs-tri`       |
| **Framework manifest**            | Pins packages + bridges + Styles into one composable deliverable                     | `rs-gaap/v1.json`                                                                                          |
| **Reporting Styles**              | Vertical / filer-profile variants **within** a framework                             | Default, Partnership, Limited Liability Company (defined inside `rs-gaap-reporting-styles/v1/`)            |
| **Bridges**                       | Equivalences between namespaces (curation ↔ upstream for filing, or curation ↔ peer) | `fac-to-rs-gaap`, future `rs-gaap-to-us-gaap` (SEC export), `rs-gaap-to-ifrs`                              |
| **Tenant CoA → curation mapping** | Per-graph: a customer's chart of accounts → rs-\* leaves                             | LINE_ITEM_RELATES_TO_ELEMENT + Associations (lives in the tenant schema, not here)                         |

The unit doesn't have to be currency. XBRL already handles non-monetary
units (MWh, barrels, tons CO₂e, headcount), and our Fact model inherits
that — energy and emissions frameworks slot into the same pattern
without special-casing.

## Framework vs. Reporting Style — don't conflate them

|                 | Framework                                                                                                                     | Reporting Style                                                                  |
| --------------- | ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| **What it is**  | A self-contained taxonomy stack with its own concept namespace, presentation, calc, and rules                                 | A vertical / filer-profile composition of named Disclosures _within_ a framework |
| **Cardinality** | A few (one per regulatory regime: GAAP, IFRS, call report, tax, FERC, …)                                                      | Many per framework (today in `rs-gaap`: Default, Partnership, LLC)               |
| **Lives at**    | `{name}/{version}.json` (+ shared `packages/` and `bridges/` siblings)                                                        | Rows inside the `*-reporting-styles/v1/` package of a framework                  |
| **Pinned by**   | Graph for availability (`Graph.taxonomy_pin` controls the tenant copy); Entity for adoption (`EntityTaxonomy` rows per basis) | Entity within a graph (`Entity.reporting_style_id`, extensions DB)               |
| **Example**     | `rs-gaap@v1` for US GAAP filers                                                                                               | `Partnership Style` for an entity in `rs-gaap@v1`                                |

A bank that files a 10-K and a call report would be pinned to **two
frameworks** (`rs-gaap` + a future `rs-call-report`) and within each one
pick the **Reporting Style** for its filer profile (a future Banking
Style in rs-gaap). Vertical Styles are deferred; `rs-gaap@v1` ships the
equity-form family only — Default (corporate), Partnership, and Limited
Liability Company.

## Current layout

```
frameworks/
├── README.md
│
├── cm/                         conceptual-model substrate (no depends_on, no bridges)
│   ├── v1.json                 framework manifest
│   └── packages/
│       └── cm/v1/              cm:Debit + cm:Credit posting roles (2 concepts)
│
├── fac/                        universal accounting substrate
│   ├── v1.json                 framework manifest (flat; v2.json can sit alongside)
│   └── packages/
│       ├── README.md
│       ├── fac-traits/v1/      universal trait vocabulary (26 axes / 100 members; seeds `traits`)
│       ├── fac/v1/             FAC concepts (Assets, Liabilities, Equity, …)
│       ├── fac-presentation/v1/
│       └── fac-calculations/v1/
│
├── rs-gaap/                    US GAAP curation; depends_on fac@v1 + cm@v1
│   ├── v1.json                 framework manifest
│   ├── packages/
│   │   ├── README.md
│   │   ├── rs-gaap/v1/                    curated us-gaap leaves
│   │   ├── rs-gaap-traits/v1/             per-element trait bindings (seeds `element_traits`)
│   │   ├── rs-gaap-hierarchy/v1/
│   │   ├── rs-gaap-presentation/v1/
│   │   ├── rs-gaap-calculations/v1/
│   │   ├── rs-gaap-type-subtype/v1/       general-special (type/subtype) arcs
│   │   ├── rs-gaap-references/v1/         ASC citation reference linkbase (attach-by-qname)
│   │   ├── rs-gaap-labels/v1/             supplementary + total-role label linkbase (attach-by-qname)
│   │   ├── rs-gaap-disclosures/v1/
│   │   ├── rs-gaap-disclosure-mechanics/v1/
│   │   ├── rs-gaap-reporting-checklist/v1/
│   │   ├── rs-gaap-reporting-styles/v1/   ★ vertical / filer-profile surface
│   │   ├── rs-gaap-rollup-rules/v1/       L2 rollup-shaped consistency rules
│   │   ├── rs-gaap-rules/v1/              L1 cross-tree consistency rules (rs-gaap-targeted)
│   │   ├── rs-metric/v1/                  metric catalog + Derive rules (standing `metric` block)
│   │   └── rs-driver/v1/                  forecast lever catalog + Derive rules (reference Structure)
│   ├── bridges/
│   │   ├── README.md
│   │   ├── fac-to-rs-gaap/v1/
│   │   └── rs-gaap-disclosures-to-rs-gaap-textblocks/v1/
│   └── tenant-exclude/
│       └── v1.json                 per-tenant copy curation (policy, NOT a package)
│
└── ontology/                   NOT a framework — the canonical RDF ontology
    └── v1/                     context.jsonld · ontology.ttl · shapes.ttl
```

### `cm/` — the conceptual-model substrate (a framework, not a reporting taxonomy)

`cm@v1` is a minimal universal upper-vocabulary forked from Charlie
Hoffman's Seattle Method [`universal`](https://github.com/seattlemethod/universal)
conceptual model. v1 is intentionally tiny — two abstract concepts,
`cm:Debit` and `cm:Credit`. A has-part arc from one of them to a
Chart-of-Accounts element declares that element as the debit or credit
leg of a Structure's posting template, which makes double-entry posting
structure a first-class, queryable atom of an Information Block instead
of opaque mechanics metadata. It is **not** a reporting taxonomy: it
owns no presentation, calc, or rules, declares no `depends_on`, and
ships no bridges (it still carries `framework_type: "reporting"` only
because that is the sole type in use today). Tenants get it with the
default pin because `rs-gaap` `depends_on` it. It expands additively
(Thing, Event, Transaction, LineItem) when event serialization pulls
those in.

### `tenant-exclude/` — per-tenant copy curation (a policy, not a package)

`tenant-exclude/v1.json` is **framework-level policy**, the same tier as the
manifest — _not_ a package. Packages are JSON-LD taxonomy content **seeded into
the DB** and listed in the manifest's `packages[]`; this artifact is a flat
qname list **consumed by the copy path** (`taxonomy/writer.py`) at provision
time to decide which rs-gaap concepts a _tenant_ receives. The full rs-gaap
catalog (~2155 concepts) stays in the **public** library (it backs the future
SEC us-gaap bridge); a tenant gets the keep-critical curation (~143) under the
`tenant_exclude_keep_critical` policy (public ~2,155 → tenant ~143) — the
high-level aggregates that render/foot/map today. Omitted: XBRL dimension members/domains,
general-special-disconnected concepts, industry verticals (peer-framework
material), and the general-special **leaf** level — the finest disaggregation
detail, inert until the granularity-selection feature ships. Disaggregation
levels are added back later via resync (add is cheap; deleting a concept a
tenant has already mapped to is not).
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
a leaf path identifies the _package_ version.

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
    { "framework": "fac", "version": "v1" },
    { "framework": "cm", "version": "v1" }
  ],
  "packages": [
    {
      "standard": "rs-gaap",
      "version": "v1",
      "ordinal": 0,
      "is_required": true
    },
    {
      "standard": "rs-gaap-traits",
      "version": "v1",
      "ordinal": 1,
      "is_required": true
    },
    {
      "standard": "rs-gaap-presentation",
      "version": "v1",
      "ordinal": 3,
      "is_required": true
    },
    {
      "standard": "rs-gaap-reporting-styles",
      "version": "v1",
      "ordinal": 11,
      "is_required": true
    }
  ],
  "bridges": [
    {
      "bridge": "fac-to-rs-gaap",
      "version": "v1",
      "ordinal": 0,
      "is_required": true
    }
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
  `ValueError`. Today only `rs-gaap → [fac, cm]`; future regulatory
  frameworks like `rs-call-report` will depend on both `fac` and
  `rs-gaap` (since call reports map from GAAP numbers with regulatory
  adjustments).
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
    `is_required`. It governs the _per-tenant copy_, not the public seed:
    a present `taxonomy.jsonld` is always seeded into the public library
    (the seeder keys off file presence + `is_required`, not this flag),
    but `tenant_copy: false` omits the package from
    `expand_framework_to_pin`, so `writer.copy_library_into_tenant`
    **does not copy it into per-tenant schemas**. Use it for content that
    should stay canonical in the library but is dormant (not used by the
    default tenant set) — keeping it out of every immutable tenant schema
    (and off the COGS line) without losing the definition. Promote later by flipping it back
    to `true` and running `operations/taxonomy_block/resync.py` (the copy
    gates each association on both element endpoints existing locally, so
    a cross-package arc into a `tenant_copy: false` package self-skips
    rather than dangling a NOT NULL FK).
- **`bridges[]`** — equivalence taxonomies owned by this framework
  (typically bridges _out of_ a dependency framework's namespace into
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

The pin is the **availability** layer only — it decides which library
content gets copied into the tenant schema at provision time. Which
taxonomies an _entity_ actually uses is the separate **adoption**
layer: the `EntityTaxonomy` join table (extensions DB,
`models/extensions/entity_taxonomy.py`) links an entity to any number
of taxonomies across bases (`reporting`, `chart_of_accounts`,
`mapping`, `schedule`), with at most one `is_primary` row per
(entity, basis). The pin stays on the Graph; adoption lives on the
Entity — so heterogeneous entities in one graph can each adopt
different reporting taxonomies, provided the pin made them available.

## One ledger, many filings — the load-bearing premise

Every U.S. business has multiple simultaneous reporting frameworks
projecting from one ledger:

| Entity profile    | Active frameworks                                                     |
| ----------------- | --------------------------------------------------------------------- |
| Public bank       | `rs-gaap` (10-K) + `rs-call-report` (FFIEC 031/041) + `rs-irs` (1120) |
| Public insurer    | `rs-gaap` (10-K) + `rs-statutory` (NAIC) + `rs-irs` (1120)            |
| Regulated utility | `rs-gaap` (10-K) + `rs-ferc` (Form 1) + `rs-irs`                      |
| Private LLC       | `rs-gaap` (book) + `rs-irs` (1065/1120)                               |
| Non-public corp   | `rs-gaap` (book) + `rs-irs` (1120)                                    |

Schedule M-1 / M-3 is the federally-mandated, line-by-line
reconciliation between book and tax — it exists _because_ book and tax
are intentionally different frameworks projecting from the same ledger.
The same dynamic applies to call-report-to-GAAP, stat-to-GAAP,
FERC-to-GAAP.

Today the library ships three frameworks (cm + fac + rs-gaap). The directory
structure and `depends_on` machinery support adding peer frameworks
without restructure. Multiple frameworks per graph is already
mechanically possible at the copy layer — a legacy flat pin can list
packages from more than one framework, and a framework can
`depends_on` another — and the adoption half is built: `EntityTaxonomy`
lets each entity adopt multiple taxonomies with one primary per basis.
What remains future is the first-class plural, role-tagged pin shape
(`[{framework, role: "book"|"regulatory"|"tax"}, ...]`) — a model-layer
evolution, not a library-layer one.

## Adding a new framework

1. Create `<name>/v1.json` matching the schema above. Declare
   `depends_on` for any prior frameworks whose atoms you reuse.
2. Add the framework's own packages under
   `<name>/packages/<standard>/<version>/taxonomy.jsonld`.
3. Add bridges owned by this framework (typically bridging _into_ one of
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
