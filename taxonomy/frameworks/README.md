# Frameworks — Named Reporting Compositions

A **framework** is a named, versioned, addressable bundle that
pins specific `(package, version)` and `(bridge, version)` tuples and
optionally `depends_on` other frameworks. The deliverable that says
"`rs-gaap` v1 = these N packages + these M bridges + everything in
`fac` v1, at these versions, in this load order."

A tenant graph pins a framework via:

```jsonc
Graph.taxonomy_pin = {"framework": "rs-gaap@v1"}
```

The resolver in `robosystems.taxonomy.pins.resolve_pin` expands this
into a flat `{standard: version}` dict, walking `depends_on` to
include atoms owned by dependency frameworks. The tenant pin stays
single; dependency expansion is invisible to the tenant.

## Framework vs. Reporting Style — what each one is for

These are different concepts. Don't conflate them.

| | Framework | Reporting Style |
| --- | --- | --- |
| **What it is** | A self-contained taxonomy stack with its own concept namespace, presentation, calc, and rules | A vertical / filer-profile composition of named Disclosures *within* a framework |
| **Cardinality** | A few (one per regulatory regime: GAAP, IFRS, call report, tax, FERC, …) | Many per framework (Default, Small Private, Banking, Insurance, Mining, Cannabis, …) |
| **Lives at** | `frameworks/{name}/{version}/` | Rows inside the `*-reporting-styles/v1/` package of a framework |
| **Pinned by** | Graph (entity → framework via `Graph.taxonomy_pin`) | Entity within a graph (provisioning-time selection) |
| **Example** | `rs-gaap@v1` for US GAAP filers | `Banking Style` for an entity in `rs-gaap@v1` |

A bank that files a 10-K and a call report is pinned to **two
frameworks** (`rs-gaap` + `rs-call-report`) and within each one picks
the **Reporting Style** appropriate for its filer profile (Banking
Style in rs-gaap).

## Manifest shape

A framework lives at `frameworks/{name}/{version}/manifest.json`:

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
    {"standard": "us-gaap-metamodel",    "version": "v1", "ordinal": 0, "is_required": true},
    {"standard": "rs-gaap",              "version": "v1", "ordinal": 1, "is_required": true},
    {"standard": "rs-gaap-presentation", "version": "v1", "ordinal": 4, "is_required": true},
    {"standard": "rs-gaap-reporting-styles", "version": "v1", "ordinal": 10, "is_required": true}
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
  depth-first before this framework's own packages load. Cycles
  raise `ValueError`. Today only `rs-gaap → fac`; future regulatory
  frameworks like `rs-call-report` will depend on both `fac` and
  `rs-gaap` (since call reports map from GAAP numbers with
  regulatory-specific adjustments).
- **`packages[]`** — atomic units owned by this framework. Each entry
  is a `(standard, version, ordinal, is_required)` tuple.
  - `ordinal` orders the load: dependencies first, then this
    framework's packages by ordinal, then this framework's bridges by
    ordinal. Within the migration's three-phase loader (elements,
    arcs, rules) ordering is mostly irrelevant since all phases run
    over all entries before the next phase starts; ordinal is for
    display and human review.
  - `is_required: false` lets the migration skip an entry whose
    `taxonomy.jsonld` doesn't exist on disk yet. Useful while
    authoring a new package.
- **`bridges[]`** — equivalence taxonomies owned by this framework
  (typically bridges *out of* the dependency framework's namespace
  into this one's). Same tuple shape as packages.

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

## Current layout

```
frameworks/
├── fac/v1/
│   ├── manifest.json    universal accounting-concept substrate
│   └── packages/        fac, fac-presentation, fac-calculations, fac-rules
│
└── rs-gaap/v1/          default framework — what every tenant gets unless they opt in elsewhere
    ├── manifest.json    depends_on: fac@v1
    ├── packages/        rs-gaap, rs-gaap-presentation, rs-gaap-calculations,
    │                    rs-gaap-disclosures, rs-gaap-disclosure-mechanics,
    │                    rs-gaap-hierarchy, rs-gaap-reporting-checklist,
    │                    rs-gaap-reporting-styles, rs-gaap-to-metamodel,
    │                    us-gaap-metamodel, type-subtype
    └── bridges/         fac-to-rs-gaap, rs-gaap-disclosures-to-rs-gaap-textblocks
```

## Future peer frameworks

The directory shape supports adding peers without restructure. Likely
near-term candidates:

```
frameworks/
├── fac/v1/                          (today)
├── rs-gaap/v1/                      (today; depends_on fac@v1)
│
├── rs-irs/v1/                       IRS Form 1120/1065 tax filings
│   └── (depends_on fac@v1 + rs-gaap@v1; Schedule M-1/M-3 bridges)
│
├── rs-call-report/v1/               FFIEC 031/041 bank call reports
│   └── (depends_on fac@v1 + rs-gaap@v1; call-report-to-GAAP bridges)
│
├── rs-ferc/v1/                      FERC Form 1 (utilities); non-monetary units (MWh)
│   └── (depends_on fac@v1 + rs-gaap@v1)
│
├── rs-statutory/v1/                 NAIC statutory (insurance blue book)
│   └── (depends_on fac@v1 + rs-gaap@v1; stat-to-GAAP bridges)
│
├── rs-ifrs/v1/                      IFRS curation (international peer to rs-gaap)
│   └── (depends_on fac@v1; rs-ifrs-to-ifrs export bridge)
│
└── rs-tri/v1/                       EPA Toxics Release Inventory; emissions in tons CO₂e
    └── (depends_on fac@v1 only; not GAAP-derived)
```

**Note**: vertical flavoring within US GAAP (Banking, Insurance,
Mining, Cannabis, etc.) is **not** a separate framework — it's a
**Reporting Style** inside `rs-gaap-reporting-styles/v1/`. The
distinction matters: a bank simultaneously files in `rs-gaap`
(Banking Style for the 10-K presentation) **and** `rs-call-report` (a
separate framework for the regulatory filing). Same ledger, two
framework projections, with a reconciliation surface between them.

## Adding a new framework

1. Create `frameworks/<name>/v1/manifest.json` matching the schema
   above. Declare `depends_on` for any prior frameworks whose atoms
   you reuse.
2. Add the framework's own packages under
   `frameworks/<name>/v1/packages/<standard>/<version>/taxonomy.jsonld`.
3. Add bridges owned by this framework (typically bridging *into* one
   of your dependencies' namespaces) under
   `frameworks/<name>/v1/bridges/<bridge>/<version>/taxonomy.jsonld`.
4. Run a fresh `just reset-local` — migration 0007 walks every
   `frameworks/*/v*/manifest.json` and inserts `frameworks` /
   `framework_packages` / `framework_bridges` rows automatically. No
   Python list to maintain.
5. Tenants opt in via `Graph.taxonomy_pin = {"framework": "<name>@v1"}`.

## Versioning

Frameworks are versioned independently of their constituent packages
and of their dependencies. A framework version bump is appropriate
when:

- Adding or removing a package, bridge, or `depends_on` entry from the
  composition.
- Bumping a `depends_on` to a different dependency version.
- Changing load order in a way that affects content.
- Changing the framework's identity (purpose, target use case).

Adding new optional packages (marked `is_required: false`) does NOT
require a framework version bump as long as existing tenants get the
same expansion they did before.
