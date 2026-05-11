# Taxonomy Frameworks — Named Compositions

A **framework** is a named, versioned, addressable composition that
pins specific (package, version) and (bridge, version) tuples into one
bundle. The deliverable that says "rs-gaap-base v1 = these N packages
+ these M bridges at these versions, in this load order."

The packages tier (`../packages/`) holds atomic taxonomy units; the
bridges tier (`../bridges/`) holds cross-namespace equivalence
taxonomies. Frameworks here are the composition layer — the artifact a
tenant graph pins via `Graph.taxonomy_pin = {"framework": "rs-gaap-base@v1"}`.

The architecture maps 1:1 to Charlie Hoffman's Seattle Method:
his "Reporting Scheme" (e.g. `us-gaap-theory.xsd`) is a framework
manifest; his individual XSDs (us-gaap, sfac6, fac, disclosures, dm,
types) are packages; his `disclosure-equivalentTextblock` arcs are a
bridge.

## Manifest shape

A framework lives at `frameworks/<name>/<version>.json` with this
shape:

```json
{
  "framework": "rs-gaap-base",
  "version": "v1",
  "title": "RoboSystems rs-gaap Base Reporting Framework",
  "description": "...",
  "framework_type": "reporting",
  "packages": [
    {"standard": "fac",     "version": "v1", "ordinal": 1, "is_required": true},
    {"standard": "rs-gaap", "version": "v1", "ordinal": 2, "is_required": true}
  ],
  "bridges": [
    {"bridge": "fac-to-rs-gaap", "version": "v1", "ordinal": 0, "is_required": true}
  ]
}
```

Fields:

- **`framework`** / **`version`** — the addressable identifier
  (`framework@version`).
- **`framework_type`** — one of `reporting` (a complete reporting
  framework like rs-gaap-base or ifrs-base), `extension` (a
  specialty overlay like banking-vertical that builds on a reporting
  framework), or `custom`.
- **`packages[]`** / **`bridges[]`** — the pin. Each entry is a
  `(name, version, ordinal, is_required)` tuple.
  - `ordinal` orders the load: packages by ordinal, then bridges by
    ordinal. Within the migration's three-phase loader (elements,
    arcs, rules) ordering is mostly irrelevant since all phases run
    over all entries before the next phase starts; ordinal is for
    display and human review.
  - `is_required: false` lets the migration skip an entry whose
    `taxonomy.jsonld` doesn't yet exist on disk. Useful while
    authoring a new framework whose Phase C packages aren't written
    yet.

## Framework resolution

`Graph.taxonomy_pin` (JSONB) controls which framework a tenant gets.
The resolver in `robosystems.taxonomy.pins.resolve_pin` accepts three
shapes:

```jsonc
// 1. null → fall back to the default framework (rs-gaap-base@v1)
null

// 2. framework reference
{"framework": "rs-gaap-base@v1"}

// 3. framework reference with per-package overrides
{"framework": "rs-gaap-base@v1", "overrides": {"rs-gaap": "v2"}}

// 4. legacy direct pin (backward compat for pre-framework graphs)
{"fac": "v1", "rs-gaap": "v1", ...}
```

Cases 1–3 expand the framework manifest into a flat
`{standard: version}` dict. Case 4 is returned as-is.

## Layout

```
frameworks/
└── rs-gaap-base/
    └── v1.json     default framework — what every tenant gets unless they opt in elsewhere
```

Future:

```
frameworks/
├── rs-gaap-base/v1.json                default
├── rs-gaap-sec-export/v1.json          rs-gaap-base + SEC bridges (rs-gaap-to-us-gaap, etc.)
├── rs-gaap-banking/v1.json             vertical: banking-specific Reporting Style
├── rs-gaap-insurance/v1.json           vertical: insurance-specific Reporting Style
└── ifrs-base/v1.json                   future: IFRS reporting framework
```

## Adding a new framework

1. Create `frameworks/<name>/v1.json` matching the schema above.
2. Pin every required package and bridge with an `ordinal` reflecting
   intended load order.
3. After Phase B lands, register the framework in the `frameworks` and
   `framework_packages` / `framework_bridges` tables (the migration
   walks `frameworks/*/v*.json` and inserts rows automatically).
4. Tenants opt in via `Graph.taxonomy_pin = {"framework": "<name>@v1"}`.

## Versioning

Frameworks are versioned independently of their constituent packages.
A framework version bump is appropriate when:

- Adding or removing a package or bridge from the composition.
- Changing the load order in a way that affects content.
- Changing the framework's identity (purpose, target use case).

Adding new optional packages (Phase C-style additions to an existing
framework) does NOT require a framework version bump as long as
existing tenants get the same expansion they did before. Mark new
entries `is_required: false` until they're stable.
