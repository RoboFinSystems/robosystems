# Taxonomy & Frameworks

RoboSystems treats accounting vocabulary as data, not code: concepts, hierarchies, calculation arcs, and rules are queryable, versionable rows seeded from JSON-LD packages. This page explains the substrate — the `fac` and `rs-gaap` frameworks, the canonical ontology, the Element and Association atoms, and traits — and shows you how to contribute your own vocabulary through the Taxonomy Block write path.

## Table of Contents

- [Overview](#overview)
- [The Two Frameworks](#the-two-frameworks)
- [The Canonical Ontology](#the-canonical-ontology)
- [The Element Atom and Associations](#the-element-atom-and-associations)
- [Traits](#traits)
- [The JSON-LD Package Library](#the-json-ld-package-library)
- [Contributing: The Taxonomy Block Write Path](#contributing-the-taxonomy-block-write-path)
- [Worked Example](#worked-example)
- [Reading the Library](#reading-the-library)
- [Common Pitfalls](#common-pitfalls)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

Two ideas anchor the entire taxonomy substrate:

1. **Everything is an Element.** A single `elements` table holds chart-of-accounts lines, rs-gaap reporting concepts, abstract groupings, and dimension axes alike. They differ in *provenance* (the `source` column) and *role* (the `element_type` column), not in structure. There is no separate `accounts` table.
2. **Taxonomy is data, not code.** Concepts and the relationships between them are rows seeded from JSON-LD packages. Adding a new framework is a content change, not a code change.

Structure — the shape of a financial statement, the way subtotals roll up — lives in `Association` rows typed by an `association_type` (the XBRL "linkbase" model). Per-element semantics (asset vs. liability, operating vs. nonoperating, cash-flow classification) live in `traits` bound to elements through a junction table. The whole thing is seeded from a JSON-LD package library that every tenant graph copies from at provision time.

## The Two Frameworks

RoboSystems ships two frameworks. One is the universal substrate; the other is the curated US-GAAP reporting vocabulary built on top of it.

| Framework | Role | Depends on | Contents |
| --- | --- | --- | --- |
| **`fac`** | Universal substrate and dependency root | — | Fundamental Accounting Concepts plus the trait vocabulary |
| **`rs-gaap`** | Canonical US-GAAP reporting vocabulary | `fac@v1` | Roughly 2,000 curated concepts plus presentation, calculation, labels, disclosures, and rules |

**`fac`** (Fundamental Accounting Concepts) is the smallest framework: a set of fundamental concepts plus the trait vocabulary that every other framework reuses. It is the dependency root — `rs-gaap` declares `depends_on: fac@v1`.

**`rs-gaap`** is the working reporting vocabulary. Every renderable presentation parent and calculation child is an `rs-gaap:*` concept. It is a curated RoboSystems framework — a frozen, intentionally narrow set of concepts chosen for clean rendering and mapping — not a live mirror of the full FASB US-GAAP taxonomy. Each tenant receives a curated subset of `rs-gaap` rather than the full public catalog (see [The JSON-LD Package Library](#the-json-ld-package-library)).

## The Canonical Ontology

Every JSON-LD seed in the library conforms to one RDF vocabulary, stored at `frameworks/ontology/v1/`. It has three co-located faces:

| File | Purpose |
| --- | --- |
| `context.jsonld` | The published `@context` every package is interpreted against |
| `ontology.ttl` | OWL class and property declarations |
| `shapes.ttl` | SHACL shapes that enforce the model |

The guiding principle is simple: **model the RoboSystems graph topology, and label it with XBRL's standard vocabulary wherever XBRL already has a term.** Balance type is `xbrli:balance`; arc roles are `xlink:arcrole`; calculation weights are `link:weight`. The `rs:` namespace is reserved only for concepts XBRL lacks. This keeps the seed data interoperable with the wider XBRL ecosystem while letting the graph carry structure XBRL cannot express directly.

## The Element Atom and Associations

The Element is the atom of the taxonomy. One row in the `elements` table can represent any of the following, distinguished by `element_type` and `source`:

- a chart-of-accounts line (`source="native"`, e.g. `acme:Cash` with code `1000`)
- an rs-gaap reporting concept (`source="rs-gaap"`, e.g. `rs-gaap:Assets`)
- an abstract grouping that has no value of its own (`element_type="abstract"`)
- a dimension axis or member (`element_type="axis"` / `"member"`)

Key element columns include `qname`, `name`, `balance_type` (`debit | credit`), `period_type` (`duration | instant`), `element_type`, `is_monetary`, `code` (the CoA code), and `source`. The `source` column captures provenance — values include `fac`, `rs-gaap`, `us-gaap`, `ifrs`, `quickbooks`, `xero`, `plaid`, `native`, `import`, and `system`. Tenant-authored elements are always written with `source="native"`.

**Associations are the structure.** Hierarchy and semantics are not stored on the Element; they live in `Association` rows, each typed by an `association_type` that mirrors an XBRL linkbase:

| `association_type` | What it expresses |
| --- | --- |
| `presentation` | The renderable tree — the order and nesting a statement displays |
| `calculation` | Parent = sum of weighted children; the spine of subtotal derivation |
| `mapping` | A chart-of-accounts element mapped to a reporting concept |
| `equivalence` | Two elements are interchangeable |
| `general-special` | A general concept refined by a more specific one |
| `essence-alias` | One element is an alias for another |

Associations are unique on `(structure_id, from, to, association_type)`, so the same two elements can participate in several linkbases at once — a concept can sit in a presentation tree *and* a calculation rollup with different parents. The `calculation` linkbase is the DAG the renderer walks to derive and foot subtotals; see [Reporting & Rendering](Reporting-and-Rendering) for how that projection works.

## Traits

Per-element accounting semantics are kept out of the Element row and stored in a `traits` vocabulary bound to elements through an `element_traits` junction. There are 26 trait categories: 24 FASB metamodel axes (elements of financial statements, liquidity, operating-vs-nonoperating, and so on), plus `flowClassification` (which cash-flow section a flow belongs to) and the RoboSystems `recurrence` axis.

Keeping traits separate from the Element lets the semantic coverage evolve independently of the concept catalog — you can enrich how an element is classified without touching the element itself. Traits also drive rendering decisions: cash-flow classification, for example, routes a flow to the correct statement section.

## The JSON-LD Package Library

Frameworks are distributed as JSON-LD packages under `frameworks/`, declared by a per-framework **pin manifest**.

```text
frameworks/
├── README.md
├── ontology/v1/   → context.jsonld, ontology.ttl, shapes.ttl
├── fac/           → v1.json + packages/{fac-traits, fac, fac-presentation, fac-calculations}
└── rs-gaap/       → v1.json + tenant-exclude/ + bridges/ + packages/{rs-gaap, rs-gaap-traits,
                      rs-gaap-hierarchy, rs-gaap-presentation, rs-gaap-calculations,
                      rs-gaap-type-subtype, rs-gaap-references, rs-gaap-labels, rs-gaap-disclosures,
                      rs-gaap-disclosure-mechanics, rs-gaap-reporting-checklist,
                      rs-gaap-reporting-styles, rs-gaap-rollup-rules, rs-gaap-rules}
```

The pin manifest (`frameworks/{name}/v1.json`) declares the framework, its version, its `framework_type` (`reporting | extension | custom`), its `depends_on` list, and the ordered `packages[]` and `bridges[]` it composes. Each package and bridge entry carries a `tenant_copy` flag:

| `tenant_copy` | Effect at provision |
| --- | --- |
| `true` | Seeded to the public library **and** copied into each new tenant |
| `false` | Seeded to the public library only; skipped on the per-tenant copy |

**Provisioning.** A tenant graph carries a `taxonomy_pin` (for example `{"framework": "rs-gaap@v1"}`). When the graph is provisioned, the library copies the pinned vocabulary from the public schema into the tenant schema, preserving stable element ids so cross-references stay valid.

**Library-origin rows are immutable inside every tenant.** Once a concept is copied from the public library into a tenant, you extend it — you never mutate it. This is enforced by immutability triggers and is the foundation of the contribution model below: tenants add native elements alongside the library, never edit the library in place.

**Editing the framework source** (the JSON-LD packages themselves) requires reseeding the library. A plain restart re-reads the baked image copy; reseed the library from source to pick up edits to `frameworks/**/taxonomy.jsonld`:

```bash
just reset-local
```

## Contributing: The Taxonomy Block Write Path

The **Taxonomy Block** is the single public write path for vocabulary. It is modeled directly on the [Information Block](Information-Blocks): a molecule (the envelope) crosses the API boundary in one transaction, never a raw atom. "No raw element writes" is the taxonomy-side twin of "no raw fact writes" — there is no per-row element or association CRUD on the public surface.

A Taxonomy Block envelope carries everything needed to stand up a piece of vocabulary atomically: the taxonomy itself, its `structures`, its `elements`, the `associations` that wire them together, and any user `rules`. The system also auto-generates structural rules (unique qnames, no cycles, no orphan arcs, parent-before-child, leaf classification for charts of accounts, and library-immutability for extensions).

### Block Types

The `taxonomy_type` field selects the block type. Three are writable from the public surface:

| `taxonomy_type` | Use when | Discipline |
| --- | --- | --- |
| **`chart_of_accounts`** | You are defining a company's ledger accounts | Declarative; every element requires a `trait`; stock concepts forced to `instant` |
| **`reporting_extension`** | You are layering native concepts onto a library reporting standard | Requires `parent_taxonomy_id`; references resolve local-first then library-fallback |
| **`custom_ontology`** | You need a free-form vocabulary with no accounting discipline | No trait requirement; every reference must resolve within the envelope |

The remaining types (`reporting_standard`, `schedule`) are seeded by an admin-only library creator. Attempting `update-taxonomy-block` on a `reporting_standard` returns `501`.

### The Operations

All writes go through the roboledger operations router as CQRS commands. Each returns an `OperationEnvelope` and accepts an `Idempotency-Key` header:

```text
POST /extensions/roboledger/{graph_id}/operations/{op_name}
```

| `op_name` | Purpose |
| --- | --- |
| `create-taxonomy-block` | Create a taxonomy block (taxonomy + structures + elements + associations + rules) atomically |
| `update-taxonomy-block` | Incrementally mutate an existing block via typed delta lists |
| `delete-taxonomy-block` | Delete a block (with a thin confirmation) |
| `link-entity-taxonomy` | Link the graph entity to a taxonomy or switch the primary chart of accounts |
| `create-mapping-association` | Add one CoA → reporting-concept mapping edge |
| `delete-mapping-association` | Drop one mapping edge by id |
| `auto-map-elements` | Run the MappingOperator asynchronously over a mapping structure |

`auto-map-elements` is a worker-backed operation that returns `202` with a `pending` envelope, then streams progress over SSE. It auto-approves mappings with confidence at or above `0.90`, flags those between `0.70` and `0.89` for review, and skips anything below `0.70`.

The request and response schemas (`CreateTaxonomyBlockRequest`, `TaxonomyBlockElementRequest`, `TaxonomyBlockAssociationRequest`, `TaxonomyBlockRuleRequest`, `TaxonomyBlockEnvelope`, and the rest) are published in the live OpenAPI spec rather than re-documented here. See [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` when running locally). A few load-bearing field facts to know before you call:

- **`qname` is the envelope-local reference token.** It is unique within the envelope's `elements` list, and `parent_ref`, `from_ref`, `to_ref`, and rule targets all reference elements by qname — never by id.
- **List caps:** elements ≤ 5,000, structures ≤ 100, associations ≤ 20,000, rules ≤ 500.
- **Only arithmetic rule patterns are user-creatable** (`SumEquals`, `RollUp`, `RollForward`, `GreaterThan`, and the like). Model-structure checks (no cycles, unique qname, parent-before-child, and so on) are system-emitted, not authored.

## Worked Example

This walkthrough authors a custom ontology, then reads it back. The examples assume a tenant graph id in `$GRAPH_ID` (set it from `.local/config.json` after running `just demo-roboledger`) and read the API key from the same config file.

### Step 1: Create a Custom Ontology

A `custom_ontology` block has no accounting discipline — no required trait, no balance type, no forced period type. This one declares an emissions vocabulary with a small presentation tree:

```bash
GRAPH_ID=<your tenant graph id>

curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/create-taxonomy-block" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Climate Disclosure Concepts",
    "taxonomy_type": "custom_ontology",
    "standard": "acme-climate",
    "elements": [
      {"qname": "acme:Emissions",       "name": "Total Emissions",   "element_type": "abstract", "is_monetary": false},
      {"qname": "acme:Scope1Emissions", "name": "Scope 1 Emissions", "is_monetary": false, "parent_ref": "acme:Emissions"},
      {"qname": "acme:Scope2Emissions", "name": "Scope 2 Emissions", "is_monetary": false, "parent_ref": "acme:Emissions"}
    ],
    "structures": [
      {"name": "emissions_tree", "block_type": "custom"}
    ],
    "associations": [
      {"structure_ref": "emissions_tree", "from_ref": "acme:Emissions", "to_ref": "acme:Scope1Emissions", "association_type": "presentation", "order_value": 1},
      {"structure_ref": "emissions_tree", "from_ref": "acme:Emissions", "to_ref": "acme:Scope2Emissions", "association_type": "presentation", "order_value": 2}
    ]
  }'
```

The response is a `TaxonomyBlockEnvelope`. Its `id` is the **taxonomy id** (not a structure id) — keep it for the read-back step.

### Step 2: Create a Chart of Accounts

The chart-of-accounts case is the declarative reference. Every element requires a `trait`, and stock concepts (assets, liabilities, equity) are forced to `period_type='instant'` regardless of what you send:

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/create-taxonomy-block" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Acme CoA",
    "taxonomy_type": "chart_of_accounts",
    "elements": [
      {"qname": "acme:Cash",            "name": "Cash",             "trait": "asset",     "balance_type": "debit",  "code": "1000"},
      {"qname": "acme:AccountsPayable", "name": "Accounts Payable", "trait": "liability", "balance_type": "credit", "code": "2000"}
    ],
    "structures": [
      {"name": "main", "block_type": "chart_of_accounts"}
    ]
  }'
```

A chart of accounts auto-links to the graph entity as its primary chart of accounts at create time. There is only one primary per entity; use `link-entity-taxonomy` to switch it.

### Step 3: Layer a Reporting Extension

A `reporting_extension` adds native concepts onto a library reporting standard. It requires `parent_taxonomy_id` (the id of an `rs-gaap` reporting standard taxonomy), and `parent_ref` may point at a library element qname — references resolve local-first, then fall back to the parent library taxonomy:

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/create-taxonomy-block" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Acme rs-gaap Extension",
    "taxonomy_type": "reporting_extension",
    "parent_taxonomy_id": "<rs-gaap reporting_standard taxonomy id>",
    "elements": [
      {"qname": "acme:NonGAAPAdjustedRevenue", "name": "Non-GAAP Adjusted Revenue", "trait": "revenue", "balance_type": "credit", "parent_ref": "rs-gaap:Revenues"}
    ],
    "structures": [
      {"name": "income_statement", "block_type": "income_statement"}
    ]
  }'
```

### Step 4: Read the Block Back

Use the tenant-scoped GraphQL endpoint and the taxonomy id from the create response. Fields are camelCase in the GraphQL schema even though the Python resolvers are snake_case:

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ taxonomyBlock(id: \"<taxonomy id from create response>\") { name taxonomyType elementCount associationCount elements { qname name trait origin } } }"}'
```

The `origin` field on each element reports `library` for copied-from-public rows and `tenant` for your native additions — a quick way to see the immutability boundary in action.

## Reading the Library

There are two GraphQL read surfaces, both Strawberry-backed with GraphiQL available in dev.

**Browse the canonical public library** through the `library` sentinel (`graph_id="library"`, `search_path=public`):

```bash
curl -X POST "http://localhost:8000/extensions/library/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ libraryElement(qname: \"rs-gaap:Assets\") { id qname name balanceType periodType source } libraryTaxonomies(standard: \"rs-gaap\") { id name version } }"}'
```

The `LibraryQuery` root exposes fields for taxonomies (`libraryTaxonomies`, `libraryTaxonomy`, `libraryTaxonomyArcs`, `libraryTaxonomyArcCount`), elements (`libraryElements`, `libraryElement`, `searchLibraryElements`, `libraryElementTree`, `libraryElementEquivalents`, `libraryElementArcs`, `libraryElementClassifications`), and the `TaxonomyBlockQuery` root exposes `taxonomyBlock(id:)` and `taxonomyBlocks(...)`.

**Read a tenant view** (the library copy plus the tenant's own CoA and extensions in one query) through the standard graph-scoped endpoint (`search_path={schema}, public`):

```text
POST /extensions/{graph_id}/graphql
```

The full field set is introspectable from GraphiQL in dev, or browse the spec at [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs).

## Common Pitfalls

### No Raw Element or Association CRUD

All vocabulary writes go through `create-taxonomy-block`, `update-taxonomy-block`, and `delete-taxonomy-block` (or the mapping-association operations for CoA → GAAP edges). There is no per-row element or association CRUD on the public surface.

### References Are by `qname`, Not Id

`parent_ref`, `from_ref`, `to_ref`, and rule targets all reference elements by their envelope-local `qname`. Inside an envelope, references resolve local-first. For `reporting_extension` they then fall back to the parent library taxonomy; for `custom_ontology` there is no fallback at all — every reference must resolve to a qname declared in the same envelope, or the create fails.

### `reporting_extension` Needs a Standard Parent

A `reporting_extension` requires `parent_taxonomy_id`, and the parent must be a `reporting_standard` (a library taxonomy). Anything else returns `422`.

### Reporting Standards Are Read-Only Here

`reporting_standard` blocks are seeded by the admin-only library creator. Calling `update-taxonomy-block` on one returns `501`.

### Library Rows Stay Immutable

Once a library concept is copied into a tenant, it can never be updated or deleted in that tenant — immutability triggers enforce this. A tenant block can be mutable, but any library-origin row inside it cannot. Deleting a library taxonomy through the envelope is rejected.

### Deletes Are Blocked by References

`delete-taxonomy-block` is rejected if the taxonomy has live facts (unless you pass `cascade_facts=true`), referencing journal line items, or cross-taxonomy mapping associations from other taxonomies.

### CoA Elements Require a Trait

Chart-of-accounts elements must carry a `trait`, and stock concepts (`asset`, `contraAsset`, `liability`, `contraLiability`, `equity`, `contraEquity`, `temporaryEquity`) are forced to `period_type='instant'` no matter what you send.

### Framework Source Edits Need a Reseed

Editing `frameworks/**/taxonomy.jsonld` does not take effect on a plain restart — the baked image copy is re-read. Run `just reset-local` to reseed the library from source.

## Related Documentation

**Wiki Guides:**

- **[Information Blocks](Information-Blocks)** - Where Elements get their *values*; the Taxonomy Block reuses the same envelope, idempotency, audit, and auto-rule machinery
- **[Reporting & Rendering](Reporting-and-Rendering)** - How statements render over these Elements; the renderer walks the `calculation` linkbase to derive and foot subtotals
- **[Custom Graph Schema](Custom-Graph-Schema)** - A different layer: the LadybugDB **graph schema** (node and relationship table DDL via `schema.json`), not the accounting **vocabulary** described here. Do not conflate "custom graph schema" with "custom ontology"

**Codebase Documentation:**

- **[Schemas README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/schemas/README.md)** - Graph schema definitions, extension naming conventions, URL and flag topology
- **[GraphQL README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - Strawberry GraphQL extensions surface, Pydantic auto-derivation, resolver patterns
- **[Extensions Models README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/extensions/README.md)** - Extensions OLTP SQLAlchemy models with schema-per-graph tenancy
- **[API Documentation](https://api.robosystems.ai/docs)** - API reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
