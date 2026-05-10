# Taxonomy Bridges — Cross-Namespace Equivalences

A **bridge** is a taxonomy whose job is to relate qnames in one sovereign
namespace to qnames in another. Bridges are the seam that lets multiple
taxonomies (FAC, rs-gaap, us-gaap, ifrs, …) coexist as peers rather than
fighting for a single canonical namespace.

The packages tier (`../packages/`) holds atomic taxonomy units; this
tier holds the equivalence arcs between them. Frameworks
(`../frameworks/`) compose specific package + bridge versions into named
bundles.

## What a bridge declares

A bridge's `taxonomy.jsonld` is shaped like any other taxonomy package
— same loader, same schema — but its content is dominated by
`equivalence`-typed associations whose `arcFrom` and `arcTo` cross
namespaces. Top-level metadata adds two fields:

```json
{
  "standard":         "fac-to-rs-gaap",
  "version":          "v1",
  "taxonomy_type":    "mapping",
  "source_namespace": "fac",
  "target_namespace": "rs-gaap",
  ...
}
```

The bridge is loaded into the same `taxonomies` / `associations` tables
as any package. Phase B (the `bridges` SQLAlchemy table) layers a
metadata overlay on top so admin tools can query "what bridges this
framework pin?" without scanning every taxonomy.

## Layout

```
bridges/
├── fac-to-rs-gaap/v1/                                  FAC ↔ rs-gaap concept equivalence
└── rs-gaap-disclosures-to-rs-gaap-textblocks/v1/       Disclosure name ↔ rs-gaap text-block element  ★ Phase C
```

Future SEC-export and IFRS bridges live here too:

```
bridges/
├── rs-gaap-to-us-gaap/v1/                              SEC export bridge
├── rs-gaap-disclosures-to-us-gaap-textblocks/v1/       SEC text-block bridge
└── rs-gaap-to-ifrs/v1/                                 IFRS export bridge
```

## When to author a new bridge

When you need a taxonomy in namespace A to be rendered, validated, or
exported as a taxonomy in namespace B without merging the two. Bridges
let rs-gaap stay sovereign while still emitting SEC-compatible filings,
and they let our Disclosure namespace resolve to underlying us-gaap text
block elements without our Disclosures becoming us-gaap concepts.

The pattern from XBRL is identical: equivalence arcs (`equivalentClass`,
`equivalentTextblock`) declared at the framework composition level.

## Adding a new bridge

1. Create `bridges/<name>/v1/taxonomy.jsonld` with the metadata fields
   above plus an `@graph` of equivalence arcs (use `arcFrom`/`arcTo`
   pointing across namespaces, `arcAssociationType: "equivalence"`).
2. Add an entry to the relevant Framework manifest in `../frameworks/`
   under `bridges[]` with an `ordinal` and `is_required` flag.

The migration auto-walks the framework manifest in load order; no Python
list to maintain.
