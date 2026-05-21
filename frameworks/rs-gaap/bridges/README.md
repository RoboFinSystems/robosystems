# rs-gaap v1 — Bridges

A **bridge** is a taxonomy whose job is to relate qnames in one
sovereign namespace to qnames in another. Bridges let multiple
taxonomies (FAC, rs-gaap, us-gaap, ifrs, …) coexist as peers rather
than fighting for a single canonical namespace.

The packages tier (`../packages/`) holds atomic taxonomy units. This
tier holds the equivalence arcs between them. The framework
(`../manifest.json`) pins specific package + bridge versions.

Bridges live with the framework that **authored** the mapping. The
`fac-to-rs-gaap` bridge lives here (under `rs-gaap`) because rs-gaap
is the curation that decided how to map onto fac. A future
`fac-to-rs-ifrs` bridge would live under `rs-ifrs`'s bridges
directory, not here.

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

The bridge is loaded into the same `taxonomies` / `associations`
tables as any package. The `bridges` SQLAlchemy table layers a
metadata overlay on top so admin tools can query "what bridges this
framework pin?" without scanning every taxonomy.

## Current layout

```
bridges/
├── fac-to-rs-gaap/v1/                                FAC concept ↔ rs-gaap concept equivalence
└── rs-gaap-disclosures-to-rs-gaap-textblocks/v1/     Disclosure name ↔ rs-gaap text-block element
```

## Future bridges (probable additions)

```
bridges/
├── rs-gaap-to-us-gaap/v1/                            SEC export bridge (when SEC export Style ships)
└── rs-gaap-disclosures-to-us-gaap-textblocks/v1/     SEC text-block bridge
```

Bridges that cross *into* a different framework's namespace live in
that framework's bridges directory if it's the authoring side. For
example, a future `rs-gaap-to-rs-ifrs` bridge (for international
filers crossing GAAP↔IFRS) would live under `rs-ifrs/bridges/`
since rs-ifrs would author the mapping.

## When to author a new bridge

When you need a taxonomy in namespace A to be rendered, validated, or
exported as a taxonomy in namespace B without merging the two. Bridges
let rs-gaap stay sovereign while still emitting SEC-compatible
filings, and they let our Disclosure namespace resolve to underlying
us-gaap text block elements without our Disclosures becoming us-gaap
concepts.

The pattern from XBRL is identical: equivalence arcs
(`equivalentClass`, `equivalentTextblock`) declared at the framework
composition level.

## Adding a new bridge

1. Create `bridges/<name>/v1/taxonomy.jsonld` with the metadata fields
   above plus an `@graph` of equivalence arcs (use `arcFrom`/`arcTo`
   pointing across namespaces, `arcAssociationType: "equivalence"`).
2. Add an entry to this framework's manifest at `../manifest.json`
   under `bridges[]` with an `ordinal` and `is_required` flag.

The migration auto-walks the framework manifest in load order; no
Python list to maintain.
