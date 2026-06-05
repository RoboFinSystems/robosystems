# rs-gaap v1 — Packages

This directory holds the **packages owned by the `rs-gaap@v1` framework** —
the curated us-gaap leaves, the presentation/calc/disclosure machinery
that lives on top of them, and the Reporting Styles surface for
vertical/filer-profile flavoring.

Packages owned by the upstream **`fac@v1`** framework (Charlie Hoffman's
universal accounting-concept substrate) live next door at
`../../../fac/packages/`. They are inherited via this framework's
`depends_on: [{framework: fac, version: v1}]` and load first.

Each package here is a versioned, self-contained JSON-LD unit;
migration 0002 walks the framework manifest to load them in dependency
+ ordinal order at extensions DB provisioning time.

For cross-namespace equivalence taxonomies (bridges) see `../bridges/`.
For framework composition rules and manifest schema see
`../../../README.md`.

## Fork model

These packages are **RoboSystems canonical forks**. A handful were
originally bootstrapped from upstream XBRL (Charlie Hoffman's Seattle
Method publications + FASB us-gaap 2017); the rest were authored
natively as part of the taxonomy library build-out.

Whichever origin they had, all packages are now maintained **directly
in JSON-LD** inside this directory:

- Edits happen in the `taxonomy.jsonld` file for the relevant standard.
- Upstream XBRL is **not** tracked. We do not re-import, rebase, or
  reconcile against upstream on any schedule.
- Migrations read these files verbatim from git — there is no network
  fetch in the ingest path.

Each package declares its provenance in its top-level JSON-LD metadata:

```json
{
  "standard": "rs-gaap",
  "forked_from": {
    "author": "FASB us-gaap 2017",
    "url": "https://xbrl.fasb.org/us-gaap/2017/",
    "format": "XBRL taxonomy"
  },
  "forked_at": "2026-04-19",
  "upstream_tracking": "frozen"
}
```

…or, for packages we authored natively:

```json
{
  "standard": "rs-gaap-calculations",
  "origin": "native",
  "created_at": "2026-04-19"
}
```

## Layout

```
packages/
├── rs-gaap/v1/                       forked  — RoboSystems canonical us-gaap (~2,000 leaves)
├── rs-gaap-traits/v1/                native  — per-element trait bindings for rs-gaap leaves;
│                                              seeds `element_traits` junction (binds rs-gaap
│                                              elements to fac-traits axes)
├── rs-gaap-hierarchy/v1/             forked  — rs-gaap class hierarchy
├── rs-gaap-presentation/v1/          native  — rs-gaap presentation hierarchies
├── rs-gaap-calculations/v1/          native  — rs-gaap calc DAG (composes with fac-calculations)
├── rs-gaap-type-subtype/v1/          forked  — rs-gaap classification linkbase (general-special arcs)
├── rs-gaap-references/v1/            forked  — ASC citation reference linkbase (attach-by-qname)
├── rs-gaap-labels/v1/                forked  — supplementary + total-role label linkbase (attach-by-qname)
├── rs-gaap-disclosures/v1/           native  — named Disclosures (~30)
├── rs-gaap-disclosure-mechanics/v1/  native  — DM rules per Disclosure
├── rs-gaap-reporting-checklist/v1/   native  — DR rules per report type
└── rs-gaap-reporting-styles/v1/      native  — vertical / filer-profile composition surface
                                                (Default, Small Private, Banking, Insurance,
                                                Mining, Cannabis, …)
```

The trait vocabulary (`fac-traits/v1`, 99 traits across 25 categories)
lives in the upstream `fac` framework and is inherited here via
`depends_on`. `rs-gaap-traits/v1/` is the rs-gaap-specific binding —
it declares which trait values apply to which rs-gaap leaves. Future
peer frameworks (rs-ifrs, rs-call-report, rs-statutory) each ship
their own `*-traits/v1` binding package, all targeting the same
fac-traits vocabulary.

## Notes on specific packages

**`rs-gaap-type-subtype`** is Charlie's classification linkbase pattern —
the general-special (type/subtype) arcs only. Theoretically reusable, but
currently scoped to rs-gaap; if/when another framework needs the same
pattern, promote to `fac@v1` or extract to its own framework.

**`rs-gaap-references`** and **`rs-gaap-labels`** are pure linkbases split
out of `rs-gaap-type-subtype` (which originally clumped arcs + ASC citations
+ labels + a redundant re-definition of every concept). They define **no
concepts** — each node references a concept defined in the `rs-gaap` base
**by qname** (the XBRL reference/label-linkbase pattern). The loader emits
them as `reference_assignments` / `label_assignments`; the seeder attaches
them in the cross-package arcs pass (same as trait assignments) once every
element exists, so the deterministic label/reference ids
(`uuid5(element_id:role:language)` / `uuid5(element_id:citation)`) reseed
byte-identically to inline labels/refs. The split was a one-shot transform of
the hand-authored source (provenance in git history; the spent
`split_type_subtype` script lives developer-local under
`local/scripts/taxonomy/`, not in the committed tree) — these JSON-LD artifacts
are now the source of truth, edit them directly. Both are `tenant_copy: true` —
tenants get the citations and labels for the concepts they keep.

**`rs-gaap-reporting-styles`** is THE vertical-flavor surface. New
industries (Mining, Cannabis, Cooperative, B-Corp, etc.) are added
here as new Reporting Style rows, **not** as new frameworks. The
framework boundary is regulatory regime (GAAP, IFRS, call report,
statutory, tax); the Reporting Style boundary is filer profile
within a regime.

**SFAC 6** doesn't have its own package — its content (Assets,
Liabilities, Equity, Revenues, Expenses, etc.) is encoded as the
`elementsOfFinancialStatements` trait category inside
`fac-traits/v1` (in the `fac` framework) and attached to rs-gaap
concepts via `rs-gaap-traits/v1`'s trait-assignment arcs.
This keeps SFAC 6 categorization queryable per-element without giving
it a separate concept namespace, and lets every rs-* framework
inherit the same axes.

## Adding a Reporting Style preset

A Reporting Style is a **selection vector** over per-statement
presentation Structures, identified by a 4-segment code
`{BS-layout}-{equity-form}-{IS-layout}-{CF-method}` (e.g.
`BSC-CORP-IS01-CF1`). A preset composes one Network (presentation
Structure) per statement type; switching a graph's Style re-renders its
statements against the composed Networks.

Adding a preset is **pure package content** — no migration. On a fresh
`reset-local`, migration `0008` re-reads this package's declarations and
seeds `reporting_style_networks` for every Style that declares a
composition.

1. **(Optional) Author a new axis Structure** in
   `rs-gaap-presentation/v1` if the preset needs a layout that doesn't
   exist yet. Mirror an existing sibling (e.g. `_:rs-gaap-pres-is-singlestep`
   mirrors `_:rs-gaap-pres-is-multistep`): a role node with `roleUri`,
   `structureName`, `blockType`, `conceptArrangementPattern`, then its
   `presentation` arcs. The calc-DAG (`rs-gaap-calculations`) is global,
   so a presentation Structure only selects *which* rows render —
   subtotals like `GrossProfit` are still computed even if not presented.
2. **Declare the Style** in `rs-gaap-reporting-styles/v1` by adding two
   fields to the Style's Structure node (the one carrying `roleUri`):
   - `reportingStyleCode`: the 4-segment code.
   - `reportingStyleNetworks`: an array of
     `{"statementType": …, "networkRoleUri": …}`, one per statement type
     (`balance_sheet`, `income_statement`, `cash_flow_statement`,
     `equity_statement`). Each `networkRoleUri` is a presentation
     Structure's `roleUri`.
   A Style with no `reportingStyleNetworks` (e.g. `Banking`) stays a
   non-selectable placeholder.
3. `just reset-local`, then `change-reporting-style` to the preset's
   Structure id (`uuid5(roleUri, "structure")`) and re-render.

### The equity-form axis (worked example)

The second segment of the code is the entity's legal form
(`CORP`/`PART`/`LLC`/…). Because composition is per-*statement*, an
equity-form variant is a **full balance-sheet Structure** that clones the
corporate asset/liability arcs and swaps only the equity section, plus a
form-specific Statement-of-Changes rollforward:

- `BS-classified-PART` / `BS-classified-LLC` keep `rs-gaap:StockholdersEquity`
  as the equity *total* (the calc-DAG plug: `StockholdersEquity = Σ equity
  leaves`, which already sums `PartnersCapital`/`MembersEquity`), so the
  balance sheet still foots. They differ from `BS-classified` only in which
  child the equity section presents — `PartnersCapital` / `MembersEquity`
  instead of the corporate stack.
- `Equity-rollforward-PART` / `Equity-rollforward-LLC` root the rollforward
  at the form's capital concept.

The `Partnership` / `LimitedLiabilityCompany` presets then compose those
Networks (`BSC-PART-IS02-CF1`, `BSC-LLC-IS02-CF1`) with the shared
`IS-multistep` + `CashFlow-indirect`. New graphs default to the matching
Style from the entity's `entity_type` at creation (see
`operations/graph/reporting_style_defaults.py`).

**Known limit:** auto-derived Retained Earnings is corporate-specific —
partnerships/LLCs roll undistributed earnings into the capital account, not
a separate RE line, so a PART/LLC equity section only foots cleanly once
form-aware earnings rollup lands. `SOLE`/`NFP` need new leaf concepts
(`ProprietorCapital`, net-asset classes) before they can be authored.

## Editing packages

The JSON-LD is the source of truth. Edit it directly. We're past the
bootstrap phase — packages are now curated and crafted by hand, not
regenerated from canned scripts.

After editing, re-run migrations against a fresh extensions database
to confirm the packages still load cleanly:

```bash
just reset-local                  # full reset; reloads all packages + bridges
# — or, narrower —
just migrate-down extensions -1   # rewind the taxonomy-library migration
just migrate-up   extensions      # re-apply
```

## Adding a new package

1. Create `packages/<name>/v1/taxonomy.jsonld` with top-level metadata
   (`standard`, `version`, `taxonomy_type`, `namespace_uri`,
   `description`, and either `forked_from`/`forked_at`/`upstream_tracking`
   or `origin`/`created_at`).
2. Add an entry to this framework's manifest at `../manifest.json`
   under `packages[]` with an `ordinal` (load order) and `is_required`
   flag.
3. If the new package introduces a new `source` value for elements or
   a new `association_type` for associations, widen the corresponding
   CHECK constraints in migration `0002_taxonomy_library.py`.

The migration auto-walks the framework manifest, so no explicit
Python list to update beyond the manifest itself.
