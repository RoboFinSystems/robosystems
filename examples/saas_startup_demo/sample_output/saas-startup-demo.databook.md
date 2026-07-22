---
id: https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT
type: DataBook
title: "Cadence Labs Demo — Cadence Labs, Inc."
version: 1.0.0
authors:
  - name: "RoboSystems Report Engine"
license: CC-BY-4.0
description: >
  Published financial report as a DataBook — the report as a
  collection of Information Blocks (balance sheet, income
  statement, cash flow, statement of changes in equity), each a
  table plus an addressable RDF/Turtle slice, with SHACL + XBRL
  2.1 validation evidence inlined.
tags:
  - financial
  - reporting
  - xbrl
  - rs-gaap
  - databook
provenance:
  source: "Cadence Labs, Inc."
  method: "Materialized RoboSystems Report rpt_01KY5TMHRW0EAJFVTBCT7P11HT (generation 1, draft)"
manifest:
  entrypoints:
    - block: balance_sheet
    - block: income_statement
    - block: cash_flow_statement
    - block: equity_statement
    - block: regulatory_disclosure
    - block: regulatory_disclosure
  blocks:
    balance_sheet:
      type: turtle
      description: "rs-gaap — Balance Sheet — Classified"
    income_statement:
      type: turtle
      description: "rs-gaap — Income Statement — Multi-step"
    cash_flow_statement:
      type: turtle
      description: "rs-gaap — Cash Flow Statement — Indirect"
    equity_statement:
      type: turtle
      description: "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)"
    regulatory_disclosure:
      type: turtle
      description: "Significant Accounting Policies"
    regulatory_disclosure:
      type: turtle
      description: "Disaggregation of Revenue"
graph:
  facts: 85
  href: saas-startup-demo.holon.jsonld
  graphs:
    - id: scene
      iri: https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT#scene
      description: "Instance facts — the values this report reports"
      disposition: inline
    - id: boundary
      iri: https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT#boundary
      description: "Calculation network — the rollup rules the facts must obey"
      disposition: reference
      derived_from: rs-gaap-calculations@v1
    - id: projection
      iri: https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT#projection
      description: "Presentation network — order, indentation, subtotals"
      disposition: reference
      derived_from: rs-gaap-presentation@v1
      reporting_style: 025f5d48-12ce-5d65-b9eb-4f137a10ef06
    - id: lineage
      description: "Event lineage — fact → event → entry → line item → CoA"
      disposition: internal
      note: "the books, not published — a report is an aggregation of the ledger, which is internal; substantiation available to authorized parties"
report:
  reporting_style: 025f5d48-12ce-5d65-b9eb-4f137a10ef06
  report_id: rpt_01KY5TMHRW0EAJFVTBCT7P11HT
  generation_count: 1
  filing_status: draft
  periods:
    - { label: "2024-07-01 → 2026-06-30", start: 2024-07-01, end: 2026-06-30 }
    - { label: "2025-07-01 → 2026-06-30", start: 2025-07-01, end: 2026-06-30 }
  framework_pins:
    - { framework: fac-traits, version: v1 }
    - { framework: cm, version: v1 }
    - { framework: rs-gaap, version: v1 }
    - { framework: rs-gaap-traits, version: v1 }
    - { framework: rs-gaap-hierarchy, version: v1 }
    - { framework: rs-gaap-presentation, version: v1 }
    - { framework: rs-gaap-calculations, version: v1 }
    - { framework: rs-gaap-type-subtype, version: v1 }
    - { framework: rs-gaap-references, version: v1 }
    - { framework: rs-gaap-labels, version: v1 }
    - { framework: rs-gaap-disclosures, version: v1 }
    - { framework: rs-gaap-reporting-styles, version: v1 }
    - { framework: rs-gaap-rollup-rules, version: v1 }
    - { framework: rs-gaap-rules, version: v1 }
    - { framework: rs-metric, version: v1 }
---

# Cadence Labs Demo — Cadence Labs, Inc.

A report **is** a collection of Information Blocks, and this DataBook is a projection of one report holon (see the `graph:` map above). The **scene** graph — the facts — renders twice per block here: a markdown table (human view) and a foldable, addressable `turtle` slice (machine view, the same facts as RDF). The **boundary** (calculation) and **projection** (presentation) graphs live as real named graphs in the companion `saas-startup-demo.holon.jsonld` — dataset-form JSON-LD, the API-native holon — and derive from their versioned framework, referenced here rather than inlined since they're shared by every report on that framework. The **lineage** graph — the ledger behind the facts — is internal and not published: a report is an aggregation of the books, not the books. The `Validation evidence` section is the published substantiation that the referenced rules hold. Everything here derives from `saas-startup-demo.jsonld`.


## Balance Sheet

- **Structure**: rs-gaap — Balance Sheet — Classified
- **Information Block**: `b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d`
- **FactSet**: `fs_01KY5TMHV2KSGVFSNVF4ED88CQ`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `rs-gaap:CashAndCashEquivalentsAtCarryingValue` |     Cash and Cash Equivalents, at Carrying Value | — | $1,913,398.80 |
| `rs-gaap:ReceivablesNetCurrent` |     Receivables, Net, Current | — | $9,600.00 |
| `rs-gaap:PrepaidExpenseCurrent` |     Prepaid Expense, Current | — | $33,000.00 |
| `rs-gaap:AssetsCurrent` |   **Assets, Current** | — | $1,955,998.80 |
| `rs-gaap:PropertyPlantAndEquipmentNet` |     Property, Plant and Equipment, Net | — | $56,333.42 |
| `rs-gaap:AssetsNoncurrent` |   **Assets, Noncurrent** | — | $56,333.42 |
| `rs-gaap:Assets` | **Assets** | — | $2,012,332.22 |
| `rs-gaap:AccountsPayableCurrent` |       Accounts Payable, Current | — | $0.00 |
| `rs-gaap:DeferredRevenueCurrent` |       Deferred Revenue, Current | — | $1,153,999.56 |
| `rs-gaap:LiabilitiesCurrent` |     **Liabilities, Current** | — | $1,153,999.56 |
| `rs-gaap:Liabilities` |   **Liabilities** | — | $1,153,999.56 |
| `rs-gaap:AdditionalPaidInCapital` |     Additional Paid in Capital | — | $2,835,000.00 |
| `rs-gaap:RetainedEarningsAccumulatedDeficit` |     Retained Earnings (Accumulated Deficit) | — | $(538,668.10) |
| `rs-gaap:StockholdersEquity` |   **Stockholders' Equity Attributable to Parent** | — | $2,296,331.90 |
| `rs-gaap:LiabilitiesAndStockholdersEquity` | **Liabilities and Equity** | — | $3,450,331.46 |

<details>
<summary>▸ Balance Sheet — scene RDF / Turtle (500 triples · 30.3 KB)</summary>

```turtle {#balance_sheet}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT5S> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT5S" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT5T> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT5T" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT5V> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT5V" ;
    rs:numericValue 1913398.7999999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT5X> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT5X" ;
    rs:numericValue 1153999.5599999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT61> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT61" ;
    rs:numericValue 33000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT62> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT62" ;
    rs:numericValue 9600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT64> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT64" ;
    rs:numericValue -538668.1000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6A> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6A" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6B> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6B" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6C> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6C" ;
    rs:numericValue 2767021.77 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6E> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6E" ;
    rs:numericValue 922021.97 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6J> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6J" ;
    rs:numericValue 33000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6K> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6K" ;
    rs:numericValue 4800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6N> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6N" ;
    rs:numericValue -711067.06 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPA" ;
    rs:numericValue 56333.42 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPB" ;
    rs:numericValue 48333.34 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPM" ;
    rs:numericValue 1153999.5599999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPN" ;
    rs:numericValue 1153999.5599999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPQ" ;
    rs:numericValue 2296331.9 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPR" ;
    rs:numericValue 1955998.7999999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPS" ;
    rs:numericValue 2012332.2199999997 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPV" ;
    rs:numericValue 56333.42 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQ0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQ0" ;
    rs:numericValue 3450331.46 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQ6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQ6" ;
    rs:numericValue 922021.97 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQ7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQ7" ;
    rs:numericValue 922021.97 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQ9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQ9" ;
    rs:numericValue 2123932.94 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQA" ;
    rs:numericValue 2804821.77 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQB" ;
    rs:numericValue 2853155.11 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQE" ;
    rs:numericValue 48333.34 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQK" ;
    rs:numericValue 3045954.91 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/ib/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Balance Sheet — Classified" ;
    rs:blockType "balance_sheet" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CQ> ;
    rs:internalId "b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

rs-gaap:AccountsPayableCurrent a rs:Element ;
    skos:prefLabel "Accounts Payable, Current" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "9ddbc9d7-c769-5800-8f65-1089d476e5c9" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:AdditionalPaidInCapital a rs:Element ;
    skos:prefLabel "Additional Paid in Capital" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "6146605c-0d63-51e1-a523-3450d6abaca3" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:Assets a rs:Element ;
    skos:prefLabel "Assets" ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "a1f04756-41d8-5d35-b821-23aa2f3b2fae" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:AssetsCurrent a rs:Element ;
    skos:prefLabel "Assets, Current" ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "0fc9ab7e-c5ce-5277-9530-344cc127fe26" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:AssetsNoncurrent a rs:Element ;
    skos:prefLabel "Assets, Noncurrent" ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "841cedeb-4cb0-532a-b0bd-c34846a13a8c" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:CashAndCashEquivalentsAtCarryingValue a rs:Element ;
    skos:prefLabel "Cash and Cash Equivalents, at Carrying Value" ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "20a6586b-880a-5745-94db-e23d397eb5e1" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:DeferredRevenueCurrent a rs:Element ;
    skos:prefLabel "Deferred Revenue, Current" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "f35c2b3a-01eb-50c8-96e3-4c07cc2a0fee" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:Liabilities a rs:Element ;
    skos:prefLabel "Liabilities" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "7af273ac-1cba-5fb3-a1c9-5c5d8fdb9bdf" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:LiabilitiesAndStockholdersEquity a rs:Element ;
    skos:prefLabel "Liabilities and Equity" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "30b2801e-e682-5298-82e6-3670e1d508f1" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:LiabilitiesCurrent a rs:Element ;
    skos:prefLabel "Liabilities, Current" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "efb036ff-3f30-5deb-bee9-1af4cd4b9800" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:PrepaidExpenseCurrent a rs:Element ;
    skos:prefLabel "Prepaid Expense, Current" ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "2225e348-90bd-53cb-9784-8b5d54980a69" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:PropertyPlantAndEquipmentNet a rs:Element ;
    skos:prefLabel "Property, Plant and Equipment, Net" ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "288099af-5cbb-5f78-8f8a-1a85675fb661" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:ReceivablesNetCurrent a rs:Element ;
    skos:prefLabel "Receivables, Net, Current" ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "44686df3-3871-5c1f-8a08-fc542d69dfa0" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:RetainedEarningsAccumulatedDeficit a rs:Element ;
    skos:prefLabel "Retained Earnings (Accumulated Deficit)" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "a9c87d60-a1e5-506b-a27e-cbf9e14e5113" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:StockholdersEquity a rs:Element ;
    skos:prefLabel "Stockholders' Equity Attributable to Parent" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "e3796201-9899-5b7b-9477-659550ba8e68" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> a rs:Period ;
    xbrli:instant "2026-06-30"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> a rs:Period ;
    xbrli:instant "2025-06-30"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8ba9be06b028c2409" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Income Statement

- **Structure**: rs-gaap — Income Statement — Multi-step
- **Information Block**: `47cd6544-03d1-5bc1-8c28-31c0cfa450f9`
- **FactSet**: `fs_01KY5TMHV2KSGVFSNVF4ED88CR`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` |     **Revenue from Contract with Customer, Excluding Assessed Tax** | — | $1,245,199.44 |
| `rs-gaap:Revenues` |   **Revenues** | — | $1,245,199.44 |
| `rs-gaap:CostOfGoodsAndServicesSold` |     Cost of Product and Service Sold | — | $266,400.00 |
| `rs-gaap:CostOfRevenue` |   **Cost of Revenue** | — | $266,400.00 |
| `rs-gaap:GrossProfit` |   **Gross Profit** | — | $978,799.44 |
| `rs-gaap:GeneralAndAdministrativeExpense` |     General and Administrative Expense | — | $492,000.00 |
| `rs-gaap:SellingAndMarketingExpense` |     Selling and Marketing Expense | — | $741,600.00 |
| `rs-gaap:ResearchAndDevelopmentExpense` |     Research and Development Expense | — | $786,000.00 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | — | $31,999.92 |
| `rs-gaap:OperatingExpenses` |   **Operating Expenses** | — | $2,051,599.92 |
| `rs-gaap:OperatingIncomeLoss` |   **Operating Income (Loss)** | — | $(1,072,800.48) |
| `rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` |   **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | — | $(1,072,800.48) |
| `rs-gaap:IncomeLossFromContinuingOperations` |   **Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent** | — | $(1,072,800.48) |
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | — | $172,398.96 |

<details>
<summary>▸ Income Statement — scene RDF / Turtle (470 triples · 28.9 KB)</summary>

```turtle {#income_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT5W> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT5W" ;
    rs:numericValue 266400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT5Y> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT5Y" ;
    rs:numericValue 31999.92 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT60> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT60" ;
    rs:numericValue 492000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT63> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ResearchAndDevelopmentExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT63" ;
    rs:numericValue 786000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT66> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT66" ;
    rs:numericValue 1245199.44 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT67> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SellingAndMarketingExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT67" ;
    rs:numericValue 741600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6D> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6D" ;
    rs:numericValue 39600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6F> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6F" ;
    rs:numericValue 6666.66 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6H> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6H" ;
    rs:numericValue 99000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6M> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ResearchAndDevelopmentExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6M" ;
    rs:numericValue 129000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6Q> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6Q" ;
    rs:numericValue 192799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFP3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SellingAndMarketingExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFP3" ;
    rs:numericValue 122400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFP4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFP4" ;
    rs:numericValue 172398.95999999996 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFP7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFP7" ;
    rs:numericValue -11067.060000000056 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPH" ;
    rs:numericValue 266400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPJ" ;
    rs:numericValue 2051599.92 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPK" ;
    rs:numericValue -1072800.48 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPX" ;
    rs:numericValue 978799.44 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPY" ;
    rs:numericValue -1072800.48 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPZ" ;
    rs:numericValue 1245199.44 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQ1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQ1" ;
    rs:numericValue -1072800.48 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQ2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQ2" ;
    rs:numericValue 39600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQ3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQ3" ;
    rs:numericValue 357066.66 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQ4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQ4" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQG" ;
    rs:numericValue 153199.8 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQH" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQJ" ;
    rs:numericValue 192799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQM" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/ib/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Income Statement — Multi-step" ;
    rs:blockType "income_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CR> ;
    rs:internalId "47cd6544-03d1-5bc1-8c28-31c0cfa450f9" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

rs-gaap:CostOfGoodsAndServicesSold a rs:Element ;
    skos:prefLabel "Cost of Product and Service Sold" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "5ca0e51f-dff1-5c2b-94f5-26620852a5f9" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:CostOfRevenue a rs:Element ;
    skos:prefLabel "Cost of Revenue" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "12ab7417-5324-55d6-946e-2456adba47c5" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:DepreciationDepletionAndAmortization a rs:Element ;
    skos:prefLabel "Depreciation, Depletion and Amortization" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "189a099a-7512-5144-9215-65d837c2c3b5" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:GeneralAndAdministrativeExpense a rs:Element ;
    skos:prefLabel "General and Administrative Expense" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "f92ba8cb-7ae2-5d40-9d15-9a94e9e3aed4" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:GrossProfit a rs:Element ;
    skos:prefLabel "Gross Profit" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "a92b3181-9fe7-543c-81d9-13ebd12bbefa" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:IncomeLossFromContinuingOperations a rs:Element ;
    skos:prefLabel "Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "d60cabda-7060-5aff-ac98-96371606a738" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest a rs:Element ;
    skos:prefLabel "Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "6b0b414f-0c76-54f0-8e51-cf53db59ca24" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:NetIncomeLoss a rs:Element ;
    skos:prefLabel "Net Income (Loss) Attributable to Parent" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "27a05717-2370-51c2-a924-db5cbcb48219" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:OperatingExpenses a rs:Element ;
    skos:prefLabel "Operating Expenses" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "71fcdebb-7145-5f76-b5e0-b4ccbf2c29d2" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:OperatingIncomeLoss a rs:Element ;
    skos:prefLabel "Operating Income (Loss)" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "16780828-0201-5609-b572-fbe3ebfcb177" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:ResearchAndDevelopmentExpense a rs:Element ;
    skos:prefLabel "Research and Development Expense" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "9cb92b07-2629-5534-9959-58c3a963559e" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax a rs:Element ;
    skos:prefLabel "Revenue from Contract with Customer, Excluding Assessed Tax" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "37252918-4301-50e2-8d7e-cf2c76986d15" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:Revenues a rs:Element ;
    skos:prefLabel "Revenues" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "b26a6cd4-072f-5bf2-b5d3-ebf928150d6c" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:SellingAndMarketingExpense a rs:Element ;
    skos:prefLabel "Selling and Marketing Expense" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "4757162f-73d0-5c6e-949e-ed2cafb2a64f" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> a rs:Period ;
    xbrli:endDate "2026-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> a rs:Period ;
    xbrli:endDate "2025-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8ba9be06b028c2409" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Cash Flow Statement

- **Structure**: rs-gaap — Cash Flow Statement — Indirect
- **Information Block**: `5473639a-2dac-56a6-b9e5-38480ea38bc1`
- **FactSet**: `fs_01KY5TMHV2KSGVFSNVF4ED88CS`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `rs-gaap:NetIncomeLoss` |     **Net Income (Loss) Attributable to Parent** | — | $172,398.96 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | — | $31,999.92 |
| `rs-gaap:IncreaseDecreaseInAccountsReceivable` |     Increase (Decrease) in Accounts Receivable | — | $(4,800.00) |
| `rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet` |     Increase (Decrease) in Other Operating Assets and Liabilities, Net | — | $(1,053,221.85) |
| `rs-gaap:NetCashProvidedByUsedInOperatingActivities` |   Cash Provided by (Used in) Operating Activity, Including Discontinued Operation | — | $(853,622.97) |
| `rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment` |     Payments to Acquire Property, Plant, and Equipment | — | — |
| `rs-gaap:NetCashProvidedByUsedInInvestingActivities` |   Cash Provided by (Used in) Investing Activity, Including Discontinued Operation | — | — |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |     Proceeds from Issuance of Common Stock | — | — |
| `rs-gaap:NetCashProvidedByUsedInFinancingActivities` |   Cash Provided by (Used in) Financing Activity, Including Discontinued Operation | — | — |
| `rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease` | **Cash and Cash Equivalents, Period Increase (Decrease)** | — | $(853,622.97) |

<details>
<summary>▸ Cash Flow Statement — scene RDF / Turtle (276 triples · 16.7 KB)</summary>

```turtle {#cash_flow_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT5Z> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT5Z" ;
    rs:numericValue 31999.92 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6G> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6G" ;
    rs:numericValue 6666.66 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFP6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFP6" ;
    rs:numericValue 172398.95999999996 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFP9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFP9" ;
    rs:numericValue -11067.060000000056 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPC" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPE" ;
    rs:numericValue -80000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInAccountsReceivable ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPF" ;
    rs:numericValue -4800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPG" ;
    rs:numericValue -1053221.85 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPT" ;
    rs:numericValue -853622.9700000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPW" ;
    rs:numericValue -853622.9700000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQ5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInFinancingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQ5" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQC" ;
    rs:numericValue -80000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQD" ;
    rs:numericValue 2750599.6 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQF" ;
    rs:numericValue -4400.400000000056 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/ib/5473639a-2dac-56a6-b9e5-38480ea38bc1> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Cash Flow Statement — Indirect" ;
    rs:blockType "cash_flow_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CS> ;
    rs:internalId "5473639a-2dac-56a6-b9e5-38480ea38bc1" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

rs-gaap:IncreaseDecreaseInAccountsReceivable a rs:Element ;
    skos:prefLabel "Increase (Decrease) in Accounts Receivable" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "93175d59-983c-5012-910f-3dfbf07ce327" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet a rs:Element ;
    skos:prefLabel "Increase (Decrease) in Other Operating Assets and Liabilities, Net" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "a3227fb2-202b-51db-9574-4e60db03c04f" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:NetCashProvidedByUsedInFinancingActivities a rs:Element ;
    skos:prefLabel "Cash Provided by (Used in) Financing Activity, Including Discontinued Operation" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "811f1cf5-836c-575f-9f3f-cd7fa477e4e5" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:NetCashProvidedByUsedInInvestingActivities a rs:Element ;
    skos:prefLabel "Cash Provided by (Used in) Investing Activity, Including Discontinued Operation" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "69b82be1-1145-5686-8613-31da9eb04a72" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment a rs:Element ;
    skos:prefLabel "Payments to Acquire Property, Plant, and Equipment" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "ff101489-15f4-573d-967b-24f75e0fc0f6" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:ProceedsFromIssuanceOfCommonStock a rs:Element ;
    skos:prefLabel "Proceeds from Issuance of Common Stock" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "2eb72b5f-d7e3-5bd5-bf93-be38b6d21820" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease a rs:Element ;
    skos:prefLabel "Cash and Cash Equivalents, Period Increase (Decrease)" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "353f790f-1ed1-5b91-880d-8029b4b687cf" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:DepreciationDepletionAndAmortization a rs:Element ;
    skos:prefLabel "Depreciation, Depletion and Amortization" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "189a099a-7512-5144-9215-65d837c2c3b5" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:NetCashProvidedByUsedInOperatingActivities a rs:Element ;
    skos:prefLabel "Cash Provided by (Used in) Operating Activity, Including Discontinued Operation" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "57ccbf45-c970-5bcd-a381-44d96b6b6d94" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:NetIncomeLoss a rs:Element ;
    skos:prefLabel "Net Income (Loss) Attributable to Parent" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "27a05717-2370-51c2-a924-db5cbcb48219" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> a rs:Period ;
    xbrli:endDate "2026-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> a rs:Period ;
    xbrli:endDate "2025-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8ba9be06b028c2409" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Statement of Changes in Equity

- **Structure**: rs-gaap — Statement of Changes in Equity — Roll Forward (Total)
- **Information Block**: `0b179e5c-5f02-506d-b8d5-860cb10c7694`
- **FactSet**: `fs_01KY5TMHV2KSGVFSNVF4ED88CT`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | — | $172,398.96 |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |   Proceeds from Issuance of Common Stock | — | — |
| `rs-gaap:StockholdersEquity` | **Stockholders' Equity Attributable to Parent** | — | $2,296,331.90 |

<details>
<summary>▸ Statement of Changes in Equity — scene RDF / Turtle (113 triples · 6.9 KB)</summary>

```turtle {#equity_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFP5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CT> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFP5" ;
    rs:numericValue 172398.95999999996 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFP8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CT> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFP8" ;
    rs:numericValue -11067.060000000056 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CT> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPD" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFPP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CT> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFPP" ;
    rs:numericValue 2296331.9 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVCJW0GPXV0B6C9JFQ8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CT> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVCJW0GPXV0B6C9JFQ8" ;
    rs:numericValue 2123932.94 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/ib/0b179e5c-5f02-506d-b8d5-860cb10c7694> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)" ;
    rs:blockType "equity_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CT> ;
    rs:internalId "0b179e5c-5f02-506d-b8d5-860cb10c7694" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_1> a rs:Period ;
    xbrli:instant "2026-06-30"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_2> a rs:Period ;
    xbrli:instant "2025-06-30"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> a rs:Period ;
    xbrli:endDate "2026-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-07-01"^^xsd:date .

rs-gaap:ProceedsFromIssuanceOfCommonStock a rs:Element ;
    skos:prefLabel "Proceeds from Issuance of Common Stock" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "2eb72b5f-d7e3-5bd5-bf93-be38b6d21820" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> a rs:Period ;
    xbrli:endDate "2025-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-07-01"^^xsd:date .

rs-gaap:NetIncomeLoss a rs:Element ;
    skos:prefLabel "Net Income (Loss) Attributable to Parent" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "27a05717-2370-51c2-a924-db5cbcb48219" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:StockholdersEquity a rs:Element ;
    skos:prefLabel "Stockholders' Equity Attributable to Parent" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "e3796201-9899-5b7b-9477-659550ba8e68" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8ba9be06b028c2409" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Significant Accounting Policies

- **Structure**: Significant Accounting Policies
- **Information Block**: `struct_01KY5TKXXADKAGW7227PW8YPAS`
- **FactSet**: `fs_01KY5TMHVJT06QWTBWYQ80PXMY`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `cadence:RevenueRecognitionPolicyTextBlock` |   Revenue Recognition Policy Text Block | — | — |
| `cadence:OperatingExpensePolicyTextBlock` |   Operating Expense Policy Text Block | — | — |

<details>
<summary>▸ Significant Accounting Policies — scene RDF / Turtle (66 triples · 5.6 KB)</summary>

```turtle {#regulatory_disclosure}
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVKDQ1GWJEN2R7EQW9A> a rs:Fact ;
    rs:contentType "text/markdown" ;
    rs:element <https://robosystems.ai/concept/cadence:RevenueRecognitionPolicyTextBlock> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHVJT06QWTBWYQ80PXMY> ;
    rs:factType "nonnumeric" ;
    rs:internalId "fact_01KY5TMHVKDQ1GWJEN2R7EQW9A" ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:stringValue """# Revenue Recognition Policy — Cadence Labs, Inc.

## Standard
Revenue is recognized under ASC 606 as the performance obligation is satisfied.

## Subscriptions (annual, prepaid)
- Customers sign annual contracts and pay the full year up front.
- Cash collected is recorded as **Deferred Revenue (2300)**, a current liability.
- Revenue is recognized **ratably** over the 12-month term: DR Deferred Revenue / CR Subscription Revenue (4000) each month.
- The deferred-revenue balance equals contracts billed but not yet delivered. It is a source of working-capital "float" — cash in hand that finances operations but is owed as future service.

## Professional Services
- Onboarding/implementation is billed net-30 and recognized as delivered: DR AR (1100) / CR Professional Services (4100).

## Why it matters for runway
Because subscriptions are prepaid, cash collected can exceed revenue recognized while the business grows — softening the cash burn. That float is a liability, not equity. When assessing how long the company can operate, subtract deferred revenue from cash before dividing by the burn."""^^xsd:string ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/struct_01KY5TKXXADKAGW7227PW8YPAS> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVKDQ1GWJEN2R7EQW9B> a rs:Fact ;
    rs:contentType "text/markdown" ;
    rs:element <https://robosystems.ai/concept/cadence:OperatingExpensePolicyTextBlock> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHVJT06QWTBWYQ80PXMY> ;
    rs:factType "nonnumeric" ;
    rs:internalId "fact_01KY5TMHVKDQ1GWJEN2R7EQW9B" ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:stringValue """# Operating Expense & Burn Policy — Cadence Labs, Inc.

## Expense classification (by function)
| Function | Account | Includes |
|---|---|---|
| Cost of Revenue | 5000 | Cloud hosting, customer support |
| Research & Development | 6000 | Engineering salaries, product development |
| Sales & Marketing | 6100 | Sales team, commissions, advertising |
| General & Administrative | 6200 / 6300 / 6400 | Admin, finance, legal, rent, software tools |

R&D is expensed as incurred (no internal-use software capitalization in this policy).

## Depreciation & Prepaids
Straight-line. Equipment over 36 months; the office build-out over 60 months (DR Depreciation Expense 7000 / CR Accumulated Depreciation 1350). Annual tooling and insurance are capitalized as prepaids and amortized over 12 months.

## Burn & runway
- **Monthly operating burn** ≈ operating loss + non-cash addbacks (depreciation/amortization), adjusted for working-capital movements.
- **Runway** = cash ÷ net monthly burn. Report it net of deferred revenue: the prepayment float inflates the cash balance with obligations owed as service."""^^xsd:string ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/struct_01KY5TKXXADKAGW7227PW8YPAS> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/ib/struct_01KY5TKXXADKAGW7227PW8YPAS> a rs:InformationBlock ;
    skos:prefLabel "Significant Accounting Policies" ;
    rs:blockType "regulatory_disclosure" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHVJT06QWTBWYQ80PXMY> ;
    rs:internalId "struct_01KY5TKXXADKAGW7227PW8YPAS" ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/struct_01KY5TKXXADKAGW7227PW8YPAS> ;
    rs:taxonomyId "tax_01KY5TKXX8AKKX3F8N7HH740DR" ;
    rs:taxonomyName "Cadence Policy Notes" .

<https://robosystems.ai/concept/cadence:OperatingExpensePolicyTextBlock> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01KY5TKXX9D3QVE7DYD09ZH5D0" ;
    rs:itemType "textBlock" ;
    rs:monetary false ;
    rs:source "native" .

<https://robosystems.ai/concept/cadence:RevenueRecognitionPolicyTextBlock> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01KY5TKXX9D3QVE7DYD09ZH5CZ" ;
    rs:itemType "textBlock" ;
    rs:monetary false ;
    rs:source "native" .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8ba9be06b028c2409" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> a rs:Period ;
    xbrli:endDate "2026-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-07-01"^^xsd:date .
```

</details>


## Disaggregation of Revenue

- **Structure**: Disaggregation of Revenue
- **Information Block**: `struct_01KY5TKV8NMKH76YM19W8Y665E`
- **FactSet**: `fs_01KY5TMHV2KSGVFSNVF4ED88CV`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `cadence:SubscriptionRevenue` |   Subscription Revenue | — | $1,156,399.44 |
| `cadence:ProfessionalServicesRevenue` |   Professional Services Revenue | — | $88,800.00 |
| `rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` | **Revenue from Contract with Customer, Excluding Assessed Tax** | — | $1,245,199.44 |

<details>
<summary>▸ Disaggregation of Revenue — scene RDF / Turtle (115 triples · 7.5 KB)</summary>

```turtle {#regulatory_disclosure}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT5Q> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:ProfessionalServicesRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CV> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT5Q" ;
    rs:numericValue 88800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/struct_01KY5TKV8NMKH76YM19W8Y665E> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT5R> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:SubscriptionRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CV> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT5R" ;
    rs:numericValue 1156399.44 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/struct_01KY5TKV8NMKH76YM19W8Y665E> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT65> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CV> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT65" ;
    rs:numericValue 1245199.44 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/struct_01KY5TKV8NMKH76YM19W8Y665E> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT68> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:ProfessionalServicesRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CV> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT68" ;
    rs:numericValue 13200.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/struct_01KY5TKV8NMKH76YM19W8Y665E> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT69> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:SubscriptionRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CV> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT69" ;
    rs:numericValue 179599.8 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/struct_01KY5TKV8NMKH76YM19W8Y665E> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/fact/fact_01KY5TMHVBHMHE8RJ9ETQ8VT6P> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CV> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5TMHVBHMHE8RJ9ETQ8VT6P" ;
    rs:numericValue 192799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/struct_01KY5TKV8NMKH76YM19W8Y665E> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/ib/struct_01KY5TKV8NMKH76YM19W8Y665E> a rs:InformationBlock ;
    skos:prefLabel "Disaggregation of Revenue" ;
    rs:blockType "regulatory_disclosure" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5TMHV2KSGVFSNVF4ED88CV> ;
    rs:internalId "struct_01KY5TKV8NMKH76YM19W8Y665E" ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/structure/struct_01KY5TKV8NMKH76YM19W8Y665E> ;
    rs:taxonomyId "tax_01KY5TKV8K1GZ4543YNZ4S0PNJ" ;
    rs:taxonomyName "Cadence Reporting Extension" .

<https://robosystems.ai/concept/cadence:ProfessionalServicesRevenue> a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01KY5TKV8K1GZ4543YNZ4S0PNM" ;
    rs:monetary true ;
    rs:source "native" .

<https://robosystems.ai/concept/cadence:SubscriptionRevenue> a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01KY5TKV8K1GZ4543YNZ4S0PNK" ;
    rs:monetary true ;
    rs:source "native" .

rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax a rs:Element ;
    skos:prefLabel "Revenue from Contract with Customer, Excluding Assessed Tax" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "37252918-4301-50e2-8d7e-cf2c76986d15" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_3> a rs:Period ;
    xbrli:endDate "2026-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/period/p_4> a rs:Period ;
    xbrli:endDate "2025-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/entity/entity_kg19f8ba9be06b028c2409> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8ba9be06b028c2409" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01KY5TMHRW0EAJFVTBCT7P11HT/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Validation evidence

Independent, standards-grade checks of the same bundle this DataBook renders — embedded so the artifact travels with its own proof.

### Cadence Labs — SHACL Ontology Conformance

#### Result: ✅ **Conforms to RoboSystems RDF Ontology v1**

- **Bundle**: `saas-startup-demo.jsonld`
- **Graph triples**: 3,786
- **rs:Fact nodes**: 85
- **rs:Association nodes**: 170
- **rs:Element nodes**: 98
- **SHACL shapes checked**: 8 (positive instance shapes + negative shapes banning the retired dialects)

Validated on the host with **pyshacl** against `frameworks/ontology/v1/shapes.ttl` — the *same* shapes that gate the framework seeds and the publish-time bundle validation, run here directly on the on-disk artifact (no API, no database, no container). Conformance means every `rs:Fact` references its aspects directly (`rs:element`/`rs:entity`/`rs:period`/`rs:unit` — no XBRL `context`), every `rs:Association` carries `xlink:from`/`to` + `xlink:arcrole`, and none of the retired dialects (`xbrli:contextRef`, `arcFrom`, direct `summationOf`) appear.

#### Violations

_None._ Zero violations.

### Cadence Labs — XBRL 2.1 Validation (Arelle)

#### Result: ✅ **Valid XBRL 2.1**

- **Package**: `saas-startup-demo.zip` (13,941 bytes)
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-lab.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 68
- **Load errors**: 0
- **Validation errors**: 0

Validated on the host with **Arelle** (the de-facto XBRL processor, also used by SEC EDGAR) directly against the on-disk report package — no API, no container. Zero load + validation errors is the structural-correctness claim: the output is valid XBRL 2.1, consumable by any standards-compliant processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system checks are not enabled (the instance isn't an SEC filing).

#### Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
