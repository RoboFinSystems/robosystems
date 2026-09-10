---
id: https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4
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
  method: "Materialized RoboSystems Report rpt_01M24B0V8W8BDQMS8QCTAVFZR4 (generation 1, draft)"
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
  facts: 86
  href: saas-startup-demo.holon.jsonld
  graphs:
    - id: scene
      iri: https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4#scene
      description: "Instance facts — the values this report reports"
      disposition: inline
    - id: boundary
      iri: https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4#boundary
      description: "Calculation network — the rollup rules the facts must obey"
      disposition: reference
      derived_from: rs-gaap-calculations@v1
    - id: projection
      iri: https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4#projection
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
  report_id: rpt_01M24B0V8W8BDQMS8QCTAVFZR4
  generation_count: 1
  filing_status: draft
  periods:
    - { label: "2024-09-01 → 2026-08-31", start: 2024-09-01, end: 2026-08-31 }
    - { label: "2025-09-01 → 2026-08-31", start: 2025-09-01, end: 2026-08-31 }
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
    - { framework: rs-driver, version: v1 }
---

# Cadence Labs Demo — Cadence Labs, Inc.

A report **is** a collection of Information Blocks, and this DataBook is a projection of one report holon (see the `graph:` map above). The **scene** graph — the facts — renders twice per block here: a markdown table (human view) and a foldable, addressable `turtle` slice (machine view, the same facts as RDF). The **boundary** (calculation) and **projection** (presentation) graphs live as real named graphs in the companion `saas-startup-demo.holon.jsonld` — dataset-form JSON-LD, the API-native holon — and derive from their versioned framework, referenced here rather than inlined since they're shared by every report on that framework. The **lineage** graph — the ledger behind the facts — is internal and not published: a report is an aggregation of the books, not the books. The `Validation evidence` section is the published substantiation that the referenced rules hold. Everything here derives from `saas-startup-demo.jsonld`.


## Balance Sheet

- **Structure**: rs-gaap — Balance Sheet — Classified
- **Information Block**: `b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d`
- **FactSet**: `fs_01M24B0VC0T58WW67WBZ393W6G`

| QName | Concept | 2024-09-01 → 2026-08-31 | 2025-09-01 → 2026-08-31 |
|---|---|---: | ---:|
| `rs-gaap:CashAndCashEquivalentsAtCarryingValue` |     Cash and Cash Equivalents, at Carrying Value | — | $1,913,398.80 |
| `rs-gaap:ReceivablesNetCurrent` |     Receivables, Net, Current | — | $9,600.00 |
| `rs-gaap:PrepaidExpenseCurrent` |     Prepaid Expense, Current | — | $36,500.00 |
| `rs-gaap:AssetsCurrent` |   **Assets, Current** | — | $1,959,498.80 |
| `rs-gaap:PropertyPlantAndEquipmentNet` |     Property, Plant and Equipment, Net | — | $59,222.30 |
| `rs-gaap:AssetsNoncurrent` |   **Assets, Noncurrent** | — | $59,222.30 |
| `rs-gaap:Assets` | **Assets** | — | $2,018,721.10 |
| `rs-gaap:AccountsPayableCurrent` |       Accounts Payable, Current | — | $0.00 |
| `rs-gaap:DeferredRevenueCurrent` |       Deferred Revenue, Current | — | $1,153,999.56 |
| `rs-gaap:LiabilitiesCurrent` |     **Liabilities, Current** | — | $1,153,999.56 |
| `rs-gaap:Liabilities` |   **Liabilities** | — | $1,153,999.56 |
| `rs-gaap:AdditionalPaidInCapital` |     Additional Paid in Capital | — | $2,835,000.00 |
| `rs-gaap:RetainedEarningsAccumulatedDeficit` |     Retained Earnings (Accumulated Deficit) | — | $(1,970,278.46) |
| `rs-gaap:StockholdersEquity` |   **Stockholders' Equity Attributable to Parent** | — | $864,721.54 |
| `rs-gaap:LiabilitiesAndStockholdersEquity` | **Liabilities and Equity** | — | $2,018,721.10 |

<details>
<summary>▸ Balance Sheet — scene RDF / Turtle (500 triples · 30.2 KB)</summary>

```turtle {#balance_sheet}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDY0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDY0" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDY1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDY1" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDY2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDY2" ;
    rs:numericValue 1913398.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDY4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDY4" ;
    rs:numericValue 1153999.56 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDY8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDY8" ;
    rs:numericValue 36500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDY9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDY9" ;
    rs:numericValue 9600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYB" ;
    rs:numericValue -1970278.46 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYH" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYJ" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYK" ;
    rs:numericValue 2767021.77 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYN" ;
    rs:numericValue 922021.97 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYS" ;
    rs:numericValue 33000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYT" ;
    rs:numericValue 4800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYW" ;
    rs:numericValue -903866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZ6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZ6" ;
    rs:numericValue 59222.3 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZ7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZ7" ;
    rs:numericValue 48333.34 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZJ" ;
    rs:numericValue 1959498.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZK" ;
    rs:numericValue 59222.3 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZM" ;
    rs:numericValue 2018721.1 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZN" ;
    rs:numericValue 1153999.56 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZQ" ;
    rs:numericValue 864721.54 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZR" ;
    rs:numericValue 2018721.1 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZS" ;
    rs:numericValue 1153999.56 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE04> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE04" ;
    rs:numericValue 2804821.77 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE06> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE06" ;
    rs:numericValue 48333.34 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE07> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE07" ;
    rs:numericValue 2853155.11 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE08> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE08" ;
    rs:numericValue 922021.97 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE0A> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE0A" ;
    rs:numericValue 1931133.14 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE0B> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE0B" ;
    rs:numericValue 2853155.11 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE0C> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE0C" ;
    rs:numericValue 922021.97 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/ib/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Balance Sheet — Classified" ;
    rs:blockType "balance_sheet" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6G> ;
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

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> a rs:Period ;
    xbrli:instant "2026-08-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> a rs:Period ;
    xbrli:instant "2025-08-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a088afc574ad2254a54" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Income Statement

- **Structure**: rs-gaap — Income Statement — Multi-step
- **Information Block**: `47cd6544-03d1-5bc1-8c28-31c0cfa450f9`
- **FactSet**: `fs_01M24B0VC0T58WW67WBZ393W6H`

| QName | Concept | 2024-09-01 → 2026-08-31 | 2025-09-01 → 2026-08-31 |
|---|---|---: | ---:|
| `rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` |     **Revenue from Contract with Customer, Excluding Assessed Tax** | — | $1,245,199.44 |
| `rs-gaap:Revenues` |   **Revenues** | — | $1,245,199.44 |
| `rs-gaap:CostOfGoodsAndServicesSold` |     Cost of Product and Service Sold | — | $266,400.00 |
| `rs-gaap:CostOfRevenue` |   **Cost of Revenue** | — | $266,400.00 |
| `rs-gaap:GrossProfit` |   **Gross Profit** | — | $978,799.44 |
| `rs-gaap:GeneralAndAdministrativeExpense` |     General and Administrative Expense | — | $488,500.00 |
| `rs-gaap:SellingAndMarketingExpense` |     Selling and Marketing Expense | — | $741,600.00 |
| `rs-gaap:ResearchAndDevelopmentExpense` |     Research and Development Expense | — | $786,000.00 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | — | $29,111.04 |
| `rs-gaap:OperatingExpenses` |   **Operating Expenses** | — | $2,045,211.04 |
| `rs-gaap:OperatingIncomeLoss` |   **Operating Income (Loss)** | — | $(1,066,411.60) |
| `rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` |   **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | — | $(1,066,411.60) |
| `rs-gaap:IncomeLossFromContinuingOperations` |   **Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent** | — | $(1,066,411.60) |
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | — | $(1,066,411.60) |

<details>
<summary>▸ Income Statement — scene RDF / Turtle (470 triples · 28.8 KB)</summary>

```turtle {#income_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDY3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDY3" ;
    rs:numericValue 266400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDY6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDY6" ;
    rs:numericValue 29111.04 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDY7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDY7" ;
    rs:numericValue 488500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ResearchAndDevelopmentExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYA" ;
    rs:numericValue 786000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYD" ;
    rs:numericValue 1245199.44 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SellingAndMarketingExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYE" ;
    rs:numericValue 741600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYM" ;
    rs:numericValue 39600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYQ" ;
    rs:numericValue 6666.66 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYR" ;
    rs:numericValue 99000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ResearchAndDevelopmentExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYV" ;
    rs:numericValue 129000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYY" ;
    rs:numericValue 192799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SellingAndMarketingExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYZ" ;
    rs:numericValue 122400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZ1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZ1" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZ4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZ4" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZE" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZF" ;
    rs:numericValue 1245199.44 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZT" ;
    rs:numericValue 2045211.04 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZV" ;
    rs:numericValue 266400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZW" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZX" ;
    rs:numericValue 978799.44 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZY" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZZ" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE00> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE00" ;
    rs:numericValue 192799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE0D> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE0D" ;
    rs:numericValue 357066.66 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE0E> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE0E" ;
    rs:numericValue 39600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE0F> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE0F" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE0G> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE0G" ;
    rs:numericValue 153199.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE0H> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE0H" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/ib/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Income Statement — Multi-step" ;
    rs:blockType "income_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6H> ;
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

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> a rs:Period ;
    xbrli:endDate "2026-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> a rs:Period ;
    xbrli:endDate "2025-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a088afc574ad2254a54" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Cash Flow Statement

- **Structure**: rs-gaap — Cash Flow Statement — Indirect
- **Information Block**: `5473639a-2dac-56a6-b9e5-38480ea38bc1`
- **FactSet**: `fs_01M24B0VC0T58WW67WBZ393W6J`

| QName | Concept | 2024-09-01 → 2026-08-31 | 2025-09-01 → 2026-08-31 |
|---|---|---: | ---:|
| `rs-gaap:NetIncomeLoss` |     **Net Income (Loss) Attributable to Parent** | — | $(1,066,411.60) |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | — | $29,111.04 |
| `rs-gaap:IncreaseDecreaseInAccountsReceivable` |     Increase (Decrease) in Accounts Receivable | — | $(4,800.00) |
| `rs-gaap:IncreaseDecreaseInPrepaidExpense` |     Increase (Decrease) in Prepaid Expense | — | $(3,500.00) |
| `rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet` |     Increase (Decrease) in Other Operating Assets and Liabilities, Net | — | $191,977.59 |
| `rs-gaap:NetCashProvidedByUsedInOperatingActivities` |   Cash Provided by (Used in) Operating Activity, Including Discontinued Operation | — | $(853,622.97) |
| `rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment` |     Payments to Acquire Property, Plant, and Equipment | — | — |
| `rs-gaap:NetCashProvidedByUsedInInvestingActivities` |   Cash Provided by (Used in) Investing Activity, Including Discontinued Operation | — | — |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |     Proceeds from Issuance of Common Stock | — | — |
| `rs-gaap:NetCashProvidedByUsedInFinancingActivities` |   Cash Provided by (Used in) Financing Activity, Including Discontinued Operation | — | — |
| `rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease` | **Cash and Cash Equivalents, Period Increase (Decrease)** | — | $(853,622.97) |

<details>
<summary>▸ Cash Flow Statement — scene RDF / Turtle (297 triples · 17.9 KB)</summary>

```turtle {#cash_flow_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDY5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDY5" ;
    rs:numericValue 29111.04 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYP" ;
    rs:numericValue 6666.66 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZ0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZ0" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZ3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZ3" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZ8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZ8" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZA" ;
    rs:numericValue -80000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInPrepaidExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZB" ;
    rs:numericValue -3500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInAccountsReceivable ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZC" ;
    rs:numericValue -4800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZD" ;
    rs:numericValue 191977.59 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZG" ;
    rs:numericValue -853622.97 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZH" ;
    rs:numericValue -853622.97 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE01> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE01" ;
    rs:numericValue -197200.2 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE02> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE02" ;
    rs:numericValue 2557799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE03> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInFinancingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE03" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE05> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE05" ;
    rs:numericValue -80000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/ib/5473639a-2dac-56a6-b9e5-38480ea38bc1> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Cash Flow Statement — Indirect" ;
    rs:blockType "cash_flow_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6J> ;
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

rs-gaap:IncreaseDecreaseInPrepaidExpense a rs:Element ;
    skos:prefLabel "Increase (Decrease) in Prepaid Expense" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "550bb6e5-53d0-5267-adb1-baf78093a0b0" ;
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

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> a rs:Period ;
    xbrli:endDate "2026-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> a rs:Period ;
    xbrli:endDate "2025-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a088afc574ad2254a54" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Statement of Changes in Equity

- **Structure**: rs-gaap — Statement of Changes in Equity — Roll Forward (Total)
- **Information Block**: `0b179e5c-5f02-506d-b8d5-860cb10c7694`
- **FactSet**: `fs_01M24B0VC0T58WW67WBZ393W6K`

| QName | Concept | 2024-09-01 → 2026-08-31 | 2025-09-01 → 2026-08-31 |
|---|---|---: | ---:|
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | — | $(1,066,411.60) |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |   Proceeds from Issuance of Common Stock | — | — |
| `rs-gaap:StockholdersEquity` | **Stockholders' Equity Attributable to Parent** | — | $864,721.54 |

<details>
<summary>▸ Statement of Changes in Equity — scene RDF / Turtle (113 triples · 6.9 KB)</summary>

```turtle {#equity_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZ2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6K> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZ2" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZ5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6K> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZ5" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZ9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6K> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZ9" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDZP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6K> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDZP" ;
    rs:numericValue 864721.54 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQE09> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6K> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQE09" ;
    rs:numericValue 1931133.14 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/ib/0b179e5c-5f02-506d-b8d5-860cb10c7694> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)" ;
    rs:blockType "equity_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6K> ;
    rs:internalId "0b179e5c-5f02-506d-b8d5-860cb10c7694" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> a rs:Period ;
    xbrli:endDate "2026-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_2> a rs:Period ;
    xbrli:instant "2026-08-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_4> a rs:Period ;
    xbrli:instant "2025-08-31"^^xsd:date ;
    xbrli:periodType "instant" .

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

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> a rs:Period ;
    xbrli:endDate "2025-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-09-01"^^xsd:date .

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

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a088afc574ad2254a54" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Significant Accounting Policies

- **Structure**: Significant Accounting Policies
- **Information Block**: `struct_01M24B06VE02PKB9W7AM5VJ4Z6`
- **FactSet**: `fs_01M24B0VCCVAC8V084THGQEV5X`

| QName | Concept | 2024-09-01 → 2026-08-31 | 2025-09-01 → 2026-08-31 |
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

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VCFPKMN6FHHX6VGCFG4> a rs:Fact ;
    rs:contentType "text/markdown" ;
    rs:element <https://robosystems.ai/concept/cadence:RevenueRecognitionPolicyTextBlock> ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VCCVAC8V084THGQEV5X> ;
    rs:factType "nonnumeric" ;
    rs:internalId "fact_01M24B0VCFPKMN6FHHX6VGCFG4" ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
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
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/struct_01M24B06VE02PKB9W7AM5VJ4Z6> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VCFPKMN6FHHX6VGCFG5> a rs:Fact ;
    rs:contentType "text/markdown" ;
    rs:element <https://robosystems.ai/concept/cadence:OperatingExpensePolicyTextBlock> ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VCCVAC8V084THGQEV5X> ;
    rs:factType "nonnumeric" ;
    rs:internalId "fact_01M24B0VCFPKMN6FHHX6VGCFG5" ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
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
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/struct_01M24B06VE02PKB9W7AM5VJ4Z6> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/ib/struct_01M24B06VE02PKB9W7AM5VJ4Z6> a rs:InformationBlock ;
    skos:prefLabel "Significant Accounting Policies" ;
    rs:blockType "regulatory_disclosure" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VCCVAC8V084THGQEV5X> ;
    rs:internalId "struct_01M24B06VE02PKB9W7AM5VJ4Z6" ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/struct_01M24B06VE02PKB9W7AM5VJ4Z6> ;
    rs:taxonomyId "tax_01M24B06VAEXF1DAM23AF0D9CN" ;
    rs:taxonomyName "Cadence Policy Notes" .

<https://robosystems.ai/concept/cadence:OperatingExpensePolicyTextBlock> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01M24B06VB4MFD6SSWNEFMQMMS" ;
    rs:itemType "textBlock" ;
    rs:monetary false ;
    rs:source "native" .

<https://robosystems.ai/concept/cadence:RevenueRecognitionPolicyTextBlock> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01M24B06VB4MFD6SSWNEFMQMMR" ;
    rs:itemType "textBlock" ;
    rs:monetary false ;
    rs:source "native" .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a088afc574ad2254a54" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> a rs:Period ;
    xbrli:endDate "2026-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-09-01"^^xsd:date .
```

</details>


## Disaggregation of Revenue

- **Structure**: Disaggregation of Revenue
- **Information Block**: `struct_01M24B00VBTCVR1E3YG7NYQJ43`
- **FactSet**: `fs_01M24B0VC0T58WW67WBZ393W6M`

| QName | Concept | 2024-09-01 → 2026-08-31 | 2025-09-01 → 2026-08-31 |
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

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDXY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:ProfessionalServicesRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6M> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDXY" ;
    rs:numericValue 88800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/struct_01M24B00VBTCVR1E3YG7NYQJ43> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDXZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:SubscriptionRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6M> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDXZ" ;
    rs:numericValue 1156399.44 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/struct_01M24B00VBTCVR1E3YG7NYQJ43> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6M> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYC" ;
    rs:numericValue 1245199.44 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/struct_01M24B00VBTCVR1E3YG7NYQJ43> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:ProfessionalServicesRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6M> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYF" ;
    rs:numericValue 13200.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/struct_01M24B00VBTCVR1E3YG7NYQJ43> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:SubscriptionRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6M> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYG" ;
    rs:numericValue 179599.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/struct_01M24B00VBTCVR1E3YG7NYQJ43> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/fact/fact_01M24B0VC4H142D4ZWAFCGQDYX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6M> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M24B0VC4H142D4ZWAFCGQDYX" ;
    rs:numericValue 192799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/struct_01M24B00VBTCVR1E3YG7NYQJ43> ;
    rs:unit <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/ib/struct_01M24B00VBTCVR1E3YG7NYQJ43> a rs:InformationBlock ;
    skos:prefLabel "Disaggregation of Revenue" ;
    rs:blockType "regulatory_disclosure" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M24B0VC0T58WW67WBZ393W6M> ;
    rs:internalId "struct_01M24B00VBTCVR1E3YG7NYQJ43" ;
    rs:structure <https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/structure/struct_01M24B00VBTCVR1E3YG7NYQJ43> ;
    rs:taxonomyId "tax_01M24B00V8YH1VX6X8HTFZCN4N" ;
    rs:taxonomyName "Cadence Reporting Extension" .

<https://robosystems.ai/concept/cadence:ProfessionalServicesRevenue> a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01M24B00V9DX8MGG5ATMPQKC6J" ;
    rs:monetary true ;
    rs:source "native" .

<https://robosystems.ai/concept/cadence:SubscriptionRevenue> a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01M24B00V9DX8MGG5ATMPQKC6H" ;
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

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_1> a rs:Period ;
    xbrli:endDate "2026-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/period/p_3> a rs:Period ;
    xbrli:endDate "2025-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/entity/entity_kg1a088afc574ad2254a54> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a088afc574ad2254a54" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M24B0V8W8BDQMS8QCTAVFZR4/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Validation evidence

Independent, standards-grade checks of the same bundle this DataBook renders — embedded so the artifact travels with its own proof.

### Cadence Labs — SHACL Ontology Conformance

#### Result: ✅ **Conforms to RoboSystems RDF Ontology v1**

- **Bundle**: `saas-startup-demo.jsonld`
- **Graph triples**: 3,801
- **rs:Fact nodes**: 86
- **rs:Association nodes**: 170
- **rs:Element nodes**: 98
- **SHACL shapes checked**: 8 (positive instance shapes + negative shapes banning the retired dialects)

Validated on the host with **pyshacl** against `frameworks/ontology/v1/shapes.ttl` — the *same* shapes that gate the framework seeds and the publish-time bundle validation, run here directly on the on-disk artifact (no API, no database, no container). Conformance means every `rs:Fact` references its aspects directly (`rs:element`/`rs:entity`/`rs:period`/`rs:unit` — no XBRL `context`), every `rs:Association` carries `xlink:from`/`to` + `xlink:arcrole`, and none of the retired dialects (`xbrli:contextRef`, `arcFrom`, direct `summationOf`) appear.

#### Violations

_None._ Zero violations.

### Cadence Labs — XBRL 2.1 Validation (Arelle)

#### Result: ✅ **Valid XBRL 2.1**

- **Package**: `saas-startup-demo.zip` (13,931 bytes)
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-lab.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 69
- **Load errors**: 0
- **Validation errors**: 0

Validated on the host with **Arelle** (the de-facto XBRL processor, also used by SEC EDGAR) directly against the on-disk report package — no API, no container. Zero load + validation errors is the structural-correctness claim: the output is valid XBRL 2.1, consumable by any standards-compliant processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system checks are not enabled (the instance isn't an SEC filing).

#### Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
