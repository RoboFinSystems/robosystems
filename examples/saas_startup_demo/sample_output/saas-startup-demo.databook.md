---
id: https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH
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
  method: "Materialized RoboSystems Report rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH (generation 1, draft)"
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
      iri: https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH#scene
      description: "Instance facts — the values this report reports"
      disposition: inline
    - id: boundary
      iri: https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH#boundary
      description: "Calculation network — the rollup rules the facts must obey"
      disposition: reference
      derived_from: rs-gaap-calculations@v1
    - id: projection
      iri: https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH#projection
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
  report_id: rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH
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
- **FactSet**: `fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7`

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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FRV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FRV" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FRW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FRW" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FRX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FRX" ;
    rs:numericValue 1913398.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FRZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FRZ" ;
    rs:numericValue 1153999.56 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FS3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FS3" ;
    rs:numericValue 36500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FS4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FS4" ;
    rs:numericValue 9600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FS6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FS6" ;
    rs:numericValue -1970278.46 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSC" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSD" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSE" ;
    rs:numericValue 2767021.77 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSG" ;
    rs:numericValue 922021.97 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSM" ;
    rs:numericValue 33000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSN" ;
    rs:numericValue 4800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSQ" ;
    rs:numericValue -903866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FT1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FT1" ;
    rs:numericValue 59222.3 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FT2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FT2" ;
    rs:numericValue 48333.34 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTD" ;
    rs:numericValue 1959498.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTE" ;
    rs:numericValue 59222.3 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTF" ;
    rs:numericValue 2018721.1 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTG" ;
    rs:numericValue 1153999.56 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTJ" ;
    rs:numericValue 864721.54 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTK" ;
    rs:numericValue 2018721.1 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTM" ;
    rs:numericValue 1153999.56 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTZ" ;
    rs:numericValue 2804821.77 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6J> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6J" ;
    rs:numericValue 48333.34 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6K> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6K" ;
    rs:numericValue 2853155.11 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6M> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6M" ;
    rs:numericValue 922021.97 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6P> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6P" ;
    rs:numericValue 1931133.14 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6Q> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6Q" ;
    rs:numericValue 2853155.11 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6R> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6R" ;
    rs:numericValue 922021.97 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/ib/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Balance Sheet — Classified" ;
    rs:blockType "balance_sheet" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX7> ;
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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> a rs:Period ;
    xbrli:instant "2026-08-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> a rs:Period ;
    xbrli:instant "2025-08-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a07f28ad2b28bf365b8" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Income Statement

- **Structure**: rs-gaap — Income Statement — Multi-step
- **Information Block**: `47cd6544-03d1-5bc1-8c28-31c0cfa450f9`
- **FactSet**: `fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8`

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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FRY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FRY" ;
    rs:numericValue 266400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FS0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FS0" ;
    rs:numericValue 29111.04 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FS2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FS2" ;
    rs:numericValue 488500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FS5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ResearchAndDevelopmentExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FS5" ;
    rs:numericValue 786000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FS7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FS7" ;
    rs:numericValue 1245199.44 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FS9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SellingAndMarketingExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FS9" ;
    rs:numericValue 741600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSF" ;
    rs:numericValue 39600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSH" ;
    rs:numericValue 6666.66 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSK" ;
    rs:numericValue 99000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ResearchAndDevelopmentExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSP" ;
    rs:numericValue 129000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSR" ;
    rs:numericValue 192799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FST> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SellingAndMarketingExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FST" ;
    rs:numericValue 122400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSV" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSY" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FT9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FT9" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTA" ;
    rs:numericValue 1245199.44 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTN" ;
    rs:numericValue 2045211.04 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTP" ;
    rs:numericValue 266400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTQ" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTR" ;
    rs:numericValue 978799.44 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTS" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTT" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTV" ;
    rs:numericValue 192799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6S> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6S" ;
    rs:numericValue 357066.66 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6T> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6T" ;
    rs:numericValue 39600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6V> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6V" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6W> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6W" ;
    rs:numericValue 153199.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6X> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6X" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/ib/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Income Statement — Multi-step" ;
    rs:blockType "income_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX8> ;
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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> a rs:Period ;
    xbrli:endDate "2026-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> a rs:Period ;
    xbrli:endDate "2025-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a07f28ad2b28bf365b8" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Cash Flow Statement

- **Structure**: rs-gaap — Cash Flow Statement — Indirect
- **Information Block**: `5473639a-2dac-56a6-b9e5-38480ea38bc1`
- **FactSet**: `fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9`

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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FS1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FS1" ;
    rs:numericValue 29111.04 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSJ" ;
    rs:numericValue 6666.66 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSX" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FT0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FT0" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FT3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FT3" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FT5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FT5" ;
    rs:numericValue -80000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FT6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInPrepaidExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FT6" ;
    rs:numericValue -3500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FT7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInAccountsReceivable ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FT7" ;
    rs:numericValue -4800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FT8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FT8" ;
    rs:numericValue 191977.59 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTB" ;
    rs:numericValue -853622.97 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTC" ;
    rs:numericValue -853622.97 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTW" ;
    rs:numericValue -197200.2 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTX" ;
    rs:numericValue 2557799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInFinancingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTY" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6H> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6H" ;
    rs:numericValue -80000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/ib/5473639a-2dac-56a6-b9e5-38480ea38bc1> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Cash Flow Statement — Indirect" ;
    rs:blockType "cash_flow_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKX9> ;
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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> a rs:Period ;
    xbrli:endDate "2026-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> a rs:Period ;
    xbrli:endDate "2025-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a07f28ad2b28bf365b8" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Statement of Changes in Equity

- **Structure**: rs-gaap — Statement of Changes in Equity — Roll Forward (Total)
- **Information Block**: `0b179e5c-5f02-506d-b8d5-860cb10c7694`
- **FactSet**: `fs_01M1ZK1ZXTQ9R6N5APZTK6BKXA`

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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSW" ;
    rs:numericValue -1066411.6 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSZ" ;
    rs:numericValue -203866.86 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FT4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FT4" ;
    rs:numericValue 2835000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FTH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FTH" ;
    rs:numericValue 864721.54 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY1Z2CB34JBGTX8NF6N> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY1Z2CB34JBGTX8NF6N" ;
    rs:numericValue 1931133.14 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/ib/0b179e5c-5f02-506d-b8d5-860cb10c7694> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)" ;
    rs:blockType "equity_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXA> ;
    rs:internalId "0b179e5c-5f02-506d-b8d5-860cb10c7694" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_1> a rs:Period ;
    xbrli:instant "2026-08-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_2> a rs:Period ;
    xbrli:instant "2025-08-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> a rs:Period ;
    xbrli:endDate "2026-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-09-01"^^xsd:date .

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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> a rs:Period ;
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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a07f28ad2b28bf365b8" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Significant Accounting Policies

- **Structure**: Significant Accounting Policies
- **Information Block**: `struct_01M1ZK1D4P6AVPB56A512ACZDX`
- **FactSet**: `fs_01M1ZK1ZYA9Q7JQ3NRQWQQG78W`

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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZYBG1NFB3B7XXDZM3CJ> a rs:Fact ;
    rs:contentType "text/markdown" ;
    rs:element <https://robosystems.ai/concept/cadence:RevenueRecognitionPolicyTextBlock> ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZYA9Q7JQ3NRQWQQG78W> ;
    rs:factType "nonnumeric" ;
    rs:internalId "fact_01M1ZK1ZYBG1NFB3B7XXDZM3CJ" ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
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
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/struct_01M1ZK1D4P6AVPB56A512ACZDX> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZYBG1NFB3B7XXDZM3CK> a rs:Fact ;
    rs:contentType "text/markdown" ;
    rs:element <https://robosystems.ai/concept/cadence:OperatingExpensePolicyTextBlock> ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZYA9Q7JQ3NRQWQQG78W> ;
    rs:factType "nonnumeric" ;
    rs:internalId "fact_01M1ZK1ZYBG1NFB3B7XXDZM3CK" ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
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
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/struct_01M1ZK1D4P6AVPB56A512ACZDX> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/ib/struct_01M1ZK1D4P6AVPB56A512ACZDX> a rs:InformationBlock ;
    skos:prefLabel "Significant Accounting Policies" ;
    rs:blockType "regulatory_disclosure" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZYA9Q7JQ3NRQWQQG78W> ;
    rs:internalId "struct_01M1ZK1D4P6AVPB56A512ACZDX" ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/struct_01M1ZK1D4P6AVPB56A512ACZDX> ;
    rs:taxonomyId "tax_01M1ZK1D4MNABVQYFX38KYK5K0" ;
    rs:taxonomyName "Cadence Policy Notes" .

<https://robosystems.ai/concept/cadence:OperatingExpensePolicyTextBlock> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01M1ZK1D4NHKY2JQV7B0JFCKJJ" ;
    rs:itemType "textBlock" ;
    rs:monetary false ;
    rs:source "native" .

<https://robosystems.ai/concept/cadence:RevenueRecognitionPolicyTextBlock> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01M1ZK1D4NHKY2JQV7B0JFCKJH" ;
    rs:itemType "textBlock" ;
    rs:monetary false ;
    rs:source "native" .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a07f28ad2b28bf365b8" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> a rs:Period ;
    xbrli:endDate "2026-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-09-01"^^xsd:date .
```

</details>


## Disaggregation of Revenue

- **Structure**: Disaggregation of Revenue
- **Information Block**: `struct_01M1ZK19H8V4RE3N1CQ61M2B3H`
- **FactSet**: `fs_01M1ZK1ZXTQ9R6N5APZTK6BKXB`

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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FRS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:ProfessionalServicesRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FRS" ;
    rs:numericValue 88800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/struct_01M1ZK19H8V4RE3N1CQ61M2B3H> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FRT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:SubscriptionRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FRT" ;
    rs:numericValue 1156399.44 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/struct_01M1ZK19H8V4RE3N1CQ61M2B3H> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FS8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FS8" ;
    rs:numericValue 1245199.44 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/struct_01M1ZK19H8V4RE3N1CQ61M2B3H> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:ProfessionalServicesRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSA" ;
    rs:numericValue 13200.0 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/struct_01M1ZK19H8V4RE3N1CQ61M2B3H> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/cadence:SubscriptionRevenue> ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSB" ;
    rs:numericValue 179599.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/struct_01M1ZK19H8V4RE3N1CQ61M2B3H> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/fact/fact_01M1ZK1ZY0JTBQFNSE1XRT0FSS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01M1ZK1ZY0JTBQFNSE1XRT0FSS" ;
    rs:numericValue 192799.8 ;
    rs:period <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/struct_01M1ZK19H8V4RE3N1CQ61M2B3H> ;
    rs:unit <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/ib/struct_01M1ZK19H8V4RE3N1CQ61M2B3H> a rs:InformationBlock ;
    skos:prefLabel "Disaggregation of Revenue" ;
    rs:blockType "regulatory_disclosure" ;
    rs:factSet <https://robosystems.ai/factset/fs_01M1ZK1ZXTQ9R6N5APZTK6BKXB> ;
    rs:internalId "struct_01M1ZK19H8V4RE3N1CQ61M2B3H" ;
    rs:structure <https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/structure/struct_01M1ZK19H8V4RE3N1CQ61M2B3H> ;
    rs:taxonomyId "tax_01M1ZK19H50G6FGDVYYRAQPC1D" ;
    rs:taxonomyName "Cadence Reporting Extension" .

<https://robosystems.ai/concept/cadence:ProfessionalServicesRevenue> a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01M1ZK19H65Z0GF8RCFJFH56J9" ;
    rs:monetary true ;
    rs:source "native" .

<https://robosystems.ai/concept/cadence:SubscriptionRevenue> a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01M1ZK19H65Z0GF8RCFJFH56J8" ;
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

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_3> a rs:Period ;
    xbrli:endDate "2026-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/period/p_4> a rs:Period ;
    xbrli:endDate "2025-08-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-09-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/entity/entity_kg1a07f28ad2b28bf365b8> a rs:Entity ;
    skos:prefLabel "Cadence Labs, Inc." ;
    rs:country "US" ;
    rs:internalId "entity_kg1a07f28ad2b28bf365b8" ;
    rs:legalName "Cadence Labs, Inc." .

<https://robosystems.ai/report/rpt_01M1ZK1ZWEZ2Z01E1GX55PXWKH/unit/u_USD> a rs:Unit ;
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

- **Package**: `saas-startup-demo.zip` (13,906 bytes)
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-lab.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 69
- **Load errors**: 0
- **Validation errors**: 0

Validated on the host with **Arelle** (the de-facto XBRL processor, also used by SEC EDGAR) directly against the on-disk report package — no API, no container. Zero load + validation errors is the structural-correctness claim: the output is valid XBRL 2.1, consumable by any standards-compliant processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system checks are not enabled (the instance isn't an SEC filing).

#### Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
