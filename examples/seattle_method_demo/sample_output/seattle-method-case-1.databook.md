---
id: https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT
type: DataBook
title: "Seattle Method Demo — Lemonade Stand (Charlie Hoffman Test Case 1)"
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
  source: "Lemonade Stand (Charlie Hoffman Test Case 1)"
  method: "Materialized RoboSystems Report rpt_01KWRGK66V55V8NY74RJVF8WPT (generation 1, draft)"
manifest:
  entrypoints:
    - block: balance_sheet
    - block: income_statement
    - block: cash_flow_statement
    - block: equity_statement
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
graph:
  facts: 48
  href: seattle-method-case-1.holon.jsonld
  graphs:
    - id: scene
      iri: https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT#scene
      description: "Instance facts — the values this report reports"
      disposition: inline
    - id: boundary
      iri: https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT#boundary
      description: "Calculation network — the rollup rules the facts must obey"
      disposition: reference
      derived_from: rs-gaap-calculations@v1
    - id: projection
      iri: https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT#projection
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
  report_id: rpt_01KWRGK66V55V8NY74RJVF8WPT
  generation_count: 1
  filing_status: draft
  periods:
    - { label: "2023-10-02 → 2024-03-31", start: 2023-10-02, end: 2024-03-31 }
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
---

# Seattle Method Demo — Lemonade Stand (Charlie Hoffman Test Case 1)

A report **is** a collection of Information Blocks, and this DataBook is a projection of one report holon (see the `graph:` map above). The **scene** graph — the facts — renders twice per block here: a markdown table (human view) and a foldable, addressable `turtle` slice (machine view, the same facts as RDF). The **boundary** (calculation) and **projection** (presentation) graphs live as real named graphs in the companion `seattle-method-case-1.holon.jsonld` — dataset-form JSON-LD, the API-native holon — and derive from their versioned framework, referenced here rather than inlined since they're shared by every report on that framework. The **lineage** graph — the ledger behind the facts — is internal and not published: a report is an aggregation of the books, not the books. The `Validation evidence` section is the published substantiation that the referenced rules hold. Everything here derives from `seattle-method-case-1.jsonld`.


## Balance Sheet

- **Structure**: rs-gaap — Balance Sheet — Classified
- **Information Block**: `b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d`
- **FactSet**: `fs_01KWRGK67MQSW5ECD8R7YH47H2`

| QName | Concept | 2023-10-02 → 2024-03-31 |
|---|---|---:|
| `rs-gaap:CashAndCashEquivalentsAtCarryingValue` |     Cash and Cash Equivalents, at Carrying Value | $10,850.00 |
| `rs-gaap:ReceivablesNetCurrent` |     Receivables, Net, Current | $0.00 |
| `rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings` |     Inventory, Net of Allowances, Customer Advances and Progress Billings | $2,700.00 |
| `rs-gaap:AssetsCurrent` |   **Assets, Current** | $13,550.00 |
| `rs-gaap:PropertyPlantAndEquipmentNet` |     Property, Plant and Equipment, Net | $900.00 |
| `rs-gaap:AssetsNoncurrent` |   **Assets, Noncurrent** | $900.00 |
| `rs-gaap:Assets` | **Assets** | $14,450.00 |
| `rs-gaap:AccountsPayableCurrent` |       Accounts Payable, Current | $1,000.00 |
| `rs-gaap:AccruedLiabilitiesCurrent` |       Accrued Liabilities, Current | $400.00 |
| `rs-gaap:LiabilitiesCurrent` |     **Liabilities, Current** | $1,400.00 |
| `rs-gaap:LongTermDebtAndCapitalLeaseObligations` |       Long-Term Debt and Lease Obligation | $1,000.00 |
| `rs-gaap:LiabilitiesNoncurrent` |     **Liabilities, Noncurrent** | $1,000.00 |
| `rs-gaap:Liabilities` |   **Liabilities** | $2,400.00 |
| `rs-gaap:AdditionalPaidInCapital` |     Additional Paid in Capital | $10,000.00 |
| `rs-gaap:RetainedEarningsAccumulatedDeficit` |     Retained Earnings (Accumulated Deficit) | $2,050.00 |
| `rs-gaap:StockholdersEquity` |   **Stockholders' Equity Attributable to Parent** | $12,050.00 |
| `rs-gaap:LiabilitiesAndStockholdersEquity` | **Liabilities and Equity** | $14,450.00 |

<details>
<summary>▸ Balance Sheet — scene RDF / Turtle (380 triples · 21.9 KB)</summary>

```turtle {#balance_sheet}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDA" ;
    rs:numericValue 1000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccruedLiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDB" ;
    rs:numericValue 400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDC" ;
    rs:numericValue 10000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDD" ;
    rs:numericValue 10850.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDK" ;
    rs:numericValue 2700.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LongTermDebtAndCapitalLeaseObligations ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDM" ;
    rs:numericValue 1000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDN" ;
    rs:numericValue 900.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDP" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDR" ;
    rs:numericValue 2050.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDS" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDT" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TE6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TE6" ;
    rs:numericValue 1400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NG5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NG5" ;
    rs:numericValue 2400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NG6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NG6" ;
    rs:numericValue 14450.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NG7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NG7" ;
    rs:numericValue 900.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NGB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NGB" ;
    rs:numericValue 14450.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NGD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NGD" ;
    rs:numericValue 12050.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NGE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NGE" ;
    rs:numericValue 13550.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NGJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NGJ" ;
    rs:numericValue 1000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/ib/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Balance Sheet — Classified" ;
    rs:blockType "balance_sheet" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H2> ;
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

rs-gaap:AccruedLiabilitiesCurrent a rs:Element ;
    skos:prefLabel "Accrued Liabilities, Current" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "4decfae2-2ca3-5dd3-8c06-88557583c13d" ;
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

rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings a rs:Element ;
    skos:prefLabel "Inventory, Net of Allowances, Customer Advances and Progress Billings" ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "4afa5950-85ac-5a85-a9cb-01c387c6ab08" ;
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

rs-gaap:LiabilitiesNoncurrent a rs:Element ;
    skos:prefLabel "Liabilities, Noncurrent" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "f41fe34d-88ea-5e20-a781-2d3e256a6abf" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:LongTermDebtAndCapitalLeaseObligations a rs:Element ;
    skos:prefLabel "Long-Term Debt and Lease Obligation" ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "091373d9-8a82-51bd-adf8-d09b73beb32e" ;
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

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_3> a rs:Period ;
    xbrli:instant "2023-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

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

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> a rs:Period ;
    xbrli:instant "2024-03-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> a rs:Entity ;
    skos:prefLabel "Lemonade Stand (Charlie Hoffman Test Case 1)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f310982ab9e45ea66e" ;
    rs:legalName "Lemonade Stand (Charlie Hoffman Test Case 1)" .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Income Statement

- **Structure**: rs-gaap — Income Statement — Multi-step
- **Information Block**: `47cd6544-03d1-5bc1-8c28-31c0cfa450f9`
- **FactSet**: `fs_01KWRGK67MQSW5ECD8R7YH47H3`

| QName | Concept | 2023-10-02 → 2024-03-31 |
|---|---|---:|
| `rs-gaap:Revenues` |   **Revenues** | $8,000.00 |
| `rs-gaap:CostOfGoodsAndServicesSold` |     Cost of Product and Service Sold | $5,300.00 |
| `rs-gaap:CostOfRevenue` |   **Cost of Revenue** | $5,300.00 |
| `rs-gaap:GrossProfit` |   **Gross Profit** | $2,700.00 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | $100.00 |
| `rs-gaap:OperatingExpenses` |   **Operating Expenses** | $100.00 |
| `rs-gaap:OperatingIncomeLoss` |   **Operating Income (Loss)** | $2,600.00 |
| `rs-gaap:InterestExpense` |     Interest Expense, Operating and Nonoperating | $150.00 |
| `rs-gaap:NonoperatingIncomeExpense` |   **Nonoperating Income (Expense)** | $(150.00) |
| `rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` |   **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | $2,450.00 |
| `rs-gaap:IncomeTaxExpenseBenefit` |   Income Tax Expense (Benefit) | $400.00 |
| `rs-gaap:IncomeLossFromContinuingOperations` |   **Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent** | $2,050.00 |
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | $2,050.00 |

<details>
<summary>▸ Income Statement — scene RDF / Turtle (278 triples · 16.0 KB)</summary>

```turtle {#income_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDE" ;
    rs:numericValue 5300.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDG" ;
    rs:numericValue 100.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeTaxExpenseBenefit ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDH" ;
    rs:numericValue 400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InterestExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDJ" ;
    rs:numericValue 150.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDQ" ;
    rs:numericValue 8000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDW" ;
    rs:numericValue 2050.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TE7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TE7" ;
    rs:numericValue 2050.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TE8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TE8" ;
    rs:numericValue 2600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TE9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TE9" ;
    rs:numericValue 2450.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TEA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TEA" ;
    rs:numericValue 100.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TEB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NonoperatingIncomeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TEB" ;
    rs:numericValue -150.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NGA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NGA" ;
    rs:numericValue 5300.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NGF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NGF" ;
    rs:numericValue 2700.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/ib/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Income Statement — Multi-step" ;
    rs:blockType "income_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H3> ;
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

rs-gaap:IncomeTaxExpenseBenefit a rs:Element ;
    skos:prefLabel "Income Tax Expense (Benefit)" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "d629316b-5674-58d9-afcd-b7e7fd34232d" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:InterestExpense a rs:Element ;
    skos:prefLabel "Interest Expense, Operating and Nonoperating" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "890e4f8c-8fed-57e2-96fd-e70455201b11" ;
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

rs-gaap:NonoperatingIncomeExpense a rs:Element ;
    skos:prefLabel "Nonoperating Income (Expense)" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "45aae2c2-fa56-50c9-b381-df8079e7d33a" ;
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

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> a rs:Entity ;
    skos:prefLabel "Lemonade Stand (Charlie Hoffman Test Case 1)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f310982ab9e45ea66e" ;
    rs:legalName "Lemonade Stand (Charlie Hoffman Test Case 1)" .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> a rs:Period ;
    xbrli:endDate "2024-03-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Cash Flow Statement

- **Structure**: rs-gaap — Cash Flow Statement — Indirect
- **Information Block**: `5473639a-2dac-56a6-b9e5-38480ea38bc1`
- **FactSet**: `fs_01KWRGK67MQSW5ECD8R7YH47H4`

| QName | Concept | 2023-10-02 → 2024-03-31 |
|---|---|---:|
| `rs-gaap:NetIncomeLoss` |     **Net Income (Loss) Attributable to Parent** | $2,050.00 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | $100.00 |
| `rs-gaap:IncreaseDecreaseInInventories` |     Increase (Decrease) in Inventories | $(2,700.00) |
| `rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet` |     Increase (Decrease) in Other Operating Assets and Liabilities, Net | $1,000.00 |
| `rs-gaap:IncreaseDecreaseInAccruedLiabilities` |     Increase (Decrease) in Accrued Liabilities | $400.00 |
| `rs-gaap:NetCashProvidedByUsedInOperatingActivities` |   Cash Provided by (Used in) Operating Activity, Including Discontinued Operation | $850.00 |
| `rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment` |     Payments to Acquire Property, Plant, and Equipment | $(1,000.00) |
| `rs-gaap:NetCashProvidedByUsedInInvestingActivities` |   Cash Provided by (Used in) Investing Activity, Including Discontinued Operation | $(1,000.00) |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |     Proceeds from Issuance of Common Stock | $10,000.00 |
| `rs-gaap:ProceedsFromIssuanceOfLongTermDebt` |     Proceeds from Issuance of Long-Term Debt | $2,000.00 |
| `rs-gaap:RepaymentsOfLongTermDebt` |     Repayments of Long-Term Debt | $(1,000.00) |
| `rs-gaap:NetCashProvidedByUsedInFinancingActivities` |   Cash Provided by (Used in) Financing Activity, Including Discontinued Operation | $11,000.00 |
| `rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease` | **Cash and Cash Equivalents, Period Increase (Decrease)** | $10,850.00 |

<details>
<summary>▸ Cash Flow Statement — scene RDF / Turtle (278 triples · 16.5 KB)</summary>

```turtle {#cash_flow_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDF" ;
    rs:numericValue 100.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDV" ;
    rs:numericValue 2050.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfLongTermDebt ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDY" ;
    rs:numericValue 2000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDZ" ;
    rs:numericValue 10000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TE1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RepaymentsOfLongTermDebt ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TE1" ;
    rs:numericValue -1000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TE2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TE2" ;
    rs:numericValue -1000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TE3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInInventories ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TE3" ;
    rs:numericValue -2700.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TE4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInAccruedLiabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TE4" ;
    rs:numericValue 400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TE5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TE5" ;
    rs:numericValue 1000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NG8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NG8" ;
    rs:numericValue 850.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NG9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NG9" ;
    rs:numericValue -1000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NGG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInFinancingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NGG" ;
    rs:numericValue 11000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NGH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NGH" ;
    rs:numericValue 10850.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/ib/5473639a-2dac-56a6-b9e5-38480ea38bc1> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Cash Flow Statement — Indirect" ;
    rs:blockType "cash_flow_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H4> ;
    rs:internalId "5473639a-2dac-56a6-b9e5-38480ea38bc1" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

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

rs-gaap:IncreaseDecreaseInAccruedLiabilities a rs:Element ;
    skos:prefLabel "Increase (Decrease) in Accrued Liabilities" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "e92e6488-dcc7-5877-ad4b-f2498bdf7bae" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:IncreaseDecreaseInInventories a rs:Element ;
    skos:prefLabel "Increase (Decrease) in Inventories" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "c8b0722b-7993-592f-8ef1-5b0964ac8a10" ;
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

rs-gaap:ProceedsFromIssuanceOfLongTermDebt a rs:Element ;
    skos:prefLabel "Proceeds from Issuance of Long-Term Debt" ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "1802f6ae-7d95-5029-af7a-b36015b2c525" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:RepaymentsOfLongTermDebt a rs:Element ;
    skos:prefLabel "Repayments of Long-Term Debt" ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "85890c87-aac7-5702-8f69-b1a14139dc41" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> a rs:Entity ;
    skos:prefLabel "Lemonade Stand (Charlie Hoffman Test Case 1)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f310982ab9e45ea66e" ;
    rs:legalName "Lemonade Stand (Charlie Hoffman Test Case 1)" .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> a rs:Period ;
    xbrli:endDate "2024-03-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Statement of Changes in Equity

- **Structure**: rs-gaap — Statement of Changes in Equity — Roll Forward (Total)
- **Information Block**: `0b179e5c-5f02-506d-b8d5-860cb10c7694`
- **FactSet**: `fs_01KWRGK67MQSW5ECD8R7YH47H5`

| QName | Concept | 2023-10-02 → 2024-03-31 |
|---|---|---:|
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | $2,050.00 |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |   Proceeds from Issuance of Common Stock | $10,000.00 |
| `rs-gaap:StockholdersEquity` | **Stockholders' Equity Attributable to Parent** | $12,050.00 |

<details>
<summary>▸ Statement of Changes in Equity — scene RDF / Turtle (81 triples · 5.0 KB)</summary>

```turtle {#equity_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TDX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H5> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TDX" ;
    rs:numericValue 2050.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67SHZZAAC5EFSXP8TE0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H5> ;
    rs:internalId "fact_01KWRGK67SHZZAAC5EFSXP8TE0" ;
    rs:numericValue 10000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/fact/fact_01KWRGK67T049W69G5AGEN3NGC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H5> ;
    rs:internalId "fact_01KWRGK67T049W69G5AGEN3NGC" ;
    rs:numericValue 12050.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/ib/0b179e5c-5f02-506d-b8d5-860cb10c7694> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)" ;
    rs:blockType "equity_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGK67MQSW5ECD8R7YH47H5> ;
    rs:internalId "0b179e5c-5f02-506d-b8d5-860cb10c7694" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_1> a rs:Period ;
    xbrli:instant "2024-03-31"^^xsd:date ;
    xbrli:periodType "instant" .

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

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/period/p_2> a rs:Period ;
    xbrli:endDate "2024-03-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/entity/entity_kg19f310982ab9e45ea66e> a rs:Entity ;
    skos:prefLabel "Lemonade Stand (Charlie Hoffman Test Case 1)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f310982ab9e45ea66e" ;
    rs:legalName "Lemonade Stand (Charlie Hoffman Test Case 1)" .

<https://robosystems.ai/report/rpt_01KWRGK66V55V8NY74RJVF8WPT/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Validation evidence

Independent, standards-grade checks of the same bundle this DataBook renders — embedded so the artifact travels with its own proof.

### Seattle Method — SHACL Ontology Conformance

#### Result: ✅ **Conforms to RoboSystems RDF Ontology v1**

- **Bundle**: `seattle-method-case-1.jsonld`
- **Graph triples**: 3,127
- **rs:Fact nodes**: 48
- **rs:Association nodes**: 162
- **rs:Element nodes**: 93
- **SHACL shapes checked**: 8 (positive instance shapes + negative shapes banning the retired dialects)

Validated on the host with **pyshacl** against `frameworks/ontology/v1/shapes.ttl` — the *same* shapes that gate the framework seeds and the publish-time bundle validation, run here directly on the on-disk artifact (no API, no database, no container). Conformance means every `rs:Fact` references its aspects directly (`rs:element`/`rs:entity`/`rs:period`/`rs:unit` — no XBRL `context`), every `rs:Association` carries `xlink:from`/`to` + `xlink:arcrole`, and none of the retired dialects (`xbrli:contextRef`, `arcFrom`, direct `summationOf`) appear.

#### Violations

_None._ Zero violations.

### Seattle Method — XBRL 2.1 Validation (Arelle)

#### Result: ✅ **Valid XBRL 2.1**

- **Package**: `seattle-method-case-1.zip` (13,592 bytes)
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-lab.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 43
- **Load errors**: 0
- **Validation errors**: 0

Validated on the host with **Arelle** (the de-facto XBRL processor, also used by SEC EDGAR) directly against the on-disk report package — no API, no container. Zero load + validation errors is the structural-correctness claim: the output is valid XBRL 2.1, consumable by any standards-compliant processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system checks are not enabled (the instance isn't an SEC filing).

#### Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
