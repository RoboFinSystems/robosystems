---
id: https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ
type: DataBook
title: "Driftline Coffee Roasters Demo — Driftline Coffee Roasters"
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
  source: "Driftline Coffee Roasters"
  method: "Materialized RoboSystems Report rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ (generation 1, draft)"
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
      description: "Inventory Components"
graph:
  facts: 90
  href: coffee-roaster-demo.holon.jsonld
  graphs:
    - id: scene
      iri: https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ#scene
      description: "Instance facts — the values this report reports"
      disposition: inline
    - id: boundary
      iri: https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ#boundary
      description: "Calculation network — the rollup rules the facts must obey"
      disposition: reference
      derived_from: rs-gaap-calculations@v1
    - id: projection
      iri: https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ#projection
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
  report_id: rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ
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

# Driftline Coffee Roasters Demo — Driftline Coffee Roasters

A report **is** a collection of Information Blocks, and this DataBook is a projection of one report holon (see the `graph:` map above). The **scene** graph — the facts — renders twice per block here: a markdown table (human view) and a foldable, addressable `turtle` slice (machine view, the same facts as RDF). The **boundary** (calculation) and **projection** (presentation) graphs live as real named graphs in the companion `coffee-roaster-demo.holon.jsonld` — dataset-form JSON-LD, the API-native holon — and derive from their versioned framework, referenced here rather than inlined since they're shared by every report on that framework. The **lineage** graph — the ledger behind the facts — is internal and not published: a report is an aggregation of the books, not the books. The `Validation evidence` section is the published substantiation that the referenced rules hold. Everything here derives from `coffee-roaster-demo.jsonld`.


## Balance Sheet

- **Structure**: rs-gaap — Balance Sheet — Classified
- **Information Block**: `b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d`
- **FactSet**: `fs_01KY5P1WD7ZRDCGDMNEDBZWXN8`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `rs-gaap:CashAndCashEquivalentsAtCarryingValue` |     Cash and Cash Equivalents, at Carrying Value | — | $31,166.49 |
| `rs-gaap:ReceivablesNetCurrent` |     Receivables, Net, Current | — | $153,333.33 |
| `rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings` |     **Inventory, Net of Allowances, Customer Advances and Progress Billings** | — | $96,000.00 |
| `rs-gaap:PrepaidExpenseCurrent` |     Prepaid Expense, Current | — | $10,000.00 |
| `rs-gaap:AssetsCurrent` |   **Assets, Current** | — | $290,499.82 |
| `rs-gaap:PropertyPlantAndEquipmentNet` |     Property, Plant and Equipment, Net | — | $65,528.70 |
| `rs-gaap:AssetsNoncurrent` |   **Assets, Noncurrent** | — | $65,528.70 |
| `rs-gaap:Assets` | **Assets** | — | $356,028.52 |
| `rs-gaap:AccountsPayableCurrent` |       Accounts Payable, Current | — | $0.00 |
| `rs-gaap:DeferredRevenueCurrent` |       Deferred Revenue, Current | — | $51,999.99 |
| `rs-gaap:LiabilitiesCurrent` |     **Liabilities, Current** | — | $51,999.99 |
| `rs-gaap:Liabilities` |   **Liabilities** | — | $51,999.99 |
| `rs-gaap:AdditionalPaidInCapital` |     Additional Paid in Capital | — | $100,000.00 |
| `rs-gaap:RetainedEarningsAccumulatedDeficit` |     Retained Earnings (Accumulated Deficit) | — | $204,028.53 |
| `rs-gaap:StockholdersEquity` |   **Stockholders' Equity Attributable to Parent** | — | $304,028.53 |
| `rs-gaap:LiabilitiesAndStockholdersEquity` | **Liabilities and Equity** | — | $356,028.52 |

<details>
<summary>▸ Balance Sheet — scene RDF / Turtle (532 triples · 32.4 KB)</summary>

```turtle {#balance_sheet}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWST> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWST" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWSV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWSV" ;
    rs:numericValue 100000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWSW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWSW" ;
    rs:numericValue 31166.48999999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWSY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWSY" ;
    rs:numericValue 51999.98999999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWT3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWT3" ;
    rs:numericValue 96000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWT4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWT4" ;
    rs:numericValue 10000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWT5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWT5" ;
    rs:numericValue 153333.32999999996 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWT6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWT6" ;
    rs:numericValue 204028.53000000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWTC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWTC" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWTD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWTD" ;
    rs:numericValue 100000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWTE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWTE" ;
    rs:numericValue 71944.40000000002 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWTG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWTG" ;
    rs:numericValue 38777.770000000004 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXR6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXR6" ;
    rs:numericValue 22000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXR7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXR7" ;
    rs:numericValue 15500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXR8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXR8" ;
    rs:numericValue 17333.33 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXR9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXR9" ;
    rs:numericValue 44785.69999999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRJ" ;
    rs:numericValue 65528.7 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRK" ;
    rs:numericValue 56785.74 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRY" ;
    rs:numericValue 51999.98999999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRZ" ;
    rs:numericValue 51999.98999999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXS1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXS1" ;
    rs:numericValue 304028.53 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXS2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXS2" ;
    rs:numericValue 290499.81999999995 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXS3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXS3" ;
    rs:numericValue 356028.51999999996 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXS5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXS5" ;
    rs:numericValue 65528.7 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSA" ;
    rs:numericValue 356028.52 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSG" ;
    rs:numericValue 38777.770000000004 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSH" ;
    rs:numericValue 38777.770000000004 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSK" ;
    rs:numericValue 144785.69999999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSM" ;
    rs:numericValue 126777.73000000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSN" ;
    rs:numericValue 183563.47000000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSR" ;
    rs:numericValue 56785.74 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSX" ;
    rs:numericValue 183563.46999999997 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/ib/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Balance Sheet — Classified" ;
    rs:blockType "balance_sheet" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN8> ;
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

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> a rs:Period ;
    xbrli:instant "2026-06-30"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> a rs:Period ;
    xbrli:instant "2025-06-30"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> a rs:Entity ;
    skos:prefLabel "Driftline Coffee Roasters" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8b605f6fa6d94e569" ;
    rs:legalName "Driftline Coffee Roasters" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Income Statement

- **Structure**: rs-gaap — Income Statement — Multi-step
- **Information Block**: `47cd6544-03d1-5bc1-8c28-31c0cfa450f9`
- **FactSet**: `fs_01KY5P1WD7ZRDCGDMNEDBZWXN9`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` |     Revenue from Contract with Customer, Excluding Assessed Tax | — | $1,226,399.87 |
| `rs-gaap:Revenues` |   **Revenues** | — | $1,226,399.87 |
| `rs-gaap:CostOfGoodsAndServicesSold` |     Cost of Product and Service Sold | — | $508,000.00 |
| `rs-gaap:CostOfRevenue` |   **Cost of Revenue** | — | $508,000.00 |
| `rs-gaap:GrossProfit` |   **Gross Profit** | — | $718,399.87 |
| `rs-gaap:GeneralAndAdministrativeExpense` |     General and Administrative Expense | — | $369,500.00 |
| `rs-gaap:SellingAndMarketingExpense` |     Selling and Marketing Expense | — | $174,400.00 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | — | $15,257.04 |
| `rs-gaap:OperatingExpenses` |   **Operating Expenses** | — | $559,157.04 |
| `rs-gaap:OperatingIncomeLoss` |   **Operating Income (Loss)** | — | $159,242.83 |
| `rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` |   **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | — | $159,242.83 |
| `rs-gaap:IncomeLossFromContinuingOperations` |   **Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent** | — | $159,242.83 |
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | — | $159,242.83 |

<details>
<summary>▸ Income Statement — scene RDF / Turtle (438 triples · 27.0 KB)</summary>

```turtle {#income_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWSX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWSX" ;
    rs:numericValue 508000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWSZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWSZ" ;
    rs:numericValue 15257.04 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWT1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWT1" ;
    rs:numericValue 369500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWT7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWT7" ;
    rs:numericValue 1226399.87 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWT8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SellingAndMarketingExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWT8" ;
    rs:numericValue 174400.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWTF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWTF" ;
    rs:numericValue 84000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWTH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWTH" ;
    rs:numericValue 3214.26 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWTK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWTK" ;
    rs:numericValue 73000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRA" ;
    rs:numericValue 189599.96 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SellingAndMarketingExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRB" ;
    rs:numericValue 20600.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRC" ;
    rs:numericValue 159242.83000000007 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRF" ;
    rs:numericValue 8785.699999999983 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRV" ;
    rs:numericValue 508000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRW" ;
    rs:numericValue 559157.04 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRX" ;
    rs:numericValue 159242.83000000007 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXS7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXS7" ;
    rs:numericValue 718399.8700000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXS8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXS8" ;
    rs:numericValue 159242.83000000007 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXS9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXS9" ;
    rs:numericValue 1226399.87 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSB" ;
    rs:numericValue 159242.83000000007 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSC" ;
    rs:numericValue 84000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSD" ;
    rs:numericValue 96814.26 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSE" ;
    rs:numericValue 8785.699999999997 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXST> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXST" ;
    rs:numericValue 105599.95999999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSV" ;
    rs:numericValue 8785.699999999997 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSW" ;
    rs:numericValue 189599.96 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSY" ;
    rs:numericValue 8785.699999999997 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/ib/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Income Statement — Multi-step" ;
    rs:blockType "income_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXN9> ;
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

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> a rs:Period ;
    xbrli:endDate "2026-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> a rs:Period ;
    xbrli:endDate "2025-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> a rs:Entity ;
    skos:prefLabel "Driftline Coffee Roasters" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8b605f6fa6d94e569" ;
    rs:legalName "Driftline Coffee Roasters" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Cash Flow Statement

- **Structure**: rs-gaap — Cash Flow Statement — Indirect
- **Information Block**: `5473639a-2dac-56a6-b9e5-38480ea38bc1`
- **FactSet**: `fs_01KY5P1WD7ZRDCGDMNEDBZWXNA`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `rs-gaap:NetIncomeLoss` |     **Net Income (Loss) Attributable to Parent** | — | $159,242.83 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | — | $15,257.04 |
| `rs-gaap:IncreaseDecreaseInAccountsReceivable` |     Increase (Decrease) in Accounts Receivable | — | $(136,000.00) |
| `rs-gaap:IncreaseDecreaseInInventories` |     Increase (Decrease) in Inventories | — | $(74,000.00) |
| `rs-gaap:IncreaseDecreaseInPrepaidExpense` |     Increase (Decrease) in Prepaid Expense | — | $5,500.00 |
| `rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet` |     Increase (Decrease) in Other Operating Assets and Liabilities, Net | — | $(10,777.78) |
| `rs-gaap:NetCashProvidedByUsedInOperatingActivities` |   Cash Provided by (Used in) Operating Activity, Including Discontinued Operation | — | $(40,777.91) |
| `rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment` |     Payments to Acquire Property, Plant, and Equipment | — | — |
| `rs-gaap:NetCashProvidedByUsedInInvestingActivities` |   Cash Provided by (Used in) Investing Activity, Including Discontinued Operation | — | — |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |     Proceeds from Issuance of Common Stock | — | — |
| `rs-gaap:NetCashProvidedByUsedInFinancingActivities` |   Cash Provided by (Used in) Financing Activity, Including Discontinued Operation | — | — |
| `rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease` | **Cash and Cash Equivalents, Period Increase (Decrease)** | — | $(40,777.91) |

<details>
<summary>▸ Cash Flow Statement — scene RDF / Turtle (318 triples · 19.1 KB)</summary>

```turtle {#cash_flow_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWT0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWT0" ;
    rs:numericValue 15257.04 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWTJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWTJ" ;
    rs:numericValue 3214.26 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRE" ;
    rs:numericValue 159242.83000000007 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRH" ;
    rs:numericValue 8785.699999999983 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRM" ;
    rs:numericValue 100000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRP" ;
    rs:numericValue -90000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInPrepaidExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRQ" ;
    rs:numericValue 5500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInInventories ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRR" ;
    rs:numericValue -74000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInAccountsReceivable ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRS" ;
    rs:numericValue -135999.99999999994 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRT" ;
    rs:numericValue -10777.780000000166 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXS4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXS4" ;
    rs:numericValue -40777.91000000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXS6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXS6" ;
    rs:numericValue -40777.91000000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInFinancingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSF" ;
    rs:numericValue 100000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSP" ;
    rs:numericValue -90000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSQ" ;
    rs:numericValue 21999.959999999985 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSS" ;
    rs:numericValue 11999.959999999983 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/ib/5473639a-2dac-56a6-b9e5-38480ea38bc1> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Cash Flow Statement — Indirect" ;
    rs:blockType "cash_flow_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNA> ;
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

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> a rs:Period ;
    xbrli:endDate "2026-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> a rs:Period ;
    xbrli:endDate "2025-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> a rs:Entity ;
    skos:prefLabel "Driftline Coffee Roasters" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8b605f6fa6d94e569" ;
    rs:legalName "Driftline Coffee Roasters" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Statement of Changes in Equity

- **Structure**: rs-gaap — Statement of Changes in Equity — Roll Forward (Total)
- **Information Block**: `0b179e5c-5f02-506d-b8d5-860cb10c7694`
- **FactSet**: `fs_01KY5P1WD7ZRDCGDMNEDBZWXNB`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | — | $159,242.83 |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |   Proceeds from Issuance of Common Stock | — | — |
| `rs-gaap:StockholdersEquity` | **Stockholders' Equity Attributable to Parent** | — | $304,028.53 |

<details>
<summary>▸ Statement of Changes in Equity — scene RDF / Turtle (113 triples · 7.0 KB)</summary>

```turtle {#equity_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRD" ;
    rs:numericValue 159242.83000000007 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRG" ;
    rs:numericValue 8785.699999999983 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXRN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXRN" ;
    rs:numericValue 100000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXS0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXS0" ;
    rs:numericValue 304028.53 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDMHNRWJHWWGDGTGXSJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNB> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDMHNRWJHWWGDGTGXSJ" ;
    rs:numericValue 144785.69999999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/ib/0b179e5c-5f02-506d-b8d5-860cb10c7694> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)" ;
    rs:blockType "equity_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNB> ;
    rs:internalId "0b179e5c-5f02-506d-b8d5-860cb10c7694" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> a rs:Period ;
    xbrli:instant "2026-06-30"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> a rs:Period ;
    xbrli:instant "2025-06-30"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> a rs:Period ;
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

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> a rs:Period ;
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

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> a rs:Entity ;
    skos:prefLabel "Driftline Coffee Roasters" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8b605f6fa6d94e569" ;
    rs:legalName "Driftline Coffee Roasters" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Significant Accounting Policies

- **Structure**: Significant Accounting Policies
- **Information Block**: `struct_01KY5P1809CD82E3GXDR4CXH8J`
- **FactSet**: `fs_01KY5P1WDWQYX4N7VH6YVTMS0V`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `driftline:InventoryPolicyTextBlock` |   Inventory Policy Text Block | — | — |
| `driftline:RevenueRecognitionPolicyTextBlock` |   Revenue Recognition Policy Text Block | — | — |
| `driftline:DepreciationPolicyTextBlock` |   Depreciation Policy Text Block | — | — |

<details>
<summary>▸ Significant Accounting Policies — scene RDF / Turtle (83 triples · 7.8 KB)</summary>

```turtle {#regulatory_disclosure}
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WE0MRJGWGR48WKBBQMH> a rs:Fact ;
    rs:contentType "text/markdown" ;
    rs:element <https://robosystems.ai/concept/driftline:InventoryPolicyTextBlock> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WDWQYX4N7VH6YVTMS0V> ;
    rs:factType "nonnumeric" ;
    rs:internalId "fact_01KY5P1WE0MRJGWGR48WKBBQMH" ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:stringValue """# Inventory & Cost of Goods Sold Policy — Driftline Coffee Roasters

## Method
Inventory is carried at cost (FIFO). Green coffee is purchased in bulk from the importer ahead of demand, roasted, and sold through DTC and wholesale channels.

## Lifecycle
1. **Purchase** — green coffee received from the importer: DR Inventory — Green Coffee (1400) / CR Cash or Accounts Payable (2000). Cash leaves the business when the coffee is bought, not when it is sold.
2. **Sale / consumption** — when finished coffee is sold, recognize cost of goods sold: DR Cost of Goods Sold (5000) / CR Inventory — Green Coffee (1400).

## Watch-out: the cash trap
Buying green coffee ahead of a wholesale launch or the holiday season builds the Inventory asset and drains cash *before* the corresponding revenue and COGS are recognized. A month can show strong gross profit while operating cash falls — the difference is sitting in inventory. During close, compare the change in inventory to the change in cash.

## Gross Margin
Target blended gross margin ≈ 55%. Investigate months where COGS as a share of revenue moves materially off trend."""^^xsd:string ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P1809CD82E3GXDR4CXH8J> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WE0MRJGWGR48WKBBQMJ> a rs:Fact ;
    rs:contentType "text/markdown" ;
    rs:element <https://robosystems.ai/concept/driftline:RevenueRecognitionPolicyTextBlock> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WDWQYX4N7VH6YVTMS0V> ;
    rs:factType "nonnumeric" ;
    rs:internalId "fact_01KY5P1WE0MRJGWGR48WKBBQMJ" ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:stringValue """# Revenue Recognition Policy — Driftline Coffee Roasters

## Standard
Revenue is recognized under ASC 606 as control of the coffee transfers to the customer.

## Channels

| Channel | Account | Recognition Timing | Cash Timing |
|---|---|---|---|
| DTC Subscriptions | Subscription Revenue — DTC (4000) | Ratably as each month's coffee ships | **Prepaid** — billed a quarter ahead into Deferred Subscription Revenue (2300) |
| DTC Single Orders | Single-Order Revenue — DTC (4200) | On shipment | Immediate (card at checkout) |
| Wholesale | Wholesale Revenue (4100) | On delivery to the grocer/café | **Net 30** — builds Accounts Receivable (1100) |

## Deferred Subscription Revenue
Subscriber prepayments are a liability until the coffee ships. Each month:
- DR Deferred Subscription Revenue (2300) / CR Subscription Revenue — DTC (4000) for the month earned.
- The deferred balance is cash already collected for future shipments — a source of working capital.

## Wholesale Accounts Receivable
Wholesale sells on net-30 terms. Receivables that age past 60 days, or that concentrate in a single large account, are a working-capital and credit risk — review the AR aging by customer every close."""^^xsd:string ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P1809CD82E3GXDR4CXH8J> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WE0MRJGWGR48WKBBQMK> a rs:Fact ;
    rs:contentType "text/markdown" ;
    rs:element <https://robosystems.ai/concept/driftline:DepreciationPolicyTextBlock> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WDWQYX4N7VH6YVTMS0V> ;
    rs:factType "nonnumeric" ;
    rs:internalId "fact_01KY5P1WE0MRJGWGR48WKBBQMK" ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:stringValue """# Depreciation & Prepaid Policy — Driftline Coffee Roasters

## Depreciation
Straight-line, no salvage value unless specified. Entry: DR Depreciation Expense (7000) / CR Accumulated Depreciation (1550).

| Asset | Cost | Useful Life | Monthly |
|---|---|---|---|
| Roasting & Packaging Equipment | $90,000.00 | 84 months | $1,071.43 |
| Automated Packaging Line | $24,000.00 | 60 months | $400.00 |

## Prepaid Expense Amortization
Purchases with a term greater than one month are capitalized and amortized straight-line. Entry: DR expense / CR prepaid account.

| Item | Cost | Term | Monthly | Debit | Credit |
|---|---|---|---|---|---|
| Business Insurance | $12,000.00 | 12 months | $1,000.00 | Insurance (6500) | Prepaid Insurance (1200) |
| Annual Platform Prepay | $6,000.00 | 12 months | $500.00 | Software & Subscriptions (6400) | Prepaid Software (1210) |

During each close, review prepaid and fixed-asset accounts for new purchases that lack a schedule, and create one."""^^xsd:string ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P1809CD82E3GXDR4CXH8J> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/ib/struct_01KY5P1809CD82E3GXDR4CXH8J> a rs:InformationBlock ;
    skos:prefLabel "Significant Accounting Policies" ;
    rs:blockType "regulatory_disclosure" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WDWQYX4N7VH6YVTMS0V> ;
    rs:internalId "struct_01KY5P1809CD82E3GXDR4CXH8J" ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P1809CD82E3GXDR4CXH8J> ;
    rs:taxonomyId "tax_01KY5P1807QH5CF3T8J6T9NPJ7" ;
    rs:taxonomyName "Driftline Policy Notes" .

<https://robosystems.ai/concept/driftline:DepreciationPolicyTextBlock> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01KY5P1808A9SZ0XJMK5KFCWY9" ;
    rs:itemType "textBlock" ;
    rs:monetary false ;
    rs:source "native" .

<https://robosystems.ai/concept/driftline:InventoryPolicyTextBlock> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01KY5P1808A9SZ0XJMK5KFCWY7" ;
    rs:itemType "textBlock" ;
    rs:monetary false ;
    rs:source "native" .

<https://robosystems.ai/concept/driftline:RevenueRecognitionPolicyTextBlock> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01KY5P1808A9SZ0XJMK5KFCWY8" ;
    rs:itemType "textBlock" ;
    rs:monetary false ;
    rs:source "native" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> a rs:Entity ;
    skos:prefLabel "Driftline Coffee Roasters" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8b605f6fa6d94e569" ;
    rs:legalName "Driftline Coffee Roasters" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> a rs:Period ;
    xbrli:endDate "2026-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-07-01"^^xsd:date .
```

</details>


## Inventory Components

- **Structure**: Inventory Components
- **Information Block**: `struct_01KY5P149S058ZVQ0APPZR3EEE`
- **FactSet**: `fs_01KY5P1WD7ZRDCGDMNEDBZWXNC`

| QName | Concept | 2024-07-01 → 2026-06-30 | 2025-07-01 → 2026-06-30 |
|---|---|---: | ---:|
| `driftline:InventoryRawMaterials` |   Inventory Raw Materials | — | $88,000.00 |
| `driftline:InventoryWorkInProcess` |   Inventory Work In Process | — | $3,000.00 |
| `driftline:InventoryFinishedGoods` |   Inventory Finished Goods | — | $5,000.00 |
| `rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings` | **Inventory, Net of Allowances, Customer Advances and Progress Billings** | — | $96,000.00 |

<details>
<summary>▸ Inventory Components — scene RDF / Turtle (151 triples · 9.8 KB)</summary>

```turtle {#regulatory_disclosure}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWSQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/driftline:InventoryFinishedGoods> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNC> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWSQ" ;
    rs:numericValue 5000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P149S058ZVQ0APPZR3EEE> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWSR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/driftline:InventoryRawMaterials> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNC> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWSR" ;
    rs:numericValue 88000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P149S058ZVQ0APPZR3EEE> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWSS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/driftline:InventoryWorkInProcess> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNC> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWSS" ;
    rs:numericValue 3000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P149S058ZVQ0APPZR3EEE> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWT2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNC> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWT2" ;
    rs:numericValue 96000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P149S058ZVQ0APPZR3EEE> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWT9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/driftline:InventoryFinishedGoods> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNC> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWT9" ;
    rs:numericValue 5000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P149S058ZVQ0APPZR3EEE> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWTA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/driftline:InventoryRawMaterials> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNC> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWTA" ;
    rs:numericValue 14000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P149S058ZVQ0APPZR3EEE> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKAQ17YBMH6S1TYWTB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element <https://robosystems.ai/concept/driftline:InventoryWorkInProcess> ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNC> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKAQ17YBMH6S1TYWTB" ;
    rs:numericValue 3000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P149S058ZVQ0APPZR3EEE> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/fact/fact_01KY5P1WDKHNRWJHWWGDGTGXR5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:entity <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNC> ;
    rs:factType "numeric" ;
    rs:internalId "fact_01KY5P1WDKHNRWJHWWGDGTGXR5" ;
    rs:numericValue 22000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P149S058ZVQ0APPZR3EEE> ;
    rs:unit <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/ib/struct_01KY5P149S058ZVQ0APPZR3EEE> a rs:InformationBlock ;
    skos:prefLabel "Inventory Components" ;
    rs:blockType "regulatory_disclosure" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KY5P1WD7ZRDCGDMNEDBZWXNC> ;
    rs:internalId "struct_01KY5P149S058ZVQ0APPZR3EEE" ;
    rs:structure <https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/structure/struct_01KY5P149S058ZVQ0APPZR3EEE> ;
    rs:taxonomyId "tax_01KY5P149PFMDTE2CFHBHDTJ62" ;
    rs:taxonomyName "Driftline Reporting Extension" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_1> a rs:Period ;
    xbrli:instant "2026-06-30"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_2> a rs:Period ;
    xbrli:instant "2025-06-30"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/concept/driftline:InventoryFinishedGoods> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01KY5P149PFMDTE2CFHBHDTJ65" ;
    rs:monetary true ;
    rs:source "native" .

<https://robosystems.ai/concept/driftline:InventoryRawMaterials> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01KY5P149PFMDTE2CFHBHDTJ63" ;
    rs:monetary true ;
    rs:source "native" .

<https://robosystems.ai/concept/driftline:InventoryWorkInProcess> a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "elem_01KY5P149PFMDTE2CFHBHDTJ64" ;
    rs:monetary true ;
    rs:source "native" .

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

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_3> a rs:Period ;
    xbrli:endDate "2026-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/period/p_4> a rs:Period ;
    xbrli:endDate "2025-06-30"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-07-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/entity/entity_kg19f8b605f6fa6d94e569> a rs:Entity ;
    skos:prefLabel "Driftline Coffee Roasters" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f8b605f6fa6d94e569" ;
    rs:legalName "Driftline Coffee Roasters" .

<https://robosystems.ai/report/rpt_01KY5P1WBF2PECGK2RYHQ8A2GZ/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Validation evidence

Independent, standards-grade checks of the same bundle this DataBook renders — embedded so the artifact travels with its own proof.

### Driftline Coffee Roasters — SHACL Ontology Conformance

#### Result: ✅ **Conforms to RoboSystems RDF Ontology v1**

- **Bundle**: `coffee-roaster-demo.jsonld`
- **Graph triples**: 3,937
- **rs:Fact nodes**: 90
- **rs:Association nodes**: 177
- **rs:Element nodes**: 101
- **SHACL shapes checked**: 8 (positive instance shapes + negative shapes banning the retired dialects)

Validated on the host with **pyshacl** against `frameworks/ontology/v1/shapes.ttl` — the *same* shapes that gate the framework seeds and the publish-time bundle validation, run here directly on the on-disk artifact (no API, no database, no container). Conformance means every `rs:Fact` references its aspects directly (`rs:element`/`rs:entity`/`rs:period`/`rs:unit` — no XBRL `context`), every `rs:Association` carries `xlink:from`/`to` + `xlink:arcrole`, and none of the retired dialects (`xbrli:contextRef`, `arcFrom`, direct `summationOf`) appear.

#### Violations

_None._ Zero violations.

### Driftline Coffee Roasters — XBRL 2.1 Validation (Arelle)

#### Result: ✅ **Valid XBRL 2.1**

- **Package**: `coffee-roaster-demo.zip` (13,987 bytes)
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-lab.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 70
- **Load errors**: 0
- **Validation errors**: 0

Validated on the host with **Arelle** (the de-facto XBRL processor, also used by SEC EDGAR) directly against the on-disk report package — no API, no container. Zero load + validation errors is the structural-correctness claim: the output is valid XBRL 2.1, consumable by any standards-compliant processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system checks are not enabled (the instance isn't an SEC filing).

#### Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
