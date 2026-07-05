---
id: https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69
type: DataBook
title: "World Online Demo — The World Online (Charlie Hoffman demo)"
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
  source: "The World Online (Charlie Hoffman demo)"
  method: "Materialized RoboSystems Report rpt_01KWRGZE0Q574X3Z17TK0T7F69 (generation 1, draft)"
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
  facts: 55
  href: world-online.holon.jsonld
  graphs:
    - id: scene
      iri: https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69#scene
      description: "Instance facts — the values this report reports"
      disposition: inline
    - id: boundary
      iri: https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69#boundary
      description: "Calculation network — the rollup rules the facts must obey"
      disposition: reference
      derived_from: rs-gaap-calculations@v1
    - id: projection
      iri: https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69#projection
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
  report_id: rpt_01KWRGZE0Q574X3Z17TK0T7F69
  generation_count: 1
  filing_status: draft
  periods:
    - { label: "2018-12-31 → 2028-12-31", start: 2018-12-31, end: 2028-12-31 }
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

# World Online Demo — The World Online (Charlie Hoffman demo)

A report **is** a collection of Information Blocks, and this DataBook is a projection of one report holon (see the `graph:` map above). The **scene** graph — the facts — renders twice per block here: a markdown table (human view) and a foldable, addressable `turtle` slice (machine view, the same facts as RDF). The **boundary** (calculation) and **projection** (presentation) graphs live as real named graphs in the companion `world-online.holon.jsonld` — dataset-form JSON-LD, the API-native holon — and derive from their versioned framework, referenced here rather than inlined since they're shared by every report on that framework. The **lineage** graph — the ledger behind the facts — is internal and not published: a report is an aggregation of the books, not the books. The `Validation evidence` section is the published substantiation that the referenced rules hold. Everything here derives from `world-online.jsonld`.


## Balance Sheet

- **Structure**: rs-gaap — Balance Sheet — Classified
- **Information Block**: `b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d`
- **FactSet**: `fs_01KWRGZE5K6GF3BDBE9S2SNFWB`

| QName | Concept | 2018-12-31 → 2028-12-31 |
|---|---|---:|
| `rs-gaap:CashAndCashEquivalentsAtCarryingValue` |     Cash and Cash Equivalents, at Carrying Value | $(648,551.94) |
| `rs-gaap:ReceivablesNetCurrent` |     Receivables, Net, Current | $2,035,468.27 |
| `rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings` |     Inventory, Net of Allowances, Customer Advances and Progress Billings | $451,842.19 |
| `rs-gaap:AssetsCurrent` |   **Assets, Current** | $1,838,758.52 |
| `rs-gaap:PropertyPlantAndEquipmentNet` |     Property, Plant and Equipment, Net | $1,245,567.16 |
| `rs-gaap:AssetsNoncurrent` |   **Assets, Noncurrent** | $1,245,567.16 |
| `rs-gaap:Assets` | **Assets** | $3,084,325.68 |
| `rs-gaap:AccountsPayableCurrent` |       Accounts Payable, Current | $2,689,452.31 |
| `rs-gaap:LiabilitiesCurrent` |     **Liabilities, Current** | $2,689,452.31 |
| `rs-gaap:LongTermDebtAndCapitalLeaseObligations` |       Long-Term Debt and Lease Obligation | $338,349.05 |
| `rs-gaap:LiabilitiesNoncurrent` |     **Liabilities, Noncurrent** | $338,349.05 |
| `rs-gaap:Liabilities` |   **Liabilities** | $3,027,801.36 |
| `rs-gaap:AdditionalPaidInCapital` |     Additional Paid in Capital | $1,407,646.64 |
| `rs-gaap:RetainedEarningsAccumulatedDeficit` |     Retained Earnings (Accumulated Deficit) | $(1,351,122.32) |
| `rs-gaap:StockholdersEquity` |   **Stockholders' Equity Attributable to Parent** | $56,524.32 |
| `rs-gaap:LiabilitiesAndStockholdersEquity` | **Liabilities and Equity** | $3,084,325.68 |

<details>
<summary>▸ Balance Sheet — scene RDF / Turtle (500 triples · 31.6 KB)</summary>

```turtle {#balance_sheet}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQE" ;
    rs:numericValue 2689452.3100000005 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQF" ;
    rs:numericValue 1407646.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQG" ;
    rs:numericValue -648551.94 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQP" ;
    rs:numericValue 451842.18999999994 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LongTermDebtAndCapitalLeaseObligations ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQQ" ;
    rs:numericValue 338349.05 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQR" ;
    rs:numericValue 1245567.1600000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQS" ;
    rs:numericValue 2035468.27 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQV" ;
    rs:numericValue -1351122.3200000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQW" ;
    rs:numericValue 1595349.42 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQX" ;
    rs:numericValue 1407646.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQY" ;
    rs:numericValue 398937.76 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQZ" ;
    rs:numericValue 467010.2 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DR0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LongTermDebtAndCapitalLeaseObligations ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DR0" ;
    rs:numericValue 361285.69 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DR1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DR1" ;
    rs:numericValue 1266995.3199999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DR2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DR2" ;
    rs:numericValue 1231338.4700000002 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DR3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DR3" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRA" ;
    rs:numericValue 2689452.3100000005 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRG" ;
    rs:numericValue 3027801.3600000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRH" ;
    rs:numericValue 3084325.6799999997 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRJ" ;
    rs:numericValue 1245567.1600000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRN" ;
    rs:numericValue 3084325.68 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRQ" ;
    rs:numericValue 56524.3199999996 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRR" ;
    rs:numericValue 1838758.52 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRV" ;
    rs:numericValue 338349.05 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRW" ;
    rs:numericValue 1595349.42 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRX" ;
    rs:numericValue 1956635.1099999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRY" ;
    rs:numericValue 3364281.75 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRZ" ;
    rs:numericValue 1266995.3199999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DS0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DS0" ;
    rs:numericValue 3364281.75 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DS2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DS2" ;
    rs:numericValue 1407646.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DS3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DS3" ;
    rs:numericValue 2097286.43 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DS4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DS4" ;
    rs:numericValue 361285.69 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/ib/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Balance Sheet — Classified" ;
    rs:blockType "balance_sheet" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWB> ;
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

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> a rs:Period ;
    xbrli:instant "2028-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> a rs:Period ;
    xbrli:instant "2023-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> a rs:Entity ;
    skos:prefLabel "The World Online (Charlie Hoffman demo)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f310eb3b8272a718bd" ;
    rs:legalName "The World Online (Charlie Hoffman demo)" .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Income Statement

- **Structure**: rs-gaap — Income Statement — Multi-step
- **Information Block**: `47cd6544-03d1-5bc1-8c28-31c0cfa450f9`
- **FactSet**: `fs_01KWRGZE5K6GF3BDBE9S2SNFWC`

| QName | Concept | 2018-12-31 → 2028-12-31 |
|---|---|---:|
| `rs-gaap:Revenues` |   **Revenues** | $2,604,048.36 |
| `rs-gaap:CostOfGoodsAndServicesSold` |     Cost of Product and Service Sold | $886,041.18 |
| `rs-gaap:CostOfRevenue` |   **Cost of Revenue** | $886,041.18 |
| `rs-gaap:GrossProfit` |   **Gross Profit** | $1,718,007.18 |
| `rs-gaap:GeneralAndAdministrativeExpense` |     General and Administrative Expense | $3,049,867.27 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | $21,428.16 |
| `rs-gaap:OperatingExpenses` |   **Operating Expenses** | $3,071,295.43 |
| `rs-gaap:OperatingIncomeLoss` |   **Operating Income (Loss)** | $(1,353,288.25) |
| `rs-gaap:InterestExpense` |     Interest Expense, Operating and Nonoperating | $(2,165.93) |
| `rs-gaap:NonoperatingIncomeExpense` |   **Nonoperating Income (Expense)** | $2,165.93 |
| `rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` |   **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | $(1,351,122.32) |
| `rs-gaap:IncomeLossFromContinuingOperations` |   **Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent** | $(1,351,122.32) |
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | $(1,351,122.32) |

<details>
<summary>▸ Income Statement — scene RDF / Turtle (278 triples · 16.1 KB)</summary>

```turtle {#income_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQH" ;
    rs:numericValue 886041.1799999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQJ" ;
    rs:numericValue 21428.16 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQM" ;
    rs:numericValue 3049867.27 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InterestExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQN" ;
    rs:numericValue -2165.929999999993 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQT" ;
    rs:numericValue 2604048.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DR4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DR4" ;
    rs:numericValue -1351122.3199999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRB" ;
    rs:numericValue -1351122.3200000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRC" ;
    rs:numericValue -1353288.2500000002 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRD" ;
    rs:numericValue -1351122.3200000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRE" ;
    rs:numericValue 3071295.43 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NonoperatingIncomeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRF" ;
    rs:numericValue 2165.929999999993 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRM" ;
    rs:numericValue 886041.1799999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRS" ;
    rs:numericValue 1718007.18 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/ib/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Income Statement — Multi-step" ;
    rs:blockType "income_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWC> ;
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

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> a rs:Entity ;
    skos:prefLabel "The World Online (Charlie Hoffman demo)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f310eb3b8272a718bd" ;
    rs:legalName "The World Online (Charlie Hoffman demo)" .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> a rs:Period ;
    xbrli:endDate "2028-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Cash Flow Statement

- **Structure**: rs-gaap — Cash Flow Statement — Indirect
- **Information Block**: `5473639a-2dac-56a6-b9e5-38480ea38bc1`
- **FactSet**: `fs_01KWRGZE5K6GF3BDBE9S2SNFWD`

| QName | Concept | 2018-12-31 → 2028-12-31 |
|---|---|---:|
| `rs-gaap:NetIncomeLoss` |     **Net Income (Loss) Attributable to Parent** | $(1,351,122.32) |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | $21,428.16 |
| `rs-gaap:IncreaseDecreaseInAccountsReceivable` |     Increase (Decrease) in Accounts Receivable | $(804,129.80) |
| `rs-gaap:IncreaseDecreaseInInventories` |     Increase (Decrease) in Inventories | $15,168.01 |
| `rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet` |     Increase (Decrease) in Other Operating Assets and Liabilities, Net | $1,071,166.25 |
| `rs-gaap:NetCashProvidedByUsedInOperatingActivities` |   Cash Provided by (Used in) Operating Activity, Including Discontinued Operation | $(1,047,489.70) |
| `rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease` | **Cash and Cash Equivalents, Period Increase (Decrease)** | $(1,047,489.70) |

<details>
<summary>▸ Cash Flow Statement — scene RDF / Turtle (158 triples · 9.6 KB)</summary>

```turtle {#cash_flow_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DQK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWD> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DQK" ;
    rs:numericValue 21428.16 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DR6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWD> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DR6" ;
    rs:numericValue -1351122.3199999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DR7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInInventories ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWD> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DR7" ;
    rs:numericValue 15168.010000000068 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DR8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInAccountsReceivable ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWD> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DR8" ;
    rs:numericValue -804129.7999999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DR9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWD> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DR9" ;
    rs:numericValue 1071166.2499999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWD> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRK" ;
    rs:numericValue -1047489.6999999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWD> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRT" ;
    rs:numericValue -1047489.6999999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/ib/5473639a-2dac-56a6-b9e5-38480ea38bc1> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Cash Flow Statement — Indirect" ;
    rs:blockType "cash_flow_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWD> ;
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

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> a rs:Entity ;
    skos:prefLabel "The World Online (Charlie Hoffman demo)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f310eb3b8272a718bd" ;
    rs:legalName "The World Online (Charlie Hoffman demo)" .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> a rs:Period ;
    xbrli:endDate "2028-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Statement of Changes in Equity

- **Structure**: rs-gaap — Statement of Changes in Equity — Roll Forward (Total)
- **Information Block**: `0b179e5c-5f02-506d-b8d5-860cb10c7694`
- **FactSet**: `fs_01KWRGZE5K6GF3BDBE9S2SNFWE`

| QName | Concept | 2018-12-31 → 2028-12-31 |
|---|---|---:|
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | $(1,351,122.32) |
| `rs-gaap:StockholdersEquity` | **Stockholders' Equity Attributable to Parent** | $56,524.32 |

<details>
<summary>▸ Statement of Changes in Equity — scene RDF / Turtle (74 triples · 4.7 KB)</summary>

```turtle {#equity_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DR5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWE> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DR5" ;
    rs:numericValue -1351122.3199999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DRP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWE> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DRP" ;
    rs:numericValue 56524.3199999996 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/fact/fact_01KWRGZE5QWCSVRT20DRKD0DS1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWE> ;
    rs:internalId "fact_01KWRGZE5QWCSVRT20DRKD0DS1" ;
    rs:numericValue 1407646.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/ib/0b179e5c-5f02-506d-b8d5-860cb10c7694> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)" ;
    rs:blockType "equity_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGZE5K6GF3BDBE9S2SNFWE> ;
    rs:internalId "0b179e5c-5f02-506d-b8d5-860cb10c7694" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_1> a rs:Period ;
    xbrli:instant "2028-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_2> a rs:Period ;
    xbrli:endDate "2028-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/period/p_3> a rs:Period ;
    xbrli:instant "2023-12-31"^^xsd:date ;
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

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/entity/entity_kg19f310eb3b8272a718bd> a rs:Entity ;
    skos:prefLabel "The World Online (Charlie Hoffman demo)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f310eb3b8272a718bd" ;
    rs:legalName "The World Online (Charlie Hoffman demo)" .

<https://robosystems.ai/report/rpt_01KWRGZE0Q574X3Z17TK0T7F69/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Validation evidence

Independent, standards-grade checks of the same bundle this DataBook renders — embedded so the artifact travels with its own proof.

### World Online — SHACL Ontology Conformance

#### Result: ✅ **Conforms to RoboSystems RDF Ontology v1**

- **Bundle**: `world-online.jsonld`
- **Graph triples**: 3,204
- **rs:Fact nodes**: 55
- **rs:Association nodes**: 162
- **rs:Element nodes**: 93
- **SHACL shapes checked**: 8 (positive instance shapes + negative shapes banning the retired dialects)

Validated on the host with **pyshacl** against `frameworks/ontology/v1/shapes.ttl` — the *same* shapes that gate the framework seeds and the publish-time bundle validation, run here directly on the on-disk artifact (no API, no database, no container). Conformance means every `rs:Fact` references its aspects directly (`rs:element`/`rs:entity`/`rs:period`/`rs:unit` — no XBRL `context`), every `rs:Association` carries `xlink:from`/`to` + `xlink:arcrole`, and none of the retired dialects (`xbrli:contextRef`, `arcFrom`, direct `summationOf`) appear.

#### Violations

_None._ Zero violations.

### World Online — XBRL 2.1 Validation (Arelle)

#### Result: ✅ **Valid XBRL 2.1**

- **Package**: `world-online.zip` (13,834 bytes)
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-lab.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 50
- **Load errors**: 0
- **Validation errors**: 0

Validated on the host with **Arelle** (the de-facto XBRL processor, also used by SEC EDGAR) directly against the on-disk report package — no API, no container. Zero load + validation errors is the structural-correctness claim: the output is valid XBRL 2.1, consumable by any standards-compliant processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system checks are not enabled (the instance isn't an SEC filing).

#### Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
