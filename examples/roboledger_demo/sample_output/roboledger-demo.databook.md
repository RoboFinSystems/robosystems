---
id: https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746
type: DataBook
title: "RoboLedger Demo — Cascade Advisory Group LLC"
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
  source: "Cascade Advisory Group LLC"
  method: "Materialized RoboSystems Report rpt_01KWRGHPDMGA1FKQCRD6TZP746 (generation 1, draft)"
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
  facts: 41
  href: roboledger-demo.holon.jsonld
  graphs:
    - id: scene
      iri: https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746#scene
      description: "Instance facts — the values this report reports"
      disposition: inline
    - id: boundary
      iri: https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746#boundary
      description: "Calculation network — the rollup rules the facts must obey"
      disposition: reference
      derived_from: rs-gaap-calculations@v1
    - id: projection
      iri: https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746#projection
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
  report_id: rpt_01KWRGHPDMGA1FKQCRD6TZP746
  generation_count: 1
  filing_status: draft
  periods:
    - { label: "2024-01-02 → 2025-12-31", start: 2024-01-02, end: 2025-12-31 }
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

# RoboLedger Demo — Cascade Advisory Group LLC

A report **is** a collection of Information Blocks, and this DataBook is a projection of one report holon (see the `graph:` map above). The **scene** graph — the facts — renders twice per block here: a markdown table (human view) and a foldable, addressable `turtle` slice (machine view, the same facts as RDF). The **boundary** (calculation) and **projection** (presentation) graphs live as real named graphs in the companion `roboledger-demo.holon.jsonld` — dataset-form JSON-LD, the API-native holon — and derive from their versioned framework, referenced here rather than inlined since they're shared by every report on that framework. The **lineage** graph — the ledger behind the facts — is internal and not published: a report is an aggregation of the books, not the books. The `Validation evidence` section is the published substantiation that the referenced rules hold. Everything here derives from `roboledger-demo.jsonld`.


## Balance Sheet

- **Structure**: rs-gaap — Balance Sheet — Classified
- **Information Block**: `b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d`
- **FactSet**: `fs_01KWRGHPGB8KDN5PR9ZK9ZC04W`

| QName | Concept | 2024-01-02 → 2025-12-31 |
|---|---|---:|
| `rs-gaap:CashAndCashEquivalentsAtCarryingValue` |     Cash and Cash Equivalents, at Carrying Value | $57,240.00 |
| `rs-gaap:ReceivablesNetCurrent` |     Receivables, Net, Current | $0.00 |
| `rs-gaap:PrepaidExpenseCurrent` |     Prepaid Expense, Current | $2,100.00 |
| `rs-gaap:AssetsCurrent` |   **Assets, Current** | $59,340.00 |
| `rs-gaap:PropertyPlantAndEquipmentNet` |     Property, Plant and Equipment, Net | $5,083.36 |
| `rs-gaap:AssetsNoncurrent` |   **Assets, Noncurrent** | $5,083.36 |
| `rs-gaap:Assets` | **Assets** | $64,423.36 |
| `rs-gaap:AccountsPayableCurrent` |       Accounts Payable, Current | $0.00 |
| `rs-gaap:AccruedLiabilitiesCurrent` |       Accrued Liabilities, Current | $800.00 |
| `rs-gaap:LiabilitiesCurrent` |     **Liabilities, Current** | $800.00 |
| `rs-gaap:Liabilities` |   **Liabilities** | $800.00 |
| `rs-gaap:AdditionalPaidInCapital` |     Additional Paid in Capital | $49,800.00 |
| `rs-gaap:RetainedEarningsAccumulatedDeficit` |     Retained Earnings (Accumulated Deficit) | $13,823.36 |
| `rs-gaap:StockholdersEquity` |   **Stockholders' Equity Attributable to Parent** | $63,623.36 |
| `rs-gaap:LiabilitiesAndStockholdersEquity` | **Liabilities and Equity** | $64,423.36 |

<details>
<summary>▸ Balance Sheet — scene RDF / Turtle (340 triples · 19.6 KB)</summary>

```turtle {#balance_sheet}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99530> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99530" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99531> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccruedLiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99531" ;
    rs:numericValue 800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99532> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99532" ;
    rs:numericValue 49800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99533> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99533" ;
    rs:numericValue 57240.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99537> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99537" ;
    rs:numericValue 2100.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99538> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99538" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953A> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953A" ;
    rs:numericValue 13823.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953B> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953B" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953C> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953C" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953G> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953G" ;
    rs:numericValue 5083.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953Q> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953Q" ;
    rs:numericValue 800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953W> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953W" ;
    rs:numericValue 800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953X> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953X" ;
    rs:numericValue 64423.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953Y> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953Y" ;
    rs:numericValue 5083.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99541> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99541" ;
    rs:numericValue 64423.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99543> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99543" ;
    rs:numericValue 63623.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99544> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99544" ;
    rs:numericValue 59340.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/ib/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Balance Sheet — Classified" ;
    rs:blockType "balance_sheet" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04W> ;
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

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_2> a rs:Period ;
    xbrli:instant "2024-12-31"^^xsd:date ;
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

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> a rs:Period ;
    xbrli:instant "2025-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> a rs:Entity ;
    skos:prefLabel "Cascade Advisory Group LLC" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f3108471c06fdeb55e" ;
    rs:legalName "Cascade Advisory Group LLC" .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Income Statement

- **Structure**: rs-gaap — Income Statement — Multi-step
- **Information Block**: `47cd6544-03d1-5bc1-8c28-31c0cfa450f9`
- **FactSet**: `fs_01KWRGHPGB8KDN5PR9ZK9ZC04X`

| QName | Concept | 2024-01-02 → 2025-12-31 |
|---|---|---:|
| `rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` |     Revenue from Contract with Customer, Excluding Assessed Tax | $133,500.00 |
| `rs-gaap:Revenues` |   **Revenues** | $133,500.00 |
| `rs-gaap:GrossProfit` |   **Gross Profit** | $133,500.00 |
| `rs-gaap:GeneralAndAdministrativeExpense` |     General and Administrative Expense | $118,460.00 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | $1,216.64 |
| `rs-gaap:OperatingExpenses` |   **Operating Expenses** | $119,676.64 |
| `rs-gaap:OperatingIncomeLoss` |   **Operating Income (Loss)** | $13,823.36 |
| `rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` |   **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | $13,823.36 |
| `rs-gaap:IncomeLossFromContinuingOperations` |   **Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent** | $13,823.36 |
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | $13,823.36 |

<details>
<summary>▸ Income Statement — scene RDF / Turtle (218 triples · 12.8 KB)</summary>

```turtle {#income_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99534> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99534" ;
    rs:numericValue 1216.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99536> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99536" ;
    rs:numericValue 118460.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99539> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99539" ;
    rs:numericValue 133500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953D> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953D" ;
    rs:numericValue 13823.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953R> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953R" ;
    rs:numericValue 13823.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953S> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953S" ;
    rs:numericValue 13823.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953T> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953T" ;
    rs:numericValue 13823.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953V> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953V" ;
    rs:numericValue 119676.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99545> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99545" ;
    rs:numericValue 133500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99548> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99548" ;
    rs:numericValue 133500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/ib/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Income Statement — Multi-step" ;
    rs:blockType "income_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04X> ;
    rs:internalId "47cd6544-03d1-5bc1-8c28-31c0cfa450f9" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

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

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> a rs:Entity ;
    skos:prefLabel "Cascade Advisory Group LLC" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f3108471c06fdeb55e" ;
    rs:legalName "Cascade Advisory Group LLC" .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> a rs:Period ;
    xbrli:endDate "2025-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Cash Flow Statement

- **Structure**: rs-gaap — Cash Flow Statement — Indirect
- **Information Block**: `5473639a-2dac-56a6-b9e5-38480ea38bc1`
- **FactSet**: `fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y`

| QName | Concept | 2024-01-02 → 2025-12-31 |
|---|---|---:|
| `rs-gaap:NetIncomeLoss` |     **Net Income (Loss) Attributable to Parent** | $13,823.36 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation, Depletion and Amortization | $1,216.64 |
| `rs-gaap:IncreaseDecreaseInPrepaidExpense` |     Increase (Decrease) in Prepaid Expense | $(2,100.00) |
| `rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet` |     Increase (Decrease) in Other Operating Assets and Liabilities, Net | $(1,500.00) |
| `rs-gaap:IncreaseDecreaseInAccruedLiabilities` |     Increase (Decrease) in Accrued Liabilities | $800.00 |
| `rs-gaap:NetCashProvidedByUsedInOperatingActivities` |   Cash Provided by (Used in) Operating Activity, Including Discontinued Operation | $12,240.00 |
| `rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment` |     Payments to Acquire Property, Plant, and Equipment | $(4,800.00) |
| `rs-gaap:NetCashProvidedByUsedInInvestingActivities` |   Cash Provided by (Used in) Investing Activity, Including Discontinued Operation | $(4,800.00) |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |     Proceeds from Issuance of Common Stock | $49,800.00 |
| `rs-gaap:NetCashProvidedByUsedInFinancingActivities` |   Cash Provided by (Used in) Financing Activity, Including Discontinued Operation | $49,800.00 |
| `rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease` | **Cash and Cash Equivalents, Period Increase (Decrease)** | $57,240.00 |

<details>
<summary>▸ Cash Flow Statement — scene RDF / Turtle (238 triples · 14.2 KB)</summary>

```turtle {#cash_flow_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99535> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99535" ;
    rs:numericValue 1216.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953F> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953F" ;
    rs:numericValue 13823.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953H> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953H" ;
    rs:numericValue 49800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953K> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953K" ;
    rs:numericValue -4800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953M> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInPrepaidExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953M" ;
    rs:numericValue -2100.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953N> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInAccruedLiabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953N" ;
    rs:numericValue 800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953P> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953P" ;
    rs:numericValue -1500.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953Z> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953Z" ;
    rs:numericValue 12240.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99540> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99540" ;
    rs:numericValue -4800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99546> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInFinancingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99546" ;
    rs:numericValue 49800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99547> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99547" ;
    rs:numericValue 57240.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/ib/5473639a-2dac-56a6-b9e5-38480ea38bc1> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Cash Flow Statement — Indirect" ;
    rs:blockType "cash_flow_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Y> ;
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

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> a rs:Entity ;
    skos:prefLabel "Cascade Advisory Group LLC" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f3108471c06fdeb55e" ;
    rs:legalName "Cascade Advisory Group LLC" .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> a rs:Period ;
    xbrli:endDate "2025-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Statement of Changes in Equity

- **Structure**: rs-gaap — Statement of Changes in Equity — Roll Forward (Total)
- **Information Block**: `0b179e5c-5f02-506d-b8d5-860cb10c7694`
- **FactSet**: `fs_01KWRGHPGB8KDN5PR9ZK9ZC04Z`

| QName | Concept | 2024-01-02 → 2025-12-31 |
|---|---|---:|
| `rs-gaap:NetIncomeLoss` |   **Net Income (Loss) Attributable to Parent** | $13,823.36 |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |   Proceeds from Issuance of Common Stock | $49,800.00 |
| `rs-gaap:StockholdersEquity` | **Stockholders' Equity Attributable to Parent** | $63,623.36 |

<details>
<summary>▸ Statement of Changes in Equity — scene RDF / Turtle (81 triples · 4.9 KB)</summary>

```turtle {#equity_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953E> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Z> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953E" ;
    rs:numericValue 13823.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ9953J> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Z> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ9953J" ;
    rs:numericValue 49800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/fact/fact_01KWRGHPGJVHKKY13KEEJ99542> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Z> ;
    rs:internalId "fact_01KWRGHPGJVHKKY13KEEJ99542" ;
    rs:numericValue 63623.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/ib/0b179e5c-5f02-506d-b8d5-860cb10c7694> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)" ;
    rs:blockType "equity_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KWRGHPGB8KDN5PR9ZK9ZC04Z> ;
    rs:internalId "0b179e5c-5f02-506d-b8d5-860cb10c7694" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_1> a rs:Period ;
    xbrli:instant "2025-12-31"^^xsd:date ;
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

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/period/p_3> a rs:Period ;
    xbrli:endDate "2025-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/entity/entity_kg19f3108471c06fdeb55e> a rs:Entity ;
    skos:prefLabel "Cascade Advisory Group LLC" ;
    rs:country "US" ;
    rs:internalId "entity_kg19f3108471c06fdeb55e" ;
    rs:legalName "Cascade Advisory Group LLC" .

<https://robosystems.ai/report/rpt_01KWRGHPDMGA1FKQCRD6TZP746/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```

</details>


## Validation evidence

Independent, standards-grade checks of the same bundle this DataBook renders — embedded so the artifact travels with its own proof.

### RoboLedger — SHACL Ontology Conformance

#### Result: ✅ **Conforms to RoboSystems RDF Ontology v1**

- **Bundle**: `roboledger-demo.jsonld`
- **Graph triples**: 3,050
- **rs:Fact nodes**: 41
- **rs:Association nodes**: 162
- **rs:Element nodes**: 93
- **SHACL shapes checked**: 8 (positive instance shapes + negative shapes banning the retired dialects)

Validated on the host with **pyshacl** against `frameworks/ontology/v1/shapes.ttl` — the *same* shapes that gate the framework seeds and the publish-time bundle validation, run here directly on the on-disk artifact (no API, no database, no container). Conformance means every `rs:Fact` references its aspects directly (`rs:element`/`rs:entity`/`rs:period`/`rs:unit` — no XBRL `context`), every `rs:Association` carries `xlink:from`/`to` + `xlink:arcrole`, and none of the retired dialects (`xbrli:contextRef`, `arcFrom`, direct `summationOf`) appear.

#### Violations

_None._ Zero violations.

### RoboLedger — XBRL 2.1 Validation (Arelle)

#### Result: ✅ **Valid XBRL 2.1**

- **Package**: `roboledger-demo.zip` (13,480 bytes)
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-lab.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 36
- **Load errors**: 0
- **Validation errors**: 0

Validated on the host with **Arelle** (the de-facto XBRL processor, also used by SEC EDGAR) directly against the on-disk report package — no API, no container. Zero load + validation errors is the structural-correctness claim: the output is valid XBRL 2.1, consumable by any standards-compliant processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system checks are not enabled (the instance isn't an SEC filing).

#### Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
