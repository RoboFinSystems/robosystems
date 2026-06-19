---
id: https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2
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
  method: "Materialized RoboSystems Report rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2 (generation 1, draft)"
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
report:
  reporting_style: 025f5d48-12ce-5d65-b9eb-4f137a10ef06
  report_id: rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2
  generation_count: 1
  filing_status: draft
  periods:
    - { label: "2024-01-02 → 2025-12-31", start: 2024-01-02, end: 2025-12-31 }
  framework_pins:
    - { framework: fac-traits, version: v1 }
    - { framework: fac, version: v1 }
    - { framework: fac-presentation, version: v1 }
    - { framework: fac-calculations, version: v1 }
    - { framework: fac-rules, version: v1 }
    - { framework: rs-gaap, version: v1 }
    - { framework: rs-gaap-traits, version: v1 }
    - { framework: rs-gaap-hierarchy, version: v1 }
    - { framework: rs-gaap-presentation, version: v1 }
    - { framework: rs-gaap-calculations, version: v1 }
    - { framework: rs-gaap-type-subtype, version: v1 }
    - { framework: rs-gaap-disclosures, version: v1 }
    - { framework: rs-gaap-disclosure-mechanics, version: v1 }
    - { framework: rs-gaap-reporting-checklist, version: v1 }
    - { framework: rs-gaap-reporting-styles, version: v1 }
    - { framework: rs-gaap-rollup-rules, version: v1 }
    - { framework: fac-to-rs-gaap, version: v1 }
    - { framework: rs-gaap-disclosures-to-rs-gaap-textblocks, version: v1 }
---

# RoboLedger Demo — Cascade Advisory Group LLC

A report **is** a collection of Information Blocks. Each block below is shown twice: a markdown table (human view) and an addressable `turtle` block (machine view — the same facts as RDF), keyed by the id declared in the frontmatter `manifest`. Everything is derived from `roboledger-demo.jsonld`; the bundle and this DataBook are two skins of one graph.


## Balance Sheet

- **Structure**: rs-gaap — Balance Sheet — Classified
- **Information Block**: `b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d`
- **FactSet**: `fs_01KSV8AYSKTWGGB1B7WWF5R33M`

| QName | Concept | 2024-01-02 → 2025-12-31 |
|---|---|---:|
| `rs-gaap:CashCashEquivalentsAndShortTermInvestments` |     Cash Cash Equivalents And Short Term Investments | $72,120.00 |
| `rs-gaap:ReceivablesNetCurrent` |     Receivables Net Current | $0.00 |
| `rs-gaap:PrepaidExpenseCurrent` |     Prepaid Expense Current | $2,100.00 |
| `rs-gaap:AssetsCurrent` |   **Assets Current** | $74,220.00 |
| `rs-gaap:PropertyPlantAndEquipmentNet` |     Property Plant And Equipment Net | $4,766.70 |
| `rs-gaap:AssetsNoncurrent` |   **Assets Noncurrent** | $4,766.70 |
| `rs-gaap:Assets` | **Assets** | $78,986.70 |
| `rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent` |       Accounts Payable And Accrued Liabilities Current | $800.00 |
| `rs-gaap:LiabilitiesCurrent` |     **Liabilities Current** | $800.00 |
| `rs-gaap:Liabilities` |   **Liabilities** | $800.00 |
| `rs-gaap:AdditionalPaidInCapital` |     Additional Paid In Capital | $49,800.00 |
| `rs-gaap:RetainedEarningsAccumulatedDeficit` |     Retained Earnings Accumulated Deficit | $28,386.70 |
| `rs-gaap:StockholdersEquity` |   **Stockholders Equity** | $78,186.70 |
| `rs-gaap:LiabilitiesAndStockholdersEquity` | **Liabilities And Stockholders Equity** | $78,986.70 |

```turtle {#balance_sheet}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM0Y> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM0Y" ;
    rs:numericValue 800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM0Z> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM0Z" ;
    rs:numericValue 49800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM10> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashCashEquivalentsAndShortTermInvestments ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM10" ;
    rs:numericValue 72120.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM13> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM13" ;
    rs:numericValue 2100.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM14> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM14" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM17> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM17" ;
    rs:numericValue 28386.70000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM18> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM18" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM19> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM19" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1D> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1D" ;
    rs:numericValue 4766.7 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1M> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1M" ;
    rs:numericValue 78986.70000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1P> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1P" ;
    rs:numericValue 78986.7 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1S> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1S" ;
    rs:numericValue 74220.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1W> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1W" ;
    rs:numericValue 800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1Y> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1Y" ;
    rs:numericValue 4766.7 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1Z> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1Z" ;
    rs:numericValue 800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM22> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM22" ;
    rs:numericValue 78186.70000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/ib/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Balance Sheet — Classified" ;
    rs:blockType "balance_sheet" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33M> ;
    rs:internalId "b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "7c177cde-7755-53f3-8512-e60b35a5b5c6" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:Assets a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "a1f04756-41d8-5d35-b821-23aa2f3b2fae" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:AssetsCurrent a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "0fc9ab7e-c5ce-5277-9530-344cc127fe26" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:AssetsNoncurrent a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "841cedeb-4cb0-532a-b0bd-c34846a13a8c" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:CashCashEquivalentsAndShortTermInvestments a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "319dac01-1574-50b7-9124-959febd6c28e" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:Liabilities a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "7af273ac-1cba-5fb3-a1c9-5c5d8fdb9bdf" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:LiabilitiesAndStockholdersEquity a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "30b2801e-e682-5298-82e6-3670e1d508f1" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:LiabilitiesCurrent a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "efb036ff-3f30-5deb-bee9-1af4cd4b9800" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:PrepaidExpenseCurrent a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "2225e348-90bd-53cb-9784-8b5d54980a69" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:PropertyPlantAndEquipmentNet a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "288099af-5cbb-5f78-8f8a-1a85675fb661" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:ReceivablesNetCurrent a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "44686df3-3871-5c1f-8a08-fc542d69dfa0" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:StockholdersEquity a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "e3796201-9899-5b7b-9477-659550ba8e68" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_2> a rs:Period ;
    xbrli:instant "2024-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

rs-gaap:AdditionalPaidInCapital a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "6146605c-0d63-51e1-a523-3450d6abaca3" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:RetainedEarningsAccumulatedDeficit a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "a9c87d60-a1e5-506b-a27e-cbf9e14e5113" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> a rs:Period ;
    xbrli:instant "2025-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> a rs:Entity ;
    skos:prefLabel "Cascade Advisory Group LLC" ;
    rs:country "US" ;
    rs:internalId "entity_kg19e75ce934a3a00a17ea" ;
    rs:legalName "Cascade Advisory Group LLC" .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```


## Income Statement

- **Structure**: rs-gaap — Income Statement — Multi-step
- **Information Block**: `47cd6544-03d1-5bc1-8c28-31c0cfa450f9`
- **FactSet**: `fs_01KSV8AYSKTWGGB1B7WWF5R33N`

| QName | Concept | 2024-01-02 → 2025-12-31 |
|---|---|---:|
| `rs-gaap:SalesRevenueNet` |     Sales Revenue Net | $175,000.00 |
| `rs-gaap:Revenues` |   **Revenues** | $175,000.00 |
| `rs-gaap:GrossProfit` |   **Gross Profit** | $175,000.00 |
| `rs-gaap:SellingGeneralAndAdministrativeExpense` |     Selling General And Administrative Expense | $145,080.00 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation Depletion And Amortization | $1,533.30 |
| `rs-gaap:OperatingExpenses` |   **Operating Expenses** | $146,613.30 |
| `rs-gaap:OperatingIncomeLoss` |   **Operating Income Loss** | $28,386.70 |
| `rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` |   **Income Loss From Continuing Operations Before Income Taxes Extraordinary Items Noncontrolling Interest** | $28,386.70 |
| `rs-gaap:IncomeLossFromContinuingOperations` |   **Income Loss From Continuing Operations** | $28,386.70 |
| `rs-gaap:NetIncomeLoss` |   **Net Income Loss** | $28,386.70 |

```turtle {#income_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM11> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM11" ;
    rs:numericValue 1533.3 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM15> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SalesRevenueNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM15" ;
    rs:numericValue 175000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM16> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SellingGeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM16" ;
    rs:numericValue 145080.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1A> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1A" ;
    rs:numericValue 28386.70000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1K> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1K" ;
    rs:numericValue 146613.3 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1Q> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1Q" ;
    rs:numericValue 175000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1R> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1R" ;
    rs:numericValue 28386.70000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1V> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1V" ;
    rs:numericValue 175000.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM23> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM23" ;
    rs:numericValue 28386.70000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM24> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM24" ;
    rs:numericValue 28386.70000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/ib/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Income Statement — Multi-step" ;
    rs:blockType "income_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33N> ;
    rs:internalId "47cd6544-03d1-5bc1-8c28-31c0cfa450f9" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

rs-gaap:DepreciationDepletionAndAmortization a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "189a099a-7512-5144-9215-65d837c2c3b5" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:GrossProfit a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "a92b3181-9fe7-543c-81d9-13ebd12bbefa" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:IncomeLossFromContinuingOperations a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "d60cabda-7060-5aff-ac98-96371606a738" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "6b0b414f-0c76-54f0-8e51-cf53db59ca24" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:NetIncomeLoss a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "27a05717-2370-51c2-a924-db5cbcb48219" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:OperatingExpenses a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "71fcdebb-7145-5f76-b5e0-b4ccbf2c29d2" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:OperatingIncomeLoss a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "16780828-0201-5609-b572-fbe3ebfcb177" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:Revenues a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "b26a6cd4-072f-5bf2-b5d3-ebf928150d6c" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:SalesRevenueNet a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "7c122782-e96d-5288-93a6-1f42089d701d" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:SellingGeneralAndAdministrativeExpense a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "050fac09-f306-514d-80ef-f0d10ce05de9" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> a rs:Entity ;
    skos:prefLabel "Cascade Advisory Group LLC" ;
    rs:country "US" ;
    rs:internalId "entity_kg19e75ce934a3a00a17ea" ;
    rs:legalName "Cascade Advisory Group LLC" .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> a rs:Period ;
    xbrli:endDate "2025-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```


## Cash Flow Statement

- **Structure**: rs-gaap — Cash Flow Statement — Indirect
- **Information Block**: `5473639a-2dac-56a6-b9e5-38480ea38bc1`
- **FactSet**: `fs_01KSV8AYSKTWGGB1B7WWF5R33P`

| QName | Concept | 2024-01-02 → 2025-12-31 |
|---|---|---:|
| `rs-gaap:NetIncomeLoss` |     **Net Income Loss** | $28,386.70 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation Depletion And Amortization | $1,533.30 |
| `rs-gaap:IncreaseDecreaseInPrepaidExpense` |     Increase Decrease In Prepaid Expense | $(2,100.00) |
| `rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities` |     Increase Decrease In Accounts Payable And Accrued Liabilities | $800.00 |
| `rs-gaap:NetCashProvidedByUsedInOperatingActivities` |   Net Cash Provided By Used In Operating Activities | $28,620.00 |
| `rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment` |     Payments To Acquire Property Plant And Equipment | $(4,800.00) |
| `rs-gaap:NetCashProvidedByUsedInInvestingActivities` |   Net Cash Provided By Used In Investing Activities | $(4,800.00) |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |     Proceeds From Issuance Of Common Stock | $49,800.00 |
| `rs-gaap:NetCashProvidedByUsedInFinancingActivities` |   Net Cash Provided By Used In Financing Activities | $49,800.00 |
| `rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease` | **Cash And Cash Equivalents Period Increase Decrease** | $73,620.00 |

```turtle {#cash_flow_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM12> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM12" ;
    rs:numericValue 1533.3 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1C> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1C" ;
    rs:numericValue 28386.70000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1E> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1E" ;
    rs:numericValue 49800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1G> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1G" ;
    rs:numericValue -4800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1H> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1H" ;
    rs:numericValue 800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1J> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInPrepaidExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1J" ;
    rs:numericValue -2100.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1N> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1N" ;
    rs:numericValue 28620.00000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1T> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1T" ;
    rs:numericValue -4800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1X> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInFinancingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1X" ;
    rs:numericValue 49800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM20> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM20" ;
    rs:numericValue 73620.00000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/ib/5473639a-2dac-56a6-b9e5-38480ea38bc1> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Cash Flow Statement — Indirect" ;
    rs:blockType "cash_flow_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33P> ;
    rs:internalId "5473639a-2dac-56a6-b9e5-38480ea38bc1" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "353f790f-1ed1-5b91-880d-8029b4b687cf" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:DepreciationDepletionAndAmortization a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "189a099a-7512-5144-9215-65d837c2c3b5" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "dc7408c9-cba5-5697-8254-32ac46485214" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:IncreaseDecreaseInPrepaidExpense a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "550bb6e5-53d0-5267-adb1-baf78093a0b0" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:NetCashProvidedByUsedInFinancingActivities a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "811f1cf5-836c-575f-9f3f-cd7fa477e4e5" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:NetCashProvidedByUsedInInvestingActivities a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "69b82be1-1145-5686-8613-31da9eb04a72" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:NetCashProvidedByUsedInOperatingActivities a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "57ccbf45-c970-5bcd-a381-44d96b6b6d94" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:NetIncomeLoss a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "27a05717-2370-51c2-a924-db5cbcb48219" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "ff101489-15f4-573d-967b-24f75e0fc0f6" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:ProceedsFromIssuanceOfCommonStock a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "2eb72b5f-d7e3-5bd5-bf93-be38b6d21820" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> a rs:Entity ;
    skos:prefLabel "Cascade Advisory Group LLC" ;
    rs:country "US" ;
    rs:internalId "entity_kg19e75ce934a3a00a17ea" ;
    rs:legalName "Cascade Advisory Group LLC" .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> a rs:Period ;
    xbrli:endDate "2025-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```


## Statement of Changes in Equity

- **Structure**: rs-gaap — Statement of Changes in Equity — Roll Forward (Total)
- **Information Block**: `0b179e5c-5f02-506d-b8d5-860cb10c7694`
- **FactSet**: `fs_01KSV8AYSKTWGGB1B7WWF5R33Q`

| QName | Concept | 2024-01-02 → 2025-12-31 |
|---|---|---:|
| `rs-gaap:NetIncomeLoss` |   **Net Income Loss** | $28,386.70 |
| `rs-gaap:ProceedsFromIssuanceOfCommonStock` |   Proceeds From Issuance Of Common Stock | $49,800.00 |
| `rs-gaap:StockholdersEquity` | **Stockholders Equity** | $78,186.70 |

```turtle {#equity_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1B> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33Q> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1B" ;
    rs:numericValue 28386.70000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM1F> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33Q> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM1F" ;
    rs:numericValue 49800.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/fact/fact_01KSV8AYSRWVB9R93FRYJXAM21> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33Q> ;
    rs:internalId "fact_01KSV8AYSRWVB9R93FRYJXAM21" ;
    rs:numericValue 78186.70000000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/ib/0b179e5c-5f02-506d-b8d5-860cb10c7694> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)" ;
    rs:blockType "equity_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV8AYSKTWGGB1B7WWF5R33Q> ;
    rs:internalId "0b179e5c-5f02-506d-b8d5-860cb10c7694" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_1> a rs:Period ;
    xbrli:instant "2025-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

rs-gaap:NetIncomeLoss a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "27a05717-2370-51c2-a924-db5cbcb48219" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:ProceedsFromIssuanceOfCommonStock a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "2eb72b5f-d7e3-5bd5-bf93-be38b6d21820" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:StockholdersEquity a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "e3796201-9899-5b7b-9477-659550ba8e68" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/period/p_3> a rs:Period ;
    xbrli:endDate "2025-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2025-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/entity/entity_kg19e75ce934a3a00a17ea> a rs:Entity ;
    skos:prefLabel "Cascade Advisory Group LLC" ;
    rs:country "US" ;
    rs:internalId "entity_kg19e75ce934a3a00a17ea" ;
    rs:legalName "Cascade Advisory Group LLC" .

<https://robosystems.ai/report/rpt_01KSV8AYR7KX2R3HJP2SYY1GZ2/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```


## Validation evidence

Independent, standards-grade checks of the same bundle this DataBook renders — embedded so the artifact travels with its own proof.

### RoboLedger — SHACL Ontology Conformance

#### Result: ✅ **Conforms to RoboSystems RDF Ontology v1**

- **Bundle**: `roboledger-demo.jsonld`
- **Graph triples**: 2,709
- **rs:Fact nodes**: 39
- **rs:Association nodes**: 150
- **rs:Element nodes**: 87
- **SHACL shapes checked**: 8 (positive instance shapes + negative shapes banning the retired dialects)

Validated on the host with **pyshacl** against `frameworks/ontology/v1/shapes.ttl` — the *same* shapes that gate the framework seeds and the publish-time bundle validation, run here directly on the on-disk artifact (no API, no database, no container). Conformance means every `rs:Fact` references its aspects directly (`rs:element`/`rs:entity`/`rs:period`/`rs:unit` — no XBRL `context`), every `rs:Association` carries `xlink:from`/`to` + `xlink:arcrole`, and none of the retired dialects (`xbrli:contextRef`, `arcFrom`, direct `summationOf`) appear.

#### Violations

_None._ Zero violations.

### RoboLedger — XBRL 2.1 Validation (Arelle)

#### Result: ✅ **Valid XBRL 2.1**

- **Package**: `roboledger-demo.zip` (12,342 bytes)
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-lab.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 34
- **Load errors**: 0
- **Validation errors**: 0

Validated on the host with **Arelle** (the de-facto XBRL processor, also used by SEC EDGAR) directly against the on-disk report package — no API, no container. Zero load + validation errors is the structural-correctness claim: the output is valid XBRL 2.1, consumable by any standards-compliant processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system checks are not enabled (the instance isn't an SEC filing).

#### Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
