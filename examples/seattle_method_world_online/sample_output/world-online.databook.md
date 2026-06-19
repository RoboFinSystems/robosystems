---
id: https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4
type: DataBook
title: "The World Online — The World Online (Charlie Hoffman demo)"
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
  method: "Materialized RoboSystems Report rpt_01KSV87YSV6H8A6NSRA5HGPDS4 (generation 1, draft)"
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
  report_id: rpt_01KSV87YSV6H8A6NSRA5HGPDS4
  generation_count: 1
  filing_status: draft
  periods:
    - { label: "2018-12-31 → 2028-12-31", start: 2018-12-31, end: 2028-12-31 }
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

# The World Online — The World Online (Charlie Hoffman demo)

A report **is** a collection of Information Blocks. Each block below is shown twice: a markdown table (human view) and an addressable `turtle` block (machine view — the same facts as RDF), keyed by the id declared in the frontmatter `manifest`. Everything is derived from `world-online.jsonld`; the bundle and this DataBook are two skins of one graph.


## Balance Sheet

- **Structure**: rs-gaap — Balance Sheet — Classified
- **Information Block**: `b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d`
- **FactSet**: `fs_01KSV87Z3AX8S6W5QVAHDW0H06`

| QName | Concept | 2018-12-31 → 2028-12-31 |
|---|---|---:|
| `rs-gaap:CashCashEquivalentsAndShortTermInvestments` |     Cash Cash Equivalents And Short Term Investments | $(648,551.94) |
| `rs-gaap:ReceivablesNetCurrent` |     Receivables Net Current | $2,035,468.27 |
| `rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings` |     Inventory Net Of Allowances Customer Advances And Progress Billings | $451,842.19 |
| `rs-gaap:AssetsCurrent` |   **Assets Current** | $1,838,758.52 |
| `rs-gaap:PropertyPlantAndEquipmentNet` |     Property Plant And Equipment Net | $1,245,567.16 |
| `rs-gaap:AssetsNoncurrent` |   **Assets Noncurrent** | $1,245,567.16 |
| `rs-gaap:Assets` | **Assets** | $3,084,325.68 |
| `rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent` |       Accounts Payable And Accrued Liabilities Current | $2,689,452.31 |
| `rs-gaap:LiabilitiesCurrent` |     **Liabilities Current** | $2,689,452.31 |
| `rs-gaap:LongTermDebtAndCapitalLeaseObligations` |       Long Term Debt And Capital Lease Obligations | $338,349.05 |
| `rs-gaap:LiabilitiesNoncurrent` |     **Liabilities Noncurrent** | $338,349.05 |
| `rs-gaap:Liabilities` |   **Liabilities** | $3,027,801.36 |
| `rs-gaap:AdditionalPaidInCapital` |     Additional Paid In Capital | $1,407,646.64 |
| `rs-gaap:RetainedEarningsAccumulatedDeficit` |     Retained Earnings Accumulated Deficit | $(1,351,122.32) |
| `rs-gaap:StockholdersEquity` |   **Stockholders Equity** | $56,524.32 |
| `rs-gaap:LiabilitiesAndStockholdersEquity` | **Liabilities And Stockholders Equity** | $3,084,325.68 |

```turtle {#balance_sheet}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETA4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETA4" ;
    rs:numericValue 2689452.3100000005 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETA5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETA5" ;
    rs:numericValue 1407646.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETA6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashCashEquivalentsAndShortTermInvestments ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETA6" ;
    rs:numericValue -648551.94 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAB" ;
    rs:numericValue 451842.18999999994 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LongTermDebtAndCapitalLeaseObligations ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAC" ;
    rs:numericValue 338349.05 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAD" ;
    rs:numericValue 1245567.1600000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAE" ;
    rs:numericValue 2035468.27 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAH" ;
    rs:numericValue -1351122.3200000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AccountsPayableAndAccruedLiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAJ" ;
    rs:numericValue 1595349.42 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAK" ;
    rs:numericValue 1407646.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashCashEquivalentsAndShortTermInvestments ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAM" ;
    rs:numericValue 398937.76 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAN" ;
    rs:numericValue 467010.2 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LongTermDebtAndCapitalLeaseObligations ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAP" ;
    rs:numericValue 361285.69 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAQ" ;
    rs:numericValue 1266995.3199999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAR" ;
    rs:numericValue 1231338.4700000002 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAS" ;
    rs:numericValue 0.0 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETB1> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETB1" ;
    rs:numericValue 3084325.6799999997 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETB3> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETB3" ;
    rs:numericValue 3084325.68 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETB5> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETB5" ;
    rs:numericValue 338349.05 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETB6> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETB6" ;
    rs:numericValue 1838758.52 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETB8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETB8" ;
    rs:numericValue 3027801.3600000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBA" ;
    rs:numericValue 1245567.1600000001 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBC> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBC" ;
    rs:numericValue 2689452.3100000005 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBF" ;
    rs:numericValue 56524.3199999996 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBJ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBJ" ;
    rs:numericValue 3364281.75 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBK> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Assets ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBK" ;
    rs:numericValue 3364281.75 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBM> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBM" ;
    rs:numericValue 361285.69 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBN> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBN" ;
    rs:numericValue 2097286.43 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBP> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Liabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBP" ;
    rs:numericValue 1956635.1099999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBQ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBQ" ;
    rs:numericValue 1266995.3199999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBR> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBR" ;
    rs:numericValue 1595349.42 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBT" ;
    rs:numericValue 1407646.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/ib/b6dfb8d2-8ee9-5597-9a3b-8aeee625ff0d> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Balance Sheet — Classified" ;
    rs:blockType "balance_sheet" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H06> ;
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

rs-gaap:AdditionalPaidInCapital a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "6146605c-0d63-51e1-a523-3450d6abaca3" ;
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

rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "4afa5950-85ac-5a85-a9cb-01c387c6ab08" ;
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

rs-gaap:LiabilitiesNoncurrent a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "f41fe34d-88ea-5e20-a781-2d3e256a6abf" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:LongTermDebtAndCapitalLeaseObligations a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "091373d9-8a82-51bd-adf8-d09b73beb32e" ;
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

rs-gaap:RetainedEarningsAccumulatedDeficit a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "a9c87d60-a1e5-506b-a27e-cbf9e14e5113" ;
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

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> a rs:Period ;
    xbrli:instant "2028-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> a rs:Period ;
    xbrli:instant "2023-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> a rs:Entity ;
    skos:prefLabel "The World Online (Charlie Hoffman demo)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19e75cd88a3785aae2c6" ;
    rs:legalName "The World Online (Charlie Hoffman demo)" .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```


## Income Statement

- **Structure**: rs-gaap — Income Statement — Multi-step
- **Information Block**: `47cd6544-03d1-5bc1-8c28-31c0cfa450f9`
- **FactSet**: `fs_01KSV87Z3AX8S6W5QVAHDW0H07`

| QName | Concept | 2018-12-31 → 2028-12-31 |
|---|---|---:|
| `rs-gaap:Revenues` |   **Revenues** | $2,604,048.36 |
| `rs-gaap:CostOfGoodsSold` |     Cost Of Goods Sold | $886,041.18 |
| `rs-gaap:CostOfRevenue` |   **Cost Of Revenue** | $886,041.18 |
| `rs-gaap:GrossProfit` |   **Gross Profit** | $1,718,007.18 |
| `rs-gaap:SellingGeneralAndAdministrativeExpense` |     Selling General And Administrative Expense | $3,049,867.27 |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation Depletion And Amortization | $21,428.16 |
| `rs-gaap:OperatingExpenses` |   **Operating Expenses** | $3,071,295.43 |
| `rs-gaap:OperatingIncomeLoss` |   **Operating Income Loss** | $(1,353,288.25) |
| `rs-gaap:InterestExpense` |     Interest Expense | $(2,165.93) |
| `rs-gaap:NonoperatingIncomeExpense` |   **Nonoperating Income Expense** | $2,165.93 |
| `rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest` |   **Income Loss From Continuing Operations Before Income Taxes Extraordinary Items Noncontrolling Interest** | $(1,351,122.32) |
| `rs-gaap:IncomeLossFromContinuingOperations` |   **Income Loss From Continuing Operations** | $(1,351,122.32) |
| `rs-gaap:NetIncomeLoss` |   **Net Income Loss** | $(1,351,122.32) |

```turtle {#income_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETA7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfGoodsSold ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETA7" ;
    rs:numericValue 886041.1799999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETA8> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETA8" ;
    rs:numericValue 21428.16 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAA> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:InterestExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAA" ;
    rs:numericValue -2165.929999999993 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAF> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:Revenues ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAF" ;
    rs:numericValue 2604048.36 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:SellingGeneralAndAdministrativeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAG" ;
    rs:numericValue 3049867.27 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAT> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAT" ;
    rs:numericValue -1351122.3199999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETB0> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETB0" ;
    rs:numericValue 3071295.43 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETB4> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETB4" ;
    rs:numericValue -1353288.2500000002 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETB7> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:GrossProfit ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETB7" ;
    rs:numericValue 1718007.18 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETB9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETB9" ;
    rs:numericValue 886041.1799999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBB> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NonoperatingIncomeExpense ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBB" ;
    rs:numericValue 2165.929999999993 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBG> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBG" ;
    rs:numericValue -1351122.3200000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBH> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBH" ;
    rs:numericValue -1351122.3200000003 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/ib/47cd6544-03d1-5bc1-8c28-31c0cfa450f9> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Income Statement — Multi-step" ;
    rs:blockType "income_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H07> ;
    rs:internalId "47cd6544-03d1-5bc1-8c28-31c0cfa450f9" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

rs-gaap:CostOfGoodsSold a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "b9fdd02a-e336-5359-a2eb-303b560094bd" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:CostOfRevenue a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "12ab7417-5324-55d6-946e-2456adba47c5" ;
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

rs-gaap:InterestExpense a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "890e4f8c-8fed-57e2-96fd-e70455201b11" ;
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

rs-gaap:NonoperatingIncomeExpense a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "45aae2c2-fa56-50c9-b381-df8079e7d33a" ;
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

rs-gaap:SellingGeneralAndAdministrativeExpense a rs:Element ;
    xbrli:balance "debit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "050fac09-f306-514d-80ef-f0d10ce05de9" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> a rs:Entity ;
    skos:prefLabel "The World Online (Charlie Hoffman demo)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19e75cd88a3785aae2c6" ;
    rs:legalName "The World Online (Charlie Hoffman demo)" .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> a rs:Period ;
    xbrli:endDate "2028-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```


## Cash Flow Statement

- **Structure**: rs-gaap — Cash Flow Statement — Indirect
- **Information Block**: `5473639a-2dac-56a6-b9e5-38480ea38bc1`
- **FactSet**: `fs_01KSV87Z3AX8S6W5QVAHDW0H08`

| QName | Concept | 2018-12-31 → 2028-12-31 |
|---|---|---:|
| `rs-gaap:NetIncomeLoss` |     **Net Income Loss** | $(1,351,122.32) |
| `rs-gaap:DepreciationDepletionAndAmortization` |     Depreciation Depletion And Amortization | $21,428.16 |
| `rs-gaap:IncreaseDecreaseInAccountsReceivable` |     Increase Decrease In Accounts Receivable | $(804,129.80) |
| `rs-gaap:IncreaseDecreaseInInventories` |     Increase Decrease In Inventories | $15,168.01 |
| `rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities` |     Increase Decrease In Accounts Payable And Accrued Liabilities | $1,094,102.89 |
| `rs-gaap:NetCashProvidedByUsedInOperatingActivities` |   Net Cash Provided By Used In Operating Activities | $(1,024,553.06) |
| `rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease` | **Cash And Cash Equivalents Period Increase Decrease** | $(1,024,553.06) |

```turtle {#cash_flow_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETA9> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H08> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETA9" ;
    rs:numericValue 21428.16 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAW> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H08> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAW" ;
    rs:numericValue -1351122.3199999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAX> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H08> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAX" ;
    rs:numericValue 1094102.8900000006 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAY> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInAccountsReceivable ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H08> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAY" ;
    rs:numericValue -804129.7999999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAZ> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:IncreaseDecreaseInInventories ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H08> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAZ" ;
    rs:numericValue 15168.010000000068 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETB2> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H08> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETB2" ;
    rs:numericValue -1024553.059999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBD> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H08> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBD" ;
    rs:numericValue -1024553.059999999 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/5473639a-2dac-56a6-b9e5-38480ea38bc1> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/ib/5473639a-2dac-56a6-b9e5-38480ea38bc1> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Cash Flow Statement — Indirect" ;
    rs:blockType "cash_flow_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H08> ;
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

rs-gaap:IncreaseDecreaseInAccountsReceivable a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "93175d59-983c-5012-910f-3dfbf07ce327" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

rs-gaap:IncreaseDecreaseInInventories a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "duration" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "c8b0722b-7993-592f-8ef1-5b0964ac8a10" ;
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

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> a rs:Entity ;
    skos:prefLabel "The World Online (Charlie Hoffman demo)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19e75cd88a3785aae2c6" ;
    rs:legalName "The World Online (Charlie Hoffman demo)" .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> a rs:Period ;
    xbrli:endDate "2028-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```


## Statement of Changes in Equity

- **Structure**: rs-gaap — Statement of Changes in Equity — Roll Forward (Total)
- **Information Block**: `0b179e5c-5f02-506d-b8d5-860cb10c7694`
- **FactSet**: `fs_01KSV87Z3AX8S6W5QVAHDW0H09`

| QName | Concept | 2018-12-31 → 2028-12-31 |
|---|---|---:|
| `rs-gaap:NetIncomeLoss` |   **Net Income Loss** | $(1,351,122.32) |
| `rs-gaap:StockholdersEquity` | **Stockholders Equity** | $56,524.32 |

```turtle {#equity_statement}
@prefix iso4217: <http://www.xbrl.org/2003/iso4217#> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix xbrli: <http://www.xbrl.org/2003/instance#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETAV> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H09> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETAV" ;
    rs:numericValue -1351122.3199999998 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBE> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H09> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBE" ;
    rs:numericValue 56524.3199999996 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/fact/fact_01KSV87Z3E68G4DS8XWEDWETBS> a rs:Fact ;
    rs:decimals "INF" ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:entity <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H09> ;
    rs:internalId "fact_01KSV87Z3E68G4DS8XWEDWETBS" ;
    rs:numericValue 1407646.64 ;
    rs:period <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> ;
    rs:structure <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/structure/0b179e5c-5f02-506d-b8d5-860cb10c7694> ;
    rs:unit <https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/ib/0b179e5c-5f02-506d-b8d5-860cb10c7694> a rs:InformationBlock ;
    skos:prefLabel "rs-gaap — Statement of Changes in Equity — Roll Forward (Total)" ;
    rs:blockType "equity_statement" ;
    rs:factSet <https://robosystems.ai/factset/fs_01KSV87Z3AX8S6W5QVAHDW0H09> ;
    rs:internalId "0b179e5c-5f02-506d-b8d5-860cb10c7694" ;
    rs:taxonomyId "cf7178a0-e2d4-58df-995a-2f0233d15466" ;
    rs:taxonomyName "rs-gaap-presentation v1" .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_1> a rs:Period ;
    xbrli:instant "2028-12-31"^^xsd:date ;
    xbrli:periodType "instant" .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_2> a rs:Period ;
    xbrli:endDate "2028-12-31"^^xsd:date ;
    xbrli:periodType "duration" ;
    xbrli:startDate "2024-01-01"^^xsd:date .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/period/p_3> a rs:Period ;
    xbrli:instant "2023-12-31"^^xsd:date ;
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

rs-gaap:StockholdersEquity a rs:Element ;
    xbrli:balance "credit" ;
    xbrli:periodType "instant" ;
    rs:abstract false ;
    rs:elementType "concept" ;
    rs:internalId "e3796201-9899-5b7b-9477-659550ba8e68" ;
    rs:monetary true ;
    rs:source "rs-gaap" ;
    rs:substitutionGroup xbrli:item .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/entity/entity_kg19e75cd88a3785aae2c6> a rs:Entity ;
    skos:prefLabel "The World Online (Charlie Hoffman demo)" ;
    rs:country "US" ;
    rs:internalId "entity_kg19e75cd88a3785aae2c6" ;
    rs:legalName "The World Online (Charlie Hoffman demo)" .

<https://robosystems.ai/report/rpt_01KSV87YSV6H8A6NSRA5HGPDS4/unit/u_USD> a rs:Unit ;
    xbrli:measure iso4217:USD .
```


## Validation evidence

Independent, standards-grade checks of the same bundle this DataBook renders — embedded so the artifact travels with its own proof.

### The World Online — SHACL Ontology Conformance

#### Result: ✅ **Conforms to RoboSystems RDF Ontology v1**

- **Bundle**: `world-online.jsonld`
- **Graph triples**: 2,885
- **rs:Fact nodes**: 55
- **rs:Association nodes**: 150
- **rs:Element nodes**: 87
- **SHACL shapes checked**: 8 (positive instance shapes + negative shapes banning the retired dialects)

Validated on the host with **pyshacl** against `frameworks/ontology/v1/shapes.ttl` — the *same* shapes that gate the framework seeds and the publish-time bundle validation, run here directly on the on-disk artifact (no API, no database, no container). Conformance means every `rs:Fact` references its aspects directly (`rs:element`/`rs:entity`/`rs:period`/`rs:unit` — no XBRL `context`), every `rs:Association` carries `xlink:from`/`to` + `xlink:arcrole`, and none of the retired dialects (`xbrli:contextRef`, `arcFrom`, direct `summationOf`) appear.

#### Violations

_None._ Zero violations.

### The World Online — XBRL 2.1 Validation (Arelle)

#### Result: ✅ **Valid XBRL 2.1**

- **Package**: `world-online.zip` (12,728 bytes)
- **Files in zip**: 5 (`instance.xml, report-cal.xml, report-lab.xml, report-pre.xml, report.xsd`)
- **Facts loaded by Arelle**: 50
- **Load errors**: 0
- **Validation errors**: 0

Validated on the host with **Arelle** (the de-facto XBRL processor, also used by SEC EDGAR) directly against the on-disk report package — no API, no container. Zero load + validation errors is the structural-correctness claim: the output is valid XBRL 2.1, consumable by any standards-compliant processor. This is **base XBRL 2.1** validation; SEC/EFM disclosure-system checks are not enabled (the instance isn't an SEC filing).

#### Errors

_None._ Arelle reported no load errors and no XBRL 2.1 validation errors against the emitted instance + schema + linkbases.
