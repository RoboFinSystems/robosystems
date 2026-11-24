---
id: "https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/databook"
title: "Cascade Advisory Group LLC: for the period ended 2025-12-31"
type: "databook"
version: "1.0.0"
created: "2026-10-27"
description: "Cascade Advisory Group LLC: for the period ended 2025-12-31, as a document to read: each section of the report as a table of its consolidated values, with the same facts beside it as RDF. A projection of the report's holon, which holds everything."
domain: "https://robosystems.ai/vocab/"
subject:
  - "financial report"
  - "XBRL"
  - "holon"
graph:
  namespace: "https://robosystems.ai/vocab/"
  named_graph: "https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#scene"
  triple_count: 308
  subjects: 44
  rdf_version: "1.1"
  turtle_version: "1.1"
  reification: false
shapes:
  - "https://robosystems.ai/vocab/shapes/scene"
  - "https://robosystems.ai/vocab/shapes/projection"
  - "https://robosystems.ai/vocab/shapes/boundary"
  - "https://robosystems.ai/vocab/shapes/dataset"
process:
  transformer: "xbrlkit 0.23.1"
  transformer_type: "script"
  transformer_iri: "https://github.com/RoboFinSystems/xbrlkit"
  inputs:
    - iri: "https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB"
      role: "primary"
      description: "The report's holon (sha256:5d525029091d2e923c1b7247c14c5e0a3132f45900ba575f72401e7037cecb30). This DataBook is a projection of it; every value here is a fact in its #scene graph."
    - {iri: "https://robosystems.ai/vocab/shapes/", role: "constraint", description: "The holon format's SHACL shapes, one set per named graph."}
  timestamp: "2026-11-24T20:08:02Z"
  note: "Written from the holon alone: nothing here was read from anywhere else, so the DataBook can be made again from the file it describes."
xbrl_holon:
  report:
    iri: "https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB"
    id: "rpt_01M5Z89P5VYMC22BFPZFXMHHQB"
    entity: "Cascade Advisory Group LLC"
    filed: "2026-10-27"
    reporting_style: "025f5d48-12ce-5d65-b9eb-4f137a10ef06"
  holon:
    iri: "https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB"
    sha256: "5d525029091d2e923c1b7247c14c5e0a3132f45900ba575f72401e7037cecb30"
    href: "roboledger-demo.holon.jsonld"
  graphs:
    - {name: "scene", iri: "https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#scene", disposition: "projected", triples: 1118}
    - {name: "projection", iri: "https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#projection", disposition: "referenced", triples: 3297}
    - {name: "boundary", iri: "https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#boundary", disposition: "referenced", triples: 780}
    - {name: "lineage", iri: "https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#lineage", disposition: "absent"}
  sections:
    - {id: "balance-sheet", name: "Balance Sheet", facts: 20, drawn: true}
    - {id: "income-statement", name: "Income Statement", facts: 10, drawn: true}
    - {id: "cash-flow-statement", name: "Cash Flow Statement", facts: 11, drawn: true}
    - {id: "statement-of-changes-in-equity", name: "Statement of Changes in Equity", facts: 5, drawn: true}
  validation:
    conforms: true
    graphs:
      - {name: "scene", conforms: true, violations: 0, warnings: 0}
      - {name: "projection", conforms: true, violations: 0, warnings: 0}
      - {name: "boundary", conforms: true, violations: 0, warnings: 0}
      - {name: "dataset", conforms: true, violations: 0, warnings: 0}
    report:
      ok: true
      facts: 46
      calculations_checked: 17
      calculations_footed: 17
      problems: 0
---

# Cascade Advisory Group LLC: for the period ended 2025-12-31

## Overview

This is a report as a document to read. Each section below is one information block of the report: a table of its consolidated values, and beneath it the same facts as RDF. It is a **projection of the report's holon** ([`roboledger-demo.holon.jsonld`](roboledger-demo.holon.jsonld)), and the holon holds everything this leaves out: how each section is arranged, the roll-ups the values must obey, the breakdowns by segment or other axis, and, where the producer attached it, the ledger behind each figure.

| Section | Facts | Shown here |
|---|---:|---:|
| [Balance Sheet](#balance-sheet) | 20 | 19 |
| [Income Statement](#income-statement) | 10 | 10 |
| [Cash Flow Statement](#cash-flow-statement) | 11 | 10 |
| [Statement of Changes in Equity](#statement-of-changes-in-equity) | 5 | 5 |

<a id="balance-sheet"></a>

## Balance Sheet

| | 2025-12-31 | 2024-12-31 |
|---|---:|---:|
| **Assets** | 55,763.35 |  |
| &nbsp;&nbsp;**Assets, Current** | 50,205.00 |  |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash and Cash Equivalents, at Carrying Value | 48,705.00 |  |
| &nbsp;&nbsp;&nbsp;&nbsp;Receivables, Net, Current | 0.00 |  |
| &nbsp;&nbsp;&nbsp;&nbsp;Prepaid Expense, Current | 1,500.00 |  |
| &nbsp;&nbsp;**Assets, Noncurrent** | 5,558.35 |  |
| &nbsp;&nbsp;&nbsp;&nbsp;Property, Plant and Equipment, Net | 5,558.35 |  |
| **Liabilities and Equity** | 55,763.35 | 0.00 |
| &nbsp;&nbsp;**Liabilities** | 800.00 |  |
| &nbsp;&nbsp;&nbsp;&nbsp;**Liabilities, Current** | 800.00 |  |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Accounts Payable, Current | 0.00 |  |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Accrued Liabilities, Current | 800.00 |  |
| &nbsp;&nbsp;**Stockholders' Equity Attributable to Parent** | 54,963.35 | 0.00 |
| &nbsp;&nbsp;&nbsp;&nbsp;Additional Paid in Capital | 49,800.00 | 0.00 |
| &nbsp;&nbsp;&nbsp;&nbsp;Retained Earnings (Accumulated Deficit) | 5,163.35 | 0.00 |

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/BS-classified`

<details>
<summary>RDF: 19 facts above (133 triples)</summary>

<!-- databook:id: balance-sheet-facts -->
<!-- databook:label: Balance Sheet: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5D6>
    a rs:Fact ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5D7>
    a rs:Fact ;
    rs:element rs-gaap:AccruedLiabilitiesCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5D8>
    a rs:Fact ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "49800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5D9>
    a rs:Fact ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "48705"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DD>
    a rs:Fact ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "1500"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DE>
    a rs:Fact ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DG>
    a rs:Fact ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "5163.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DH>
    a rs:Fact ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DJ>
    a rs:Fact ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DP>
    a rs:Fact ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "5558.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DY>
    a rs:Fact ;
    rs:element rs-gaap:Liabilities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DZ>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1H>
    a rs:Fact ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "5558.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1M>
    a rs:Fact ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "50205"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1N>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "55763.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1Q>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "54963.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1R>
    a rs:Fact ;
    rs:element rs-gaap:Assets ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "55763.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1Z>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N21>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="income-statement"></a>

## Income Statement

| | 2025-01-01 to 2025-12-31 |
|---|---:|
| **Revenues** | 85,000.00 |
| &nbsp;&nbsp;Revenue from Contract with Customer, Excluding Assessed Tax | 85,000.00 |
| **Gross Profit** | 85,000.00 |
| **Operating Expenses** | 79,836.65 |
| &nbsp;&nbsp;General and Administrative Expense | 79,095.00 |
| &nbsp;&nbsp;Depreciation, Depletion and Amortization | 741.65 |
| **Operating Income (Loss)** | 5,163.35 |
| **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | 5,163.35 |
| **Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent** | 5,163.35 |
| **Net Income (Loss) Attributable to Parent** | 5,163.35 |

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/IS-multistep`

<details>
<summary>RDF: 10 facts above (70 triples)</summary>

<!-- databook:id: income-statement-facts -->
<!-- databook:label: Income Statement: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DA>
    a rs:Fact ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "741.65"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DC>
    a rs:Fact ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "79095"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DF>
    a rs:Fact ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "85000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DK>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "5163.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DX>
    a rs:Fact ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "5163.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1S>
    a rs:Fact ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "5163.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1T>
    a rs:Fact ;
    rs:element rs-gaap:GrossProfit ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "85000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1V>
    a rs:Fact ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "5163.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1X>
    a rs:Fact ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "79836.65"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1Y>
    a rs:Fact ;
    rs:element rs-gaap:Revenues ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "85000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="cash-flow-statement"></a>

## Cash Flow Statement

| | 2025-01-01 to 2025-12-31 |
|---|---:|
| **Statement of Cash Flows [Abstract]** |  |
| &nbsp;&nbsp;**Cash and Cash Equivalents, Period Increase (Decrease)** | 48,705.00 |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash Provided by (Used in) Operating Activity, Including Discontinued Operation | 5,205.00 |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Net Income (Loss) Attributable to Parent | 5,163.35 |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Depreciation, Depletion and Amortization | 741.65 |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Increase (Decrease) in Prepaid Expense | (1,500.00) |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Increase (Decrease) in Accrued Liabilities | 800.00 |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash Provided by (Used in) Investing Activity, Including Discontinued Operation | (6,300.00) |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Payments to Acquire Property, Plant, and Equipment | (6,300.00) |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash Provided by (Used in) Financing Activity, Including Discontinued Operation | 49,800.00 |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Proceeds from Issuance of Common Stock | 49,800.00 |

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/CashFlow-indirect`

<details>
<summary>RDF: 10 facts above (70 triples)</summary>

<!-- databook:id: cash-flow-statement-facts -->
<!-- databook:label: Cash Flow Statement: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DB>
    a rs:Fact ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "741.65"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DN>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "5163.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DQ>
    a rs:Fact ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "49800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DR>
    a rs:Fact ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "-6300"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DT>
    a rs:Fact ;
    rs:element rs-gaap:IncreaseDecreaseInAccruedLiabilities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DV>
    a rs:Fact ;
    rs:element rs-gaap:IncreaseDecreaseInPrepaidExpense ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "-1500"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1G>
    a rs:Fact ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "48705"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1J>
    a rs:Fact ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "-6300"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1K>
    a rs:Fact ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "5205"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1W>
    a rs:Fact ;
    rs:element rs-gaap:NetCashProvidedByUsedInFinancingActivities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "49800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="statement-of-changes-in-equity"></a>

## Statement of Changes in Equity

| | 2025-01-01 to 2025-12-31 | 2025-12-31 | 2024-12-31 | 2023-12-31 |
|---|---:|---:|---:|---:|
| Stockholders' Equity Attributable to Parent, beginning balance |  | 54,963.35 | 0.00 | 0.00 |
| Net Income (Loss) Attributable to Parent | 5,163.35 |  |  |  |
| Stock Issued During Period, Value, New Issues | 49,800.00 |  |  |  |
| **Stockholders' Equity Attributable to Parent, ending balance** |  | 54,963.35 | 0.00 | 0.00 |

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/Equity-rollforward`

<details>
<summary>RDF: 5 facts above (35 triples)</summary>

<!-- databook:id: statement-of-changes-in-equity-facts -->
<!-- databook:label: Statement of Changes in Equity: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DM>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "5163.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9B062Q92SWJAGNF5DS>
    a rs:Fact ;
    rs:element rs-gaap:StockIssuedDuringPeriodValueNewIssues ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "49800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N1P>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "54963.35"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N20>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/fact/fact_01M5Z89P9CVD25HCHWF3CP8N22>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

## Validation

The holon this was written from **conforms to the holon format**. Each named graph is checked against its own SHACL shapes, and the graphs together for references that cross them.

| Graph | Verdict | Triples | Violations | Warnings |
|---|---|---:|---:|---:|
| `#scene` | conforms | 1,118 | 0 | 0 |
| `#projection` | conforms | 3,297 | 0 | 0 |
| `#boundary` | conforms | 780 | 0 | 0 |
| `#lineage` | absent | | | |
| the graphs together | conforms | 4,736 | 0 | 0 |

Read back, the report **holds together**: 46 facts, 17 of 17 calculations foot. This is a check that the report agrees with itself, not that it is complete or true.

## The holon

Everything in this document comes from one file, and the file has more.

- **Report**: `https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB`
- **Holon**: [`roboledger-demo.holon.jsonld`](roboledger-demo.holon.jsonld)
- **SHA-256**: `5d525029091d2e923c1b7247c14c5e0a3132f45900ba575f72401e7037cecb30`

| Named graph | Holds | Triples | In this document |
|---|---|---:|---|
| `https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#scene` | what the report states: the facts and what each points at | 1,118 | projected: the tables and Turtle blocks above |
| `https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#projection` | how it is arranged: structures, presentation and definition arcs | 3,297 | in the holon only |
| `https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#boundary` | the roll-ups the facts must obey: calculation arcs | 780 | in the holon only |
| `https://robosystems.ai/report/rpt_01M5Z89P5VYMC22BFPZFXMHHQB#lineage` | the ledger behind the facts: contributions, lines, entries, events |  | not in this holon |

To read the whole report: `xbrlkit view <holon>` opens it rendered, `xbrlkit serve` puts it behind tools a model can call, and `xbrlkit validate <holon>` repeats the check above.
