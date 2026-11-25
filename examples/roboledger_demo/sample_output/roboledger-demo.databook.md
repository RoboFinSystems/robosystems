---
id: "https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/databook"
title: "Cascade Advisory Group LLC: for the period ended 2025-12-31"
type: "databook"
version: "1.0.0"
created: "2026-11-25"
description: "Cascade Advisory Group LLC: for the period ended 2025-12-31, as a document to read: each section of the report as a table of its consolidated values, with the same facts beside it as RDF. A projection of the report's holon, which holds everything."
domain: "https://robosystems.ai/vocab/"
subject:
  - "financial report"
  - "XBRL"
  - "holon"
graph:
  namespace: "https://robosystems.ai/vocab/"
  named_graph: "https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#scene"
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
  transformer: "xbrlkit 0.25.2"
  transformer_type: "script"
  transformer_iri: "https://github.com/RoboFinSystems/xbrlkit"
  inputs:
    - iri: "https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ"
      role: "primary"
      description: "The report's holon (sha256:362a018d6ee51d60f995c3efa338e188760737390bd9abc80e4ac073992ba910). This DataBook is a projection of it; every value here is a fact in its #scene graph."
    - {iri: "https://robosystems.ai/vocab/shapes/", role: "constraint", description: "The holon format's SHACL shapes, one set per named graph."}
  timestamp: "2026-11-25T05:13:56Z"
  note: "Written from the holon alone: nothing here was read from anywhere else, so the DataBook can be made again from the file it describes."
xbrl_holon:
  report:
    iri: "https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ"
    id: "rpt_01M88HZSNHZA8GDYTZDR7WA8BQ"
    entity: "Cascade Advisory Group LLC"
    filed: "2026-11-25"
    reporting_style: "025f5d48-12ce-5d65-b9eb-4f137a10ef06"
  holon:
    iri: "https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ"
    sha256: "362a018d6ee51d60f995c3efa338e188760737390bd9abc80e4ac073992ba910"
    href: "rpt_01M88HZSNHZA8GDYTZDR7WA8BQ-g2.holon.jsonld"
  graphs:
    - {name: "scene", iri: "https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#scene", disposition: "projected", triples: 1119}
    - {name: "projection", iri: "https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#projection", disposition: "referenced", triples: 3357}
    - {name: "boundary", iri: "https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#boundary", disposition: "referenced", triples: 780}
    - {name: "lineage", iri: "https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#lineage", disposition: "absent"}
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

This is a report as a document to read. Each section below is one information block of the report: a table of its consolidated values, and beneath it the same facts as RDF. It is a **projection of the report's holon** ([`rpt_01M88HZSNHZA8GDYTZDR7WA8BQ-g2.holon.jsonld`](rpt_01M88HZSNHZA8GDYTZDR7WA8BQ-g2.holon.jsonld)), and the holon holds everything this leaves out: how each section is arranged, the roll-ups the values must obey, the breakdowns by segment or other axis, and, where the producer attached it, the ledger behind each figure.

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
| **Assets** | [51,481.68][f1] |  |
| &nbsp;&nbsp;**Assets, Current** | [45,765.00][f2] |  |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash and Cash Equivalents, at Carrying Value | [44,565.00][f3] |  |
| &nbsp;&nbsp;&nbsp;&nbsp;Receivables, Net, Current | [0.00][f4] |  |
| &nbsp;&nbsp;&nbsp;&nbsp;Prepaid Expense, Current | [1,200.00][f5] |  |
| &nbsp;&nbsp;**Assets, Noncurrent** | [5,716.68][f6] |  |
| &nbsp;&nbsp;&nbsp;&nbsp;Property, Plant and Equipment, Net | [5,716.68][f7] |  |
| **Liabilities and Equity** | [51,481.68][f8] | [0.00][f9] |
| &nbsp;&nbsp;**Liabilities** | [800.00][f10] |  |
| &nbsp;&nbsp;&nbsp;&nbsp;**Liabilities, Current** | [800.00][f11] |  |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Accounts Payable, Current | [0.00][f12] |  |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Accrued Liabilities, Current | [800.00][f13] |  |
| &nbsp;&nbsp;**Stockholders' Equity Attributable to Parent** | [50,681.68][f14] | [0.00][f15] |
| &nbsp;&nbsp;&nbsp;&nbsp;Additional Paid in Capital | [49,800.00][f16] | [0.00][f17] |
| &nbsp;&nbsp;&nbsp;&nbsp;Retained Earnings (Accumulated Deficit) | [881.68][f18] | [0.00][f19] |

[f1]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZA>
[f2]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ4>
[f3]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYA>
[f4]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYF>
[f5]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYE>
[f6]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ1>
[f7]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYQ>
[f8]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ5>
[f9]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZG>
[f10]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ2>
[f11]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ3>
[f12]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZY7>
[f13]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZY8>
[f14]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ7>
[f15]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZJ>
[f16]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZY9>
[f17]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYJ>
[f18]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYH>
[f19]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYK>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/BS-classified`

<details>
<summary>RDF: 19 facts above (133 triples)</summary>

<!-- databook:id: balance-sheet-facts -->
<!-- databook:label: Balance Sheet: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZY7>
    a rs:Fact ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZY8>
    a rs:Fact ;
    rs:element rs-gaap:AccruedLiabilitiesCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZY9>
    a rs:Fact ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "49800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYA>
    a rs:Fact ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "44565"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYE>
    a rs:Fact ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "1200"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYF>
    a rs:Fact ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYH>
    a rs:Fact ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "881.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYJ>
    a rs:Fact ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYK>
    a rs:Fact ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYQ>
    a rs:Fact ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "5716.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ1>
    a rs:Fact ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "5716.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ2>
    a rs:Fact ;
    rs:element rs-gaap:Liabilities ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ3>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ4>
    a rs:Fact ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "45765"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ5>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "51481.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ7>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "50681.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZA>
    a rs:Fact ;
    rs:element rs-gaap:Assets ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "51481.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZG>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZJ>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
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
| **Revenues** | [67,500.00][f20] |
| &nbsp;&nbsp;Revenue from Contract with Customer, Excluding Assessed Tax | [67,500.00][f21] |
| **Gross Profit** | [67,500.00][f22] |
| **Operating Expenses** | [66,618.32][f23] |
| &nbsp;&nbsp;General and Administrative Expense | [66,035.00][f24] |
| &nbsp;&nbsp;Depreciation, Depletion and Amortization | [583.32][f25] |
| **Operating Income (Loss)** | [881.68][f26] |
| **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | [881.68][f27] |
| **Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent** | [881.68][f28] |
| **Net Income (Loss) Attributable to Parent** | [881.68][f29] |

[f20]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYY>
[f21]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYG>
[f22]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZB>
[f23]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZF>
[f24]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYD>
[f25]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYB>
[f26]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZE>
[f27]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZC>
[f28]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZD>
[f29]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYM>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/IS-multistep`

<details>
<summary>RDF: 10 facts above (70 triples)</summary>

<!-- databook:id: income-statement-facts -->
<!-- databook:label: Income Statement: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYB>
    a rs:Fact ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "583.32"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYD>
    a rs:Fact ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "66035"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYG>
    a rs:Fact ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "67500"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYM>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "881.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYY>
    a rs:Fact ;
    rs:element rs-gaap:Revenues ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "67500"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZB>
    a rs:Fact ;
    rs:element rs-gaap:GrossProfit ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "67500"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZC>
    a rs:Fact ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "881.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZD>
    a rs:Fact ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "881.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZE>
    a rs:Fact ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "881.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZF>
    a rs:Fact ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "66618.32"^^xsd:decimal ;
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
| &nbsp;&nbsp;**Cash and Cash Equivalents, Period Increase (Decrease)** | [44,565.00][f30] |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash Provided by (Used in) Operating Activity, Including Discontinued Operation | [1,065.00][f31] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Net Income (Loss) Attributable to Parent | [881.68][f32] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Depreciation, Depletion and Amortization | [583.32][f33] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;(Increase) Decrease in Prepaid Expense | [(1,200.00)][f34] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Increase (Decrease) in Accrued Liabilities | [800.00][f35] |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash Provided by (Used in) Investing Activity, Including Discontinued Operation | [(6,300.00)][f36] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Payments to Acquire Property, Plant, and Equipment | [(6,300.00)][f37] |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash Provided by (Used in) Financing Activity, Including Discontinued Operation | [49,800.00][f38] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Proceeds from Issuance of Common Stock | [49,800.00][f39] |

[f30]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ0>
[f31]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ9>
[f32]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYP>
[f33]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYC>
[f34]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYW>
[f35]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYV>
[f36]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ8>
[f37]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYS>
[f38]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYZ>
[f39]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYR>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/CashFlow-indirect`

<details>
<summary>RDF: 10 facts above (70 triples)</summary>

<!-- databook:id: cash-flow-statement-facts -->
<!-- databook:label: Cash Flow Statement: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYC>
    a rs:Fact ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "583.32"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYP>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "881.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYR>
    a rs:Fact ;
    rs:element rs-gaap:ProceedsFromIssuanceOfCommonStock ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "49800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYS>
    a rs:Fact ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "-6300"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYV>
    a rs:Fact ;
    rs:element rs-gaap:IncreaseDecreaseInAccruedLiabilities ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYW>
    a rs:Fact ;
    rs:element rs-gaap:IncreaseDecreaseInPrepaidExpense ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "-1200"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYZ>
    a rs:Fact ;
    rs:element rs-gaap:NetCashProvidedByUsedInFinancingActivities ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "49800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ0>
    a rs:Fact ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "44565"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ8>
    a rs:Fact ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "-6300"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ9>
    a rs:Fact ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "1065"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="statement-of-changes-in-equity"></a>

## Statement of Changes in Equity

| | 2025-01-01 to 2025-12-31 | 2025-12-31 | 2024-12-31 | 2023-12-31 |
|---|---:|---:|---:|---:|
| Stockholders' Equity Attributable to Parent, beginning balance |  | [50,681.68][f40] | [0.00][f41] | [0.00][f42] |
| Net Income (Loss) Attributable to Parent | [881.68][f43] |  |  |  |
| Stock Issued During Period, Value, New Issues | [49,800.00][f44] |  |  |  |
| **Stockholders' Equity Attributable to Parent, ending balance** |  | [50,681.68][f40] | [0.00][f41] | [0.00][f42] |

[f40]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ6>
[f41]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZH>
[f42]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZK>
[f43]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYN>
[f44]: <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYT>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/Equity-rollforward`

<details>
<summary>RDF: 5 facts above (35 triples)</summary>

<!-- databook:id: statement-of-changes-in-equity-facts -->
<!-- databook:label: Statement of Changes in Equity: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYN>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "881.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZYT>
    a rs:Fact ;
    rs:element rs-gaap:StockIssuedDuringPeriodValueNewIssues ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_4> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "49800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZ6>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "50681.68"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZH>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/fact/fact_01M88HZSTYV4SVQ2B5TRVSQZZK>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ/unit/u_USD> ;
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
| `#scene` | conforms | 1,119 | 0 | 0 |
| `#projection` | conforms | 3,357 | 0 | 0 |
| `#boundary` | conforms | 780 | 0 | 0 |
| `#lineage` | absent | | | |
| the graphs together | conforms | 4,796 | 0 | 0 |

Read back, the report **holds together**: 46 facts, 17 of 17 calculations foot. This is a check that the report agrees with itself, not that it is complete or true.

## The holon

Everything in this document comes from one file, and the file has more.

- **Report**: `https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ`
- **Holon**: [`rpt_01M88HZSNHZA8GDYTZDR7WA8BQ-g2.holon.jsonld`](rpt_01M88HZSNHZA8GDYTZDR7WA8BQ-g2.holon.jsonld)
- **SHA-256**: `362a018d6ee51d60f995c3efa338e188760737390bd9abc80e4ac073992ba910`

| Named graph | Holds | Triples | In this document |
|---|---|---:|---|
| `https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#scene` | what the report states: the facts and what each points at | 1,119 | projected: the tables and Turtle blocks above |
| `https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#projection` | how it is arranged: structures, presentation and definition arcs | 3,357 | in the holon only |
| `https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#boundary` | the roll-ups the facts must obey: calculation arcs | 780 | in the holon only |
| `https://robosystems.ai/report/rpt_01M88HZSNHZA8GDYTZDR7WA8BQ#lineage` | the ledger behind the facts: contributions, lines, entries, events |  | not in this holon |

To read the whole report: `xbrlkit view <holon>` opens it rendered, `xbrlkit serve` puts it behind tools a model can call, and `xbrlkit validate <holon>` repeats the check above.
