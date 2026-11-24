---
id: "https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/databook"
title: "Cadence Labs, Inc.: for the period ended 2026-08-31"
type: "databook"
version: "1.0.0"
created: "2026-10-27"
description: "Cadence Labs, Inc.: for the period ended 2026-08-31, as a document to read: each section of the report as a table of its consolidated values, with the same facts beside it as RDF. A projection of the report's holon, which holds everything."
domain: "https://robosystems.ai/vocab/"
subject:
  - "financial report"
  - "XBRL"
  - "holon"
graph:
  namespace: "https://robosystems.ai/vocab/"
  named_graph: "https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#scene"
  triple_count: 406
  subjects: 58
  rdf_version: "1.1"
  turtle_version: "1.1"
  reification: false
shapes:
  - "https://robosystems.ai/vocab/shapes/scene"
  - "https://robosystems.ai/vocab/shapes/projection"
  - "https://robosystems.ai/vocab/shapes/boundary"
  - "https://robosystems.ai/vocab/shapes/dataset"
process:
  transformer: "xbrlkit 0.24.1"
  transformer_type: "script"
  transformer_iri: "https://github.com/RoboFinSystems/xbrlkit"
  inputs:
    - iri: "https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3"
      role: "primary"
      description: "The report's holon (sha256:271fe21ba7ca8f9b2c20f52060af0a1a58e477698f1ac7d01601a66911e34c93). This DataBook is a projection of it; every value here is a fact in its #scene graph."
    - {iri: "https://robosystems.ai/vocab/shapes/", role: "constraint", description: "The holon format's SHACL shapes, one set per named graph."}
  timestamp: "2026-11-24T20:59:26Z"
  note: "Written from the holon alone: nothing here was read from anywhere else, so the DataBook can be made again from the file it describes."
xbrl_holon:
  report:
    iri: "https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3"
    id: "rpt_01M5Z8B1DCY95F0FGG8EDP42E3"
    entity: "Cadence Labs, Inc."
    filed: "2026-10-27"
    reporting_style: "025f5d48-12ce-5d65-b9eb-4f137a10ef06"
  holon:
    iri: "https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3"
    sha256: "271fe21ba7ca8f9b2c20f52060af0a1a58e477698f1ac7d01601a66911e34c93"
    href: "saas-startup-demo.holon.jsonld"
  graphs:
    - {name: "scene", iri: "https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#scene", disposition: "projected", triples: 1406}
    - {name: "projection", iri: "https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#projection", disposition: "referenced", triples: 3412}
    - {name: "boundary", iri: "https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#boundary", disposition: "referenced", triples: 800}
    - {name: "lineage", iri: "https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#lineage", disposition: "absent"}
  sections:
    - {id: "balance-sheet", name: "Balance Sheet", facts: 30, drawn: true}
    - {id: "income-statement", name: "Income Statement", facts: 14, drawn: true}
    - {id: "cash-flow-statement", name: "Cash Flow Statement", facts: 10, drawn: true}
    - {id: "statement-of-changes-in-equity", name: "Statement of Changes in Equity", facts: 3, drawn: true}
    - {id: "significant-accounting-policies", name: "Significant Accounting Policies", facts: 2, drawn: true}
    - {id: "disaggregation-of-revenue", name: "Disaggregation of Revenue", facts: 3, drawn: true}
  validation:
    conforms: true
    graphs:
      - {name: "scene", conforms: true, violations: 0, warnings: 0}
      - {name: "projection", conforms: true, violations: 0, warnings: 0}
      - {name: "boundary", conforms: true, violations: 0, warnings: 0}
      - {name: "dataset", conforms: true, violations: 0, warnings: 0}
    report:
      ok: true
      facts: 62
      calculations_checked: 24
      calculations_footed: 24
      problems: 0
---

# Cadence Labs, Inc.: for the period ended 2026-08-31

## Overview

This is a report as a document to read. Each section below is one information block of the report: a table of its consolidated values, and beneath it the same facts as RDF. It is a **projection of the report's holon** ([`saas-startup-demo.holon.jsonld`](saas-startup-demo.holon.jsonld)), and the holon holds everything this leaves out: how each section is arranged, the roll-ups the values must obey, the breakdowns by segment or other axis, and, where the producer attached it, the ledger behind each figure.

| Section | Facts | Shown here |
|---|---:|---:|
| [Balance Sheet](#balance-sheet) | 30 | 30 |
| [Income Statement](#income-statement) | 14 | 14 |
| [Cash Flow Statement](#cash-flow-statement) | 10 | 8 |
| [Statement of Changes in Equity](#statement-of-changes-in-equity) | 3 | 3 |
| [Significant Accounting Policies](#significant-accounting-policies) | 2 | 2 |
| [Disaggregation of Revenue](#disaggregation-of-revenue) | 3 | 3 |

<a id="balance-sheet"></a>

## Balance Sheet

| | 2026-08-31 | 2025-08-31 |
|---|---:|---:|
| **Assets** | [2,112,526.73][f1] | [2,897,577.38][f2] |
| &nbsp;&nbsp;**Assets, Current** | [2,053,304.43][f3] | [2,847,021.82][f4] |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash and Cash Equivalents, at Carrying Value | [2,007,604.43][f5] | [2,806,121.82][f6] |
| &nbsp;&nbsp;&nbsp;&nbsp;Receivables, Net, Current | [9,200.00][f7] | [4,400.00][f8] |
| &nbsp;&nbsp;&nbsp;&nbsp;Prepaid Expense, Current | [36,500.00][f9] | [36,500.00][f10] |
| &nbsp;&nbsp;**Assets, Noncurrent** | [59,222.30][f11] | [50,555.56][f12] |
| &nbsp;&nbsp;&nbsp;&nbsp;Property, Plant and Equipment, Net | [59,222.30][f13] | [50,555.56][f14] |
| **Liabilities and Equity** | [2,112,526.73][f15] | [2,897,577.38][f16] |
| &nbsp;&nbsp;**Liabilities** | [1,149,538.51][f17] | [895,255.30][f18] |
| &nbsp;&nbsp;&nbsp;&nbsp;**Liabilities, Current** | [1,149,538.51][f19] | [895,255.30][f20] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Accounts Payable, Current | [0.00][f21] | [0.00][f22] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Deferred Revenue, Current | [1,149,538.51][f23] | [895,255.30][f24] |
| &nbsp;&nbsp;**Stockholders' Equity Attributable to Parent** | [962,988.22][f25] | [2,002,322.08][f26] |
| &nbsp;&nbsp;&nbsp;&nbsp;Additional Paid in Capital | [2,835,000.00][f27] | [2,835,000.00][f28] |
| &nbsp;&nbsp;&nbsp;&nbsp;Retained Earnings (Accumulated Deficit) | [(1,872,011.78)][f29] | [(832,677.92)][f30] |

[f1]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NG>
[f2]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NY>
[f3]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NT>
[f4]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09P4>
[f5]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MD>
[f6]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MW>
[f7]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MM>
[f8]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MZ>
[f9]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MK>
[f10]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MY>
[f11]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09ND>
[f12]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NX>
[f13]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N4>
[f14]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N5>
[f15]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NJ>
[f16]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09P0>
[f17]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NQ>
[f18]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09P3>
[f19]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NH>
[f20]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NZ>
[f21]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MB>
[f22]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MT>
[f23]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MF>
[f24]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MX>
[f25]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NN>
[f26]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09P2>
[f27]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MC>
[f28]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MV>
[f29]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MP>
[f30]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N0>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/BS-classified`

<details>
<summary>RDF: 30 facts above (210 triples)</summary>

<!-- databook:id: balance-sheet-facts -->
<!-- databook:label: Balance Sheet: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MB>
    a rs:Fact ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MC>
    a rs:Fact ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2835000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MD>
    a rs:Fact ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2007604.43"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MF>
    a rs:Fact ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "1149538.51"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MK>
    a rs:Fact ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "36500"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MM>
    a rs:Fact ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "9200"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MP>
    a rs:Fact ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-1872011.78"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MT>
    a rs:Fact ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MV>
    a rs:Fact ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2835000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MW>
    a rs:Fact ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2806121.82"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MX>
    a rs:Fact ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "895255.3"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MY>
    a rs:Fact ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "36500"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MZ>
    a rs:Fact ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "4400"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N0>
    a rs:Fact ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-832677.92"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N4>
    a rs:Fact ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "59222.3"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N5>
    a rs:Fact ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "50555.56"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09ND>
    a rs:Fact ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "59222.3"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NG>
    a rs:Fact ;
    rs:element rs-gaap:Assets ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2112526.73"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NH>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "1149538.51"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NJ>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2112526.73"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NN>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "962988.22"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NQ>
    a rs:Fact ;
    rs:element rs-gaap:Liabilities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "1149538.51"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NT>
    a rs:Fact ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2053304.43"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NX>
    a rs:Fact ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "50555.56"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NY>
    a rs:Fact ;
    rs:element rs-gaap:Assets ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2897577.38"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NZ>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "895255.3"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09P0>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2897577.38"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09P2>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2002322.08"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09P3>
    a rs:Fact ;
    rs:element rs-gaap:Liabilities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "895255.3"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09P4>
    a rs:Fact ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2847021.82"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="income-statement"></a>

## Income Statement

| | 2025-09-01 to 2026-08-31 |
|---|---:|
| **Revenues** | [1,181,999.40][f31] |
| &nbsp;&nbsp;Revenue from Contract with Customer, Excluding Assessed Tax | [1,181,999.40][f32] |
| **Cost of Revenue** | [252,000.00][f33] |
| &nbsp;&nbsp;Cost of Product and Service Sold | [252,000.00][f34] |
| **Gross Profit** | [929,999.40][f35] |
| **Operating Expenses** | [1,969,333.26][f36] |
| &nbsp;&nbsp;General and Administrative Expense | [480,000.00][f37] |
| &nbsp;&nbsp;Selling and Marketing Expense | [708,000.00][f38] |
| &nbsp;&nbsp;Research and Development Expense | [750,000.00][f39] |
| &nbsp;&nbsp;Depreciation, Depletion and Amortization | [31,333.26][f40] |
| **Operating Income (Loss)** | [(1,039,333.86)][f41] |
| **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | [(1,039,333.86)][f42] |
| **Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent** | [(1,039,333.86)][f43] |
| **Net Income (Loss) Attributable to Parent** | [(1,039,333.86)][f44] |

[f31]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NF>
[f32]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MQ>
[f33]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NP>
[f34]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09ME>
[f35]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NB>
[f36]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NK>
[f37]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MJ>
[f38]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MS>
[f39]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MN>
[f40]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MG>
[f41]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NA>
[f42]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NR>
[f43]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NC>
[f44]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N1>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/IS-multistep`

<details>
<summary>RDF: 14 facts above (98 triples)</summary>

<!-- databook:id: income-statement-facts -->
<!-- databook:label: Income Statement: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09ME>
    a rs:Fact ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "252000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MG>
    a rs:Fact ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "31333.26"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MJ>
    a rs:Fact ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "480000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MN>
    a rs:Fact ;
    rs:element rs-gaap:ResearchAndDevelopmentExpense ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "750000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MQ>
    a rs:Fact ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "1181999.4"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MS>
    a rs:Fact ;
    rs:element rs-gaap:SellingAndMarketingExpense ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "708000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N1>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-1039333.86"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NA>
    a rs:Fact ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-1039333.86"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NB>
    a rs:Fact ;
    rs:element rs-gaap:GrossProfit ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "929999.4"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NC>
    a rs:Fact ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-1039333.86"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NF>
    a rs:Fact ;
    rs:element rs-gaap:Revenues ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "1181999.4"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NK>
    a rs:Fact ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "1969333.26"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NP>
    a rs:Fact ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "252000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NR>
    a rs:Fact ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-1039333.86"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="cash-flow-statement"></a>

## Cash Flow Statement

| | 2025-09-01 to 2026-08-31 |
|---|---:|
| **Statement of Cash Flows [Abstract]** |  |
| &nbsp;&nbsp;**Cash and Cash Equivalents, Period Increase (Decrease)** | [(798,517.39)][f45] |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash Provided by (Used in) Operating Activity, Including Discontinued Operation | [(758,517.39)][f46] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Net Income (Loss) Attributable to Parent | [(1,039,333.86)][f47] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Depreciation, Depletion and Amortization | [31,333.26][f48] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Increase (Decrease) in Accounts Receivable | [(4,800.00)][f49] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Increase (Decrease) in Contract with Customer, Liability | [254,283.21][f50] |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash Provided by (Used in) Investing Activity, Including Discontinued Operation | [(40,000.00)][f51] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Payments to Acquire Property, Plant, and Equipment | [(40,000.00)][f52] |

[f45]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NE>
[f46]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NV>
[f47]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N3>
[f48]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MH>
[f49]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N7>
[f50]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N6>
[f51]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NS>
[f52]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N8>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/CashFlow-indirect`

<details>
<summary>RDF: 8 facts above (56 triples)</summary>

<!-- databook:id: cash-flow-statement-facts -->
<!-- databook:label: Cash Flow Statement: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MH>
    a rs:Fact ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "31333.26"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N3>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-1039333.86"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N6>
    a rs:Fact ;
    rs:element rs-gaap:IncreaseDecreaseInContractWithCustomerLiability ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "254283.21"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N7>
    a rs:Fact ;
    rs:element rs-gaap:IncreaseDecreaseInAccountsReceivable ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-4800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N8>
    a rs:Fact ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-40000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NE>
    a rs:Fact ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-798517.39"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NS>
    a rs:Fact ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-40000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NV>
    a rs:Fact ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-758517.39"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="statement-of-changes-in-equity"></a>

## Statement of Changes in Equity

| | 2025-09-01 to 2026-08-31 | 2026-08-31 | 2025-08-31 |
|---|---:|---:|---:|
| Stockholders' Equity Attributable to Parent, beginning balance |  | [962,988.22][f53] | [2,002,322.08][f54] |
| Net Income (Loss) Attributable to Parent | [(1,039,333.86)][f55] |  |  |
| **Stockholders' Equity Attributable to Parent, ending balance** |  | [962,988.22][f53] | [2,002,322.08][f54] |

[f53]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NM>
[f54]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09P1>
[f55]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N2>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/Equity-rollforward`

<details>
<summary>RDF: 3 facts above (21 triples)</summary>

<!-- databook:id: statement-of-changes-in-equity-facts -->
<!-- databook:label: Statement of Changes in Equity: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09N2>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "-1039333.86"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09NM>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "962988.22"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09P1>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "2002322.08"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="significant-accounting-policies"></a>

## Significant Accounting Policies

| | 2025-09-01 to 2026-08-31 |
|---|---:|
| &nbsp;&nbsp;Revenue Recognition Policy | [*text, 1,080 characters*][f56] |
| &nbsp;&nbsp;Operating Expense and Burn Policy | [*text, 1,102 characters*][f57] |

[f56]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XMEPPZFZ0RHB6Z90A4>
[f57]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XMEPPZFZ0RHB6Z90A3>

**Revenue Recognition Policy**

> Revenue Recognition Policy — Cadence Labs, Inc. Standard Revenue is recognized under ASC 606 as the performance obligation is satisfied. Subscriptions (annual, prepaid) Customers sign annual contracts and pay the full year up front. Cash collected is recorded as Deferred Revenue (2300) , a current liability. Revenue is recognized ratably over the 12-month term: DR Deferred Revenue / CR Subscription Revenue (4000) each month. The deferred-revenue balance equals contracts billed but not yet delivered. It is a source of working-capital &quot;float&quot; — cash in hand that finances operations but is owed as future service. Professional Services Onboarding/implementation is billed net-30 and recognized as delivered: DR AR (1100) / CR Professional Services (4100). Why it matters for runway Because subscriptions are prepaid, cash collected can exceed revenue recognized while the business *[continues in the holon]*

**Operating Expense and Burn Policy**

> Operating Expense &amp; Burn Policy — Cadence Labs, Inc. Expense classification (by function) | Function | Account | Includes | |---|---|---| | Cost of Revenue | 5000 | Cloud hosting, customer support | | Research &amp; Development | 6000 | Engineering salaries, product development | | Sales &amp; Marketing | 6100 | Sales team, commissions, advertising | | General &amp; Administrative | 6200 / 6300 / 6400 | Admin, finance, legal, rent, software tools | R&amp;D is expensed as incurred (no internal-use software capitalization in this policy). Depreciation &amp; Prepaids Straight-line. Equipment over 36 months; the office build-out over 60 months (DR Depreciation Expense 7000 / CR Accumulated Depreciation 1350). Annual tooling and insurance are capitalized as prepaids and amortized over 12 months. Burn &amp; runway Monthly operating burn ≈ operating loss + non-cash addbacks *[continues in the holon]*

- Section role: `https://cadencelabs.example.com/roles/disclosures/AccountingPolicies`

<a id="disaggregation-of-revenue"></a>

## Disaggregation of Revenue

| | 2025-09-01 to 2026-08-31 |
|---|---:|
| **Revenue from Contract with Customer, Excluding Assessed Tax** | [1,181,999.40][f58] |
| &nbsp;&nbsp;Subscription Revenue | [1,097,999.40][f59] |
| &nbsp;&nbsp;Professional Services Revenue | [84,000.00][f60] |

[f58]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MR>
[f59]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MA>
[f60]: <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09M9>

- Section role: `https://cadencelabs.example.com/roles/disclosures/DisaggregationOfRevenue`

<details>
<summary>RDF: 3 facts above (21 triples)</summary>

<!-- databook:id: disaggregation-of-revenue-facts -->
<!-- databook:label: Disaggregation of Revenue: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#scene -->
<!-- mode=reference -->
```turtle
@prefix cadence: <https://robosystems.ai/taxonomy/cadence/> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09M9>
    a rs:Fact ;
    rs:element cadence:ProfessionalServicesRevenue ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "84000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MA>
    a rs:Fact ;
    rs:element cadence:SubscriptionRevenue ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "1097999.4"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/fact/fact_01M5Z8B1XEB4QAWVSZ03VN09MR>
    a rs:Fact ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3/unit/u_USD> ;
    rs:numericValue "1181999.4"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

## Validation

The holon this was written from **conforms to the holon format**. Each named graph is checked against its own SHACL shapes, and the graphs together for references that cross them.

| Graph | Verdict | Triples | Violations | Warnings |
|---|---|---:|---:|---:|
| `#scene` | conforms | 1,406 | 0 | 0 |
| `#projection` | conforms | 3,412 | 0 | 0 |
| `#boundary` | conforms | 800 | 0 | 0 |
| `#lineage` | absent | | | |
| the graphs together | conforms | 5,094 | 0 | 0 |

Read back, the report **holds together**: 62 facts, 24 of 24 calculations foot. This is a check that the report agrees with itself, not that it is complete or true.

## The holon

Everything in this document comes from one file, and the file has more.

- **Report**: `https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3`
- **Holon**: [`saas-startup-demo.holon.jsonld`](saas-startup-demo.holon.jsonld)
- **SHA-256**: `271fe21ba7ca8f9b2c20f52060af0a1a58e477698f1ac7d01601a66911e34c93`

| Named graph | Holds | Triples | In this document |
|---|---|---:|---|
| `https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#scene` | what the report states: the facts and what each points at | 1,406 | projected: the tables and Turtle blocks above |
| `https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#projection` | how it is arranged: structures, presentation and definition arcs | 3,412 | in the holon only |
| `https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#boundary` | the roll-ups the facts must obey: calculation arcs | 800 | in the holon only |
| `https://robosystems.ai/report/rpt_01M5Z8B1DCY95F0FGG8EDP42E3#lineage` | the ledger behind the facts: contributions, lines, entries, events |  | not in this holon |

To read the whole report: `xbrlkit view <holon>` opens it rendered, `xbrlkit serve` puts it behind tools a model can call, and `xbrlkit validate <holon>` repeats the check above.
