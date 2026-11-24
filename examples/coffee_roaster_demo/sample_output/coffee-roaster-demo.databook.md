---
id: "https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/databook"
title: "Driftline Coffee Roasters: for the period ended 2026-08-31"
type: "databook"
version: "1.0.0"
created: "2026-10-27"
description: "Driftline Coffee Roasters: for the period ended 2026-08-31, as a document to read: each section of the report as a table of its consolidated values, with the same facts beside it as RDF. A projection of the report's holon, which holds everything."
domain: "https://robosystems.ai/vocab/"
subject:
  - "financial report"
  - "XBRL"
  - "holon"
graph:
  namespace: "https://robosystems.ai/vocab/"
  named_graph: "https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#scene"
  triple_count: 567
  subjects: 81
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
    - iri: "https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K"
      role: "primary"
      description: "The report's holon (sha256:5c35ee392fc8feb17d7841790996def6447f12ec863ee00f8b0db04656834203). This DataBook is a projection of it; every value here is a fact in its #scene graph."
    - {iri: "https://robosystems.ai/vocab/shapes/", role: "constraint", description: "The holon format's SHACL shapes, one set per named graph."}
  timestamp: "2026-11-24T20:59:25Z"
  note: "Written from the holon alone: nothing here was read from anywhere else, so the DataBook can be made again from the file it describes."
xbrl_holon:
  report:
    iri: "https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K"
    id: "rpt_01M5Z6B3VBT6AX0H67HDPCYH3K"
    entity: "Driftline Coffee Roasters"
    filed: "2026-10-27"
    reporting_style: "025f5d48-12ce-5d65-b9eb-4f137a10ef06"
  holon:
    iri: "https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K"
    sha256: "5c35ee392fc8feb17d7841790996def6447f12ec863ee00f8b0db04656834203"
    href: "coffee-roaster-demo.holon.jsonld"
  graphs:
    - {name: "scene", iri: "https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#scene", disposition: "projected", triples: 1915}
    - {name: "projection", iri: "https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#projection", disposition: "referenced", triples: 3475}
    - {name: "boundary", iri: "https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#boundary", disposition: "referenced", triples: 820}
    - {name: "lineage", iri: "https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#lineage", disposition: "absent"}
  sections:
    - {id: "balance-sheet", name: "Balance Sheet", facts: 40, drawn: true}
    - {id: "income-statement", name: "Income Statement", facts: 16, drawn: true}
    - {id: "cash-flow-statement", name: "Cash Flow Statement", facts: 16, drawn: true}
    - {id: "statement-of-changes-in-equity", name: "Statement of Changes in Equity", facts: 3, drawn: true}
    - {id: "significant-accounting-policies", name: "Significant Accounting Policies", facts: 3, drawn: true}
    - {id: "inventory-components", name: "Inventory Components", facts: 8, drawn: true}
  validation:
    conforms: true
    graphs:
      - {name: "scene", conforms: true, violations: 0, warnings: 0}
      - {name: "projection", conforms: true, violations: 0, warnings: 0}
      - {name: "boundary", conforms: true, violations: 0, warnings: 0}
      - {name: "dataset", conforms: true, violations: 0, warnings: 0}
    report:
      ok: true
      facts: 86
      calculations_checked: 26
      calculations_footed: 26
      problems: 0
---

# Driftline Coffee Roasters: for the period ended 2026-08-31

## Overview

This is a report as a document to read. Each section below is one information block of the report: a table of its consolidated values, and beneath it the same facts as RDF. It is a **projection of the report's holon** ([`coffee-roaster-demo.holon.jsonld`](coffee-roaster-demo.holon.jsonld)), and the holon holds everything this leaves out: how each section is arranged, the roll-ups the values must obey, the breakdowns by segment or other axis, and, where the producer attached it, the ledger behind each figure.

| Section | Facts | Shown here |
|---|---:|---:|
| [Balance Sheet](#balance-sheet) | 40 | 40 |
| [Income Statement](#income-statement) | 16 | 16 |
| [Cash Flow Statement](#cash-flow-statement) | 16 | 14 |
| [Statement of Changes in Equity](#statement-of-changes-in-equity) | 3 | 3 |
| [Significant Accounting Policies](#significant-accounting-policies) | 3 | 3 |
| [Inventory Components](#inventory-components) | 8 | 8 |

<a id="balance-sheet"></a>

## Balance Sheet

| | 2026-08-31 | 2025-08-31 |
|---|---:|---:|
| **Assets** | [421,027.73][f1] | [204,844.90][f2] |
| &nbsp;&nbsp;**Assets, Current** | [320,147.61][f3] | [98,987.74][f4] |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash and Cash Equivalents, at Carrying Value | [106,225.94][f5] | [50,066.07][f6] |
| &nbsp;&nbsp;&nbsp;&nbsp;Receivables, Net, Current | [107,666.67][f7] | [16,666.67][f8] |
| &nbsp;&nbsp;&nbsp;&nbsp;Due from Affiliate, Current | [1,255.00][f9] | [1,255.00][f10] |
| &nbsp;&nbsp;&nbsp;&nbsp;Inventory, Net of Allowances, Customer Advances and Progress Billings | [94,000.00][f11] | [20,000.00][f12] |
| &nbsp;&nbsp;&nbsp;&nbsp;Prepaid Expense, Current | [11,000.00][f13] | [11,000.00][f14] |
| &nbsp;&nbsp;**Assets, Noncurrent** | [100,880.12][f15] | [105,857.16][f16] |
| &nbsp;&nbsp;&nbsp;&nbsp;Property, Plant and Equipment, Net | [67,000.12][f17] | [57,857.16][f18] |
| &nbsp;&nbsp;&nbsp;&nbsp;Long-Term Investments and Receivables, Net | [5,000.00][f19] | [5,000.00][f20] |
| &nbsp;&nbsp;&nbsp;&nbsp;Investments in and Advance to Affiliates, Subsidiaries, Associates, and Joint Ventures | [26,800.00][f21] | [39,000.00][f22] |
| &nbsp;&nbsp;&nbsp;&nbsp;Equity Method Investments | [2,080.00][f23] | [4,000.00][f24] |
| **Liabilities and Equity** | [421,027.73][f25] | [204,844.90][f26] |
| &nbsp;&nbsp;**Liabilities** | [51,244.43][f27] | [37,644.44][f28] |
| &nbsp;&nbsp;&nbsp;&nbsp;**Liabilities, Current** | [51,244.43][f29] | [37,644.44][f30] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Accounts Payable, Current | [0.00][f31] | [0.00][f32] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Deferred Revenue, Current | [51,244.43][f33] | [37,644.44][f34] |
| &nbsp;&nbsp;**Stockholders' Equity Attributable to Parent** | [369,783.30][f35] | [167,200.46][f36] |
| &nbsp;&nbsp;&nbsp;&nbsp;Additional Paid in Capital | [124,000.00][f37] | [124,000.00][f38] |
| &nbsp;&nbsp;&nbsp;&nbsp;Retained Earnings (Accumulated Deficit) | [245,783.30][f39] | [43,200.46][f40] |

[f1]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6E>
[f2]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6Y>
[f3]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6C>
[f4]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6W>
[f5]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4P>
[f6]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5F>
[f7]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP56>
[f8]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5R>
[f9]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4V>
[f10]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5H>
[f11]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP50>
[f12]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5K>
[f13]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP55>
[f14]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5Q>
[f15]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6D>
[f16]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6X>
[f17]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5X>
[f18]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5Y>
[f19]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP54>
[f20]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5P>
[f21]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP53>
[f22]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5N>
[f23]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4W>
[f24]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5J>
[f25]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6K>
[f26]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP71>
[f27]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6T>
[f28]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP73>
[f29]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6Q>
[f30]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP72>
[f31]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4M>
[f32]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5D>
[f33]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4R>
[f34]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5G>
[f35]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6H>
[f36]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6Z>
[f37]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4N>
[f38]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5E>
[f39]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP57>
[f40]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5S>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/BS-classified`

<details>
<summary>RDF: 40 facts above (280 triples)</summary>

<!-- databook:id: balance-sheet-facts -->
<!-- databook:label: Balance Sheet: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4M>
    a rs:Fact ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4N>
    a rs:Fact ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "124000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4P>
    a rs:Fact ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "106225.94"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4R>
    a rs:Fact ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "51244.43"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4V>
    a rs:Fact ;
    rs:element rs-gaap:DueFromAffiliateCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "1255"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4W>
    a rs:Fact ;
    rs:element rs-gaap:EquityMethodInvestments ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "2080"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP50>
    a rs:Fact ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "94000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP53>
    a rs:Fact ;
    rs:element rs-gaap:InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "26800"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP54>
    a rs:Fact ;
    rs:element rs-gaap:LongTermInvestmentsAndReceivablesNet ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "5000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP55>
    a rs:Fact ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "11000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP56>
    a rs:Fact ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "107666.67"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP57>
    a rs:Fact ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "245783.3"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5D>
    a rs:Fact ;
    rs:element rs-gaap:AccountsPayableCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "0"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5E>
    a rs:Fact ;
    rs:element rs-gaap:AdditionalPaidInCapital ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "124000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5F>
    a rs:Fact ;
    rs:element rs-gaap:CashAndCashEquivalentsAtCarryingValue ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "50066.07"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5G>
    a rs:Fact ;
    rs:element rs-gaap:DeferredRevenueCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "37644.44"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5H>
    a rs:Fact ;
    rs:element rs-gaap:DueFromAffiliateCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "1255"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5J>
    a rs:Fact ;
    rs:element rs-gaap:EquityMethodInvestments ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "4000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5K>
    a rs:Fact ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "20000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5N>
    a rs:Fact ;
    rs:element rs-gaap:InvestmentsInAffiliatesSubsidiariesAssociatesAndJointVentures ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "39000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5P>
    a rs:Fact ;
    rs:element rs-gaap:LongTermInvestmentsAndReceivablesNet ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "5000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5Q>
    a rs:Fact ;
    rs:element rs-gaap:PrepaidExpenseCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "11000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5R>
    a rs:Fact ;
    rs:element rs-gaap:ReceivablesNetCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "16666.67"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5S>
    a rs:Fact ;
    rs:element rs-gaap:RetainedEarningsAccumulatedDeficit ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "43200.46"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5X>
    a rs:Fact ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "67000.12"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5Y>
    a rs:Fact ;
    rs:element rs-gaap:PropertyPlantAndEquipmentNet ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "57857.16"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6C>
    a rs:Fact ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "320147.61"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6D>
    a rs:Fact ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "100880.12"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6E>
    a rs:Fact ;
    rs:element rs-gaap:Assets ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "421027.73"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6H>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "369783.3"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6K>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "421027.73"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6Q>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "51244.43"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6T>
    a rs:Fact ;
    rs:element rs-gaap:Liabilities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "51244.43"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6W>
    a rs:Fact ;
    rs:element rs-gaap:AssetsCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "98987.74"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6X>
    a rs:Fact ;
    rs:element rs-gaap:AssetsNoncurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "105857.16"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6Y>
    a rs:Fact ;
    rs:element rs-gaap:Assets ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "204844.9"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6Z>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "167200.46"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP71>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesAndStockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "204844.9"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP72>
    a rs:Fact ;
    rs:element rs-gaap:LiabilitiesCurrent ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "37644.44"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP73>
    a rs:Fact ;
    rs:element rs-gaap:Liabilities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "37644.44"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="income-statement"></a>

## Income Statement

| | 2025-09-01 to 2026-08-31 |
|---|---:|
| **Revenues** | [1,173,239.88][f41] |
| &nbsp;&nbsp;Revenue from Contract with Customer, Excluding Assessed Tax | [1,173,239.88][f42] |
| **Cost of Revenue** | [476,000.00][f43] |
| &nbsp;&nbsp;Cost of Product and Service Sold | [476,000.00][f44] |
| **Gross Profit** | [697,239.88][f45] |
| **Operating Expenses** | [530,857.04][f46] |
| &nbsp;&nbsp;General and Administrative Expense | [356,000.00][f47] |
| &nbsp;&nbsp;Selling and Marketing Expense | [160,000.00][f48] |
| &nbsp;&nbsp;Depreciation, Depletion and Amortization | [14,857.04][f49] |
| **Operating Income (Loss)** | [166,382.84][f50] |
| **Nonoperating Income (Expense)** | [36,200.00][f51] |
| &nbsp;&nbsp;Investment Income, Interest | [300.00][f52] |
| &nbsp;&nbsp;Gain (Loss) on Disposition of Stock in Subsidiary | [35,900.00][f53] |
| **Income (Loss) from Continuing Operations before Income Taxes, Noncontrolling Interest** | [202,582.84][f54] |
| **Income (Loss) from Continuing Operations, Net of Tax, Attributable to Parent** | [202,582.84][f55] |
| **Net Income (Loss) Attributable to Parent** | [202,582.84][f56] |

[f41]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6M>
[f42]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP58>
[f43]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6N>
[f44]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4Q>
[f45]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP69>
[f46]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6R>
[f47]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4Z>
[f48]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP59>
[f49]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4S>
[f50]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6P>
[f51]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6G>
[f52]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP52>
[f53]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4X>
[f54]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP68>
[f55]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6A>
[f56]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5T>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/IS-multistep`

<details>
<summary>RDF: 16 facts above (112 triples)</summary>

<!-- databook:id: income-statement-facts -->
<!-- databook:label: Income Statement: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4Q>
    a rs:Fact ;
    rs:element rs-gaap:CostOfGoodsAndServicesSold ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "476000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4S>
    a rs:Fact ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "14857.04"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4X>
    a rs:Fact ;
    rs:element rs-gaap:GainOrLossOnSaleOfStockInSubsidiary ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "35900"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4Z>
    a rs:Fact ;
    rs:element rs-gaap:GeneralAndAdministrativeExpense ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "356000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP52>
    a rs:Fact ;
    rs:element rs-gaap:InvestmentIncomeInterest ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "300"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP58>
    a rs:Fact ;
    rs:element rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "1173239.88"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP59>
    a rs:Fact ;
    rs:element rs-gaap:SellingAndMarketingExpense ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "160000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5T>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "202582.84"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP68>
    a rs:Fact ;
    rs:element rs-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "202582.84"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP69>
    a rs:Fact ;
    rs:element rs-gaap:GrossProfit ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "697239.88"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6A>
    a rs:Fact ;
    rs:element rs-gaap:IncomeLossFromContinuingOperations ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "202582.84"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6G>
    a rs:Fact ;
    rs:element rs-gaap:NonoperatingIncomeExpense ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "36200"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6M>
    a rs:Fact ;
    rs:element rs-gaap:Revenues ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "1173239.88"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6N>
    a rs:Fact ;
    rs:element rs-gaap:CostOfRevenue ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "476000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6P>
    a rs:Fact ;
    rs:element rs-gaap:OperatingIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "166382.84"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6R>
    a rs:Fact ;
    rs:element rs-gaap:OperatingExpenses ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "530857.04"^^xsd:decimal ;
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
| &nbsp;&nbsp;**Cash and Cash Equivalents, Period Increase (Decrease)** | [56,159.87][f57] |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash Provided by (Used in) Operating Activity, Including Discontinued Operation | [30,139.87][f58] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Net Income (Loss) Attributable to Parent | [202,582.84][f59] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Depreciation, Depletion and Amortization | [14,857.04][f60] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Gain (Loss) on Disposition of Stock in Subsidiary | [35,900.00][f61] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Increase (Decrease) in Accounts Receivable | [(91,000.00)][f62] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Increase (Decrease) in Inventories | [(74,000.00)][f63] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Increase (Decrease) in Contract with Customer, Liability | [13,599.99][f64] |
| &nbsp;&nbsp;&nbsp;&nbsp;Cash Provided by (Used in) Investing Activity, Including Discontinued Operation | [26,020.00][f65] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Payments to Acquire Property, Plant, and Equipment | [(24,000.00)][f66] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Payments to Acquire Businesses, Net of Cash Acquired | [(31,500.00)][f67] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Payments to Acquire Interest in Subsidiaries and Affiliates | [(4,000.00)][f68] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Proceeds from Divestiture of Interest in Subsidiaries and Affiliates | [83,600.00][f69] |
| &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Proceeds from Sale of Equity Method Investments | [1,920.00][f70] |

[f57]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6F>
[f58]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6S>
[f59]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5W>
[f60]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4T>
[f61]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4Y>
[f62]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP64>
[f63]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP65>
[f64]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP63>
[f65]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6B>
[f66]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP66>
[f67]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5Z>
[f68]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP62>
[f69]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP60>
[f70]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP61>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/CashFlow-indirect`

<details>
<summary>RDF: 14 facts above (98 triples)</summary>

<!-- databook:id: cash-flow-statement-facts -->
<!-- databook:label: Cash Flow Statement: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4T>
    a rs:Fact ;
    rs:element rs-gaap:DepreciationDepletionAndAmortization ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "14857.04"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4Y>
    a rs:Fact ;
    rs:element rs-gaap:GainOrLossOnSaleOfStockInSubsidiary ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "35900"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5W>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "202582.84"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5Z>
    a rs:Fact ;
    rs:element rs-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "-31500"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP60>
    a rs:Fact ;
    rs:element rs-gaap:ProceedsFromDivestitureOfInterestInSubsidiariesAndAffiliates ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "83600"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP61>
    a rs:Fact ;
    rs:element rs-gaap:ProceedsFromSaleOfEquityMethodInvestments ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "1920"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP62>
    a rs:Fact ;
    rs:element rs-gaap:PaymentsToAcquireInterestInSubsidiariesAndAffiliates ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "-4000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP63>
    a rs:Fact ;
    rs:element rs-gaap:IncreaseDecreaseInContractWithCustomerLiability ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "13599.99"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP64>
    a rs:Fact ;
    rs:element rs-gaap:IncreaseDecreaseInAccountsReceivable ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "-91000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP65>
    a rs:Fact ;
    rs:element rs-gaap:IncreaseDecreaseInInventories ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "-74000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP66>
    a rs:Fact ;
    rs:element rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "-24000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6B>
    a rs:Fact ;
    rs:element rs-gaap:NetCashProvidedByUsedInInvestingActivities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "26020"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6F>
    a rs:Fact ;
    rs:element rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "56159.87"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6S>
    a rs:Fact ;
    rs:element rs-gaap:NetCashProvidedByUsedInOperatingActivities ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "30139.87"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="statement-of-changes-in-equity"></a>

## Statement of Changes in Equity

| | 2025-09-01 to 2026-08-31 | 2026-08-31 | 2025-08-31 |
|---|---:|---:|---:|
| Stockholders' Equity Attributable to Parent, beginning balance |  | [369,783.30][f71] | [167,200.46][f72] |
| Net Income (Loss) Attributable to Parent | [202,582.84][f73] |  |  |
| **Stockholders' Equity Attributable to Parent, ending balance** |  | [369,783.30][f71] | [167,200.46][f72] |

[f71]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6J>
[f72]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP70>
[f73]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5V>

- Section role: `https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/Equity-rollforward`

<details>
<summary>RDF: 3 facts above (21 triples)</summary>

<!-- databook:id: statement-of-changes-in-equity-facts -->
<!-- databook:label: Statement of Changes in Equity: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#scene -->
<!-- mode=reference -->
```turtle
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5V>
    a rs:Fact ;
    rs:element rs-gaap:NetIncomeLoss ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_2> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "202582.84"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP6J>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "369783.3"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP70>
    a rs:Fact ;
    rs:element rs-gaap:StockholdersEquity ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "167200.46"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

<a id="significant-accounting-policies"></a>

## Significant Accounting Policies

| | 2025-09-01 to 2026-08-31 |
|---|---:|
| &nbsp;&nbsp;Inventory and Cost of Goods Sold Policy | [*text, 1,091 characters*][f74] |
| &nbsp;&nbsp;Revenue Recognition Policy | [*text, 1,166 characters*][f75] |
| &nbsp;&nbsp;Depreciation and Prepaid Policy | [*text, 980 characters*][f76] |

[f74]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B485MW59C7ZG3T8NB3EB>
[f75]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B485MW59C7ZG3T8NB3ED>
[f76]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B485MW59C7ZG3T8NB3EC>

**Inventory and Cost of Goods Sold Policy**

> Inventory &amp; Cost of Goods Sold Policy — Driftline Coffee Roasters Method Inventory is carried at cost (FIFO). Green coffee is purchased in bulk from the importer ahead of demand, roasted, and sold through DTC and wholesale channels. Lifecycle Purchase — green coffee received from the importer: DR Inventory — Green Coffee (1400) / CR Cash or Accounts Payable (2000). Cash leaves the business when the coffee is bought, not when it is sold. Sale / consumption — when finished coffee is sold, recognize cost of goods sold: DR Cost of Goods Sold (5000) / CR Inventory — Green Coffee (1400). Watch-out: the cash trap Buying green coffee ahead of a wholesale launch or the holiday season builds the Inventory asset and drains cash before the corresponding revenue and COGS are recognized. A month can show strong gross profit while operating cash falls — the difference is sitting in inventory. *[continues in the holon]*

**Revenue Recognition Policy**

> Revenue Recognition Policy — Driftline Coffee Roasters Standard Revenue is recognized under ASC 606 as control of the coffee transfers to the customer. Channels | Channel | Account | Recognition Timing | Cash Timing | |---|---|---|---| | DTC Subscriptions | Subscription Revenue — DTC (4000) | Ratably as each month's coffee ships | Prepaid — billed a quarter ahead into Deferred Subscription Revenue (2300) | | DTC Single Orders | Single-Order Revenue — DTC (4200) | On shipment | Immediate (card at checkout) | | Wholesale | Wholesale Revenue (4100) | On delivery to the grocer/café | Net 30 — builds Accounts Receivable (1100) | Deferred Subscription Revenue Subscriber prepayments are a liability until the coffee ships. Each month: DR Deferred Subscription Revenue (2300) / CR Subscription Revenue — DTC (4000) for the month earned. The deferred balance is cash already collected for future *[continues in the holon]*

**Depreciation and Prepaid Policy**

> Depreciation &amp; Prepaid Policy — Driftline Coffee Roasters Depreciation Straight-line, no salvage value unless specified. Entry: DR Depreciation Expense (7000) / CR Accumulated Depreciation (1550). | Asset | Cost | Useful Life | Monthly | |---|---|---|---| | Roasting &amp; Packaging Equipment | $90,000.00 | 84 months | $1,071.43 | | Automated Packaging Line | $24,000.00 | 60 months | $400.00 | Prepaid Expense Amortization Purchases with a term greater than one month are capitalized and amortized straight-line. Entry: DR expense / CR prepaid account. | Item | Cost | Term | Monthly | Debit | Credit | |---|---|---|---|---|---| | Business Insurance | $12,000.00 | 12 months | $1,000.00 | Insurance (6500) | Prepaid Insurance (1200) | | Annual Platform Prepay | $6,000.00 | 12 months | $500.00 | Software &amp; Subscriptions (6400) | Prepaid Software (1210) | During each close, review prepaid *[continues in the holon]*

- Section role: `https://driftlinecoffee.example.com/roles/disclosures/AccountingPolicies`

<a id="inventory-components"></a>

## Inventory Components

| | 2026-08-31 | 2025-08-31 |
|---|---:|---:|
| **Inventory, Net of Allowances, Customer Advances and Progress Billings** | [94,000.00][f77] | [20,000.00][f78] |
| &nbsp;&nbsp;Raw Materials — Green Coffee | [86,000.00][f79] | [12,000.00][f80] |
| &nbsp;&nbsp;Work in Process — Roasting | [3,000.00][f81] | [3,000.00][f82] |
| &nbsp;&nbsp;Finished Goods — Bagged Coffee | [5,000.00][f83] | [5,000.00][f84] |

[f77]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP51>
[f78]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5M>
[f79]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4J>
[f80]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5B>
[f81]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4K>
[f82]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5C>
[f83]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4H>
[f84]: <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5A>

- Section role: `https://driftlinecoffee.example.com/roles/disclosures/InventoryComponents`

<details>
<summary>RDF: 8 facts above (56 triples)</summary>

<!-- databook:id: inventory-components-facts -->
<!-- databook:label: Inventory Components: the facts in the table above -->
<!-- databook:graph: https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#scene -->
<!-- mode=reference -->
```turtle
@prefix driftline: <https://robosystems.ai/taxonomy/driftline/> .
@prefix rs: <https://robosystems.ai/vocab/> .
@prefix rs-gaap: <https://robosystems.ai/taxonomy/rs-gaap/v1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4H>
    a rs:Fact ;
    rs:element driftline:InventoryFinishedGoods ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "5000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4J>
    a rs:Fact ;
    rs:element driftline:InventoryRawMaterials ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "86000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP4K>
    a rs:Fact ;
    rs:element driftline:InventoryWorkInProcess ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "3000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP51>
    a rs:Fact ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_1> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "94000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5A>
    a rs:Fact ;
    rs:element driftline:InventoryFinishedGoods ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "5000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5B>
    a rs:Fact ;
    rs:element driftline:InventoryRawMaterials ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "12000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5C>
    a rs:Fact ;
    rs:element driftline:InventoryWorkInProcess ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "3000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .

<https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/fact/fact_01M5Z6B47N01AGSZDB6KW7YP5M>
    a rs:Fact ;
    rs:element rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings ;
    rs:period <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/period/p_3> ;
    rs:unit <https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K/unit/u_USD> ;
    rs:numericValue "20000"^^xsd:decimal ;
    rs:decimals "2" ;
    rs:factType "numeric" .
```

The element, period and unit each fact points at are described in the holon.

</details>

## Validation

The holon this was written from **conforms to the holon format**. Each named graph is checked against its own SHACL shapes, and the graphs together for references that cross them.

| Graph | Verdict | Triples | Violations | Warnings |
|---|---|---:|---:|---:|
| `#scene` | conforms | 1,915 | 0 | 0 |
| `#projection` | conforms | 3,475 | 0 | 0 |
| `#boundary` | conforms | 820 | 0 | 0 |
| `#lineage` | absent | | | |
| the graphs together | conforms | 5,503 | 0 | 0 |

Read back, the report **holds together**: 86 facts, 26 of 26 calculations foot. This is a check that the report agrees with itself, not that it is complete or true.

## The holon

Everything in this document comes from one file, and the file has more.

- **Report**: `https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K`
- **Holon**: [`coffee-roaster-demo.holon.jsonld`](coffee-roaster-demo.holon.jsonld)
- **SHA-256**: `5c35ee392fc8feb17d7841790996def6447f12ec863ee00f8b0db04656834203`

| Named graph | Holds | Triples | In this document |
|---|---|---:|---|
| `https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#scene` | what the report states: the facts and what each points at | 1,915 | projected: the tables and Turtle blocks above |
| `https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#projection` | how it is arranged: structures, presentation and definition arcs | 3,475 | in the holon only |
| `https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#boundary` | the roll-ups the facts must obey: calculation arcs | 820 | in the holon only |
| `https://robosystems.ai/report/rpt_01M5Z6B3VBT6AX0H67HDPCYH3K#lineage` | the ledger behind the facts: contributions, lines, entries, events |  | not in this holon |

To read the whole report: `xbrlkit view <holon>` opens it rendered, `xbrlkit serve` puts it behind tools a model can call, and `xbrlkit validate <holon>` repeats the check above.
