# SEC Data Model & Query Rules

This page is for integrators querying the hosted `sec` graph — the shared repository of public-company XBRL filings — with Cypher. It describes the node and relationship model, the four rules that decide whether a number read from it is right, and a set of worked queries that follow them.

**Quick Start:** Before writing Cypher, try the ready-made views: `financial-statement-analysis` returns a company's statement by ticker, and `build-fact-grid` pivots facts across companies and periods. Both already apply the rules on this page. When you do write Cypher, filter `Fact {has_dimensions: false}`, match `Element.canonical_concept`, pin the `Period` shape, and `RETURN DISTINCT` from an `Entity` anchor.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`, and load filings with [SEC XBRL Pipeline](SEC-XBRL-Pipeline). See [Local Development](Local-Development).

## Table of Contents

- [Before You Write Cypher](#before-you-write-cypher)
- [The Data Model](#the-data-model)
- [The Four Rules](#the-four-rules)
- [Worked Queries](#worked-queries)
- [Other Limits](#other-limits)

## Before You Write Cypher

Access to `sec` is a subscription (see [Shared Repositories](Shared-Repositories)). Every example authenticates with an API key in `X-API-Key`:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
```

### Ready-Made Views

Four operations answer the common questions without Cypher. Each is an MCP tool of the same name on the `sec` connector, and a REST operation at `POST https://api.robosystems.ai/extensions/roboledger/sec/operations/{name}` that returns an `OperationEnvelope`:

| Operation | Answers | Reads |
| --- | --- | --- |
| **`financial-statement-analysis`** | One company's income statement, balance sheet, cash flow or equity statement, resolved to its latest filing by `ticker` | The graph |
| **`build-fact-grid`** | Chosen concepts across companies, periods and dimensions, deduplicated | The graph |
| **`disclosures`** | The map of a filing's sections: one row per note or statement | The filing held whole |
| **`information-block`** | One section read whole: its rows, breakdowns by its own axes, calculation footing, and text | The filing held whole |

```bash
# A company's latest annual income statement
curl -X POST "https://api.robosystems.ai/extensions/roboledger/sec/operations/financial-statement-analysis" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"statement_type": "income_statement", "ticker": "NVDA", "period_type": "annual"}'

# Revenue for two companies' 10-K filings, by canonical concept
curl -X POST "https://api.robosystems.ai/extensions/roboledger/sec/operations/build-fact-grid" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"canonical_concepts": ["revenue"], "entities": ["NVDA", "AMD"], "form": "10-K", "period_type": "annual"}'
```

`statement_type` takes `income_statement`, `balance_sheet`, `cash_flow_statement` or `equity_statement`. The full request and response schemas are in the [API reference](https://robosystems.ai/docs/api) (operations `financialStatementAnalysis`, `buildFactGrid`, `disclosures`, `informationBlock`).

### Concept Lookup

To find which element qnames carry a concept, call the MCP tool **`resolve-element`** with a phrase such as `"operating lease liability"`, optionally scoped to a `ticker` or `report_id`. It returns the matching canonical concept, the qnames filers use for it with fact counts, and a query hint. It is served on the `sec` connector only; there is no REST equivalent.

### Raw Cypher

When no view fits, post Cypher to `POST /v1/graphs/sec/query/cypher` (or call the MCP tool `read-graph-cypher`). Always pass values as named `$parameters`:

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/sec/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (ent:Entity {ticker: $ticker}) RETURN ent.name AS name, ent.cik AS cik, ent.fiscal_year_end AS fiscal_year_end", "parameters": {"ticker": "NVDA"}}'
```

Request fields, response modes and limits are covered in [Querying the Analytical Graph](Querying-the-Analytical-Graph). The MCP tool `get-example-queries` returns working patterns that follow the rules below.

## The Data Model

The graph holds each filing as XBRL facts linked to the aspects that give them meaning — the reporting entity, the concept, the period, the unit, and any dimensional qualifiers — plus the filing's own presentation and calculation networks. The schema is the same one [xbrlkit](https://xbrlkit.com) uses for a single filing, so Cypher written against a one-filing xbrlkit build runs unchanged on `sec`. `get-graph-schema` (MCP) and `GET /v1/graphs/sec/schema` return it live.

### Node Types

**Reporting nodes** — the data:

| Node | Holds | Key properties |
| --- | --- | --- |
| **`Entity`** | A filer | `ticker`, `cik`, `name`, `sic`, `sic_description`, `fiscal_year_end`, `state_of_incorporation` |
| **`Report`** | One filing | `form` (`10-K`, `10-Q`, `20-F`, `40-F`), `filing_date`, `report_date` (period end), `accession_number`, `fiscal_year_focus`, `fiscal_period_focus` (`FY`, `Q1`–`Q3`), `fiscal_year_end_month` |
| **`Fact`** | One reported value | `numeric_value`, `value`, `fact_type` (`Numeric` / `Nonnumeric`), `decimals`, `has_dimensions`, `dimension_count` |
| **`Element`** | The concept a fact reports | `qname` (e.g. `us-gaap:Revenues`), `name`, `period_type`, `balance`, `canonical_concept`, `canonical_confidence`, `is_numeric`, `is_textblock` |
| **`Period`** | When a fact applies | `period_type` (`instant` / `duration` / `forever`), `start_date`, `end_date`, `duration_type`, `calendar_year`, `calendar_quarter`, `calendar_period_key` |
| **`Unit`** | Unit of measure | `measure` (e.g. `iso4217:USD`, `shares`) |
| **`Dimension`** | An axis–member qualifier | `axis`, `member`, `axis_uri`, `member_uri`, `dimension_type` |

**Structural nodes** — how a filing arranges its facts:

| Node | Holds | Key properties |
| --- | --- | --- |
| **`Structure`** | One network of a filing (a statement, note, table or detail) | `name`, `definition`, `network_uri`, `canonical_type` |
| **`FactSet`** | The facts one structure produced for its filing | `factset_type`, `provenance` |
| **`Association`** | A parent–child arc within a structure | `association_type`, `weight`, `order_value`, `preferred_label` |
| **`Classification`** | The pattern an arrangement forms (e.g. a roll-up) | `category`, `type` |

**Reference nodes** — taxonomy metadata: **`Label`** (human-readable element labels), **`Reference`** (authoritative literature references), and **`Taxonomy`**.

A `Period` places a fact on the calendar, not on the filer's fiscal calendar: `calendar_year` and `calendar_quarter` are for matching across companies. A company's own fiscal year and period are on its `Report` (`fiscal_year_focus`, `fiscal_period_focus`).

### Relationships

| Relationship | From → To |
| --- | --- |
| `ENTITY_HAS_REPORT` | `Entity` → `Report` |
| `REPORT_HAS_FACT` | `Report` → `Fact` |
| `REPORT_HAS_FACT_SET` | `Report` → `FactSet` |
| `REPORT_USES_TAXONOMY` | `Report` → `Taxonomy` |
| `FACT_HAS_ENTITY` | `Fact` → `Entity` |
| `FACT_HAS_ELEMENT` | `Fact` → `Element` |
| `FACT_HAS_PERIOD` | `Fact` → `Period` |
| `FACT_HAS_UNIT` | `Fact` → `Unit` |
| `FACT_HAS_DIMENSION` | `Fact` → `Dimension` |
| `FACT_SET_CONTAINS_FACT` | `FactSet` → `Fact` |
| `STRUCTURE_HAS_FACT_SET` | `Structure` → `FactSet` |
| `STRUCTURE_HAS_ASSOCIATION` | `Structure` → `Association` |
| `STRUCTURE_HAS_TAXONOMY` | `Structure` → `Taxonomy` |
| `ASSOCIATION_HAS_FROM_ELEMENT` / `ASSOCIATION_HAS_TO_ELEMENT` | `Association` → `Element` (parent / child) |
| `ASSOCIATION_HAS_CLASSIFICATION` | `Association` → `Classification` |
| `DIMENSION_HAS_AXIS_ELEMENT` / `DIMENSION_HAS_MEMBER_ELEMENT` | `Dimension` → `Element` |
| `ELEMENT_HAS_LABEL` / `ELEMENT_HAS_REFERENCE` | `Element` → `Label` / `Reference` |
| `TAXONOMY_HAS_LABEL` / `TAXONOMY_HAS_REFERENCE` | `Taxonomy` → `Label` / `Reference` |

### Key Traversal Patterns

**Fact level** — one reported value and its aspects. `Fact` links straight to its `Entity`, so a query for one company does not need to pass through `Report`:

```text
Entity ◀─[:FACT_HAS_ENTITY]─ Fact ─[:FACT_HAS_ELEMENT]─▶ Element (canonical_concept)
                              ├─[:FACT_HAS_PERIOD]─▶ Period
                              ├─[:FACT_HAS_UNIT]─▶ Unit
                              └─[:FACT_HAS_DIMENSION]─▶ Dimension   (breakdown facts only)
```

**Filing level** — the facts of one filing, when the question is "as this filing reported it":

```text
Entity ─[:ENTITY_HAS_REPORT]─▶ Report ─[:REPORT_HAS_FACT]─▶ Fact
```

**Statement level** — a whole statement, through the filing's structure. This is the path `financial-statement-analysis` takes:

```text
Report ─[:REPORT_HAS_FACT]─▶ Fact ◀─[:FACT_SET_CONTAINS_FACT]─ FactSet ◀─[:STRUCTURE_HAS_FACT_SET]─ Structure (canonical_type)
```

Every filing has its own `Structure` nodes, so tens of thousands share `canonical_type: 'income_statement'`. Anchor on the `Report` or `Entity` and reach `Structure` last; a query that leads with `Structure {canonical_type: ...}` scans them all and times out.

**Taxonomy level** — how a filing's elements roll up, through `Structure → Association → Element`. These arcs are in the graph, but a calculation check over one section is what `information-block` returns (every total with its children and a footing check), and it is the faster route.

### Enriched Properties

Two properties are added during processing, beyond what the filings carry:

- **`Element.canonical_concept`** — a normalized concept id shared across filers, with `canonical_confidence` beside it. Income statement: `revenue`, `cost_of_revenue`, `gross_profit`, `research_and_development`, `selling_general_admin`, `operating_expenses`, `operating_income`, `interest_income`, `interest_expense`, `nonoperating_income`, `pretax_income`, `income_tax_expense`, `net_income`, `eps_basic`, `eps_diluted`, `weighted_average_shares_basic`, `weighted_average_shares_diluted`. Balance sheet: `cash_and_equivalents`, `accounts_receivable`, `inventory`, `current_assets`, `property_plant_equipment`, `goodwill`, `intangible_assets`, `total_assets`, `accounts_payable`, `deferred_revenue`, `current_liabilities`, `long_term_debt`, `total_debt`, `total_liabilities`, `retained_earnings`, `stockholders_equity`, `total_liabilities_and_equity`, among others. Cash flow: `operating_cash_flow`, `capital_expenditures`, `investing_cash_flow`, `financing_cash_flow`, `dividends_paid`, `share_repurchases`, `net_change_in_cash`. An element with no mapping has a null `canonical_concept`.
- **`Structure.canonical_type`** — the statement a structure is: `income_statement`, `balance_sheet`, `cash_flow_statement`, `equity_statement` or `comprehensive_income`. Other structures carry a pattern label or null.

## The Four Rules

A query that ignores one of these returns a plausible wrong number, not an error. The platform's MCP server gives the same four to every client connected to `sec`.

### Rule 1: Consolidated Facts Only — `Fact {has_dimensions: false}`

A filer reports each figure as a consolidated total and often again broken down by segment, product line or geography. The breakdown facts carry dimensions; the total does not. Without the filter, the total and its breakdowns come back together, and summing them double-counts.

**Wrong:**

```cypher
MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'})
WHERE e.canonical_concept = 'revenue' AND p.end_date = $fy_end
RETURN sum(f.numeric_value) AS revenue
```

For one large filer's fiscal-year revenue, the unfiltered match returned the consolidated total plus fourteen segment and geography rows; summed, they came to about six times the reported figure.

**Right:**

```cypher
MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'})
WHERE e.canonical_concept = 'revenue' AND p.end_date = $fy_end
RETURN DISTINCT e.qname AS qname, f.numeric_value AS revenue
```

When you want the breakdown, filter `has_dimensions: true` and return the `Dimension` beside the value — and don't add the rows back to the total.

### Rule 2: Canonical Concepts Over a Single qname — `Element.canonical_concept`

Filers tag the same concept with different elements. Revenue is `us-gaap:Revenues` for some and `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` for others. A query on one qname silently drops every filer that chose the other.

**Wrong:**

```cypher
MATCH (ent:Entity)<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element {qname: 'us-gaap:Revenues'}),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'})
WHERE ent.ticker IN $tickers
RETURN DISTINCT ent.ticker AS ticker, p.end_date AS end_date, f.numeric_value AS revenue
```

In a three-company peer group, two of the three tag revenue `RevenueFromContractWithCustomerExcludingAssessedTax`, so this returned revenue for one company and nothing for the other two.

**Right:**

```cypher
MATCH (ent:Entity)<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'})
WHERE ent.ticker IN $tickers AND e.canonical_concept = 'revenue'
RETURN DISTINCT ent.ticker AS ticker, e.qname AS qname, p.end_date AS end_date,
       f.numeric_value AS revenue
```

Return `e.qname` beside the concept and read it. A concept can map to more than one element in the same filing — a subtotal and one of its components, for instance — so check that each row is the line you mean. `resolve-element` shows which qnames a concept covers for a given `ticker`.

### Rule 3: Pin the Period Shape — `duration_type` for Flows, `period_type: 'instant'` for Balances

Income-statement and cash-flow figures cover a span of time; balance-sheet figures are a point in time. A 10-Q reports a quarter and the year-to-date on the same end date, and a 10-K reports the year on the same end date as the last quarter. An unpinned `Period` mixes them.

**Wrong:**

```cypher
MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period)
WHERE e.canonical_concept = 'revenue'
RETURN DISTINCT p.end_date AS end_date, f.numeric_value AS revenue
ORDER BY end_date DESC
```

Rows that share an `end_date` can be a quarter, six or nine months, or a year, with nothing in the result to tell them apart.

**Right** — for a flow, pin `duration_type` (`quarterly`, `semi_annual`, `nine_months`, `annual`, or `other`):

```cypher
MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'quarterly'})
WHERE e.canonical_concept = 'revenue'
RETURN DISTINCT p.start_date AS start_date, p.end_date AS end_date, f.numeric_value AS revenue
ORDER BY end_date DESC
```

For a balance, pin `Period {period_type: 'instant'}` and match `end_date`, which carries the instant's date. `Element.period_type` says which shape a concept expects; it is a different property from `Period.period_type`.

### Rule 4: `RETURN DISTINCT`, Anchored on the Entity

The same fact reaches a query by more than one path: a figure sits in more than one `FactSet` of its filing, and later filings report it again as a comparative. Without `DISTINCT`, those paths come back as repeated rows. And a query must start from something selective — an `Entity` by `ticker` or `cik`, or one `Report` — so that it touches one filer's facts rather than the whole repository.

**Wrong:**

```cypher
MATCH (s:Structure {canonical_type: 'income_statement'})-[:STRUCTURE_HAS_FACT_SET]->(fs:FactSet)
      -[:FACT_SET_CONTAINS_FACT]->(f:Fact {has_dimensions: false})-[:FACT_HAS_ENTITY]->(ent:Entity {ticker: $ticker}),
      (f)-[:FACT_HAS_ELEMENT]->(e:Element)
RETURN e.qname, f.numeric_value
```

This leads with a node that tens of thousands of filings share, so it scans all of them before reaching the one company, and times out. Had it finished, it would have repeated rows.

**Right:**

```cypher
MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'}),
      (fs:FactSet)-[:FACT_SET_CONTAINS_FACT]->(f),
      (s:Structure {canonical_type: 'income_statement'})-[:STRUCTURE_HAS_FACT_SET]->(fs)
WHERE f.numeric_value IS NOT NULL
RETURN DISTINCT e.canonical_concept AS concept, e.qname AS qname,
       p.end_date AS end_date, f.numeric_value AS value
ORDER BY end_date DESC
LIMIT 40
```

`DISTINCT` collapses identical rows. If a later filing revised a figure, both values come back; anchor on one `Report` to read a figure as that filing reported it. Write every joined pattern into a single `MATCH` separated by commas — separate `MATCH` clauses on the same node can time out — and always end with `LIMIT`. Alias returned columns (`AS value`): an unaliased expression after `DISTINCT` comes back under a column name that includes the keyword.

## Worked Queries

Each query below follows all four rules. Pass the values in `parameters`.

### A Statement From a Company's Latest 10-K

Find the filing first, then read its income statement through the statement path, anchored on that `Report`:

```cypher
-- 1. The latest annual filing
MATCH (ent:Entity {ticker: $ticker})-[:ENTITY_HAS_REPORT]->(r:Report)
WHERE r.form IN ['10-K', '20-F', '40-F']
RETURN r.identifier AS report_id, r.form AS form, r.filing_date AS filing_date,
       r.fiscal_year_focus AS fiscal_year
ORDER BY filing_date DESC
LIMIT 1
```

```cypher
-- 2. Its income statement lines, current and comparative years
MATCH (r:Report {identifier: $report_id})-[:REPORT_HAS_FACT]->(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'}),
      (fs:FactSet)-[:FACT_SET_CONTAINS_FACT]->(f),
      (s:Structure {canonical_type: 'income_statement'})-[:STRUCTURE_HAS_FACT_SET]->(fs)
WHERE f.numeric_value IS NOT NULL
RETURN DISTINCT e.canonical_concept AS concept, e.qname AS qname,
       p.start_date AS start_date, p.end_date AS end_date, f.numeric_value AS value
ORDER BY end_date DESC, qname
LIMIT 60
```

For a cash flow statement, use `canonical_type: 'cash_flow_statement'`. For a balance sheet, use `'balance_sheet'` with `Period {period_type: 'instant'}`. `financial-statement-analysis` runs this same pair of steps and also keeps the most precise of duplicate facts.

### One Metric Across Eight Quarters

```cypher
MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'quarterly'})
WHERE e.canonical_concept = $concept AND f.numeric_value IS NOT NULL
RETURN DISTINCT e.qname AS qname, p.start_date AS start_date, p.end_date AS end_date,
       f.numeric_value AS value
ORDER BY end_date DESC
LIMIT 8
```

A fourth fiscal quarter is usually missing. A 10-K reports the year, not the quarter that ends it, so there is typically no three-month fact for Q4. Derive it as the annual figure minus the first three quarters of the same fiscal year, and say so when you report it. The eight rows above can therefore span more than two fiscal years.

### Peer Comparison by Canonical Concept

```cypher
MATCH (ent:Entity)<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'})
WHERE ent.ticker IN $tickers
  AND e.canonical_concept IN ['revenue', 'gross_profit']
  AND p.end_date >= $since
  AND f.numeric_value IS NOT NULL
RETURN DISTINCT ent.ticker AS ticker, e.canonical_concept AS concept, e.qname AS qname,
       p.end_date AS end_date, f.numeric_value AS value
ORDER BY ticker, end_date DESC, concept
LIMIT 50
```

The result shows each company's own `qname` for revenue beside the shared concept. Fiscal years differ: compare each company's fiscal year by its own `end_date` rather than assuming the years line up on the calendar.

### Balance Sheet at a Date

```cypher
MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {period_type: 'instant'}),
      (f)-[:FACT_HAS_UNIT]->(u:Unit)
WHERE p.end_date = $as_of
  AND e.canonical_concept IN ['cash_and_equivalents', 'current_assets', 'total_assets',
                              'current_liabilities', 'total_liabilities', 'stockholders_equity']
  AND f.numeric_value IS NOT NULL
RETURN DISTINCT e.canonical_concept AS concept, e.qname AS qname,
       f.numeric_value AS value, u.measure AS unit
ORDER BY concept
LIMIT 20
```

`$as_of` must be a date the company reports a balance sheet on — its fiscal quarter- or year-end — not an arbitrary calendar date. The latest filing's `report_date` is one.

### A Segment Breakdown

When the question is the breakdown itself, reverse rule 1 and return the dimension beside each value:

```cypher
MATCH (ent:Entity {ticker: $ticker})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: true})
      -[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'}),
      (f)-[:FACT_HAS_DIMENSION]->(d:Dimension)
WHERE e.canonical_concept = 'revenue' AND p.end_date = $fy_end
RETURN DISTINCT e.qname AS qname, d.axis AS axis, d.member AS member,
       f.numeric_value AS value, f.dimension_count AS dimensions
ORDER BY axis, value DESC
LIMIT 50
```

A filer usually breaks one figure down along several axes (segment, geography, product), and a fact with `dimension_count` above 1 sits on more than one axis at once. Sum within one axis only, and only over facts with `dimension_count = 1`. To read a whole note with its breakdowns and footing, `information-block` is the better tool.

## Other Limits

- **Text is not a Cypher question.** String matching (`CONTAINS`, `STARTS WITH`, `ENDS WITH`, `=~`) on `Fact.value` or `Fact.uri` is refused on `sec`. Search filing text with `search-documents` (MCP); read one disclosure whole with `disclosures`, then `information-block`.
- **Read-only.** `sec` and its subgraphs refuse every write, and the staging-table SQL endpoint returns 403 for shared repositories. Query the graph with Cypher.
- **Scope.** The repository holds 10-K, 10-Q, 20-F and 40-F XBRL filings filed since January 2024, updated daily. For one filing on your own machine, with no account, [xbrlkit](https://xbrlkit.com) reads it with the same schema.
- **Rate limits** follow your subscription plan; see [Shared Repositories](Shared-Repositories).

## Related Documentation

**Wiki Guides:**
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph)** - The Cypher endpoint, response modes, parameters, and the MCP query tools
- **[Shared Repositories](Shared-Repositories)** - Subscribing to `sec`, plans, and access
- **[AI Operators and MCP](AI-Operators-and-MCP)** - The MCP tool surface, including the views and `resolve-element`
- **[Search & AI Retrieval](Search-and-AI-Retrieval)** - Full-text and semantic search over filing text
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Loading SEC filings into a local stack

**Product Guide:**
- **[Analyze SEC filings](https://robosystems.ai/docs/guides/sec-filings)** - Connecting an AI client to the SEC graph, and what to ask of it

**Codebase Documentation:**
- **[SEC Adapter](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/sec/README.md)** - The SEC pipeline, enrichment, and MCP helpers
- **[Graph Schemas](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/schemas/README.md)** - The schema definitions behind the model

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
