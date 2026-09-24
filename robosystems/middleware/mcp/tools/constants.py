"""Guidance text shared across MCP tool descriptions."""

QUERY_PATTERN_GUIDANCE = """**⚠️ QUERY PATTERN NOTE:**
When joining multiple relationships from the same node, use comma-separated patterns
in a SINGLE MATCH clause (not multiple MATCH clauses):
- ✅ GOOD: `MATCH (f:Fact)-[:R1]->(a), (f)-[:R2]->(b)`
- ❌ BAD: `MATCH (f:Fact)-[:R1]->(a) MATCH (f)-[:R2]->(b)` (may timeout)"""

PERIOD_TYPE_GUIDANCE = """**📅 PERIOD.period_type VALUES:**
Period nodes classify time context into three types:
- `instant` - Point-in-time (balance sheet dates)
- `duration` - A date range (income statement periods) — use `duration_type` for subtype
- `forever` - Unbounded period (rare)

**📅 PERIOD.duration_type VALUES** (only when period_type = 'duration'):
- `quarterly` - ~3 months duration
- `semi_annual` - ~6 months duration
- `nine_months` - ~9 months duration
- `annual` - ~12 months duration
- `other` - Non-standard durations
Note: Element.period_type indicates the expected period type for that metric - different from Period.period_type!"""

LEDGER_STATUS_GUIDANCE = """**⚠️ LEDGER STATUS FILTERING (Event / Entry / Transaction / LineItem):**
The graph is a faithful mirror of the ledger and KEEPS cancelled and replaced rows —
voided/superseded events and draft/reversed entries are NOT removed (they are real audit
history). When you COUNT or AGGREGATE ledger-spine data you MUST restrict to live rows,
or cancelled amounts inflate the result.

**Use the materialized `is_live` boolean — one rule on EVERY spine node:**
`(e:Entry) WHERE e.is_live`, `(li:LineItem) WHERE li.is_live`,
`(ev:Event) WHERE ev.is_live`, `(t:Transaction) WHERE t.is_live`. It is the safe default;
prefer it over hand-writing per-node status filters (which differ by node and are easy to
get wrong).

What `is_live` means per node (the equivalent `status` filter, if you need finer control):
- `Entry.is_live` ⇔ `status IN ('posted','reversed')` (∈ {draft, posted, reversed}). Both
  have landed in the books. A `reversed` original KEEPS its line items: the reversing entry
  that corrects it is an ordinary `posted` row, so the pair nets to zero only if you sum
  both halves — drop the original and the balance is off by the entry, with the sign
  flipped. `draft` is excluded and covers entries of voided events.
- `LineItem.is_live` ⇔ its parent Entry has landed. LineItem has no status of its own; the
  flag is denormalized so you can filter without joining back to Entry.
- `Event.is_live` ⇔ `status NOT IN ('voided','superseded')` (∈ {captured, classified,
  committed, pending, fulfilled, voided, superseded}). It KEEPS open obligations
  (pending/committed/fulfilled) — for a specific realized set, filter `status` explicitly.
- `Transaction.is_live` ⇔ `status <> 'void'`. (The `pending` boolean is also still exposed.)
- `Fact` nodes (the XBRL hypercube / published statements) have NO status and are already
  filtered at generation time — always safe to aggregate. This note applies ONLY to the
  ledger spine, not to Fact queries."""

LEDGER_ANCHOR_GUIDANCE = """**⚠️ LEDGER TRAVERSAL ANCHOR — start at `Entry`, not `Transaction`:**
`Transaction` is NOT the top of the ledger and NOT a layer every entry passes through.
It exists only where a source system had a record of the thing (QuickBooks, a bank feed)
or where a manual journal entry minted one. **`Entry.transaction_id` is nullable by
design**, and the schedule engine and event handlers create entries with NO parent
Transaction at all — every depreciation, amortization, accrual and other period-close
adjusting entry is parentless.

So this pattern silently loses data:
- ❌ `MATCH (t:Transaction)-[:TRANSACTION_HAS_ENTRY]->(e:Entry)-[:ENTRY_HAS_LINE_ITEM]->(li)`
- ✅ `MATCH (e:Entry)-[:ENTRY_HAS_LINE_ITEM]->(li:LineItem)`

It returns rows, raises no error, and omits every parentless entry. On a real tenant this
dropped 30 of 77 entries in a month, and a depreciation-expense question anchored on
Transaction returned **zero rows** against a real non-zero answer.

**The rule:**
- Amounts, balances, "what was posted", anything accounting → anchor at `Entry` or
  `LineItem`. Join UP to `Transaction` with OPTIONAL MATCH only if you need it.
- Source-system attributes (merchant name, due date, reference number, connection) →
  `Transaction` is the right node, because those fields live nowhere else.
- Business occurrences ("how many X happened") → anchor at `Event`. Events are the
  canonical layer; transactions are derived from a SUBSET of them. Many events
  legitimately have no Transaction and no Entry.

`Fact` queries are unaffected — the hypercube is generated from posted entries already."""

# The OLTP ledger and the close tools speak cents; the graph holds dollars. A
# model carrying the cents rule over from another tool reports every figure
# a hundred times too small.
LEDGER_AMOUNT_GUIDANCE = """**⚠️ LEDGER AMOUNTS ARE IN DOLLARS, NOT CENTS:**
`LineItem.debit_amount`, `LineItem.credit_amount`, `Transaction.amount` and
`Event.amount` are decimal amounts in the ledger's currency: `2835000.0` is
$2,835,000.00. Report them as returned and never divide by 100. Only the close
and schedule tools (`list-period-drafts`, schedule and allocation inputs) work in
integer cents; that convention does not apply to graph queries."""

# The same trap: the API returns cost_basis / current_value in cents, the
# graph's Position holds dollars under the same names.
INVESTOR_AMOUNT_GUIDANCE = """**⚠️ POSITION AMOUNTS ARE IN DOLLARS IN THE GRAPH:**
`Position.cost_basis` and `Position.current_value` are decimal amounts in the
position's `currency`: `125000.5` is $125,000.50. The RoboInvestor API and GraphQL
return the same field names as integer cents, with `cost_basis_dollars` and
`current_value_dollars` beside them. Never divide a graph amount by 100, and
convert before comparing a graph amount with an API or GraphQL one."""
