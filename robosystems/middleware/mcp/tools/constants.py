"""
Shared constants for MCP tools.
"""

# Query pattern guidance shared across multiple tools
QUERY_PATTERN_GUIDANCE = """**⚠️ QUERY PATTERN NOTE:**
When joining multiple relationships from the same node, use comma-separated patterns
in a SINGLE MATCH clause (not multiple MATCH clauses):
- ✅ GOOD: `MATCH (f:Fact)-[:R1]->(a), (f)-[:R2]->(b)`
- ❌ BAD: `MATCH (f:Fact)-[:R1]->(a) MATCH (f)-[:R2]->(b)` (may timeout)"""

# Period type documentation shared across multiple tools
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

# Ledger lifecycle / status filtering — shared across tools that may touch the
# tenant roboledger ledger spine (Event / Transaction / Entry / LineItem). The
# graph mirrors the FULL ledger including cancelled/replaced rows; readers must
# filter to live rows or voided/reversed amounts inflate counts and sums.
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
- `Entry.is_live` ⇔ `status = 'posted'` (∈ {draft, posted, reversed}). Only posted entries
  affect balances; `draft` includes entries of voided events, `reversed` = corrected by a
  reversing entry.
- `LineItem.is_live` ⇔ its parent Entry is posted. LineItem has no status of its own; the
  flag is denormalized so you can filter without joining back to Entry.
- `Event.is_live` ⇔ `status NOT IN ('voided','superseded')` (∈ {captured, classified,
  committed, pending, fulfilled, voided, superseded}). It KEEPS open obligations
  (pending/committed/fulfilled) — for a specific realized set, filter `status` explicitly.
- `Transaction.is_live` ⇔ `status <> 'void'`. (The `pending` boolean is also still exposed.)
- `Fact` nodes (the XBRL hypercube / published statements) have NO status and are already
  filtered at generation time — always safe to aggregate. This note applies ONLY to the
  ledger spine, not to Fact queries."""

# Ledger traversal anchor — which node a ledger read starts from. Companion to
# LEDGER_STATUS_GUIDANCE: that one is about which rows are live, this one is
# about which rows are reachable at all. Transaction is a PARTIAL layer (it
# exists only where a source system had a record), so anchoring a traversal on
# it silently drops every parentless Entry — no error, just a short answer.
# Same failure shape as the status omission, and it reproduces at scale for the
# same reason: Operators generate Cypher dynamically.
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
