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
LEDGER_STATUS_GUIDANCE = """**⚠️ LEDGER STATUS FILTERING (Event / Entry / Transaction):**
The graph is a faithful mirror of the ledger and KEEPS cancelled and replaced rows —
voided and superseded entries are NOT removed (they are real audit history). When you
COUNT or AGGREGATE ledger-spine data you MUST filter to live rows, or voided/reversed
amounts will inflate the result:
- `Entry.status` ∈ {draft, posted, reversed}. For balances and debit/credit sums,
  match ONLY `e.status = 'posted'`. `draft` = unposted (includes entries belonging to
  voided events); `reversed` = superseded by a reversing entry.
- `Event.status` ∈ {captured, classified, committed, pending, fulfilled, voided,
  superseded}. EXCLUDE `voided` and `superseded` from counts/sums. For open
  obligations use the positive set the question implies (e.g. committed/fulfilled/pending).
- `Transaction` exposes only a `pending` boolean in the graph (NOT the full status),
  so a voided transaction is indistinguishable at the Transaction node. To measure
  realized economic effect, aggregate through `Entry`/`LineItem` filtered to
  `Entry.status = 'posted'` — do NOT sum `Transaction.amount` directly.
- `Fact` nodes (the XBRL hypercube / published statements) have NO status and are
  already filtered at generation time — they are always safe to aggregate. This note
  applies ONLY to the ledger spine, not to Fact queries."""
