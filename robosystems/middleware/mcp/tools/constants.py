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
Note: Element.period_type uses XBRL semantics (instant/duration) - different property!"""

# Numeric value documentation
NUMERIC_VALUE_GUIDANCE = """**🔢 NUMERIC VALUE:**
`Fact.numeric_value` stores the actual reported value (no scaling applied).
The `decimals` attribute indicates precision/rounding, not a multiplier."""

# Dimensional facts warning for facts-related tools
DIMENSIONAL_FACTS_WARNING = """**⚠️ CRITICAL - Dimensional Facts:**
~40% of facts have dimensional breakdowns (segments, geography, products).
To get CONSOLIDATED TOTALS only (avoiding duplicates), always filter:
- `WHERE f.has_dimensions = false` (recommended - uses indexed property)
- `WHERE NOT (f)-[:FACT_HAS_DIMENSION]->()`  (alternative pattern)
Without this filter, revenue queries return segment breakdowns + totals mixed together!

**Dimension Node:** The `Dimension` node represents axis-member pairs for segmentation.
- XBRL dimensions have `axis_uri`, `member_uri`, `is_explicit`, `is_typed` properties
- Generic dimensions use `axis`, `member`, `dimension_type` properties
- Relationships: `DIMENSION_HAS_AXIS_ELEMENT` and `DIMENSION_HAS_MEMBER_ELEMENT` link to Element nodes"""
