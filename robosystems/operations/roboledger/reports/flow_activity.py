"""Which concepts the cash flow reads gross, from the cash lines themselves.

A concept with an ``activityType`` trait is a flow: cash received or paid,
whichever section it sits in. Investing and financing flows are all of
that kind; an operating one is the exception the indirect method needs, a
receipt that is neither net income nor a change in working capital (a
distribution from an equity-method investee). The indirect method's
reconciling items (add-backs, working-capital changes) carry no activity
trait, which is how the two are told apart.
"""

FLOW_ACTIVITIES: tuple[str, ...] = (
  "investingActivity",
  "financingActivity",
  "operatingActivity",
)

# For SQL: ``t.identifier IN {FLOW_ACTIVITIES_SQL}``.
FLOW_ACTIVITIES_SQL = "(" + ", ".join(f"'{a}'" for a in FLOW_ACTIVITIES) + ")"
