"""Workflow-playbook MCP tools.

These tools carry no data and touch no database. Their only job is to
**inform a multi-step workflow** — the canonical tool sequence, the
decisions an operator has to make, and the gotchas that aren't obvious
from any single tool's schema. They are the generic-workflow layer of the
three-layer guidance model: generic playbook → per-tenant procedure doc →
per-tenant data.

Why a tool and not external docs: the content is **version-locked to the
deployed build**. It ships in the same image as the tools it describes, so
it can never drift from the actual tool names, schemas, or sequence — and
it reaches every MCP client (Claude Code/Cowork *and* raw JSON-RPC
callers), not just ones that happen to load a skill file.

``get-close-playbook`` is the first one (the month-end close is the
highest-value, fully-working MCP workflow today). It's a pure read, so it
stays available on read-only graphs — an operator can learn the workflow
before they have write access.
"""

from typing import Any

# ── Playbook content (version-locked to this build) ──────────────────────────
#
# Authoritative source for the generic close workflow. Edit here when the
# tool surface changes; keep tenant-specific steps in the per-tenant
# procedures document, not in this constant.

_RECURRING_SEQUENCE: list[str] = [
  "get-fiscal-calendar — orient: read closed_through, close_target, "
  "closeable_now, blockers, catch_up_sequence. Confirm the period you intend "
  "to close is exactly closed_through + 1.",
  "If blockers include sync_stale: run sync-connection (incremental, no "
  "arguments needed — it auto-resolves the graph's sync connection). The "
  "default incremental window reaches back ~60 days: when the user edited "
  "months older than that, pass an explicit since_date covering the oldest "
  "edit or those changes are silently missed. Then VERIFY the sync: "
  "get-graph-sync-status reports each connection's last_sync_result "
  "(captured / updated / drift / dispatch_failed counts; a 'failed' status "
  "means the attempt raised and last_sync_at did not advance) — and poll "
  "get-fiscal-calendar until last_sync_at advances and the blocker clears. "
  "Prefer all of this over closing with allow_stale_sync=true.",
  "If the sync outcome reports reconciling_items > 0: those are "
  "transactions edited in the source system after they were synced. "
  "That's normal customer behavior, not an alarm. List them with "
  "list-event-blocks (is_reconciling_item=true), then work them one at a "
  "time: preview-reconciling-item shows the posted entries against the "
  "accepted payload and the per-account difference; agree the disposition "
  "with the user; resolve-reconciling-item carries it out and clears the "
  "flag. The three dispositions are RESTATE (regenerate the entries in "
  "place — prior months' figures change; the usual default inside the "
  "current fiscal year, and unavailable once a period is closed unless you "
  "reopen it), CATCH_UP (post the difference as an alignment entry in an "
  "open period — prior statements stand; the right answer when the "
  "reporting cadence has locked those months), and ACKNOWLEDGE (the "
  "difference was already handled by an entry you authored — pass a note "
  "and reference_event_id, and nothing is posted). Resolving is what makes "
  "a disposition stick: the flag returns on every sync until then. "
  "resolve-reconciling-item builds its catch-up entry local-only "
  "(source='system', publish_to_source=false) because it mirrors an edit "
  "already made upstream and publishing it would double-apply it. Don't "
  "close over unreviewed reconciling items.",
  "Schedules ending this period? (an asset sold or transferred, a prepaid "
  "cancelled or refunded): terminate BEFORE promoting obligations, or the "
  "sweep drafts entries the termination says should not exist. "
  "terminate-schedule ends a schedule at a month-end cutoff with no entry "
  "(truncates forward facts + voids the remaining obligation chain) — use "
  "it when the GL effect is already booked or none is wanted. "
  "create-event-block(event_type='asset_disposed') additionally posts the "
  "disposal/derecognition entry atomically — use it when that entry still "
  "needs to be booked.",
  "get-period-close-status (period_start/period_end as YYYY-MM-DD) — see "
  "which schedules are pending / drafted / posted and their amounts.",
  "promote-obligations (dispatch_handlers=true) — draft every matured "
  "schedule's closing entry for the period in one sweep. This is how "
  "schedule-derived drafts come into being; there is no create-closing-entry "
  "tool. Reaches stranded obligations too (already 'classified' by a "
  "flip-only background sweep but never drafted — reported via "
  "stranded_count). Idempotent. Then verify: list-period-drafts should show "
  "one draft per active schedule for the period; investigate any gap before "
  "closing.",
  "(optional) create-event-block(event_type='journal_entry_recorded') — for "
  "any manual / one-off adjusting entries that aren't schedule-driven.",
  "list-period-drafts (period as YYYY-MM) — review every draft with DR/CR "
  "detail; check all_balanced and the QB outbox fields "
  "(will_publish_to_qb / qb_publish_count / local_only_count).",
  "Summarize to the user (totals, balance check, per-schedule amounts, what "
  "will publish to QuickBooks) and get explicit approval.",
  "close-period (period as YYYY-MM) — atomic: posts the drafts, runs the "
  "balance-sheet equation check, advances closed_through. Use "
  "allow_stale_sync=true only when the user has verified QB is complete "
  "despite a stale sync — normally run sync-connection instead (see the "
  "sync_stale step above). The close runs on the worker, so a heavy month "
  "can outlast the call: if you get status='in_progress' with an "
  "operation_id, the close is already running and NEVER needs re-calling. "
  "Poll get-period-close-status (period_status flips to closed and "
  "close_receipt fills in) or get-fiscal-calendar (has_close_receipt) "
  "every ~10s, then read the receipt. worker_started=false just means it "
  "is queued behind a busy worker; the instruction is the same.",
  "Post-close verification — read the receipt back to the user, don't "
  "assume: entries_posted should equal the reviewed draft count "
  "(entries_published_to_qb / entries_posted_locally carry the split); "
  "statements_stamped=true with statement_rule_summary passing; "
  "get-period-close-status shows every schedule posted. The close marks "
  "the graph stale and the rebuild pipeline picks it up — poll "
  "get-graph-sync-status until sync_status=fresh before running graph "
  "reads (statements, fact grids) against the newly closed month.",
  "If the tenant runs forecast scenarios: re-run compute-forecast for each "
  "forecast block after the close so the actuals/forecast seam advances to "
  "the new closed_through. Nothing needs re-basing — the walk re-anchors on "
  "the newest closed month by itself and reports the move in its "
  "diagnostics, so the scenario's levers stay as authored. Read "
  "anchor_period on the response to confirm it advanced. A scenario whose "
  "horizon the close has passed entirely computes no months and says so in "
  "its diagnostics — its stale months are cleared and its levers kept, so "
  "extending horizon_months and re-running is all it needs. Scenarios "
  "deliberately built on a counterfactual base carry base_anchor='fixed' "
  "and stay put. Note the anchor only ever advances onto a month the close "
  "stamped: a report published for an open month does not move it.",
]

_INITIATE_SEQUENCE: list[str] = [
  "ORIENT FIRST — call get-fiscal-calendar and read `closed_through` (the "
  "watermark) and `close_target`. `closed_through` may be a BASELINE set at "
  "initialization (e.g. '2026-05') even when those months were never closed "
  "period-by-period in RoboLedger — the books were maintained elsewhere "
  "(QuickBooks) and the watermark says 'treat everything through here as "
  "already handled'. The first close you'll run is `closed_through + 1`. "
  "Note this date — every schedule you create must carry the matching "
  "`closed_through` (see SCHEDULE AUTHORING).",
  "Discover the recurring adjusting entries this company books every month "
  "(depreciation, amortization, prepaid roll-off, accruals). Two "
  "complementary bootstrap modes — use both:",
  "  • Derive from history — query a prior posted close to reconstruct the "
  "recurring entries: read-graph-cypher / financial-statement-analysis, or "
  "list-period-drafts on an already-closed period. History gives you the "
  "resulting amounts and the accounts touched.",
  "  • Interview the user — history gives amounts but NOT the schedule "
  "parameters (cost basis, useful life, method, roll-off, disposal terms) "
  "needed for forward-looking schedules. Ask for the source schedules (e.g. "
  "the depreciation / amortization / prepaid worksheets). You can't infer "
  "'5-yr straight-line on $2,544' from a $42.41/mo line.",
  "For each recurring entry, create a schedule with "
  "create-information-block(block_type='schedule'). See "
  "SCHEDULE AUTHORING below — especially one-pair-per-schedule and the "
  "closed_through watermark rule (a schedule that omits it will block the "
  "first close).",
  "(recommended) Write a per-tenant 'Month-End Close Procedures' document "
  "with create-document capturing THIS company's schedules, accounts, and "
  "quirks. Have it reference this playbook for the generic mechanics. Future "
  "closes start by reading it (search-documents).",
  "Run the first close using the recurring sequence.",
]

_SCHEDULE_AUTHORING: list[str] = [
  "ONE SCHEDULE = ONE debit element + ONE credit element pair. The "
  "entry_template has a single debit_element_id and a single "
  "credit_element_id — there is no multi-leg form. A real multi-line entry "
  "(e.g. depreciation across several asset classes) is therefore MULTIPLE "
  "schedules, one per asset/element pair, each with its own basis and its "
  "own SumEquals proof. Do not try to cram several accounts into one "
  "schedule.",
  "USE COA ELEMENT IDs, NOT QNAMES. debit_element_id / credit_element_id / "
  "element_ids take chart-of-accounts element ids (the `id` from "
  "get-unmapped-elements / get-graph-schema), NOT taxonomy qnames like "
  "'us-gaap:Depreciation'. Discover the real ids first with "
  "get-unmapped-elements (match by name/code), get-graph-schema, "
  "suggest-mapping, or read-graph-cypher — never invent them. (The "
  "rollforward block type is the exception: it takes *_qname fields and "
  "resolves them server-side.)",
  "PERIOD HORIZON: period_start..period_end is the schedule's full life — "
  "for depreciation/amortization that's period_start + useful life; for a "
  "prepaid it's the roll-off horizon. These schedules are finite by design, "
  "not open-ended; if you don't know the useful life, that's exactly what "
  "the interview must establish (history gives the monthly amount, not the "
  "horizon).",
  "OMITTING schedule_metadata IS SAFE. The SumEquals proof is only generated "
  "when you supply original_amount (cost basis); a monthly_amount-only "
  "schedule simply books monthly_amount each period with no failing rule. "
  "Add schedule_metadata later when you have the basis/useful life.",
  "AMOUNTS ARE IN CENTS. monthly_amount and every schedule_metadata amount "
  "(original_amount, residual_value) are integer cents. $194.83 → 19483.",
  "STRAIGHT-LINE vs CUSTOM CURVE: the default method distributes "
  "monthly_amount evenly (final period absorbs rounding). For a non-linear "
  "curve (effective-interest amortization, variable leases) set "
  "schedule_metadata.periodic_amounts to explicit per-period cents — its "
  "length must match the number of months and its sum must equal "
  "original_amount.",
  "MATCH THE CALENDAR WATERMARK — set the schedule's closed_through to the "
  "last day of the calendar's closed_through month (calendar '2026-05' → "
  "schedule closed_through '2026-05-31'). This flags every period at/before "
  "the watermark as 'historical' and emits their obligations as 'voided', so "
  "the close workflow starts drafting at the first open period (e.g. June). "
  "This is the SAME rule whether those prior months were actually closed in "
  "RoboLedger or just baseline-watermarked at initialization. WARNING: if "
  "you omit closed_through (or set it earlier), every pre-watermark period "
  "becomes a 'pending' obligation, and the first close is BLOCKED with the "
  "pending_obligations gate (earliest_pending_period pointing months back). "
  "Conversely, re-drafting an already-closed month creates duplicate "
  "entries — the watermark prevents both.",
]

_KEY_RULES: list[str] = [
  "PERIOD IDENTIFIERS VARY BY TOOL: list-period-drafts and close-period take "
  "period='YYYY-MM'; get-period-close-status takes "
  "period_start/period_end='YYYY-MM-DD'; the schedule payload's "
  "period_start/period_end are dates ('YYYY-MM-DD'). Match each tool's shape.",
  "NO create-closing-entry TOOL. Schedule drafts come from "
  "promote-obligations (sweep) or "
  "create-event-block(event_type='schedule_entry_due') (single schedule); "
  "manual entries from create-event-block(event_type='journal_entry_recorded').",
  "block_type ENUM OVERSTATES CAPABILITY: only 'schedule' and 'rollforward' "
  "are constructible via create-information-block. The statement types "
  "(balance_sheet, income_statement, cash_flow_statement, equity_statement, "
  "comprehensive_income) and 'metric' return HTTP 501 — statements are built "
  "via create-report.",
  "CLOSE BLOCKERS: sequence_violation (close in order), period_incomplete "
  "(month not over yet), sync_stale (QuickBooks sync older than period end "
  "— run sync-connection and poll get-fiscal-calendar until it clears), "
  "calendar_not_initialized, pending_obligations (matured "
  "schedule_entry_due events still 'pending' at/before the period — run "
  "promote-obligations to draft them, or, if they belong to a "
  "pre-watermark month that should never close, the schedule's "
  "closed_through was set wrong; earliest_pending_period names the oldest "
  "one), and stranded_obligations (matured obligations already "
  "'classified' but with no drafted closing entry — adjusting entries the "
  "close would otherwise silently omit; promote-obligations with "
  "dispatch_handlers=true drafts them, or void the obligation). Resolve "
  "before close-period; get-fiscal-calendar reports them.",
  "JE PUBLISH SEMANTICS: journal_entry_recorded events with source='manual' "
  "publish to QuickBooks at close through the outbox; source='system' posts "
  "locally only. Set metadata.publish_to_source to decide explicitly rather "
  "than by source: false keeps an entry local whatever its source, true "
  "publishes one that otherwise would not. Either way, an alignment/catch-up "
  "entry that mirrors an edit already made in the source system must stay "
  "local — publishing it would double-apply the edit upstream; "
  "publish_to_source=false says so in the entry itself. list-period-drafts "
  "shows the split (will_publish_to_qb per draft) before anything commits.",
  "SCHEDULE TERMINATION: terminate-schedule ends a schedule early at a "
  "month-end cutoff with no entry (truncates forward facts + voids the "
  "remaining obligation chain past the cutoff); "
  "create-event-block(event_type='asset_disposed') does the same void and "
  "also posts the disposal/derecognition entry atomically. Terminate "
  "BEFORE promote-obligations. delete-information-block is NOT a "
  "termination — it erases the schedule's history.",
  "CHECK THE PER-TENANT PROCEDURES DOC FIRST: call search-documents for a "
  "'close procedures' / 'month-end' document before authoring or closing. It "
  "captures this company's specifics and should reference this playbook.",
]


def _build_playbook(mode: str) -> dict[str, Any]:
  """Assemble the playbook payload for the requested mode."""
  overview = (
    "The month-end close turns a synced general ledger into a locked, "
    "reported fiscal period. Recurring adjusting entries (depreciation, "
    "amortization, prepaid roll-off) are modeled as SCHEDULES that "
    "auto-emit period obligations; closing a period drafts those entries, "
    "posts them, and advances the calendar. Run get-close-playbook whenever "
    "you're unsure of the sequence — it is version-locked to this build."
  )
  payload: dict[str, Any] = {
    "version_note": (
      "Version-locked to the deployed build: tool names, the sequence, and "
      "the rules below match the tools currently exposed by this server."
    ),
    "mode": mode,
    "overview": overview,
  }
  if mode in ("initiate", "overview"):
    payload["initiate_first_close"] = _INITIATE_SEQUENCE
    payload["schedule_authoring"] = _SCHEDULE_AUTHORING
  if mode in ("recurring", "overview"):
    payload["recurring_close_sequence"] = _RECURRING_SEQUENCE
  payload["key_rules"] = _KEY_RULES
  payload["related_tools"] = [
    "get-fiscal-calendar",
    "get-period-close-status",
    "create-information-block",
    "promote-obligations",
    "create-event-block",
    "list-period-drafts",
    "close-period",
    "backfill-plan-history",
    "search-documents",
    "get-graph-schema",
    "get-unmapped-elements",
    "suggest-mapping",
  ]
  return payload


# ────────────────────────────────────────────────────────────────────────────
# get-close-playbook
# ────────────────────────────────────────────────────────────────────────────


class GetClosePlaybookTool:
  """Return the version-locked month-end close playbook (guidance only)."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-close-playbook",
      "description": """Get the canonical month-end close playbook — the tool sequence, the setup decisions, and the gotchas that no single tool's schema makes obvious. **Call this FIRST when setting up or running a close** (or whenever you're unsure what to call next). Guidance only — no data, no side effects, safe on read-only graphs. Version-locked to this build, so it never drifts from the actual tools.

**WHEN TO USE:**
- Before authoring recurring schedules (depreciation/amortization/prepaid) for the first time → mode="initiate"
- Before running a monthly close → mode="recurring"
- Any time the close workflow is unclear → mode="overview" (default)

**RELATED TOOLS:** call `search-documents` for this company's own "close procedures" document — it captures tenant-specific accounts/quirks and references this generic playbook.

**RETURNS:** the close overview, the ordered tool sequence for the chosen mode, schedule-authoring rules (incl. one-schedule-per-debit/credit-pair), and key gotchas (period-identifier shapes, amounts-in-cents, which block types 501, close blockers).""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "mode": {
            "type": "string",
            "enum": ["overview", "initiate", "recurring"],
            "description": (
              "Which slice of the playbook to return. 'initiate' = first-time "
              "setup (bootstrap schedules from history + interview). "
              "'recurring' = run a monthly close. 'overview' (default) = both."
            ),
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    mode = arguments.get("mode") or "overview"
    if mode not in ("overview", "initiate", "recurring"):
      mode = "overview"
    return _build_playbook(mode)
