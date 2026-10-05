# Period Close

Closing a month in RoboLedger is one atomic operation that checks the month is ready, posts its entries, stamps its statements and records a receipt. It publishes RoboLedger-originated drafts to QuickBooks where the graph writes back, posts the rest, and advances the close boundary. This page covers the fiscal calendar the close runs against, the blockers that stop it, the account reconciliations it waits on, the close itself and its receipt, reopening a month, ending a schedule early, and the write gate that makes closed months immutable.

The product doc *Close the month with your AI assistant* at roboledger.ai describes the workflow for end users. This page covers the operations, payloads and guarantees underneath.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [Overview](#overview)
- [The Fiscal Calendar](#the-fiscal-calendar)
- [Blockers](#blockers)
- [Reconciliations](#reconciliations)
- [Preparing a Close](#preparing-a-close)
- [Closing a Period](#closing-a-period)
- [What a Close Does](#what-a-close-does)
- [Closing from MCP: Async and Polling](#closing-from-mcp-async-and-polling)
- [Reopening a Period](#reopening-a-period)
- [Ending a Schedule Early](#ending-a-schedule-early)
- [The Closed-Period Write Gate](#the-closed-period-write-gate)
- [Gotchas and Pitfalls](#gotchas-and-pitfalls)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

The close workflow lives on the RoboLedger operations surface:

| Operation | Purpose |
| --- | --- |
| `initialize` | One-time: create the fiscal calendar and seed its periods. A QuickBooks graph gets this on its first sync. |
| `set-close-target` | Set the month you are working toward. Closes nothing. |
| `promote-obligations` | Draft the month's schedule entries now instead of waiting for the background sweep |
| `terminate-schedule` | End a schedule at a month-end with no entry booked |
| `close-period` | Lock one month |
| `reopen-period` | Unlock the most recently closed month |
| `backfill-plan-history` | Stamp statements for historical months behind the boundary. See [Forecasting and Metrics](Forecasting-and-Metrics#plan-history). |
| `preview-reconciliations`, `refresh-reconciliations`, `record-statement-balance`, `set-reconciliation-policy`, `sign-off-reconciliation` | Compare the ledger's balances with independent ones at a month-end, and sign the comparisons off. See [Reconciliations](#reconciliations). |

Reads come from GraphQL (`fiscalCalendar`, `periodDrafts`, `periodCloseStatus`, `reconciliations`) or from the MCP tools `get-fiscal-calendar`, `get-period-close-status` and `list-period-drafts`. An assistant running a close over MCP should call `get-close-playbook` first. It returns the full tool sequence and the setup decisions.

```bash
export ROBOSYSTEMS_API_KEY=rfs...
export GRAPH_ID=kg...
```

## The Fiscal Calendar

The calendar tracks two cursors:

- **`closed_through`**: the system boundary, the last month actually locked. `null` before the first close.
- **`close_target`**: the user's goal, the month being worked toward. Setting it closes nothing.

Months close **sequentially**. The month passed to `close-period` must be exactly `closed_through + 1`, or, on a calendar that has never closed, the earliest open month. Months are named `YYYY-MM`. Each has a `FiscalPeriod` row whose status moves `open → closed`, and a reopened month sits at `closing` until it is closed again.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ fiscalCalendar { closedThrough closeTarget gapPeriods catchUpSequence closeableNow blockers syncStaleDays reconcilingItemCount pendingObligationCount strandedObligationCount unpostedSourceEventCount unreconciledAccountCount lastSyncAt } }"}'
```

`catchUpSequence` lists the months a run from `closedThrough` to `closeTarget` would close, in order. `closeableNow` and `blockers` describe the next of them. The same gate runs inside `close-period`, so a month the calendar shows as closeable closes unless something changes in between.

`set-close-target` moves the goal:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/set-close-target" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"period": "2026-09"}'
```

When a close catches up to the target, the target advances on its own (`target_auto_advanced` in the result).

## Blockers

The closeable gate returns structured codes. The calendar read lists them, and `close-period` refuses with them.

| Code | Meaning | Resolution | Override |
| --- | --- | --- | --- |
| `sequence_violation` | The month isn't `closed_through + 1` | Close the earlier months first, in order | — |
| `period_already_closed` | The month is at or before `closed_through` | Nothing to do, or reopen it | — |
| `period_incomplete` | The month isn't over. A month closes from the day **after** its last calendar day, so late same-day postings can't be missed. | Wait | — |
| `sync_stale` | The graph has a sync connection and its last successful sync finished before the month's last day, or it has never synced. `sync_stale_days` says by how much. | Run a sync and let it finish | `allow_stale_sync`, only after verifying the source data by hand |
| `pending_obligations` | A schedule's entry for this month or earlier is still `pending`: its obligation hasn't been promoted and drafted | `promote-obligations` | — |
| `stranded_obligations` | An obligation was promoted to `classified` but its entry was never drafted | `promote-obligations` with `dispatch_handlers: true` | `allow_stranded_obligations` |
| `reconciling_items` | A posted transaction dated in or before this month changed in QuickBooks and nobody has settled it | `preview-reconciling-item`, then `resolve-reconciling-item` | `allow_reconciling_items` |
| `unposted_source_events` | A source event dated in this month was captured or classified but never committed, so it has no entry. See [Unposted source events](#unposted-source-events). | Commit each one with `update-event-block`, or void it | `allow_unposted_source_events` |
| `unreconciled_accounts` | A reconciliation the close waits on isn't reconciled for this month, or was never compared for it. See [Reconciliations](#reconciliations). | `refresh-reconciliations` and clear what it reports, sign off where a review is required, or release the reconciliation with `set-reconciliation-policy` | `allow_unreconciled_accounts` |
| `calendar_not_initialized` | No fiscal calendar yet | `initialize`, or connect a data source | — |

Detail fields accompany some codes: `pending_obligation_count`, `pending_obligation_sample[]` (up to five, each with `event_id`, `schedule_id`, `schedule_name` and `period`), `earliest_pending_period`, `stranded_obligation_count`, `stranded_obligation_sample[]`, `reconciling_item_count`, `reconciling_item_sample[]` (source ids), `unposted_source_event_count`, `unposted_source_event_sample[]` (source ids, or event ids where there is none), `unreconciled_account_count`, `unreconciled_account_sample[]` (each reconciliation's name and why it isn't ready, such as `"Source ledger (QuickBooks): unreconciled"` or `"...: awaiting review"`) and `sync_stale_days`. The calendar read carries the same fields in camelCase.

Every override the close accepts is written into the close's audit note. A close over stranded obligations knowingly leaves those adjusting entries out of the month. A close over reconciling items stamps statements that may disagree with QuickBooks. A close over unposted source events leaves those transactions out of the month for good, and a close over unreconciled accounts closes the books without that check. Settling the item is almost always the better choice. See [QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy#reconciling-items).

### Unposted source events

A source event is a transaction that arrived from outside RoboLedger, such as a QuickBooks bill. Most post on their own: the sync commits them and their handler writes the entry. One that stopped at `captured` or `classified`, with no entry behind it, would be locked out for good once its month closed, because the write gate refuses a commit into a closed month. The gate counts every such event whose posting date falls in the month. A QuickBooks transaction whose automatic posting failed is the usual case. Manual journal entries don't count, because their drafted entry already exists and the close posts it, and neither do schedule obligations, which have blockers of their own.

To clear one, commit it with `update-event-block` (`transition_to: "committed"`) once its accounts are right, or retire it with `transition_to: "voided"` when it shouldn't be in the books. Find them with GraphQL `eventBlocks(status:)` or MCP `list-event-blocks`, filtered to `captured` or `classified`.

## Reconciliations

A reconciliation compares the ledger's balance at a month-end with a balance from somewhere outside the ledger, and records whether they agree. The close waits on the ones marked as required, so a month doesn't close on books nobody has checked against an independent source.

There are three methods, each answering a different question:

| Method | Compares | Scope | A difference means |
| --- | --- | --- | --- |
| `source_ledger` | Every account against the synced QuickBooks company's own trial balance | One block for the whole ledger | The ledger's copy of the books has drifted from the source: a transaction deleted or back-dated there after it synced, or activity not yet synced |
| `schedule_register` | Each asset account a schedule carries a balance on (a prepaid, accumulated depreciation, a fixed-asset cost account) against what its schedules say it holds | One block per account | A balance with no schedule behind it, or a scheduled amount the ledger doesn't hold |
| `statement` | One account against the ending balance of a statement recorded for it (bank, card or loan) | One block per account | Activity one side has and the other doesn't yet, or an error on either |

Balance-sheet accounts are compared cumulatively to the month-end, and income and expense accounts from the start of the fiscal year. The `source_ledger` side counts landed entries only. The `schedule_register` and `statement` sides count the ledger as the close will leave it, drafts awaiting the close included. Each reconciliation is an Information Block of type `reconciliation`, so its comparisons are FactSets like any other block's.

### Preview and refresh

`preview-reconciliations` runs one method for a month and writes nothing. `method` defaults to `source_ledger`, and `include_tied: true` adds the accounts that agree to the rows. Each row carries both balances, the difference, and a status: `tied`, `different`, `not_in_ledger` or `not_in_source`. `total_difference` is the sum of absolute differences, not a net figure.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/preview-reconciliations" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"period": "2026-08", "method": "schedule_register"}'
```

`refresh-reconciliations` runs every check that applies at the month-end and records each result on its block. Running it again replaces the month's comparison, so the answer is always as of the last run. It returns every reconciliation's standing for the month, with `notes[]` naming any check it couldn't run, such as a source comparison skipped because QuickBooks didn't answer. When no check applies at all, it returns `409`.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/refresh-reconciliations" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"period": "2026-08"}'
```

### Which checks hold the close

A block is created the first time its check applies, and whether it starts out holding the close depends on its method:

- **Source ledger.** The first refresh on a ledger synced from QuickBooks creates its block, and the close waits on it from then on.
- **Schedule register.** The first refresh creates a block for each account a schedule carries a balance on. One that ties starts out holding the close. One that doesn't is created without holding it, so a difference found on first contact is reported rather than freezing the month. Turn `required_for_close` on once it has been cleared.
- **Statement.** The first statement recorded for an account creates its block, which doesn't hold the close until `required_for_close` is turned on. A required statement is a statement owed every month.

### Recording a statement balance

`record-statement-balance` records a statement's ending balance for one balance-sheet account and reconciles the account to it for the month the statement ends in. It writes no books.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/record-statement-balance" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"element_id": "elem_...", "as_of": "2026-08-31", "balance": 18250.75, "document_id": "doc_...", "note": "August operating account statement"}'
```

| Field | Meaning |
| --- | --- |
| `element_id` | The chart account. Only an asset, liability or equity account can be reconciled to a statement; anything else returns `422`. |
| `as_of` | The statement's ending date. A statement that ends mid-month is compared with the ledger at that date. |
| `balance` | The ending balance as the statement shows it, positive in the account's normal direction: money in a bank account, the amount owed on a loan or card. Negative for the opposite, such as an overdrawn account. |
| `document_id` | Optional. The statement itself, added first with `create-document`, kept as evidence. |
| `note` | Optional. Kept with the recorded balance. |

Recording the same account and date again replaces the earlier balance. When more than one statement ends in a month, the one with the latest date stands.

### Policy

`set-reconciliation-policy` changes how much the close cares about one reconciliation. Pass the block's `structure_id` and any of these; omitted fields keep their value.

| Field | Default | Meaning |
| --- | --- | --- |
| `required_for_close` | see above | Whether the month's close waits on it |
| `materiality` | `0` | A difference up to this amount still counts as reconciled. `0` means the two sides must agree to the cent. |
| `review_required` | `false` | Whether the close also waits for a sign-off, not only for the two sides to reconcile |
| `separate_reviewer` | `false` | Whether the person who signs off must be someone other than the person who ran the comparison or recorded its balance. It can be turned on only while the graph has at least two members who can write (`409` otherwise). |

The next comparison uses a new materiality. Comparisons already recorded aren't judged again. Each change is recorded with who made it.

### Sign-off

`sign-off-reconciliation` records a review of one reconciliation for one month (`structure_id`, `period`, optional `note`). Only a month that reconciles can be signed off (`409` otherwise, with the reason), and only by a member of the graph who can write: access through an organization role isn't enough (`403`). A sign-off by the person who ran the comparison is marked `self_reviewed`, and a reconciliation with `separate_reviewer` set refuses it. Signing off a month already reviewed changes nothing.

The sign-off is pinned to a fingerprint of the balances it saw. If any of them changes afterwards, the sign-off lapses and the month needs signing off again, unless the balances return to the ones that were signed.

### Status

Each reconciliation has one status per month:

| Status | Meaning |
| --- | --- |
| `not_started` | Not compared for this month |
| `stale` | The books have changed since the comparison. Run `refresh-reconciliations` again. |
| `unreconciled` | The two sides differ by more than the materiality |
| `reconciled` | They agree within it |
| `reviewed` | Reconciled and signed off |

Only an open month can go stale. A closed month's comparison is the record the close was made on, and reopening the month puts it back in play.

The `unreconciled_accounts` blocker counts every reconciliation marked `required_for_close` that isn't ready for this month. Ready means `reconciled` or `reviewed`, or only `reviewed` where `review_required` is set. A required reconciliation that was never compared for the month blocks too, because nothing has checked it.

### Reading them

GraphQL `reconciliations(period:)` returns every reconciliation's standing for a month, oldest block first. A malformed period returns `INVALID_PERIOD`.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ reconciliations(period: \"2026-08\") { asOf reconciliations { structureId name method status requiredForClose materiality unreconciledDifference reviewedBy } } }"}'
```

Each entry also carries the balances (`ledgerBalance`, `independentBalance`, `balanceAsOf`), the parts of the independent side (`components`, one per schedule or the recorded statement), the accounts that didn't tie on a ledger-wide block (`differences`), and who compared and reviewed it. The fiscal calendar's `unreconciledAccountCount` and `unreconciledAccountSample` give the close's view in one read.

### After a sync

A QuickBooks sync refreshes the next month to close on a ledger that already has reconciliations, so a change in QuickBooks shows up in the close gate without anyone running a refresh. It refreshes existing blocks only and creates none: the first refresh is a person's decision. A failure there leaves the month's last comparison in place and doesn't fail the sync.

## Preparing a Close

A close session runs in this order. Each step clears one class of blocker or one surprise.

1. **Sync, and wait for it to finish.** Check `last_sync_result` on the connection, not just that a run started.
2. **Settle reconciling items.** Preview each one and resolve it: restate, catch up, or acknowledge. Pass the preview's `drift_detected_at` to `resolve-reconciling-item` as `expected_drift_detected_at`: if the item was flagged again after you previewed it, the resolution is refused with `409` and you preview again, instead of settling a change nobody has seen.
3. **Commit or void unposted source events.** See [Unposted source events](#unposted-source-events).
4. **End any schedule that stops this month**, with `terminate-schedule` or an `asset_disposed` event (see [Ending a Schedule Early](#ending-a-schedule-early)). Do this before step 5, so no entry is drafted for a month the schedule no longer covers.
5. **Draft the schedule entries.** A background sweep promotes matured obligations every few minutes. `promote-obligations` runs the same sweep on demand:

   ```bash
   curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/promote-obligations" \
     -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"dispatch_handlers": true}'
   ```

   The result counts `classified_count`, `dispatched_count`, `stranded_count` and `error_count`. The sweep is idempotent: re-running it skips promoted obligations and reuses existing drafts.
6. **Add one-off adjustments** as `journal_entry_recorded` events with `status: "draft"`. See [Event-Driven Ledger](Event-Driven-Ledger#posting-an-event-to-the-ledger).
7. **Review the drafts.** `periodDrafts(period:)` (or `list-period-drafts`) lists every draft with its lines, whether it balances, and `willPublishToQb`. The close follows this list exactly.
8. **Reconcile, last.** Run `refresh-reconciliations` for the month, clear what it reports, and sign off where a review is required. Any later change to the books makes the comparison `stale`, so this is the final step before the close. See [Reconciliations](#reconciliations).

## Closing a Period

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/close-period" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: close-2026-08-$(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{"period": "2026-08"}'
```

The body takes `period` (`^\d{4}-(0[1-9]|1[0-2])$`), an optional `note` for the audit trail, and the five override flags `allow_stale_sync`, `allow_stranded_obligations`, `allow_reconciling_items`, `allow_unposted_source_events` and `allow_unreconciled_accounts`. All five default to `false`.

On success, the envelope's `result` is the close summary:

```json
{
  "operation": "close-period",
  "operationId": "op_01HVF8T0M2YTAY3BBNRH0V0",
  "status": "completed",
  "result": {
    "fiscal_calendar": {
      "graph_id": "...",
      "closed_through": "2026-08",
      "close_target": "2026-08",
      "...": "..."
    },
    "period": "2026-08",
    "entries_posted": 3,
    "entries_published_to_qb": 2,
    "entries_posted_locally": 1,
    "target_auto_advanced": true,
    "rule_summary": {"pass": 38, "fail": 0, "error": 0, "skipped": 0},
    "evaluated_structure_ids": ["..."],
    "statements_stamped": true,
    "statement_stamp_note": null,
    "stamped_statement_sets": {"struct_...": "fs_..."},
    "statement_rule_summary": {"pass": 12, "fail": 0, "error": 0, "skipped": 0}
  },
  "at": "2026-09-02T00:00:00Z",
  "createdBy": "user_...",
  "idempotentReplay": false
}
```

| Field | Meaning |
| --- | --- |
| `entries_posted` | Total drafts the close posted, across both lanes |
| `entries_published_to_qb` | Drafts written to QuickBooks, each posted as it was accepted |
| `entries_posted_locally` | Drafts posted in RoboLedger only. On a `native` graph this equals `entries_posted`. |
| `rule_summary` / `evaluated_structure_ids` | Verification of the schedules with facts in the month |
| `statements_stamped` / `stamped_statement_sets` | Whether the month's statement FactSets were stamped, and `structure_id → fact_set_id` for each |
| `statement_stamp_note` | Why stamping was skipped: `no_coa_mapping`, `no_entity`, `no_statement_structures` or `no_taxonomy`. The close still succeeds. |
| `statement_rule_summary` | Verification of the stamped statements, separate from `rule_summary` |

### Refusals and errors

| Status | Body | Cause |
| --- | --- | --- |
| `422` | `{"detail": {"message", "blockers": [...], ...detail fields}}` | The gate failed. Parse `detail.blockers`, never the message. |
| `422` | `"Balance sheet equation broken for this period: ..."` | Debits and credits across the month's draft and posted entries don't agree. Checked before anything changes. |
| `422` | `{"detail": {"code": "WRITE_BACK_FAILED", "failed_events": [...]}}` | QuickBooks rejected one or more drafts. See [Rejections and retry](QuickBooks-Sync-and-Write-Policy#rejections-and-retry). |
| `422` | `{"detail": {"code": "STATEMENT_STAMP_FAILED", "message"}}` | Reporting is set up but the statements couldn't be stamped. The whole close rolled back. Fix the cause (usually the mapping or the Reporting Style) and close again. |
| `409` | message | Another close or reopen holds the month, or the month closed in the meantime. Retry. |
| `404` | `"Fiscal calendar not initialized. ..."` | No calendar |

```json
{
  "detail": {
    "message": "Cannot close period '2026-08'.",
    "blockers": ["reconciling_items"],
    "reconciling_item_count": 1,
    "reconciling_item_sample": ["qb_inv_1042"]
  }
}
```

## What a Close Does

One close runs these steps in order. The ordering carries the atomicity guarantees:

1. **Gate.** The blockers above. A failure raises before anything is touched.
2. **Balance pre-flight.** Total debits and credits across the month's draft *and* posted entries must agree, so the close never has to undo a half-posted month.
3. **QuickBooks publish.** On a write-back graph, eligible drafts are written to QuickBooks and promoted to `posted` one by one. This is the only step that commits separately: each draft's `qb_external_id` marker is committed before any failure is raised, so a retried close never writes the same entry to QuickBooks twice. Any rejection stops the close here.
4. **Post the rest.** Every remaining draft dated in the month is posted in one transition. Drafts belonging to voided or superseded events are left alone.
5. **Advance the calendar.** `closed_through` moves to this month, and `close_target` advances if the close reached it.
6. **Lock the month.** The `FiscalPeriod` row becomes `closed`, with `closed_at` and `closed_by`.
7. **Stamp the statements.** The posted ledger is read through the mapping, and the month's canonical balance sheet, income statement, cash flow and equity FactSets are written (`report_id` null), replacing any from an earlier close of the same month. If reporting isn't set up, this step is skipped with a note. If reporting is set up and stamping fails, the whole close rolls back.
8. **Verify the schedules.** Rules on schedules with facts in the month run. A failing rule shows up in `rule_summary` but doesn't fail the close.
9. **Write the receipt.** A versioned JSON receipt is stored on the period row in the same transaction as the lock. A committed close always has its receipt, and a rolled-back one never does.

After the commit, the graph is marked stale so the analytical graph rematerializes with the posted entries and stamped statements.

The stamped statements are the month's closed record. The Plan page, forecasts and metrics read them, and they don't change when the live ledger does. Reopening a month removes them, and closing it again stamps new ones.

### The receipt

The receipt is the close's own record of itself, stored with the month:

```json
{
  "version": 1,
  "period": "2026-08",
  "closed_at": "2026-09-02T16:41:07+00:00",
  "closed_by": "user_...",
  "actor_type": "agent",
  "was_reclose": false,
  "entries_posted": 3,
  "entries_published_to_qb": 2,
  "entries_posted_locally": 1,
  "target_auto_advanced": true,
  "rule_summary": {"pass": 38, "fail": 0, "error": 0, "skipped": 0},
  "evaluated_structure_ids": ["..."],
  "statements_stamped": true,
  "statement_stamp_note": null,
  "stamped_statement_sets": {"struct_...": "fs_..."},
  "statement_rule_summary": {"pass": 12, "fail": 0, "error": 0, "skipped": 0}
}
```

`actor_type` separates closes an AI assistant ran over MCP (`agent`) from those a person ran over REST (`user`). The receipt is available as `closeReceipt` on GraphQL `periodCloseStatus(periodStart:, periodEnd:)` and as `close_receipt` on MCP `get-period-close-status`. The calendar's period rows carry `hasCloseReceipt`. Months closed before receipts were stored have none.

## Closing from MCP: Async and Polling

`POST .../close-period` over REST runs synchronously and returns the summary above. The MCP `close-period` tool runs the same command on the background worker instead, because a month with many QuickBooks writes can take longer than an MCP tool call is allowed to wait. The tool waits about 18 seconds and returns one of two things:

- **The receipt.** The close finished within the wait.
- **`status: "in_progress"`** with an `operation_id`. The close is still running (or queued, when `worker_started` is `false`). **Don't call `close-period` again.** The close is atomic and still running. Poll `get-period-close-status` until `period_status` is `closed` and `close_receipt` is filled in, or poll `get-fiscal-calendar` until the period shows `has_close_receipt`.

A duplicate MCP dispatch of the same close within 30 seconds returns the same operation (`deduplicated: true`). A duplicate that arrives later queues behind the close, then comes back with the receipt instead of a refusal. The tool also checks the gate before dispatching, so a month that can't close is refused immediately with its blockers, without taking up a worker.

## Reopening a Period

`reopen-period` unlocks the **most recently closed month only**. `closed_through` moves back one month, the month returns to `closing`, and its entries become writable again. To reach an earlier month, reopen latest-first down to it, then close forward month by month. Each reopen needs its own reason.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/reopen-period" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"period": "2026-08", "reason": "Missed accrual for the August roaster lease."}'
```

`reason` is required and recorded in the calendar's audit trail with the actor. `note` is optional. The REST result is the refreshed fiscal calendar. The MCP tool also reports how many statement sets were retracted.

A reopen does three things besides moving the boundary:

- **Retracts the month's stamped statements.** A reopened month no longer makes a closed statement, and its next close stamps new ones.
- **Returns schedule facts for the month to scope**, so the re-close drafts and verifies them again.
- **Leaves posted entries posted.** Reopening does not un-post anything. To undo a posted entry, record a reversal (`create-event-block` with `event_type: "journal_entry_reversed"`), then close the month again.

Reopening an earlier month while a later one stays closed is refused with `422`, and the message lists the months to reopen in order:

```text
Cannot reopen '2026-06': only the latest closed period can be reopened (closed_through is '2026-08').
Every later closed month carries statements stamped from this month's numbers.
Reopen latest-first — 2026-08, 2026-07, 2026-06 — then close forward month by month.
```

This is also the path the [mapping lock](Chart-of-Accounts-Mapping#the-closed-period-mapping-lock) points to. A month that isn't closed returns `422`, and an unknown month returns `404`. Reopening invalidates what trusted the closed state, such as shared reports and filings, so use it sparingly.

## Ending a Schedule Early

A schedule drafts one entry per month through its last month. When an asset is sold or a prepaid policy is cancelled, end the schedule **before** that month's entries are drafted. Otherwise the close drafts an entry that shouldn't exist. There are two ways to end one:

**End it with no entry: `terminate-schedule`.** Use this when the transaction is already in the books (a sale recorded in QuickBooks, a refund already booked) or no entry is wanted.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/terminate-schedule" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "structure_id": "struct_01J...",
    "new_end_date": "2026-09-30",
    "reason": "Delivery van sold 2026-09-30; sale booked in QuickBooks."
  }'
```

`new_end_date` must be a month's last day. In one transaction the operation deletes the schedule's forward facts after the cutoff (and any stale drafts there), voids the remaining obligations after it, and rewrites the schedule's total-check rule to prove the shortened curve. History at or before the cutoff is untouched. It refuses when entries after the cutoff have already posted. The result reports `facts_deleted`, `obligations_voided` and `rule_updated`.

**End it and book the disposal: an `asset_disposed` event.** Use this when the derecognition entry still needs to be made. `create-event-block` with `event_type: "asset_disposed"`, `occurred_at` set to the disposal date, and `metadata` carrying `schedule_id`, `proceeds` (cents, `0` for a write-off), `proceeds_element_id` (required when there are proceeds) and `gain_loss_element_id` (required when proceeds differ from net book value). The handler posts the disposal entry and voids the obligations in one step.

Either way the schedule's history is kept. Deleting a schedule erases it, so end a schedule that did real work and delete only one created by mistake. `rebuild-schedule` is a different tool: it regenerates a schedule in place from its stored definition, keeping its id and mappings, against the current close boundary.

## The Closed-Period Write Gate

Once a month is closed, nothing writes into it. Every ledger write checks the posting date against the calendar: recording or reversing an entry, editing or deleting a draft, publishing an event, resolving a reconciling item, and a QuickBooks full rebuild. A write into a closed month is refused with `422`:

```text
Cannot write to closed period '2026-08' (posting_date=2026-08-14). Reopen the period first if an adjustment is needed.
```

The gate and the close share a per-month fence. A write that arrives while that month is closing waits briefly and then gets `409` instead of slipping a draft into a month whose statements are being stamped. A QuickBooks transaction that syncs into a closed month doesn't post. It waits in the inbox as `captured` until the month is reopened.

The same principle covers the mapping. After a close, an arc that fed the month's stamped statements can't change until that month is reopened. See [Chart of Accounts Mapping](Chart-of-Accounts-Mapping#the-closed-period-mapping-lock).

## Gotchas and Pitfalls

### Blockers are structured

Read `detail.blockers`. Don't match on the message text.

### Sequential, and only after the month ends

`2026-08` can't close before `2026-07` does, and it can't close on August 31. It closes on September 1 at the earliest.

### Terminate before you promote

Ending a schedule after its month's entry is drafted leaves a stale draft for a month the schedule no longer covers. End it first, then run `promote-obligations`.

### Don't retry an in-progress MCP close

`status: "in_progress"` means the close is running. Poll for the receipt.

### Overrides are recorded

`allow_stale_sync`, `allow_stranded_obligations`, `allow_reconciling_items`, `allow_unposted_source_events` and `allow_unreconciled_accounts` all leave a trace in the close's audit note. Use them only for a decision you would defend in review.

## Self-hosted deployments

On your own stack, `just demo-roboledger` provisions a tenant with 16 months of synthetic books, schedules and closed history to exercise every call here. See the [RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough). Use `http://localhost:8000` and the key from `just demo-user`.

## Related Documentation

- **[QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy)** - The outbox the close flushes, and reconciling items
- **[Chart of Accounts Mapping](Chart-of-Accounts-Mapping)** - The mapping the close stamps statements through, and its lock
- **[Forecasting and Metrics](Forecasting-and-Metrics)** - What reads the stamped statements, and the plan-history backfill
- **[Information Blocks](Information-Blocks)** - Schedules and the rules the close verifies
- **[Event-Driven Ledger](Event-Driven-Ledger)** - Drafts, reversals, and event statuses
- **[RoboLedger Operations](RoboLedger-Operations)** - The full operations catalog and envelope contract
- **[GraphQL Reads](GraphQL-Reads)** - `fiscalCalendar`, `periodDrafts`, `periodCloseStatus`, `reconciliations`

**API Reference:**

- **[Fiscal Close](https://robosystems.ai/docs/extensions/roboledger-fiscal-close)** - Every close operation's request and response schema
- **[Ledger & Events](https://robosystems.ai/docs/extensions/roboledger-ledger-events)** - The reconciliation and reconciling-item operations

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
