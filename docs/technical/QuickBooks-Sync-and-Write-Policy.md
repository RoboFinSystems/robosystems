# QuickBooks Sync & Write Policy

This page covers how a connected QuickBooks Online company moves data into a RoboLedger graph, and how entries RoboLedger drafts move back out. It covers the sync windows, what a sync loads, the write policy that decides which ledger is the book of record, the close-time write-back outbox, reconciling items for transactions edited in QuickBooks after they synced, and the two ways to disconnect.

The product docs at roboledger.ai describe this for end users (*Nothing writes to QuickBooks until you post* and *When QuickBooks changes after a sync*). This page covers the contracts underneath: the operations, their payloads, and why they behave the way they do.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [Overview](#overview)
- [Syncing](#syncing)
- [What a Sync Loads](#what-a-sync-loads)
- [Write Policy](#write-policy)
- [The Write-Back Outbox](#the-write-back-outbox)
- [Reconciling Items](#reconciling-items)
- [Round Trips](#round-trips)
- [Disconnect vs. Sever](#disconnect-vs-sever)
- [Gotchas and Pitfalls](#gotchas-and-pitfalls)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

A QuickBooks connection is a platform row (`Connection`) scoped to one graph. It carries the OAuth credentials, the realm id, the timestamp and outcome of the last sync, and a **write policy**. Data flows two ways:

| Direction | What moves | When |
| --- | --- | --- |
| **Inbound (sync)** | Chart of accounts, transactions, customers, vendors, employees, company info | On connect, and whenever a sync is requested |
| **Outbound (write-back)** | Journal entries that RoboLedger drafted (schedule entries, manual adjustments) | At period close, or when one entry is published on its own with `execute-event-block` |

The two directions are independent. A sync reads QuickBooks and never writes to it. Write-back sends only entries that originated in RoboLedger. A transaction that came in from QuickBooks is never sent back.

Set credentials once for the examples below:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...
export CONN_ID=...                  # from GET /v1/graphs/$GRAPH_ID/connections
```

## Syncing

### The three windows

A sync pulls one date window from QuickBooks. The request body picks the window:

| Request body | Window | Pre-sync wipe |
| --- | --- | --- |
| `{}` | The last 60 days | No |
| `{"since_date": "2026-01-01"}` | From that date to today | No |
| `{"full_rebuild": true}` | All history (from 2000-01-01) | Yes, of unposted events only (see below) |

`full_rebuild` takes precedence over `since_date`. The 60-day default suits day-to-day use. If someone edits a transaction in QuickBooks that is older than 60 days, the default sync never sees it. Use `since_date` from a date that covers the edit, or `full_rebuild`.

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections/$CONN_ID/sync" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: sync-$(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{"since_date": "2026-04-01"}'
```

The same call is the `sync-connection` MCP tool, which takes `full_rebuild` and `since_date`. Its `connection_id` is optional: omitted, the tool uses the graph's one sync connection, narrowed by `provider` (for example `quickbooks`) when given, and it returns the candidates when the graph has several.

### The initial sync

Completing the OAuth consent starts a sync on its own. When the connection has never synced before, that first sync is a `full_rebuild`, so the graph receives the company's full history. If you reconnect a connection that has synced before, it gets the default 60-day window.

The first sync also does two one-time setup steps:

- **Bootstraps the fiscal calendar**, so the close workflow is reachable without a separate `initialize` call. See [Period Close](Period-Close).
- **Starts AI auto-mapping** of the chart of accounts when the mapping structure has no associations yet. This is the one sync step that consumes credits. See [Chart of Accounts Mapping](Chart-of-Accounts-Mapping).

### On demand only

Syncs run when something requests them: the Connections page in the app, `POST .../sync`, or the `sync-connection` MCP tool. The platform does not schedule QuickBooks syncs. The connection's `auto_sync_enabled` field is stored but currently has no effect.

### Tracking a sync

`POST .../sync` returns `202` with a `pending` envelope whose `result.task_id` identifies the pipeline run. A sync already running on the same connection returns `409`. When the run finishes, the outcome is on the connection:

```bash
curl "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections/$CONN_ID" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

`metadata.last_sync` is the completion timestamp, and `metadata.last_sync_result` is the outcome summary:

```json
{
  "status": "succeeded",
  "synced_at": "2026-09-02T14:05:11Z",
  "window": {"since_date": "2026-04-01", "full_rebuild": false},
  "counts": {
    "events_captured": 41,
    "events_updated": 3,
    "reconciling_items": 1,
    "dispatch_failed": 0,
    "cross_source_matched": 2,
    "skipped_same_sync_token": 388,
    "skipped_stale_sync_token": 0,
    "dropped_unbalanced_entries": 0,
    "dropped_empty_transactions": 0,
    "elements": 64,
    "agents_inserted": 2,
    "agents_updated": 5
  },
  "errors": []
}
```

A failed run records `last_sync_result` without advancing `last_sync`. The distinction matters because the close gate reads `last_sync`: a failed sync never makes the books look current. On MCP, `get-graph-sync-status` reads the same fields.

## What a Sync Loads

A sync extracts accounts (active and inactive, since historical lines can reference retired accounts), the journal-entry stream, customers, vendors and employees, the per-type transaction headers (invoices, bills, payments, bill payments, purchases, sales receipts), and company info. The pipeline normalizes them into RoboLedger's shape:

- **Accounts** become chart-of-accounts `Element` rows.
- **Customers, vendors and employees** become `Agent` rows.
- **Transactions** become events. Because QuickBooks is already the book of record for them, each one posts immediately and lands `fulfilled` with its `Transaction → Entry → LineItem` rows. A transaction that can't post (an account not yet in the chart, a closed month) waits in the inbox as `captured`.
- **Entries RoboLedger wrote back** are recognized when they come round again. The loader matches them by their QuickBooks transaction id and does not create a duplicate. This is `cross_source_matched` in the counts.

Re-syncing is idempotent. Events are upserted by `(source, external_id)`, and a transaction whose QuickBooks version token hasn't changed is skipped without comparison (`skipped_same_sync_token`).

**What `full_rebuild` wipes.** A full rebuild first deletes the connection's `captured` and `classified` events together with their draft GL rows, then reloads. Posted history survives: `committed`, `fulfilled` and `voided` events are never wiped. Your mapping associations also survive, because account elements are upserted in place and keep their ids. A full rebuild that would delete rows inside a closed month is refused. The sync then fails and nothing changes.

Every successful sync marks the graph stale. That schedules a rematerialization of the analytical graph, so fact grids and Cypher reads catch up. Operations that read the ledger directly, such as `live-financial-statement`, see the new data as soon as the sync finishes.

On a ledger that already reconciles its accounts, a successful sync also refreshes the reconciliations of the next period to close, so the close gate reads a current comparison without anyone running the refresh. A ledger with no reconciliations is left alone, because the first refresh is someone's decision. The refresh is best-effort: if it fails, the period keeps its last comparison and the sync still succeeds. See [Period Close](Period-Close#reconciliations).

## Write Policy

A connection's `write_policy` decides which ledger is the book of record, and therefore whether RoboLedger-originated entries are written back.

| Policy | Book of record | RoboLedger-originated entries |
| --- | --- | --- |
| `qb_authoritative` | QuickBooks | Written to QuickBooks at close, or when published on their own with `execute-event-block`. They stay `draft` in RoboLedger until QuickBooks accepts them. |
| `native` | RoboSystems | Post locally. Nothing is written to any external system. |

A new QuickBooks connection defaults to `qb_authoritative`. Other connection types default to `native`. `native` describes a graph with no authoritative external ledger: before any connection, after a sever, or books kept natively. It does not mean "connected, but keep the ledgers apart." The platform assumes no steady state in which QuickBooks stays connected while the two ledgers diverge.

**`hybrid` is reserved and cannot be set.** The `WritePolicy` enum declares a third value, `hybrid`, for a future QuickBooks-authoritative mode that would flag exceptions (low-confidence mappings, amounts over a threshold) for review. That mode is not implemented, and the write-policy endpoint rejects it. The close and publish code paths list `hybrid` alongside `qb_authoritative` in their eligibility checks, so a row that somehow carried it would behave exactly like `qb_authoritative`. No supported path creates such a row. In practice there are two policies.

Switch policies with:

```bash
curl -X PUT "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections/$CONN_ID/write-policy" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"write_policy": "native"}'
```

The same control is the `set-write-policy` MCP tool. Changing the policy requires a member or admin role on the graph. A viewer can't opt a graph into writing to its book of record.

## The Write-Back Outbox

On a `qb_authoritative` graph, the drafts RoboLedger holds for a month are the outbox, and `close-period` flushes it. One predicate decides whether a draft publishes. The close uses that predicate to write, and the draft preview uses the same one to report, so the preview can't disagree with what the close does.

### Which drafts are written back

A draft entry in the month being closed publishes when **both** of these hold:

1. **The graph has a write-back connection.** A non-deleted QuickBooks connection with a write-back policy exists. If several exist, the newest wins.
2. **The draft's event publishes.** The event is not `voided` or `superseded`, the entry has not already been written (its id is not in the event's `metadata.qb_entry_ids`), and either:
   - its `metadata.publish_to_source` is `true`, or
   - `publish_to_source` is unset and its `source` is `schedule` or `manual`.

Synced QuickBooks transactions (`source: "quickbooks"`) already live in QuickBooks. `system` entries are local by definition. Neither is ever published unless `publish_to_source: true` says otherwise.

### Keeping one entry in RoboLedger

`publish_to_source: false` on an event's metadata keeps its entry local whatever its source. It is the override for an entry that mirrors a change already made in QuickBooks, which would be applied twice if it were written back. Set it when you record the entry:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "journal_entry_recorded",
    "event_category": "adjustment",
    "event_class": "economic",
    "occurred_at": "2026-08-31T00:00:00Z",
    "source": "manual",
    "amount": 48000,
    "currency": "USD",
    "description": "Reclass already booked in QuickBooks",
    "apply_handlers": true,
    "metadata": {
      "posting_date": "2026-08-31",
      "memo": "Mirror of QuickBooks reclass (local only)",
      "status": "draft",
      "type": "adjusting",
      "publish_to_source": false,
      "line_items": [
        {"element_id": "elem_coa_software", "debit_amount": 48000},
        {"element_id": "elem_coa_office_expense", "credit_amount": 48000}
      ]
    }
  }'
```

You can also patch the flag onto an existing event with `update-event-block` and `"metadata_patch": {"publish_to_source": false}`. Send a JSON boolean. The journal-entry handler rejects a string such as `"false"`, but only when it runs: at a create with `apply_handlers: true`, or at the commit that writes the entry. A patch onto an event whose entry is already drafted is stored as sent, and a string there is not read as `false`.

### Previewing the split

Before closing, read the month's drafts. Each carries `will_publish_to_qb`, and the summary counts both lanes:

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ periodDrafts(period: \"2026-08\") { draftCount allBalanced qbWritePolicy qbPublishCount localOnlyCount drafts { entryId memo totalDebit balanced willPublishToQb } } }"}'
```

The MCP equivalent is `list-period-drafts`.

### What happens at close

`close-period` publishes eligible drafts to QuickBooks **before** it changes anything else. Each draft entry is sent as its own QuickBooks journal entry, with the entry id as the QuickBooks request id, so a repeat inside QuickBooks' deduplication window is not booked twice. The returned QuickBooks id is recorded per entry on the event, in `metadata.qb_entry_ids` (entry id → QuickBooks id), and the draft is promoted to `posted`. Only the entries dated in the month being closed are sent. An event whose entries span months stays open until the close of each later month publishes its remaining entries, and it moves to `fulfilled` once none is left. Everything the publish step didn't send is posted locally in one bulk transition. The close receipt reports the two lanes separately: `entries_published_to_qb`, `entries_posted_locally`, and their total `entries_posted`. On a `native` graph the first is always zero. The full close sequence is on [Period Close](Period-Close#what-a-close-does).

### Rejections and retry

The publish step tries every eligible draft and collects every failure instead of stopping at the first. If QuickBooks rejects any of them, the close fails with `422` and nothing else about the close happens:

```json
{
  "detail": {
    "code": "WRITE_BACK_FAILED",
    "message": "Cannot close: 1 draft(s) failed to publish to QuickBooks. Fix the offending entries and retry the close.",
    "failed_events": [
      {
        "event_id": "evt_01J...",
        "entry_id": "je_01J...",
        "memo": "August depreciation — delivery van",
        "posting_date": "2026-08-31",
        "qb_error": {"code": "...", "message": "..."}
      }
    ]
  }
}
```

The drafts that did reach QuickBooks keep their entries in `qb_entry_ids`. Those markers are committed before the failure is raised, on purpose. A QuickBooks journal entry is an external write that has already happened. If the marker were rolled back, the retried close would publish the same entry again as a duplicate. So retrying is safe: fix the rejected entries (usually an account mapping, an unbalanced line, or a period already closed in QuickBooks), then call `close-period` again. The retry sends only the entries not yet recorded in `qb_entry_ids`.

### Publishing one entry on its own

`execute-event-block` publishes a single event outside a close:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/execute-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "evt_01J...", "connection_id": "'$CONN_ID'"}'
```

`connection_id` names the QuickBooks connection to write through. Omit it only when the event's own `metadata.connection_id` names one. With neither, the call is treated as native and writes nothing.

On a write-back connection it sends each of the event's unpublished draft entries to QuickBooks as its own journal entry, records them in `qb_entry_ids`, posts them, and moves the event to `fulfilled`. If QuickBooks rejects an entry, the entries sent before it are kept and posted, the rejection is stored on `metadata.last_outbound_error`, and the event moves to `pending`. The response reports `qb_entry_ids` (every entry of the event now in QuickBooks) and, on a rejection, `qb_error`. A `native` event, or one carrying `publish_to_source: false`, passes through with no external write.

Errors: `409` for an event with no drafted ledger entries (commit it first) or one that is `voided` or `superseded`, and `422` when its period is closed. A `401` means QuickBooks refused the credentials. The connection is flagged for re-authorization and needs a fresh OAuth consent. A `503` means Intuit was unreachable or busy. The connection is fine, and the call can be retried.

## Reconciling Items

People edit QuickBooks. When a sync brings a changed payload for a transaction that RoboLedger has already posted, the loader does **not** overwrite the posted entry. The old figures may sit in a closed month, a stamped statement, or a report someone has received. Instead the loader leaves the posted GL and the accepted payload as they are, stores the incoming payload next to them, and flags the event as a **reconciling item**. The sync's `counts.reconciling_items` reports how many it found.

The flag stays until someone settles the item. Each later sync reports it again, and the close refuses to run over it (see [Period Close](Period-Close#blockers)).

### Finding them

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ eventBlocks(isReconcilingItem: true) { id eventType externalId occurredAt amount description } }"}'
```

On MCP, this is `list-event-blocks` with `is_reconciling_item=true`.

### Previewing one

`preview-reconciling-item` shows what changed and what each disposition would do. It writes nothing.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/preview-reconciling-item" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "evt_01J..."}'
```

The result (`ReconcilingItemPlan`) carries:

| Field | Meaning |
| --- | --- |
| `prior_entries` / `accepted_entries` | The posted entries, and the entries the new payload would produce |
| `delta[]` | Per-account net change in cents, debit-positive: `prior_net`, `accepted_net`, `delta` |
| `no_gl_effect` | The edit moves no money (a memo or reference change) |
| `closed_periods` | Closed months the event's entries sit in |
| `default_disposition` | What `resolve-reconciling-item` does when you don't choose |
| `default_posting_date` | Where a catch-up entry lands by default: the end of the earliest open period |
| `restate_blockers` | Why restating isn't available, if it isn't |
| `unmapped_element_external_ids` | QuickBooks accounts in the new payload with no mapping. Map them before any disposition that writes. |

### The three dispositions

| Disposition | What it does | Use when |
| --- | --- | --- |
| `restate` | Regenerates the event's entries from the new payload, in place. The original month's figures change. | Every affected month is open. This is the usual case inside the current fiscal year, and if the month is closed you can reopen it first. |
| `catch_up` | Leaves the original entries alone and posts the difference as one alignment entry in an open period. That entry is **local-only** (`publish_to_source: false`): QuickBooks already has the edit, so writing it back would apply it twice. | The original months are closed and their statements have gone out and should stand. |
| `acknowledge` | Records that the difference was handled elsewhere and clears the flag without touching the ledger. A `note` is required, and `reference_event_id` should name the entry that covered it. | Someone already booked the difference by hand. |

The default is `restate` when every period the event touches is open and nothing blocks it, and `catch_up` otherwise. `restate` is blocked by a closed period, by an entry that was reversed or isn't posted, or by entries from elsewhere sharing the event's transaction.

### Resolving one

Pass the `drift_detected_at` from the preview you decided on as `expected_drift_detected_at`. If the item has been flagged again since, with a newer payload nobody has seen, the resolution is refused with `409` and the item must be previewed again.

```bash
# Catch up in the open month, drafted for review at close
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/resolve-reconciling-item" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt_01J...",
    "expected_drift_detected_at": "2026-09-02T14:05:11Z",
    "disposition": "catch_up",
    "posting_date": "2026-09-30",
    "status": "draft",
    "note": "March invoice amount corrected in QuickBooks; Q1 statements already with the lender."
  }'

# Mark handled: the difference was booked by hand
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/resolve-reconciling-item" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt_01J...",
    "disposition": "acknowledge",
    "note": "Covered by the August true-up entry.",
    "reference_event_id": "evt_01J..."
  }'
```

`posting_date` and `status` (`draft`, the default, or `posted`) apply to `catch_up` only. A drafted catch-up entry appears in the month's drafts and posts locally when that month closes. Omit `disposition` to take the preview's default.

Resolving an item sets the event's payload to the accepted one. The next sync then sees no difference and does not raise the flag again. The response echoes the `delta`. It includes a `catch_up` block (event, entry and posting date) when a catch-up entry was written, and a `regenerated` block (transaction and entry ids) for a restate.

Errors: `404` for an unknown event, `409` when the event isn't a reconciling item, another writer holds it, or it was re-flagged after the preview, and `422` for a blocked restate, a closed target period, an unmapped account, or an unbalanced result.

## Round Trips

An entry RoboLedger wrote back comes round again on the next sync, and the loader matches it to the event that published it instead of capturing a copy (`cross_source_matched`). The match also watches for edits made to that entry in QuickBooks:

- **Edited in QuickBooks.** When the round-tripped entry's accounts or amounts no longer match what was published, the originating RoboLedger event is flagged as a reconciling item, and you settle it like any other (see [Reconciling Items](#reconciling-items)). A change to the date alone is not compared.
- **Reverted in QuickBooks.** If the entry is changed back before anyone resolves the item, the next sync clears the flag.
- **A copy that landed anyway.** If QuickBooks' copy of a published entry was captured before the match could recognize it (a sync overlapping the close's publish, for example), the copy books the entry twice. An unposted copy is voided. A posted copy is flagged as a reconciling item to reverse: its catch-up disposition reverses it, and the close gate holds its period until then.

## Disconnect vs. Sever

Deleting a connection takes a `disposition` query parameter, and the two values end in different places:

```bash
# Disconnect (default): revoke access, keep the books, reconnectable
curl -X DELETE "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections/$CONN_ID" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"

# Sever: cut over to native books, permanently
curl -X DELETE "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections/$CONN_ID?disposition=sever" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

| | `disconnect` | `sever` |
| --- | --- | --- |
| OAuth grant | Revoked | Revoked |
| Synced books | Stay in the graph | Stay in the graph |
| Chart of accounts | Still owned by the QuickBooks connection | Stamped as the graph's own native chart |
| Write policy | n/a (the connection is inactive) | Drops to `native` |
| Reconnecting the same company | Revives the connection and picks up where it left off | Never revives it. QuickBooks can't resume over books kept natively since. |

Both require the admin role on the graph. `sever` applies to QuickBooks only, and other providers return `400`. Choose `sever` when the graph is leaving QuickBooks for good and will keep its books in RoboLedger from now on. Choose `disconnect` for everything else.

## Gotchas and Pitfalls

### The default window is 60 days

An edit to a transaction older than 60 days is invisible to a default sync, and so is any reconciling item it would raise. Sync with `since_date` from a date that covers it.

### A catch-up entry is local on purpose

Don't flip a catch-up entry's `publish_to_source` to `true`. QuickBooks already holds the edit that the entry mirrors, and writing it back doubles the change there.

### A failed write-back is not a failed publish of everything

After a `WRITE_BACK_FAILED` close, some entries may already be in QuickBooks, recorded in their event's `qb_entry_ids`. Don't delete and redraft them to "start clean". Fix only the rejected entries and retry the close.

### Multi-currency companies are not supported

The sync loads amounts in USD only. A sync that finds a transaction in another currency fails rather than coerce it, and records the failure on the connection's `last_sync_result`.

### `hybrid` can't be set

`PUT .../write-policy` accepts `native` and `qb_authoritative` only. See [Write Policy](#write-policy).

## Self-hosted deployments

Connecting QuickBooks to your own stack needs your own Intuit app, OAuth redirect and tunnel. That setup is on [Connecting QuickBooks Locally](Connecting-QuickBooks-Locally). Everything on this page applies unchanged once the connection exists. Use `http://localhost:8000` and the key from `just demo-user`.

## Related Documentation

- **[Period Close](Period-Close)** - The close sequence that flushes the outbox, and the reconciling-item blocker
- **[Chart of Accounts Mapping](Chart-of-Accounts-Mapping)** - The auto-map that runs on first sync
- **[Event-Driven Ledger](Event-Driven-Ledger)** - Event statuses, handlers, and the capture-to-post lifecycle
- **[RoboLedger Operations](RoboLedger-Operations)** - The full operations catalog and envelope contract
- **[GraphQL Reads](GraphQL-Reads)** - `periodDrafts`, `eventBlocks` and the other read fields used here
- **[Connecting QuickBooks Locally](Connecting-QuickBooks-Locally)** - Intuit app, tunnel and OAuth setup for a self-hosted stack

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
