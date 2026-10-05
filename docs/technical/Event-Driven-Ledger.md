# Event-Driven Ledger

RoboLedger treats business events as the durable truth and the general ledger as something derived from them. A sale, a payment, an asset disposal: those happen in the world, and the debits and credits that record them are an interpretation made afterwards. The **Event Block** is the one write surface that carries an event into the ledger, and a **handler** turns it into balanced journal entries.

This page is the concept. The envelope fields, the enum vocabularies, the handler registry, the template grammar, and the graph edges are in [Event Block Reference](Event-Block-Reference).

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [The Thesis: Events Before Entries](#the-thesis-events-before-entries)
- [The Three-Level Ledger](#the-three-level-ledger)
- [REA: Resources, Events, Agents](#rea-resources-events-agents)
- [The Event Lifecycle](#the-event-lifecycle)
- [Capture, Classify, Commit](#capture-classify-commit)
- [Handlers: How Events Become GL Entries](#handlers-how-events-become-gl-entries)
- [The Closed-Period Write Gate](#the-closed-period-write-gate)
- [Source Systems and Reconciling Items](#source-systems-and-reconciling-items)
- [Posting an Event to the Ledger](#posting-an-event-to-the-ledger)
- [Related Documentation](#related-documentation)
- [Support](#support)

## The Thesis: Events Before Entries

A traditional accounting API asks the caller which accounts to debit and credit. RoboLedger asks what **happened**: an invoice was issued, a payment cleared, equipment was retired. The caller records that as one event, and a handler derives the entry. The event is the source; the ledger is its projection.

Three consequences shape the rest of the subsystem:

- **One write surface.** There is no raw journal-posting endpoint. Adapters such as QuickBooks, AI operators, the app, and your own integration all create the same Event Block through the same validated path.
- **Amounts are signed integer cents**, from the graph owner's point of view: inflows positive, outflows negative. `$2,500.00` is `250000`.
- **Recording and posting are separable.** An event can be captured for review without touching the ledger and posted later, or it can fire its handler and write the entry in the same call.

## The Three-Level Ledger

The ledger keeps the business event and its accounting interpretation as separate, first-class levels:

```text
Transaction   what happened in the world
   │
   ▼
Entry         the accounting interpretation; must balance (Σ debits = Σ credits)
   │
   ▼
LineItem      one debit or one credit against a chart-of-accounts element
```

- A **Transaction** (`txn_…`) is the business event as it occurred, linked back to the Event that caused it.
- An **Entry** (`je_…`) is a journal entry: `standard`, `adjusting`, `closing`, or `reversing`, and `draft`, `posted`, or `reversed`. It needs at least two line items, and Σ debits must equal Σ credits; an unbalanced entry is rejected before anything is written.
- A **LineItem** (`li_…`) carries exactly one non-zero side against an `element_id`.

**The chart of accounts is the `Element`.** There is no separate account table. A line item points at the element it debits or credits, and the same associations that drive taxonomy roll-ups map those elements to reporting concepts. See [Chart of Accounts Mapping](Chart-of-Accounts-Mapping).

A source that already journalized the event, such as QuickBooks, supplies its entry and it is kept 1:1. A source that supplies only the business event lets a handler write the entry.

## REA: Resources, Events, Agents

The event model rests on **REA** (Resources, Events, Agents), the accounting ontology William McCarthy introduced in 1982 and later standardized in ISO/IEC 15944-4. You do not need the theory to use it, but it supplies the envelope's vocabulary:

| REA concept | In RoboLedger | Meaning |
| --- | --- | --- |
| **Agent** | `Agent` (`agt_…`), named by `agent_id` | A counterparty: a customer, vendor, employee, anyone the reporting entity transacts with. The reporting entity itself is the `Entity`. |
| **Event** | `Event` (`evt_…`), the Event Block | A business occurrence that exchanges value, optionally named by one of 19 action verbs (`event_action`) |
| **Resource** | `resource_type`, optionally `resource_element_id` | What is exchanged: goods, services, money, a right, an obligation, information, labor |

Events also link to each other. An invoice **obligates** a later payment, and the payment **discharges** it (`obligated_by_event_id`, `discharges_event_id`); a correction **replaces** the event it supersedes. Once materialized to the graph, the whole chain from event to transaction to entry to line item is walkable in one Cypher query.

## The Event Lifecycle

An event moves through a small state machine. The forward path runs from the inbox to done; two retraction states end it early.

```text
                        classify                 commit
 create ──▶ captured ─────────────▶ classified ───────────▶ committed ──▶ fulfilled
 (capture    inbox; no GL            account chosen          handler has     the work is
  only)                              (or draft written        written its     done: posted,
                                      by a handler)           draft entry     or published

 create with apply_handlers=true lands at classified (draft entry) or fulfilled (posted entry)

 pending      waiting on something outside: an upstream rejection to retry, or a
              scheduled obligation whose month has not arrived
 voided       retracted; allowed only while nothing the event wrote has posted
 superseded   replaced by a corrected event (superseded_by_id)
```

There is no separate `void` or `supersede` operation: every move goes through `update-event-block` with `transition_to`. A retraction is refused, from any status, once the event's entries or transactions have posted or it has published to QuickBooks; reverse the posted entry instead. The full transition table is in [Event Block Reference § Statuses and Transitions](Event-Block-Reference#statuses-and-transitions).

**The event's status tracks the work; the entry's status tracks the books.** Close does not look at event status to decide what to post. It posts every draft entry dated in the period whose event is not voided or superseded.

## Capture, Classify, Commit

There are two ways in, and they meet at the draft.

**Fire-handler.** `create-event-block` with `apply_handlers: true` resolves the handler and writes the entry in the same call. A draft entry lands the event at `classified`; an entry posted immediately lands it at `fulfilled`. This is the path for an integration that already knows the accounting.

**Capture, classify, commit.** A plain create (`apply_handlers` defaults to `false`) lands the event at `captured` and writes no ledger rows. It sits in the inbox until someone decides how to book it:

1. **Classify.** `update-event-block` with `transition_to: "classified"` records the choice, supplied in `metadata_patch` in the same call. Nothing posts yet. A handler that validates classifications checks the choice here and refuses one it could not post, with the reason, rather than failing later at commit. Today the bank-feed handlers are the ones that do (see [Event Block Reference § Bank-feed handlers](Event-Block-Reference#bank-feed-handlers)). For every other type the choice is checked at commit.
2. **Commit.** `update-event-block` with `transition_to: "committed"` fires the event's Python handler against the final metadata and writes the event's entry, normally as a draft. If the handler already wrote the event's entry when it was created, committing does not write a second one. A handler error rolls the whole update back, so a Python-handled event never sits at `committed` with no entry behind it. Commit fires Python handlers only: an event type covered only by a DSL handler commits with no entry, so book those with `apply_handlers: true` at create.

```bash
# Classify: record the entry's date, memo and accounts, no GL yet
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/update-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "evt_…", "transition_to": "classified",
       "metadata_patch": {"posting_date": "2026-05-31", "memo": "May office supplies",
         "line_items": [
           {"element_id": "<office_supplies_id>", "debit_amount": 12500},
           {"element_id": "<accounts_payable_id>", "credit_amount": 12500}]}}'

# Commit: fire the handler and write the draft entry
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/update-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "evt_…", "transition_to": "committed"}'
```

**What `classified` means for the next close** depends on which road the event took:

- A `classified` event whose handler has **already written a draft** (the fire-handler path, or a scheduled obligation promoted when its month matures) is booked. Its draft posts at the next close of that period unless the event is voided or superseded first.
- A `classified` event from the **capture path** has an account choice but no entry. It does not post at close; it posts once it is committed and its draft is then included in the close.

To see what a close will post, list the period's drafts with `list-period-drafts`. The close itself is covered in [Period Close](Period-Close).

Two operations move events forward without a new create:

- **`promote-obligations`** sweeps scheduled obligations whose month has arrived, moves them to `classified`, and drafts their closing entries. A background sensor runs the same sweep; the operation runs it now.
- **`execute-event-block`** publishes an event to its source-of-truth system when the event's connection has the `qb_authoritative` write policy, moving it to `fulfilled` (or `pending` if QuickBooks rejects it) and posting its drafts. On a `native` connection, or an event with no connection, it returns without an external write. See [QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy).

## Handlers: How Events Become GL Entries

A handler is the part that knows the accounting. It takes an event and produces one or more balanced entries. Handlers come from two layers, checked in order:

- **Python handlers**, a fixed registry defined by the platform, for workflows with real logic: recording and reversing journal entries, receiving payments and paying bills (which also link the settlement to the invoice or bill it discharges), disposing of assets, the schedule handlers that create a schedule's obligations and draft each month's entry, and the bank-feed handlers that post a classified bank line. Several event types share a handler: `invoice_issued`, `bill_received`, and the other source transaction types all book through the journal-entry handler, because their entry has the same shape and only the label and category differ. A Python handler wins when both layers match.
- **DSL handlers**, rows in the graph's `event_handlers` table. Each maps an `event_type` (optionally narrowed by source, agent type, resource type, or a metadata expression) to a JSON template of debit and credit legs, with `{{ event.amount }}`-style interpolation. They let a tenant add a posting pattern without code; register one with `create-event-handler`.

An event with `apply_handlers: true` whose type matches no handler is an error, not a silent capture. `preview-event-block` takes the same body as a create and returns the planned entries and a `would_succeed` flag without writing anything.

The handler list, the aliases, the matching rules, and the template grammar are in [Event Block Reference § Handlers](Event-Block-Reference#handlers).

## The Closed-Period Write Gate

Once a fiscal period is closed, its books are fixed. Any write that would put a ledger row in a closed period, or remove one that landed there, is refused with a closed-period error naming the period: posting an event whose entry is dated in it, committing one, voiding or superseding one whose entries are dated in it, drafting a schedule's entry there, or disposing of a reconciling item into it. Capturing an event writes no ledger rows, so capture is never blocked.

To change a closed month, reopen it with `reopen-period`, make the correction, and close it again. The gate and the close take the same per-period lock, so a write cannot see a period as open and commit after it has closed. See [Period Close](Period-Close).

## Source Systems and Reconciling Items

`source` names where an event came from. The platform's own values (`manual`, `system`, `schedule`) are always accepted; any other value must be a provider or source name registered as a connection on the graph. Supply `external_id` with the source's own id: events are idempotent on `(source, external_id)`, so a retried delivery does not create a duplicate.

**Write policy** decides who is the system of record for a connection. `native` means RoboLedger is; nothing is written back. `qb_authoritative` means QuickBooks is, and RoboLedger's own entries are published to it (at `execute-event-block`, or in a batch at close). These are the two policies that can be set; `hybrid` is reserved and is rejected. Detail is in [QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy).

**Reconciling items** handle a source that changes its mind. When a re-sync brings a changed payload for an event whose entry has already posted, the platform does not rewrite the books. It keeps the posted entry and the stored payload, holds the incoming one aside, and flags the event as a reconciling item (`isReconcilingItem` on `eventBlocks`). Each one waits for an explicit disposition: **restate** the affected months, **catch up** with an entry in the open period, or **acknowledge** it as handled, with a note. `preview-reconciling-item` shows what each would do and `resolve-reconciling-item` applies one. See [QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy).

## Posting an Event to the Ledger

The shortest path from an event to a ledger entry is one fire-handler create. This records an accrual as a draft entry, which posts at the month's close:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # a graph with the RoboLedger extension

curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "journal_entry_recorded",
    "event_category": "adjustment",
    "occurred_at": "2026-05-31T00:00:00Z",
    "source": "manual",
    "description": "May office supplies accrual",
    "apply_handlers": true,
    "metadata": {
      "posting_date": "2026-05-31",
      "memo": "May office supplies accrual",
      "status": "draft",
      "type": "adjusting",
      "line_items": [
        {"element_id": "<office_supplies_id>", "debit_amount": 12500},
        {"element_id": "<accounts_payable_id>", "credit_amount": 12500}
      ]
    }
  }'
```

Element ids come from the graph's own chart of accounts. The response is an `OperationEnvelope` whose `result` is the event at status `classified`. Read events back with the `eventBlocks` GraphQL field, or `list-event-blocks` and `get-event-block` from an MCP client.

Building a full integration (capturing from a source system, idempotent retries, choosing capture or fire-handler, reading back, and tracing the audit chain in the graph) is walked through in [Build a Ledger Integration](Build-a-Ledger-Integration).

## Related Documentation

**Wiki Guides:**
- **[Event Block Reference](Event-Block-Reference)** - Envelope fields, vocabularies, transitions, handlers, the template grammar, and graph edges
- **[Build a Ledger Integration](Build-a-Ledger-Integration)** - Posting events from your own system, end to end
- **[Period Close](Period-Close)** - How drafts post, and reopening a closed period
- **[QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy)** - Systems of record, write-back, and reconciling items
- **[Operations Contract](Operations-Contract)** - The operation envelope, idempotency, and errors every write shares
- **[Information Blocks](Information-Blocks)** - How the facts events produce are packaged for reporting

**Codebase Documentation:**
- **[API Reference](https://robosystems.ai/docs/extensions)** - Request and response schemas for every operation
- **[Extensions Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/extensions/README.md)** - The RoboLedger OLTP models

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
