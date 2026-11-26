# Receivables & Payables

A RoboLedger graph keeps receivables and payables as documents, not only as account balances: each invoice and bill is an event with a due date, each payment says which documents it settles, and the open balance of any document at any date is its amount less what was applied to it by then. This page covers what makes an open item, how a payment settles one, the GraphQL reads for open balances and for aging at a date, the tie-out to the receivables and payables accounts, intercompany items in a group, the two MCP tools that serve the same reads, and what a reversal does to an item.

**Quick Start:** `POST /extensions/$GRAPH_ID/graphql` with `{ agedReceivables(asOf: "2026-10-31") { buckets { currentCents overNinetyCents totalCents } tieOuts { differenceCents } } }` returns the receivables aging at October 31 and whether it ties to the receivables account.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

## Table of Contents

- [Overview](#overview)
- [What Makes an Open Item](#what-makes-an-open-item)
- [Settling an Item](#settling-an-item)
- [Reading Open Balances](#reading-open-balances)
- [Aging at a Date](#aging-at-a-date)
- [The Tie-Out](#the-tie-out)
- [A Group's Receivables and Payables](#a-groups-receivables-and-payables)
- [From an AI Client](#from-an-ai-client)
- [What a Reversal Does](#what-a-reversal-does)
- [Gotchas and Pitfalls](#gotchas-and-pitfalls)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

| Concept | What it is |
| --- | --- |
| **Open item** | An `invoice_issued` (receivables) or `bill_received` (payables) event in the books with an amount not fully settled |
| **Settlement** | A payment's link to the documents it pays: `discharges_event_id` for one document at the payment's full amount, or `discharge_allocations` for several, or part of one |
| **Due date** | The document's own `due_date`, set directly or from `terms_days`; without one it is due on the day it was issued |
| **Aging** | The open items at an `asOf` date in five buckets by days past due, with overpayments set apart as unapplied credit |
| **Tie-out** | Each entity's open items beside the posted balance of its receivables or payables accounts at the same date. The difference is reported, never plugged |

The reads come straight from the events, not from the general ledger, so an open balance is auditable back to the documents and payments that make it. The tie-out is where the two meet. Everything is per entity: an item belongs to the entity whose books hold it, and a read without `entityId` is the group parent's.

All the reads below are GraphQL queries posted to `POST /extensions/{graph_id}/graphql` (see [GraphQL Reads](GraphQL-Reads.md)); the writes are the `create-event-block` and `update-event-block` operations at `POST /extensions/roboledger/{graph_id}/operations/{name}` (see [Event Block Reference](Event-Block-Reference.md)).

**Every amount on this page is a whole number of cents.** The open-item and aging fields are typed `BigInt` and their names end in `Cents` (`openCents`, `totalOpenCents`, `controlBalanceCents`, every bucket); an event block's `amount` and an allocation's `amount` are integer cents too. `BigInt` is sent as a plain JSON number. None of these reads returns a `Float`. The ledger reads you might check a tie-out against by hand, `trialBalance` and `accountRegister`, report `Float` dollars: divide the cents by 100 before you compare.

## What Makes an Open Item

An open item is an `invoice_issued` or `bill_received` event that:

- is **in the books**: at status `committed` or `fulfilled`, or at `classified` with an entry of its own that stands posted (posted and not since reversed);
- has an `amount` (taken in magnitude, whatever its sign);
- is not fully settled at the date read.

A `sales_receipt_recorded` is paid at the sale and never an open item, whatever links to it. An event at `captured` (waiting in the inbox), `voided` or `superseded` never counts.

### From QuickBooks

On books synced from QuickBooks, invoices and bills arrive as `invoice_issued` and `bill_received` events with the `DueDate` QuickBooks holds, and their QuickBooks payments as `payment_received` and `bill_paid`. The sync writes what each payment applied to each document from QuickBooks' own links, so a payment covering several invoices settles each by the amount QuickBooks applied. That settlement belongs to QuickBooks: both settlement fields on `update-event-block` refuse a QuickBooks payment, and the change is made in QuickBooks and carried by the next sync.

### Sent directly

An integration or an AI client records an invoice or bill with `create-event-block`. Two fields set when it falls due, both only on an `invoice_issued` or a `bill_received`:

| Field | Meaning |
| --- | --- |
| `due_date` | The date it falls due |
| `terms_days` | Net terms in days from `occurred_at`, 0 to 3650 (30 for net 30). Stored as the due date it resolves to |

Give one or neither, not both; either is refused on any other event type. With neither, the document is due on the day it was issued and every read reports its `dueDateBasis` as `defaulted`.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: inv-10042" \
  -d '{
    "event_type": "invoice_issued",
    "event_category": "sales",
    "agent_id": "agt_…",
    "occurred_at": "2026-10-01T00:00:00Z",
    "terms_days": 30,
    "source": "manual",
    "amount": 480000,
    "description": "Brand refresh, phase 2",
    "apply_handlers": true,
    "metadata": {
      "posting_date": "2026-10-01",
      "memo": "Invoice 10042",
      "line_items": [
        {"element_id": "<receivables_account_id>", "debit_amount": 480000},
        {"element_id": "<revenue_account_id>", "credit_amount": 480000}
      ]
    }
  }'
```

`invoice_issued` and `bill_received` use the journal-entry handler; its metadata is described in [Event Block Reference § Handlers](Event-Block-Reference.md#handlers), and [Build a Ledger Integration](Build-a-Ledger-Integration.md) walks through sending invoices from another system.

**When it starts to count.** With `apply_handlers: true` the handler writes a draft entry and the event lands at `classified`. On books kept in RoboLedger that draft posts at the month's close, and the document stays `classified`: it becomes an open item once the close has posted its entry, and not before. A document captured without its handler counts once it is committed.

**Changing a due date.** `update-event-block` with `due_date` sets a new one, such as an extension granted. It moves no ledger row, so it is accepted in a closed period. `terms_days` is a create field only.

## Settling an Item

A payment settles a document only when the settlement is recorded on it. There are two ways, and an `update-event-block` takes one or the other, never both.

| Field | Settles |
| --- | --- |
| `discharges_event_id` | One document, at the payment's full amount. On create or update; an empty string clears it on update. Setting it replaces any allocations |
| `discharge_allocations` | `[{obligation_event_id, amount}]`: each document with the positive cents of the payment applied to it, together at most the payment's amount. Update only. Replaces whatever the payment settled before; an empty list clears it. `discharges_event_id` then reads as the first row's document |

A payment for more than its documents are open leaves the rest unapplied. One linked with `discharges_event_id` to a smaller document overpays it, and the document reads as a negative open amount: an unapplied credit.

Each settlement is checked when it is written, and a refusal is a `422` naming the reason:

- Money in settles an `invoice_issued`, money out a `bill_received`. A `payment_received` or `bill_paid` says which by its type; any other payment, such as a bank line, by the sign of its `amount`.
- The documents must be the payment's own entity's, and not `voided` or `superseded`. The counterparty may differ.
- Each document may be named once, and an `invoice_issued` or `bill_received` cannot itself settle anything.
- The allocations may not apply more than the payment's amount.
- A QuickBooks `payment_received` or `bill_paid` is refused: QuickBooks says what it settles.

**Which payments count.** A settlement counts once its payment is in the books, by the same rule as a document: `committed` or `fulfilled`, or `classified` with an entry of its own standing posted. A payment drafted on books kept in RoboLedger settles its document from the close that posts it; a bank line committed from the inbox settles at once. A payment's date is its `occurred_at`.

### Bank lines

A bank line from a feed is a payment once it is classified. Three things help settle it:

- **The suggestion.** A line that pays one open item of its counterparty for exactly its amount arrives with `metadata.suggestion_source: open_item`, the item named in `suggestion_basis`, and the receivables or payables account as `suggested_element_id` (when the entity has exactly one such account). Committing it with `metadata_patch: {accept_suggestion: true}` settles that item. An item another waiting line has already named is that line's, so two lines are never offered one item, and a suggestion whose item has been paid or claimed since is refused when it is taken.
- **`not_a_settlement`.** Classifying an `open_item` line to any other account is refused, because the item would stay open and the cost would be counted twice, unless the `metadata_patch` says `not_a_settlement: true`. On a line that had taken the suggestion and not yet posted, it also gives the item back.
- **`dischargeCandidates`.** For a line that pays several items, part of one, or a different one, this read lists what it may settle, and the commit records the choice with `discharge_allocations`. A line committed with no settlement leaves the item open.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ dischargeCandidates(eventId: \"evt_…\") { kind amountCents classifiedToControl candidates { eventId documentNumber dueDate daysPastDue openCents } } }"}'
```

`dischargeCandidates(eventId:)` returns `null` for an unknown event, and otherwise:

| Field | Meaning |
| --- | --- |
| `eventId` | The payment or bank line |
| `kind` | `receivables` for money in, `payables` for money out; `null` when the line has no amount to say which, and then `candidates` is empty |
| `amountCents` | The payment's magnitude |
| `classifiedToControl` | Whether the line is classified to a receivables or payables account, where a settlement is expected |
| `candidates` | `OpenItem`s of the line's own entity, and of its counterparty when it names one, open at the line's date: an exact amount match first, then the oldest due date |

Then commit the line with its pick, here a $9,600 deposit applied to two invoices:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/update-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "evt_…", "transition_to": "committed",
       "metadata_patch": {"classified_element_id": "<receivables_account_id>", "classified_by": "user"},
       "discharge_allocations": [
         {"obligation_event_id": "evt_inv_10042", "amount": 480000},
         {"obligation_event_id": "evt_inv_10051", "amount": 480000}]}'
```

Classify a settling line to the receivables or payables account, as the suggestion does: the settlement clears the item in the open-item reads, and the account side is what clears it in the ledger. A line settled while classified elsewhere shows up as a difference in the [tie-out](#the-tie-out). The other bank-line metadata keys are in [Event Block Reference § Bank-feed handlers](Event-Block-Reference.md#bank-feed-handlers).

## Reading Open Balances

Five reads give open balances as they stand now: every document and every payment in the books, whatever its date. For a balance at a date, use the aging reads below.

| Query | Arguments | Returns |
| --- | --- | --- |
| `openReceivables`, `openPayables` | `entityId` | `OpenBalanceAggregate`: `totalOpenCents` (overpayments net into it), `counterpartyCount` (counterparties with a nonzero open document), `openEventCount` (documents with a nonzero open amount), `currency` |
| `openReceivablesByAgent`, `openPayablesByAgent` | `entityId` | `[OpenBalanceByAgent]`: a row per counterparty with a nonzero net, largest absolute balance first. `agentId`, `openBalanceCents` (negative when overpaid), `openEventCount`, `currency`. Documents with no counterparty share a row whose `agentId` is an empty string |
| `agents { openReceivable openPayable }` | none | The same `OpenBalanceByAgent` row for each agent, from the group parent's books, or `null` when it has nothing open. Loaded once per request however many agents ask |
| `openItemsForAgent` | `agentId` (required), `kind` (required: `receivables` or `payables`), `asOf` (default today), `entityId` | `[OpenItem]`: one counterparty's open documents at `asOf`, oldest first |

`entityId` defaults to the group parent everywhere.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ agents(agentType: \"customer\", limit: 50) { id name openReceivable { openBalanceCents openEventCount } } }"}'
```

### OpenItem

The same type comes back from `openItemsForAgent`, `dischargeCandidates`, and a group aging's `intercompany` list.

| Field | Meaning |
| --- | --- |
| `eventId` | The invoice's or bill's event |
| `entityId` | The entity whose books hold it |
| `agentId` | The counterparty, or `null` |
| `occurredOn` | The date it was issued or received |
| `documentNumber` | The invoice or bill number, where the source has one (QuickBooks' document number) |
| `description` | The event's description |
| `originalCents` | Its amount |
| `settledCents` | Applied to it by payments in the books dated on or before the as-of date |
| `openCents` | `originalCents` less `settledCents`; negative when overpaid |
| `currency` | The currency code |
| `dueDate` | The stated due date, else the date it was issued |
| `dueDateBasis` | `stated` when it carries its own due date, `defaulted` when it is read as due on receipt |
| `daysPastDue` | Days from `dueDate` to the as-of date; zero or negative when not yet due |
| `bucket` | `current`, `one_to_thirty`, `thirty_one_to_sixty`, `sixty_one_to_ninety`, `over_ninety`, or `unapplied_credit` |

## Aging at a Date

```text
agedReceivables(asOf: Date, entityId: String, scope: String = "entity"): AgedBalances!
agedPayables(asOf: Date, entityId: String, scope: String = "entity"): AgedBalances!
```

- `asOf` is the date to age at; omitted, it is the server's today. A document counts if it was issued on or before `asOf` and something on it is still unsettled at the end of that day. A payment dated after `asOf` has not happened yet, so a past month-end reads as it stood then.
- `entityId` is the entity whose books to read; with `scope: "group"`, the group parent. Omitted, the group parent.
- `scope` is `entity` (one entity's books) or `group` (see [A Group's Receivables and Payables](#a-groups-receivables-and-payables)). Anything else, or an `entityId` outside the graph, is refused with a GraphQL error naming the problem.

**The buckets.** Each open item falls in one bucket by its days past due at `asOf`:

| Bucket | Days past due | `AgingBuckets` field |
| --- | --- | --- |
| Current | 0 or fewer (not yet due) | `currentCents` |
| 1–30 | 1 to 30 | `oneToThirtyCents` |
| 31–60 | 31 to 60 | `thirtyOneToSixtyCents` |
| 61–90 | 61 to 90 | `sixtyOneToNinetyCents` |
| Over 90 | more than 90 | `overNinetyCents` |
| Unapplied credit | any: an overpaid document, whatever its age | `unappliedCreditCents` (negative) |

`totalCents` is every bucket and the unapplied credit together. An overpayment is never netted into an age bucket.

**`AgedBalances`:**

| Field | Meaning |
| --- | --- |
| `kind`, `asOf`, `scope` | What was read |
| `entityId` | The entity read, or the group parent for a group |
| `memberEntityIds` | The entities summed: one, or the parent and its members |
| `buckets` | The totals by age; for a group, intercompany items excluded |
| `agents` | `[AgedAgentRow]`, a row per counterparty, largest absolute total first: `agentId` and `agentName` (both `null` for documents with no counterparty), `buckets`, `openItemCount`, and `oldestDueDate`, the earliest due date among its documents with something open |
| `defaultedDueDateCount` | Open items with no due date of their own, aged as due on receipt |
| `intercompany`, `pairs` | For a group, the items the members owe each other; empty for an entity |
| `tieOuts` | `[ControlTieOut]`, one per summed entity. See [The Tie-Out](#the-tie-out) |

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ agedReceivables(asOf: \"2026-10-31\") { asOf buckets { currentCents oneToThirtyCents thirtyOneToSixtyCents sixtyOneToNinetyCents overNinetyCents unappliedCreditCents totalCents } agents { agentName openItemCount oldestDueDate buckets { totalCents } } defaultedDueDateCount tieOuts { entityId accounts { code name } controlBalanceCents pendingCloseCents openItemsCents differenceCents } } }"}'
```

A response looks like this:

```json
{
  "data": {
    "agedReceivables": {
      "asOf": "2026-10-31",
      "buckets": {
        "currentCents": 480000,
        "oneToThirtyCents": 125000,
        "thirtyOneToSixtyCents": 0,
        "sixtyOneToNinetyCents": 0,
        "overNinetyCents": 60000,
        "unappliedCreditCents": -15000,
        "totalCents": 650000
      },
      "agents": [
        {
          "agentName": "Harbor & Pine Studio",
          "openItemCount": 2,
          "oldestDueDate": "2026-10-15",
          "buckets": { "totalCents": 605000 }
        },
        {
          "agentName": "Ridgeline Outfitters",
          "openItemCount": 2,
          "oldestDueDate": "2026-06-30",
          "buckets": { "totalCents": 45000 }
        }
      ],
      "defaultedDueDateCount": 1,
      "tieOuts": [
        {
          "entityId": "ent_…",
          "accounts": [{ "code": "1200", "name": "Accounts Receivable" }],
          "controlBalanceCents": 662500,
          "pendingCloseCents": 0,
          "openItemsCents": 650000,
          "differenceCents": -12500
        }
      ]
    }
  }
}
```

## The Tie-Out

Each `ControlTieOut` sets one entity's open items against its control accounts at `asOf`:

| Field | Meaning |
| --- | --- |
| `entityId` | The entity compared |
| `accounts` | `[ControlAccount]` (`elementId`, `code`, `name`): the entity's accounts mapped to `rs-gaap:ReceivablesNetCurrent` (receivables) or `rs-gaap:AccountsPayableCurrent` (payables) in its book mapping, and any account whose QuickBooks account type is Accounts Receivable or Accounts Payable |
| `controlBalanceCents` | Those accounts' posted balance at `asOf` (entries posted, or posted and reversed, with a posting date on or before it), in the natural sign: debit for receivables, credit for payables. `null` when the entity has no control account |
| `pendingCloseCents` | What the close will post to those accounts from documents the open items already count: draft entries dated on or before `asOf` whose event is committed or fulfilled, such as a payment committed in a month still open, in the natural sign `controlBalanceCents` uses (so a payment is negative); `0` with no control account |
| `openItemsCents` | The entity's open items at `asOf`, intercompany included |
| `differenceCents` | `openItemsCents` less `controlBalanceCents` and `pendingCloseCents`; `null` with no control account |

A difference is never plugged. It is everything that moved the accounts without a document behind it: a write-off journal, a payment not linked to its document, an opening balance, or a settling bank line classified to another account.

A document whose entry is still a draft is not a difference. The open items count a committed payment at once, while its entry posts only when the month closes; that entry is `pendingCloseCents`, and the two tie before the close. A draft whose event was voided never posts, so it is not pending. In the example above, the account holds $125 more than the open items. The app shows a tie-out without control accounts as *No control account mapped*; mapping one account to the concept fixes it (see [Chart of Accounts Mapping](Chart-of-Accounts-Mapping.md)).

## A Group's Receivables and Payables

With `scope: "group"`, the aging reads the group parent and every member consolidated in full during `asOf`'s month, and sums them. A member on the equity method is not summed, and a document owed by one stays external.

Every company in a group is also one of the group's counterparty agents ([Group Consolidation § The Group Model](Group-Consolidation.md#the-group-model)). A document whose counterparty is another summed member is **intercompany**: consolidation eliminates it, so it is left out of `buckets`, `agents` and `defaultedDueDateCount`, and listed in `intercompany` instead. Each member's `tieOuts` row still counts its intercompany items, since they sit in its own receivables or payables account.

`pairs` sets both sides of each pair of members side by side, whichever `kind` you asked for:

| Field | Meaning |
| --- | --- |
| `creditorEntityId` | The member owed |
| `debtorEntityId` | The member that owes |
| `receivableCents` | The creditor's open invoices against the debtor |
| `payableCents` | The debtor's open bills to the creditor |
| `differenceCents` | Receivable less payable; zero when the two books agree |

A nonzero `differenceCents` is a disagreement between two members' books. The consolidating entries eliminate the lower side and name the difference ([Group Consolidation § The Consolidating Entries](Group-Consolidation.md#the-consolidating-entries)), so this is the read to settle it from before they are drafted.

## From an AI Client

Two tools on every RoboLedger tenant connection serve the same reads; see [AI Operators and MCP](AI-Operators-and-MCP.md#the-roboledger-route).

**`aged-balances`** takes `kind` (required: `receivables` or `payables`), `as_of` (`YYYY-MM-DD`, default today), `entity_id` (default the group parent), `scope` (`entity` or `group`, default `entity`) and `agent_id`.

- Without `agent_id` it returns the `AgedBalances` shape above in snake_case: `buckets` (`current_cents` … `unapplied_credit_cents`, `total_cents`), `agents`, `defaulted_due_date_count`, `intercompany`, `pairs`, `tie_outs`.
- With `agent_id` it returns that counterparty's documents: `kind`, `as_of`, `agent_id`, `item_count` and `items`, each an open item as above. `agent_id` reads one entity's books, so it is refused with `scope: "group"`; name the member's `entity_id` instead.

**`list-discharge-candidates`** takes `event_id` and returns the `dischargeCandidates` shape: `event_id`, `kind`, `amount_cents`, `classified_to_control` and `candidates`. An unknown event returns `not_found`.

Both return amounts in cents, and a refusal as `{"error": …, "message": …}`: `invalid_arguments` for a bad `kind`, `scope` or `as_of`, `entity_not_found` for an entity outside the graph. The commit that records a pick is `update-event-block` with `discharge_allocations`, as over HTTP. The server's instructions tell a connected model that a settlement is a person's choice: a line with no pick commits as it is, and its bill stays open.

## What a Reversal Does

On books kept in RoboLedger, a posted document is corrected by reopening its month and reversing its entry ([Period Close § Reopening a Period](Period-Close.md#reopening-a-period)). The reads follow the entry:

- **A reversed invoice or bill leaves the open items.** Its event stays `classified`, and with no entry of its own standing posted it is no longer in the books. It can be issued again as a new event.
- **A reversed payment no longer settles.** The documents it paid are open again by what it applied.
- **The tie-out still holds.** The reversal offsets the original in the control account, so both sides drop together.

A document or payment that is `voided` or `superseded` never counts. Those retractions are refused once an event's entries have posted; reverse the posted entry instead ([Event Block Reference § Statuses and Transitions](Event-Block-Reference.md#statuses-and-transitions)).

A QuickBooks document is corrected in QuickBooks. A change that reaches a posted, synced event arrives as a reconciling item ([QuickBooks Sync & Write Policy § Reconciling Items](QuickBooks-Sync-and-Write-Policy.md#reconciling-items)).

## Gotchas and Pitfalls

### A drafted document is not open yet

On books kept in RoboLedger, an invoice written with `apply_handlers: true` is a draft until the close posts it. Query the aging for an open month and it will not be there. List the month's drafts with `periodDrafts(period:)` to see what the close will add.

### `openReceivables` is not an aging total

`openReceivables` and its siblings count every document and payment in the books, whatever their dates, including a document dated in the future. `agedReceivables(asOf:)` counts only what was issued by `asOf` and settled by its end. The two agree only when nothing is dated after today.

### `agents { openReceivable }` reads the group parent

The per-agent fields on `agents` take no `entityId`. For a subsidiary's balances by counterparty, use `openReceivablesByAgent(entityId:)` or the aging's `agents` rows.

### A control account must be mapped to the exact concept

The tie-out finds an entity's receivables accounts by a mapping to `rs-gaap:ReceivablesNetCurrent` (payables: `rs-gaap:AccountsPayableCurrent`) or by QuickBooks' account type. An account mapped to a narrower receivables concept is not a control account, and with none found `controlBalanceCents` and `differenceCents` are `null`.

### Settling a QuickBooks payment by hand is refused

A `payment_received` or `bill_paid` from QuickBooks takes its settlement from QuickBooks. Change the payment's application in QuickBooks and sync.

## Related Documentation

**Guides:**

- **[GraphQL Reads](GraphQL-Reads.md)** - The endpoint these reads are posted to, its auth model, and the units of every amount
- **[Event Block Reference](Event-Block-Reference.md)** - `create-event-block` and `update-event-block` field by field, the bank-feed metadata keys, and the transitions
- **[Event-Driven Ledger](Event-Driven-Ledger.md)** - Events before entries: the lifecycle an invoice and its payment move through
- **[Build a Ledger Integration](Build-a-Ledger-Integration.md)** - Sending invoices and bills from another system
- **[Group Consolidation](Group-Consolidation.md)** - The reporting group, its counterparty agents, and the intercompany eliminations
- **[Period Close](Period-Close.md)** - The close that posts a native document's entry, and reopening a month to reverse one
- **[AI Operators and MCP](AI-Operators-and-MCP.md)** - The tool surface, including `aged-balances` and `list-discharge-candidates`

**Product guide:**

- **[See who owes you and whom you owe](https://roboledger.ai/docs/who-owes-you-and-whom-you-owe)** - The Receivables & Payables page in the RoboLedger app

**Codebase Documentation:**

- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business logic kernel the reads delegate to
- **[GraphQL reference](https://robosystems.ai/docs/extensions/graphql)** - Every query field, with its arguments, return type and an example call, generated from the live schema

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
