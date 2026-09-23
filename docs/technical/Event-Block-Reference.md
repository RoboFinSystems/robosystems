# Event Block Reference

The reference for the Event Block: request and response fields, the enum vocabularies, the status transitions, the handler registry and template grammar, the ledger records a handler writes, and the graph edges they materialize to. For the concepts, read [Event-Driven Ledger](Event-Driven-Ledger) first; for a worked integration, [Build a Ledger Integration](Build-a-Ledger-Integration).

All operations are `POST /extensions/roboledger/{graph_id}/operations/{name}`, return an `OperationEnvelope` whose `result` carries the payload below, and accept an `Idempotency-Key` header. See [Operations Contract](Operations-Contract). Exact types are in the live spec at [robosystems.ai/docs/api](https://robosystems.ai/docs/api).

## Table of Contents

- [create-event-block Request](#create-event-block-request)
- [The Event Block Envelope](#the-event-block-envelope)
- [Vocabularies](#vocabularies)
- [Statuses and Transitions](#statuses-and-transitions)
- [update-event-block](#update-event-block)
- [preview-event-block](#preview-event-block)
- [execute-event-block](#execute-event-block)
- [Validation and Idempotency](#validation-and-idempotency)
- [Reconciling Items](#reconciling-items)
- [Handlers](#handlers)
- [Ledger Records](#ledger-records)
- [Graph Projection](#graph-projection)
- [Reads](#reads)
- [Errors](#errors)
- [Related Documentation](#related-documentation)
- [Support](#support)

## create-event-block Request

| Field | Required | Default | Meaning |
| --- | --- | --- | --- |
| `event_type` | yes | | Open string naming what happened. Selects the handler. |
| `event_category` | yes | | Closed set, and must agree with `event_class`. See [Vocabularies](#vocabularies). |
| `event_class` | no | `economic` | `economic` or `support` |
| `event_action` | no | null | One of the 19 action verbs |
| `agent_id` | no | null | The counterparty (`agt_…`) |
| `resource_type` | no | null | What is exchanged |
| `resource_element_id` | no | null | The element for the resource, when there is one |
| `occurred_at` | yes | | When it happened |
| `effective_at` | no | null | The accounting recognition date, when it differs |
| `source` | yes | | Where the event came from. See [Validation and Idempotency](#validation-and-idempotency). |
| `external_id` | no | null | The source's own id for the event; the dedup key |
| `external_url` | no | null | A link to the event in the source system |
| `amount` | no | null | Signed integer cents, from the graph owner's point of view |
| `currency` | no | `USD` | ISO 4217 |
| `description` | no | null | Free text |
| `metadata` | no | `{}` | Free object; validated by the handler when one fires |
| `dimension_ids` | no | `[]` | Dimensions to tag the event with |
| `obligated_by_event_id` | no | null | The event this one was obligated by |
| `discharges_event_id` | no | null | The event this one settles |
| `apply_handlers` | no | `false` | Fire the handler and write the entry in this call |

## The Event Block Envelope

`EventBlockEnvelope` is what every event write and read returns. It echoes the request fields (`event_type`, `event_category`, `event_class`, `event_action`, `agent_id`, `resource_type`, `resource_element_id`, `occurred_at`, `effective_at`, `source`, `external_id`, `external_url`, `amount`, `currency`, `description`, `metadata`, `dimension_ids`, `obligated_by_event_id`, `discharges_event_id`) and adds:

| Field | Meaning |
| --- | --- |
| `id` | The event id, `evt_…` |
| `status` | See [Statuses and Transitions](#statuses-and-transitions) |
| `is_reconciling_item` | The event has a changed upstream payload awaiting a disposition |
| `replaced_by_event_id`, `replaces_event_id` | The correction chain, set by a supersede |
| `created_at`, `created_by` | Audit |

## Vocabularies

**`event_class`** and **`event_category`** are paired, and the database enforces the pairing:

| `event_class` | Allowed `event_category` |
| --- | --- |
| `economic` | `sales`, `purchase`, `financing`, `payroll`, `treasury`, `adjustment`, `recognition`, `other` |
| `support` | `control`, `approval`, `reconciliation`, `inquiry` |

Support events record value-chain and audit-trail steps and have no ledger effect.

**`resource_type`**: `goods`, `services`, `money`, `right`, `obligation`, `information`, `labor`, or null.

**`event_action`**, 19 verbs aligned with Valueflows: `produce`, `raise`, `consume`, `lower`, `use`, `cite`, `work`, `deliverService`, `pickup`, `dropoff`, `accept`, `transferCustody`, `transferAllRights`, `transfer`, `move`, `modify`, `combine`, `separate`, `copy`. Optional; a capture-first event usually leaves it null.

**`source`**: `manual`, `system`, and `schedule` are always accepted. Any other value must name a provider or source registered as a connection on the graph; registering a connection is what opens a new source name.

## Statuses and Transitions

| From | Allowed `transition_to` |
| --- | --- |
| `captured` | `classified`, `committed`, `voided`, `superseded` |
| `classified` | `committed`, `pending`, `fulfilled`, `voided`, `superseded` |
| `committed` | `pending`, `fulfilled`, `voided`, `superseded` |
| `pending` | `fulfilled`, `voided`, `superseded` |
| `fulfilled` | `voided`, `superseded` |
| `voided` | none |
| `superseded` | none |

- **Landing status on create.** Capture-only: `captured`. With `apply_handlers: true`, the handler sets it (see [Handlers](#handlers)); the journal-entry handler lands `classified` for a draft and `fulfilled` for an entry posted immediately, and a DSL handler lands `classified`.
- **Retractions are guarded.** `voided` and `superseded` are refused, whatever the current status, once any of the event's entries or transactions have posted, or the event has published to QuickBooks. Reverse the posted entry instead.
- **`superseded` needs `superseded_by_id`**, naming an existing event other than itself. Both sides of the chain are set together.
- **`captured` or `classified` → `committed`** fires the event's Python handler unless the event already has an entry linked to it.
- **`captured` → `classified`** gives the handler a chance to refuse a classification it could not post.
- Scheduled obligations (`schedule_entry_due`) are created `pending` and moved to `classified` by the promotion sweep (`promote-obligations`, or its background sensor) when their month matures.

## update-event-block

| Field | Meaning |
| --- | --- |
| `event_id` | Required |
| `transition_to` | `classified`, `committed`, `pending`, `fulfilled`, `voided`, or `superseded` |
| `superseded_by_id` | Required with `superseded` |
| `description` | Replacement text; empty string clears it |
| `effective_at` | New recognition date |
| `metadata_patch` | Merged into `metadata` key by key (a supplied key replaces that key's value) |
| `event_action` | Set or correct the verb |
| `obligated_by_event_id`, `discharges_event_id` | Late-bind the duality links, for example marking a payment as settling an invoice after the fact |

The metadata patch is applied before the handler runs, so a commit sees the final shape. A handler error rolls back the whole update, status change included. Returns the updated envelope.

## preview-event-block

Takes a `create-event-block` body and writes nothing. Returns:

| Field | Meaning |
| --- | --- |
| `matched_handler` | The DSL handler that matched; null when a Python handler matched |
| `planned_transactions[]` | Per entry: `entry_index`, `debit_element_id`, `credit_element_id`, `amount_cents`, and the interpolated debit and credit amounts |
| `validation_errors[]` | Why it would fail |
| `would_succeed` | Whether a create with `apply_handlers: true` would succeed |
| `handler_metadata` | The Python handler's name and computed values, when one matched |

For multi-leg entries the plan lists the first debit and first credit of each entry.

## execute-event-block

Publishes an event to its connection's system of record. Request: `event_id`, and optionally `connection_id` to override the event's `metadata.connection_id`.

| Connection | Result |
| --- | --- |
| No connection on the event | Returned unchanged; nothing is written externally |
| `write_policy: native` | Returned unchanged; RoboLedger is the system of record |
| `write_policy: qb_authoritative` | The entry is written to QuickBooks. On success the event moves to `fulfilled`, its QuickBooks id is stored on `metadata.qb_external_id`, and its draft entries and transactions post. On rejection the event moves to `pending`, the rejection is stored on `metadata.last_outbound_error`, and the drafts stay draft for a retry. |

Response: `event_id`, `status`, `qb_external_id`, `qb_error` (`{code, message, qb_response_at}` on rejection). An event already `fulfilled` or already carrying a QuickBooks id is returned as-is; a `voided` or `superseded` event is refused. Write policies are covered in [QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy).

## Validation and Idempotency

A create is validated in three phases:

1. **Schema.** Types and enums on the request.
2. **Category and class.** The pairing in [Vocabularies](#vocabularies), enforced by a database constraint.
3. **Handler.** Only with `apply_handlers: true`: the handler validates `metadata`, and the entries it builds must balance.

A capture-only event clears the first two phases and is stored without handler logic.

**Idempotency is `(source, external_id)`.** A second create with the same pair is refused as a duplicate. Omit `external_id` and there is no dedup; supply it for anything a sender might retry. This is separate from the `Idempotency-Key` header, which replays the response to a retried request.

## Reconciling Items

When a re-sync brings a changed payload for an event whose entry has already posted, the event is flagged rather than rewritten: `is_reconciling_item` is true, the incoming payload is held on `metadata.drift_payload` with `metadata.drift_detected_at`, and the posted entry and stored payload are left as they were. List them with `eventBlocks(isReconcilingItem: true)`.

`preview-reconciling-item` shows the change and what each disposition would do; `resolve-reconciling-item` applies one:

| Disposition | Effect |
| --- | --- |
| `restate` | Rebuild the event's entries from the new payload. The default when every period the event touches is open. |
| `catch_up` | Post an entry for the difference in an open period (by default the end of the earliest one), as a draft unless `status: posted`. The default when any period the event touches is closed. |
| `acknowledge` | Mark it handled without a ledger change; a `note` is required |

Writes from a disposition are subject to the closed-period gate. Detail is in [QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy).

## Handlers

Resolution runs in order: the Python registry first, then the graph's DSL handlers. With `apply_handlers: true`, no match is an error.

### Python handlers

The registry is fixed in platform code. The core handlers:

| `event_type` | What it writes | Lands at |
| --- | --- | --- |
| `journal_entry_recorded` | One or more journal entries from metadata (shapes below) | `classified` for a draft, `fulfilled` if posted |
| `journal_entry_reversed` | A reversing entry, posted immediately | `fulfilled` |
| `payment_received` | The receipt entry, and links the payment to the invoice it settles | `classified` |
| `bill_paid` | The payment entry, and links it to the bill it settles | `classified` |
| `asset_disposed` | Disposes of a scheduled asset: posts the disposal entry (net book value, gain or loss) and voids the schedule's remaining obligations | `fulfilled` |
| `schedule_created` | The originating event of a schedule's obligation chain | `committed` |
| `schedule_entry_due` | One month's draft closing entry from the schedule's template | `classified` |

Source transaction types that book the same way share the journal-entry handler: `invoice_issued`, `bill_received`, `sales_receipt_recorded`, `cash_expense_recorded`, `check_written`, `credit_card_charge`, `credit_card_refund`, `deposit_received`, `inventory_adjusted`. Only their inbox label, category, and downstream filtering differ.

**`journal_entry_recorded` metadata** takes exactly one of two shapes; both or neither is rejected.

- **Flat**, one entry: `posting_date`, `memo`, `line_items[]` (each `element_id`, `debit_amount`, `credit_amount` in cents with exactly one non-zero, optional `description` and `metadata`), `type` (`standard` default, `adjusting`, `closing`, `reversing`), `status` (`draft` default, or `posted`), optional `transaction_id`.
- **Nested**, several entries from one event: `entries[]`, each with `posting_date`, `memo`, `line_items[]` (at least two; a line may name `element_external_id` instead of `element_id`, resolved through the source connection), `type`, and optional `external_id`. `status` applies to all of them. This is the shape an importer uses when one source transaction produces several journals.

`publish_to_source` (true or false) overrides whether the entry is published back to the source system; left unset it follows the event's `source`.

### DSL handlers

Registered with `create-event-handler` (and managed with `update-event-handler`, `list-event-handlers`, `get-event-handler`). A handler row has `name`, `description`, `event_type`, optional match fields (`event_category`, `match_source`, `match_agent_type`, `match_resource_type`, `match_metadata_expression`), a `transaction_template`, `priority`, and `is_active`.

**Matching.** Among active handlers with the event's `event_type`, a null match field is a wildcard and a set one must equal the event's value. The highest `priority` among the matches wins; two matches tied at the top priority are an error rather than a guess. A handler suggested by an AI operator is not used until someone approves it.

**Template.** The `transaction_template` lists one or more balanced entries:

```json
{
  "transactions": [
    {
      "entry_template": {
        "debit":  { "element_id": "elem_…", "amount": "{{ event.amount }}" },
        "credit": { "element_id": "elem_…", "amount": "{{ event.amount }}" }
      }
    }
  ]
}
```

| Expression | Resolves to |
| --- | --- |
| `{{ event.amount }}` | The event's amount in cents |
| `{{ event.metadata.foo }}` | A field of the event's `metadata` (dotted paths reach nested fields) |
| `{{ handler.metadata.bar }}` | A field of the handler's own metadata |
| `{{ … }} / N` | Integer division by a whole number, the only arithmetic allowed |

A missing field, division of a non-integer, or division by zero is an error. Every entry built must balance, and every amount must be non-negative.

## Ledger Records

What a handler writes, level by level:

| Record | Id | Fields |
| --- | --- | --- |
| **Transaction** | `txn_…` | `type`, `amount` (cents, non-negative), `date`, `currency`, `source`, `status` (`pending` / `posted` / `void`), `triggered_by_event_id` |
| **Entry** | `je_…` | `transaction_id`, `type` (`standard` / `adjusting` / `closing` / `reversing`), `posting_date`, `status` (`draft` / `posted` / `reversed`), `provenance`, `memo`, `triggered_by_event_id` |
| **LineItem** | `li_…` | `entry_id`, `element_id`, `debit_amount`, `credit_amount` (cents; exactly one non-zero), `flow_element_id` (the cash-flow or equity flow the line belongs to), `description`, `line_order` |

An entry needs at least two line items and must balance. A posted entry is corrected by reversing it, never by editing it, and an entry can be reversed at most once.

## Graph Projection

Events and entries are written to the extensions database and then **materialized** into the graph, where the audit chain is queryable in Cypher. The graph edges exist only after materialization.

`Agent` and `Event` belong to the graph's base schema, since REA is not specific to RoboLedger:

| Base node or edge | Meaning |
| --- | --- |
| `Agent`, `Event` | The counterparty and the occurrence |
| `ENTITY_HAS_AGENT`, `ENTITY_HAS_EVENT` | The entity owns its counterparties and events |
| `EVENT_INVOLVES_AGENT` | The event's counterparty |
| `EVENT_AFFECTS_RESOURCE` | The resource the event touches |
| `EVENT_OBLIGATED_BY_EVENT`, `EVENT_DISCHARGES_EVENT` | Obligation and settlement between events |
| `EVENT_REPLACES_EVENT` | The correction chain |

The RoboLedger extension adds the edges from an event to the ledger it produced:

| Extension edge | Meaning |
| --- | --- |
| `EVENT_TRIGGERS_TRANSACTION` | The event to the transaction it caused, built from `triggered_by_event_id` |
| `TRANSACTION_HAS_ENTRY` | A transaction's journal entries |
| `ENTRY_HAS_LINE_ITEM` | An entry's debits and credits |
| `LINE_ITEM_RELATES_TO_ELEMENT` | A line item's chart-of-accounts element |

```cypher
MATCH (e:Event)-[:EVENT_TRIGGERS_TRANSACTION]->(t:Transaction)
      -[:TRANSACTION_HAS_ENTRY]->(en:Entry)
      -[:ENTRY_HAS_LINE_ITEM]->(li:LineItem)
RETURN e.event_type, t.amount, en.status, li.debit_amount, li.credit_amount
LIMIT 10
```

Run it with the MCP `read-graph-cypher` tool or the query endpoint in [Querying the Analytical Graph](Querying-the-Analytical-Graph).

## Reads

GraphQL at `POST /extensions/{graph_id}/graphql`:

- `eventBlocks(eventType, eventCategory, status, agentId, source, isReconcilingItem, limit, offset)`
- `eventBlock(id)`

MCP: `list-event-blocks`, `get-event-block`, and the write tools `create-event-block`, `update-event-block`, `preview-event-block`, `execute-event-block`, `promote-obligations`.

## Errors

| Symptom | Cause |
| --- | --- |
| A create wrote no ledger rows | `apply_handlers` defaults to `false`; the event was captured |
| Category rejected | `event_category` does not belong to `event_class` |
| Source rejected | `source` is not a platform value or a connection registered on the graph |
| Duplicate event | The `(source, external_id)` pair already exists |
| Handler not found, or ambiguous | No active handler matches the `event_type`, or two DSL handlers tie at the top priority |
| Metadata validation failed | The handler's metadata schema rejected `metadata`, or both or neither journal shape was supplied |
| Unbalanced entry | Fewer than two line items, a line with both or neither side set, or Σ debits ≠ Σ credits |
| Invalid transition | The move is not in the transition table, or `superseded` came without `superseded_by_id` |
| Effects already landed | A void or supersede of an event whose entries have posted or that has published to QuickBooks |
| Closed period | The write would put a ledger row in, or remove one from, a closed period; reopen it first |
| Amount off by 100× | `amount` and line amounts are integer cents |

## Related Documentation

**Wiki Guides:**
- **[Event-Driven Ledger](Event-Driven-Ledger)** - The concepts: events before entries, the lifecycle, capture and commit
- **[Build a Ledger Integration](Build-a-Ledger-Integration)** - Posting events from your own system
- **[Period Close](Period-Close)** - How drafts post
- **[QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy)** - Write-back and reconciling items
- **[Operations Contract](Operations-Contract)** - The envelope, idempotency, and error format

**Codebase Documentation:**
- **[API Reference](https://robosystems.ai/docs/api)** - Request and response schemas
- **[Extensions Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/extensions/README.md)** - The RoboLedger OLTP models

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
