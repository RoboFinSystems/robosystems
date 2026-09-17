# Event-Driven Ledger

RoboLedger treats business events as the durable truth and the general ledger as something derived from them. A sale, a payment, an asset disposal — those happen in the world; the debits and credits that record them are a downstream interpretation. The **Event Block** is the single write surface that carries an event into the ledger, and handlers turn it into balanced journal entries.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [The Thesis: Events Explain, the Ledger Is Derived](#the-thesis-events-explain-the-ledger-is-derived)
- [The Three-Level Ledger](#the-three-level-ledger)
- [REA: Resources, Events, Agents](#rea-resources-events-agents)
- [The Event Block Envelope](#the-event-block-envelope)
- [The Event Lifecycle](#the-event-lifecycle)
- [Handlers: How Events Become GL Entries](#handlers-how-events-become-gl-entries)
- [The REA Graph Projection](#the-rea-graph-projection)
- [Posting an Event to the Ledger](#posting-an-event-to-the-ledger)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## The Thesis: Events Explain, the Ledger Is Derived

A traditional accounting system asks the caller to know which accounts to debit and which to credit. RoboLedger inverts that. The caller records **what happened** — an invoice was issued, a payment cleared, equipment was retired — as a single event. A handler then derives the journal entry. The ledger is the projection; the event is the source.

This has three consequences that shape the whole subsystem:

- **One write surface.** There is no raw `POST /events` endpoint and no direct journal-posting API. Adapters (QuickBooks, bank feeds), AI operators, and the UI all create the same Event Block. Every write goes through one validated, atomic path.
- **Amounts are signed integer cents** from the graph-owner's perspective: inflows positive, outflows negative. `$2,500.00` is `250000`, never a float and never dollars.
- **Capture and posting are separable.** An event can be captured for review without touching the ledger, then posted later — or it can fire its handler and post the GL in the same call. Same envelope, two modes.

## The Three-Level Ledger

The ledger has three layers, each with a distinct job. They replace the older two-level (transaction → line item) shape so that the *business event* and the *accounting interpretation* are first-class and separate.

```text
Transaction   what happened in the world (the business event)
   │  TRANSACTION_HAS_ENTRY
   ▼
Entry         the accounting interpretation — MUST balance (Σ debits = Σ credits)
   │  ENTRY_HAS_LINE_ITEM
   ▼
LineItem      one debit OR one credit against a chart-of-accounts element
```

| Level | ID prefix | Role | Key fields |
| --- | --- | --- | --- |
| **Transaction** | `txn_` | The business event as it occurred in the real world | `type`, `amount` (cents), `date`, `currency`, `source`, `status` (`pending` / `posted` / `void`), `triggered_by_event_id` |
| **Entry** | `je_` | The journal entry — an accounting interpretation that must balance | `transaction_id`, `type` (`standard` / `adjusting` / `closing` / `reversing`), `posting_date`, `status` (`draft` / `posted` / `reversed`), `provenance`, `triggered_by_event_id`, `memo` |
| **LineItem** | `li_` | A single debit or a single credit | `entry_id`, `element_id`, `debit_amount`, `credit_amount` (cents; exactly one non-zero), `line_order` |

**The balance rule.** Every Entry must satisfy `Σ debits = Σ credits`. An entry needs at least two line items, and each line item carries exactly one non-zero side. Unbalanced entries are rejected before anything is written.

**The chart of accounts is the `Element` node** — there is no separate `Account` table. A `LineItem` points at the `element_id` it debits or credits, and the same association machinery that drives XBRL taxonomy rollups maps those elements up to reporting concepts.

**One Transaction can carry one Entry, or many.** For pre-journalized sources such as QuickBooks, where the upstream system already produced a journal entry, an Entry is 1:1 with its Transaction. Sources that supply only the raw business event let a handler synthesize the Entry.

## REA: Resources, Events, Agents

The event model rests on the **REA** accounting ontology (Resources, Events, Agents), introduced by William McCarthy in 1982 and later standardized as part of ISO 15944-4. REA is the intellectual grounding for the Event Block; you do not need to know the theory to use it, but the vocabulary it gives the envelope is exactly its vocabulary.

| REA concept | In RoboLedger | Meaning |
| --- | --- | --- |
| **Agent** | `Agent` node (`agt_`) | A counterparty — customer, vendor, employee, anyone the reporting entity transacts *with* |
| **Event** | `Event` node (`evt_`) / Event Block | A business occurrence that exchanges value |
| **Resource** | `resource_type` + optional `resource_element_id` | What is exchanged — goods, services, money, and so on |

The reporting self is the `Entity`; everyone else is an `Agent`. An Event involves an Agent and affects a Resource.

**The action verb vocabulary.** An event can name the economic action it performs via `event_action`, drawn from a fixed set of 19 verbs (string values aligned with Valueflows v1.0):

```text
produce, raise, consume, lower, use, cite, work, deliverService,
pickup, dropoff, accept, transferCustody, transferAllRights,
transfer, move, modify, combine, separate, copy
```

`event_action` is optional — capture-first events frequently leave it null and let the handler infer the accounting effect from `event_type`.

## The Event Block Envelope

The Event Block is the envelope every write carries. It is created via the CQRS command surface at `POST /extensions/roboledger/{graph_id}/operations/create-event-block`, and the response is an `EventBlockEnvelope` wrapped in a standard `OperationEnvelope` (ULID `operationId`, `Idempotency-Key` support). The full request/response schema is published in the live API reference — see [the OpenAPI docs](https://api.robosystems.ai/docs).

**What the envelope carries:**

| Group | Fields | Notes |
| --- | --- | --- |
| **Identity** | `event_type` (open string), `event_category` (enum, class-conditional), `event_class` (`economic` / `support`, default `economic`), `event_action` (nullable) | `event_type` is free-form; the category/class pair is constrained |
| **REA** | `agent_id` (nullable counterparty), `resource_type` (nullable), `resource_element_id` (nullable) | The who and the what |
| **Timing** | `occurred_at` (required), `effective_at` (nullable) | `occurred_at` = when it happened; `effective_at` = accounting recognition date |
| **Provenance** | `source` (required), `external_id` (nullable dedup key), `external_url` (nullable) | The origin system and its identifier for the event |
| **Value** | `amount` (nullable, signed cents), `currency` (default `USD`) | Signed from the owner's perspective |
| **Payload** | `description`, `metadata` (free dict), `dimension_ids` (list) | `metadata` is handler-validated when posting |
| **Duality / correction** | `obligated_by_event_id`, `discharges_event_id`, `replaced_by_event_id`, `replaces_event_id` (all nullable) | Link an event to the obligation it discharges. The `obligated_by` / `discharges` pair is settable on create; the `replaces` pair is populated by the supersede flow, not supplied on a plain create |
| **Control** | `apply_handlers` (bool, default `False`) | Whether to fire a handler and post the GL in this call |

The envelope adds `id`, `status`, `created_at`, `created_by`, and `is_reconciling_item` to the response.

**`resource_type` is a closed enum; `source` deliberately is not.** `resource_type` is one of `goods, services, money, right, obligation, information, labor` (or null), enforced by a CHECK constraint. `source` carries no CHECK constraint at all: the platform-emitted values `manual`, `system`, and `schedule` are always valid, and anything else is validated at the operations layer against the graph's **registered platform Connections**. So it accepts a connected provider name (`quickbooks`, `xero`, `plaid`, …) or a `source_name` registered through an `external` connection — registering a connection opens a new source name without a schema change.

**`event_category` is gated by `event_class`** at the database level. Economic events use `sales, purchase, financing, payroll, treasury, adjustment, recognition, other`; support events use `control, approval, reconciliation, inquiry`. A `sales` category on a `support` event is rejected by a CHECK constraint — the class and category must agree.

**The three-phase validation pipeline.** Every create runs schema validation (Pydantic types and enums), then category/class consistency (the DB CHECK), and — only when `apply_handlers=true` — handler validation of `metadata` and the resulting entries. A capture-only event clears the first two phases and is stored without ever touching handler logic.

**Idempotency is `(source, external_id)`.** A partial unique index over events with a non-null `external_id` means re-posting the same `external_id` from the same `source` will not duplicate the event. Omit `external_id` and you give up that guarantee — supply it for anything an adapter might retry.

**Reconciling items are the escape hatch when an adapter changes its mind.** `committed` and `fulfilled` events are immutable to sync, so when a re-sync surfaces a changed upstream payload for an event whose GL is already posted, the platform does not rewrite the books. It sets `events.payload_drift` (a partial-indexed Boolean that drives a reconciliation queue), stashes the incoming payload in `metadata.drift_payload` with `metadata.drift_detected_at`, and surfaces the event as a **reconciling item** — the local books legitimately no longer mirror the source, and the difference awaits an explicit disposition: restate the affected months, or book a catch-up entry in the open period. The live payload and GL are untouched. Read the queue with the `isReconcilingItem` Boolean filter on the `eventBlocks` GraphQL field.

## The Event Lifecycle

An event moves through a small, constrained state machine. There are seven statuses and three of them are terminal. Every non-terminal status can move to `voided` or `superseded`; the forward path runs toward `fulfilled`.

```text
captured ──▶ committed ──▶ pending ──▶ fulfilled   (terminal)

classified ─▶ committed | pending | fulfilled

every non-terminal status also ──▶ voided      (terminal)
                              and ──▶ superseded (terminal)
```

| From | Allowed transitions |
| --- | --- |
| `captured` | `committed`, `voided`, `superseded` |
| `classified` | `committed`, `pending`, `fulfilled`, `voided`, `superseded` |
| `committed` | `pending`, `fulfilled`, `voided`, `superseded` |
| `pending` | `fulfilled`, `voided`, `superseded` |
| `fulfilled` | terminal — no transitions |
| `voided` | terminal — no transitions |
| `superseded` | terminal — no transitions |

**Two creation modes determine the landing status:**

- **Capture-only** (`apply_handlers=false`, the default): the event is recorded for review and lands at `captured`. No GL is written. This is the inbox path — adapters and the UI drop raw events here and let a human or an operator classify them later.
- **Fire-handler** (`apply_handlers=true`): a handler resolves and posts the GL atomically in the same call. Where the event lands depends on the work it did — a journal whose `metadata.status` is `posted` reaches `fulfilled`, while a `draft` journal lands at `classified` (it still needs a period close to post).

There is no standalone `void` or `supersede` operation. Both transitions go through `update-event-block` with a `transition_to` field; moving to `superseded` additionally requires a `superseded_by_id`.

**Two operations move events forward without a fresh create:**

- **`execute-event-block`** completes the capture-to-post story for graphs whose connection carries `write_policy='qb_authoritative'` or `'hybrid'`. It publishes the captured GL plan to the source-of-truth system, records the returned transaction id on the event's metadata, transitions the event to `fulfilled` (or `pending` if the upstream system rejects it), and promotes the draft GL rows to `posted`. Native-policy events fast-path through with no external write — RoboSystems is the system of record there.
- **`promote-obligations`** sweeps matured `schedule_entry_due` obligations whose period boundary has passed, promotes them to `classified`, and drafts their closing entries in the same transaction.

Both are live command operations under `/extensions/roboledger/{graph_id}/operations/` and are exposed as MCP tools.

## Handlers: How Events Become GL Entries

A handler is the piece that knows the accounting. It receives the event and produces one or more balanced entries. RoboLedger resolves handlers from a **hybrid registry** with two layers:

- **Python handlers** for complex, multi-step workflows. A fixed set of seven hub-defined handlers covers the core economic events (recording a journal entry, reversing one, receiving a payment, paying a bill, disposing of an asset, and the two schedule handlers). Python handlers win when both layers could match.
- **DSL handlers** in a database-backed `event_handlers` table. Each row is a declarative template that maps an event to debit/credit entries — simple enough that a tenant can configure new GL patterns without code. This is the fallback layer.

**Aliases mean the seven handlers answer to far more than seven `event_type`s.** The Python registry maps roughly a dozen additional event types onto `journal_entry_recorded`, because the on-approve GL shape is identical and only the inbox label, `event_category`, and downstream filtering differ. `invoice_issued`, `bill_received`, `sales_receipt_recorded`, `cash_expense_recorded`, `check_written`, `credit_card_charge`, `credit_card_refund`, `deposit_received`, and `inventory_adjusted` all resolve this way — so the `invoice_issued` event in Step 1 below *does* have a handler, even though it isn't one of the seven names.

**The DSL template shape.** A handler's `transaction_template` is a JSON document describing the entries to build:

```json
{
  "transactions": [
    {
      "entry_template": {
        "debit":  { "element_id": "elem_x", "amount": "{{ event.amount }}" },
        "credit": { "element_id": "elem_y", "amount": "{{ event.amount }}" }
      }
    }
  ]
}
```

Templates interpolate values from the event and the handler:

| Token | Resolves to |
| --- | --- |
| `{{ event.amount }}` | The event's signed-cents amount |
| `{{ event.metadata.foo }}` | A field from the event's `metadata` dict |
| `{{ handler.metadata.bar }}` | A field from the handler's own metadata |

A trailing ` / N` suffix performs integer division (for example, splitting an amount across entries). The engine enforces that every entry it builds is balanced (`debit == credit`) and that all amounts are non-negative.

**Resolution can fail, and that is an error — not a silent capture.** Posting an event whose `event_type` has no registered handler (with `apply_handlers=true`) returns a handler-not-found error rather than quietly storing the event.

You can register a DSL handler through the command surface at `POST /extensions/roboledger/{graph_id}/operations/create-event-handler`.

## The REA Graph Projection

Events and the entries they produce live first in the extensions OLTP database. They are then **materialized into the graph**, where the audit chain becomes queryable in Cypher. The split is deliberate: the graph holds durable business truth, the OLTP store holds operational state.

`Agent` and `Event` are promoted into the graph's **base** schema — they are not RoboLedger-specific, because the REA model is general. The base layer also carries the duality and correction edges:

| Base node / edge | Meaning |
| --- | --- |
| `Agent`, `Event` | The REA party and occurrence |
| `ENTITY_HAS_AGENT`, `ENTITY_HAS_EVENT` | An entity owns its counterparties and events |
| `EVENT_INVOLVES_AGENT` | An event names its counterparty |
| `EVENT_AFFECTS_RESOURCE` | An event touches a resource |
| `EVENT_OBLIGATED_BY_EVENT`, `EVENT_DISCHARGES_EVENT` | Obligation duality between events |
| `EVENT_REPLACES_EVENT` | Correction lineage |

The RoboLedger extension adds the **bridge** edges that connect an event to the ledger it produced:

| Extension edge | Meaning |
| --- | --- |
| `EVENT_TRIGGERS_TRANSACTION` | The McCarthy bridge — links an `Event` to the `Transaction` it caused |
| `TRANSACTION_HAS_ENTRY` | A transaction's journal entries |
| `ENTRY_HAS_LINE_ITEM` | An entry's debits and credits |
| `LINE_ITEM_RELATES_TO_ELEMENT` | A line item's chart-of-accounts element |

`EVENT_TRIGGERS_TRANSACTION` is materialized from each transaction's `triggered_by_event_id`, so the full event → transaction → entry → line-item trail is walkable in one query. **This edge only exists after materialization** — until then the relationship lives in the OLTP column.

## Posting an Event to the Ledger

This is the contribution path: how you actually put an event into a graph and watch it become GL. All examples target `https://api.robosystems.ai`, authenticate with `X-API-Key`, and use a RoboLedger graph.

**Prerequisites:**

- A RoboSystems account and an API key, created in the app under **Settings → API keys**. See [Quick Start](Quick-Start).
- A RoboLedger graph — on the hosted platform, one comes from roboledger.ai when you connect QuickBooks, or from **Create Graph** in the app. The command surface requires `ROBOLEDGER_ENABLED` on the deployment; on robosystems.ai it is on.
- Set your credentials:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector
```

The `elem_*` element IDs and `agt_*` agent ID below are **illustrative placeholders**. A live graph resolves real ULIDs from its own chart of accounts; list them first with a GraphQL read of your accounts before posting against real elements.

### Step 1: Capture an Event Without Touching the Ledger

A plain create defaults to `apply_handlers=false`, so the event lands in the inbox at status `captured` and writes no GL. This is how an adapter records a raw business event for later classification.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "invoice_issued",
    "event_category": "sales",
    "event_class": "economic",
    "agent_id": "agt_customer_acme",
    "resource_type": "services",
    "occurred_at": "2026-05-01T14:30:00Z",
    "source": "manual",
    "external_id": "demo-inv-0421",
    "amount": 250000,
    "currency": "USD",
    "description": "April consulting retainer — Acme Corp",
    "metadata": {"invoice_number": "INV-2026-0421", "terms": "net_30"}
  }'
```

The response is an `EventBlockEnvelope` with a fresh `evt_`-prefixed `id` and `status: "captured"`.

### Step 2: Record a Journal Entry Through the Event Layer

To post in a single call, set `apply_handlers: true`. This fires the `journal_entry_recorded` Python handler, which posts the GL atomically. Amounts are in cents, and every entry must balance.

The handler accepts **two metadata shapes**, and the validator enforces exactly one:

- **Flat** — one entry described by top-level fields (`posting_date`, `memo`, `status`, `type`, and a balanced `line_items` array). This is the native, manual, and scheduled path, and it is what the example below uses.
- **Nested** — an `entries[]` array of per-entry specs, for QuickBooks ingest and multi-entry imports where one event produces several journals (a revenue entry and a tax entry, say).

Supplying both is rejected, as is supplying neither.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "journal_entry_recorded",
    "event_category": "adjustment",
    "event_class": "economic",
    "occurred_at": "2026-05-31T00:00:00Z",
    "source": "manual",
    "amount": 12500,
    "currency": "USD",
    "description": "Office supplies accrual",
    "apply_handlers": true,
    "metadata": {
      "posting_date": "2026-05-31",
      "memo": "May office supplies accrual",
      "status": "draft",
      "type": "adjusting",
      "line_items": [
        {"element_id": "elem_coa_office_supplies", "debit_amount": 12500},
        {"element_id": "elem_coa_accounts_payable", "credit_amount": 12500}
      ]
    }
  }'
```

With `"status": "draft"` the event lands `classified` — the entry is created but still needs a period close to post. Use `"status": "posted"` and the event reaches `fulfilled`.

### Step 3: Preview the GL Plan Before Committing

`preview-event-block` takes the same body as a create but writes nothing. It returns the planned debit/credit lines and a `would_succeed` flag — a safe dry run for an operator or UI before it commits.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/preview-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "journal_entry_recorded",
    "event_category": "adjustment",
    "occurred_at": "2026-05-31T00:00:00Z",
    "source": "manual",
    "amount": 12500,
    "metadata": {
      "posting_date": "2026-05-31", "memo": "preview", "status": "draft",
      "line_items": [
        {"element_id": "elem_coa_office_supplies", "debit_amount": 12500},
        {"element_id": "elem_coa_accounts_payable", "credit_amount": 12500}
      ]
    }
  }'
```

### Step 4: Read the Events Back via GraphQL

Reads go through the typed GraphQL surface at `POST /extensions/{graph_id}/graphql`. The `graph_id` is the URL — never a query argument. The `eventBlocks` field filters by `eventType`, `eventCategory`, `status`, `agentId`, `source`, plus `limit` / `offset`; `eventBlock(id)` fetches a single event.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ eventBlocks(status: \"captured\", source: \"manual\", limit: 10) { id eventType eventCategory status amount currency occurredAt agentId } }"}'
```

Introspection over POST works on the hosted API, so you can discover the full event schema with an introspection query; the in-browser GraphiQL explorer is development-only and is not mounted on `api.robosystems.ai`.

### Step 5: Trace the Audit Chain in the Graph

After the events are materialized, the McCarthy bridge is queryable. This walks an event all the way to its line items, proving the event → GL trail:

```cypher
MATCH (e:Event)-[:EVENT_TRIGGERS_TRANSACTION]->(t:Transaction)
      -[:TRANSACTION_HAS_ENTRY]->(en:Entry)
      -[:ENTRY_HAS_LINE_ITEM]->(li:LineItem)
RETURN e.event_type, t.amount, en.status, li.debit_amount, li.credit_amount
LIMIT 10
```

Run it through the Cypher query endpoint, `POST /v1/graphs/{graph_id}/query/cypher`, or the MCP `read-graph-cypher` tool — see [Querying the Analytical Graph](Querying-the-Analytical-Graph):

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/query/cypher" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (e:Event)-[:EVENT_TRIGGERS_TRANSACTION]->(t:Transaction)-[:TRANSACTION_HAS_ENTRY]->(en:Entry)-[:ENTRY_HAS_LINE_ITEM]->(li:LineItem) RETURN e.event_type, t.amount, en.status, li.debit_amount, li.credit_amount LIMIT 10"}'
```

If the query returns nothing, the events have not been materialized yet — `EVENT_TRIGGERS_TRANSACTION` is built from `transactions.triggered_by_event_id` during materialization, not at write time.

### Doing the Same from MCP and AI Operators

The same paths are available to AI clients. Write tools `create-event-block` and `update-event-block` are auto-generated from the operation specs, and read tools `get-event-block` and `list-event-blocks` mirror the GraphQL reads. An operator captures, previews, and posts events through these tools exactly as the curl examples above do.

### Common Pitfalls

| Pitfall | Fix |
| --- | --- |
| Plain create wrote no GL | `apply_handlers` defaults to `false`. Pass `apply_handlers: true` to post in one call. |
| `event_category` rejected | The category must match `event_class`. Economic categories need `economic`; support categories need `support`. |
| Amount looks 100× too small/large | `amount` is signed integer **cents**, not dollars and not a float. |
| Handler-not-found error | An unrecognized `event_type` with `apply_handlers=true` is an error, not a silent capture. |
| Unbalanced-entry error | A journal needs at least two line items, each with exactly one non-zero side, and `Σ debits = Σ credits`. |
| Duplicate events from retries | Supply `external_id`; idempotency keys on `(source, external_id)`. Omit it and you lose the guarantee. |
| GraphQL error about `graphId` | GraphQL takes no `graphId` argument; scope is the URL `/extensions/{graph_id}/graphql`. |
| Can't find a `void` operation | There isn't one. Use `update-event-block` with `transition_to`; `superseded` also needs `superseded_by_id`. |
| Cypher trace returns nothing | The graph edge only exists after materialization. |

## Self-hosted deployments

On your own stack, `just demo-roboledger` provisions a RoboLedger graph with synthetic data and writes the `graph_id` and `api_key` into `.local/config.json`; `just demo-roboledger --skeleton` gives an empty graph, with `just demo-user` for credentials. To run the Step 5 trace directly against LadybugDB, bypassing the API:

```bash
just lbug-query $GRAPH_ID "MATCH (e:Event)-[:EVENT_TRIGGERS_TRANSACTION]->(t:Transaction)-[:TRANSACTION_HAS_ENTRY]->(en:Entry)-[:ENTRY_HAS_LINE_ITEM]->(li:LineItem) RETURN e.event_type, t.amount, en.status, li.debit_amount, li.credit_amount LIMIT 10"
```

## Related Documentation

**Wiki Guides:**
- **[Information Blocks](Information-Blocks)** - The Event Block sits at the bottom of the block stack; events produce the facts that populate Information Blocks
- **[RoboLedger Operations](RoboLedger-Operations)** - The command-write surface (`create-event-block`, `close-period`, and the rest) and the operations kernel these writes flow through
- **[Architecture Overview](Architecture-Overview)** - How the extensions surface, GraphQL reads, and CQRS command writes fit the platform

**Codebase Documentation:**
- **[API Reference](https://api.robosystems.ai/docs)** - Live OpenAPI spec for every operation, request model, and response envelope
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration and the reads/commands kernel
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - The typed read surface and resolver patterns
- **[Extensions Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/extensions/README.md)** - RoboLedger OLTP SQLAlchemy models with schema-per-graph tenancy

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
