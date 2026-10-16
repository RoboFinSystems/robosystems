# Build a Ledger Integration

A ledger integration posts business events from an external system into a RoboLedger graph, and this tutorial builds one end to end. It registers the integration as a source, finds the accounts to post to, checks an event before sending it, captures it, approves it into the ledger, and reads the result back. It is lane 1 of the three integration lanes — see [Building Custom Integrations](Building-Custom-Integrations.md#the-three-lanes).

The running example is **Driftline**, a fictional invoicing app used by a fictional design studio, **Harbor & Pine Studio**. Each invoice Driftline issues becomes a sales event in the studio's books: debit Accounts Receivable, credit Revenue.

Every step is shown twice: as `curl`, and as Python using the [`robosystems-integration-template`](https://github.com/RoboFinSystems/robosystems-integration-template), whose emitters wrap the [`robosystems-client`](https://pypi.org/project/robosystems-client/) SDK.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

## Table of Contents

- [How It Fits Together](#how-it-fits-together)
- [Prerequisites](#prerequisites)
- [Step 1: Register the Source](#step-1-register-the-source)
- [Step 2: Find the Accounts](#step-2-find-the-accounts)
- [Step 3: Shape the Event](#step-3-shape-the-event)
- [Step 4: Preview It](#step-4-preview-it)
- [Step 5: Capture the Event](#step-5-capture-the-event)
- [Step 6: Classify and Commit](#step-6-classify-and-commit)
- [Step 7: The Closed-Period Gate](#step-7-the-closed-period-gate)
- [Step 8: Materialize and Verify](#step-8-materialize-and-verify)
- [Putting It in the Template](#putting-it-in-the-template)
- [Lane 2: Metric Series Instead of Ledger Events](#lane-2-metric-series-instead-of-ledger-events)
- [Common Pitfalls](#common-pitfalls)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## How It Fits Together

An event moves through a short lifecycle, and each move is one API call:

```text
create-event-block             update-event-block             close-period
(capture)          ──►  captured  ──► (transition_to=committed) ──► committed + draft entry ──► posted
                                  └─► (transition_to=classified) ─► classified ─► committed …
                                  └─► (transition_to=voided) ─────► voided
```

- **Capture** records the event as it happened in Driftline. Nothing touches the general ledger yet; the event sits in the graph's inbox with status `captured`.
- **Commit** runs the event's handler, which turns the event's metadata into a balanced journal entry — as a **draft**.
- **Close** posts every draft in the period. Until then, a committed event can still be voided.

This is the capture-then-approve discipline: your integration delivers facts, and a person (or an operator acting for one) decides when they become books. The platform enforces double-entry balance, the closed-period gate, and one event per source record.

## Prerequisites

- **A RoboLedger graph** with a chart of accounts — one created in the app with RoboLedger enabled. Its id looks like `kg1a2b3c4d5e6f7890`.
- **An API key** from **Settings → API keys** for a user who can write to that graph (member or admin; viewers are read-only). See [Authentication and API Keys](Authentication-and-API-Keys.md).
- `curl` and `jq`, and for the Python path, a repository made from the integration template (`just venv`).

```bash
export ROBOSYSTEMS_API_KEY=rfs...           # Settings → API keys at robosystems.ai
export GRAPH_ID=kg1a2b3c4d5e6f7890          # your RoboLedger graph
```

For the template, the same values go in `.env` (or your runtime's secret store), plus the source name you will register in Step 1:

```bash
ROBOSYSTEMS_API_URL=https://api.robosystems.ai
ROBOSYSTEMS_API_KEY=rfs...
ROBOSYSTEMS_GRAPH_ID=kg1a2b3c4d5e6f7890
INTEGRATION_SOURCE_NAME=driftline
```

The Python snippets below assume:

```python
from integration.client import IntegrationClient
from integration.config import load_config

client = IntegrationClient(load_config())
```

## Step 1: Register the Source

Every event names its `source`. Besides the platform's own sources, a graph accepts only source names that are registered on it, so the first call registers `driftline` as an **external connection**. The connection holds no credentials — Driftline's credentials stay with your integration. It is a claim on the name, so every event the integration writes traces back to it and no two integrations can collide.

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
        "provider": "external",
        "external_config": {
          "source_name": "driftline",
          "display_name": "Driftline Invoicing"
        }
      }'
```

The response (HTTP 201) is the connection record, including `connection_id`, `provider`, `status`, and `source_name`.

- `source_name` is 2–64 characters of lowercase letters, digits, `-` and `_`, starting with a letter. Platform source and provider names (`manual`, `quickbooks`, and the like) are reserved.
- Registering is safe to repeat: posting the same `source_name` again returns the existing connection instead of creating a second one.

```python
from robosystems_client.api.connections import create_connection
from robosystems_client.models import CreateConnectionRequest

connection = client.unwrap(
  create_connection.sync_detailed(
    client.config.graph_id,
    client=client.sdk,
    body=CreateConnectionRequest.from_dict(
      {
        "provider": "external",
        "external_config": {
          "source_name": client.config.source_name,
          "display_name": "Driftline Invoicing",
        },
      }
    ),
  )
)
```

`create_connection` is in the SDK's generated tier rather than the template's stable set — fine for a one-time setup call. See [Versioning and Compatibility](Versioning-and-Compatibility.md#the-two-tier-sdk-contract).

## Step 2: Find the Accounts

Journal lines name accounts by **element id**, the id of the account in the graph's chart of accounts. Look them up once with GraphQL and keep them in your integration's configuration — they don't change when account names do.

```bash
curl -s -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ accounts(isActive: true, limit: 500) { accounts { id code name trait balanceType } pagination { total hasMore } } }"}' \
  | jq '.data.accounts.accounts[] | select(.name | test("Receivable|Revenue"; "i"))'
```

```json
{ "id": "elem_01K5N7A3B5C7D9E1F3G5H7J9K1", "code": "1200", "name": "Accounts Receivable", "trait": "asset", "balanceType": "debit" }
{ "id": "elem_01K5N7A3B5C7D9E1F3G5H7J9M3", "code": "4000", "name": "Design Services Revenue", "trait": "revenue", "balanceType": "credit" }
```

The graph is in the URL, so the query takes no graph argument; `classification` filters by trait (`asset`, `revenue`, …) if you want a narrower list. `accountTree` returns the same chart as a hierarchy. See [GraphQL Reads](GraphQL-Reads.md).

```python
data = client.graphql(
  "{ accounts(isActive: true, limit: 500) { accounts { id code name trait } } }"
)
by_code = {a["code"]: a["id"] for a in data["accounts"]["accounts"]}
AR, REVENUE = by_code["1200"], by_code["4000"]
```

## Step 3: Shape the Event

A Driftline invoice becomes this event:

```json
{
  "event_type": "invoice_issued",
  "event_category": "sales",
  "occurred_at": "2026-09-15T00:00:00Z",
  "source": "driftline",
  "external_id": "INV-10042",
  "external_url": "https://app.driftline.example/invoices/INV-10042",
  "amount": 480000,
  "currency": "USD",
  "description": "Brand refresh, phase 2 — Harbor & Pine Studio",
  "metadata": {
    "posting_date": "2026-09-15",
    "memo": "Driftline invoice INV-10042",
    "line_items": [
      { "element_id": "elem_01K5N7A3B5C7D9E1F3G5H7J9K1", "debit_amount": 480000 },
      { "element_id": "elem_01K5N7A3B5C7D9E1F3G5H7J9M3", "credit_amount": 480000 }
    ]
  }
}
```

The fields that matter:

| Field | Notes |
| --- | --- |
| `event_type` | Chooses the handler that will turn the event into GL lines. `invoice_issued` uses the journal-entry handler, whose metadata is shown here |
| `event_category` | The REA category — `sales`, `purchase`, `payroll`, `treasury`, `financing`, `adjustment`, `recognition`, or `other` for economic events |
| `occurred_at` | When it happened in Driftline (ISO 8601) |
| `source` | Your registered source name |
| `external_id` | Driftline's own id for the record. `(source, external_id)` is unique on the graph — this is what makes re-sending safe |
| `amount` | Integer **cents**, signed from the studio's point of view: inflows positive, outflows negative |
| `metadata` | The handler's input. For the journal-entry handler: `posting_date`, `memo`, and at least two `line_items`, each naming one `element_id` and exactly one of `debit_amount` / `credit_amount` in cents. Debits must equal credits |

Optional fields include `agent_id` (the counterparty, once you have created one), `effective_at` (an accounting date different from `occurred_at`), and `dimension_ids`. The full model is `CreateEventBlockRequest` in the [API reference](https://robosystems.ai/docs/extensions).

## Step 4: Preview It

`preview-event-block` takes exactly the body you are about to send, resolves its handler, and plans the GL lines — without writing anything. Use it while developing to learn a handler's expected metadata, and in production to catch a bad record before it reaches the inbox.

```bash
curl -s -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/preview-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d @driftline-inv-10042.json | jq .result
```

```json
{
  "matched_handler": null,
  "planned_transactions": [
    {
      "entry_index": 0,
      "debit_element_id": "elem_01K5N7A3B5C7D9E1F3G5H7J9K1",
      "credit_element_id": "elem_01K5N7A3B5C7D9E1F3G5H7J9M3",
      "amount_cents": 480000,
      "interpolated_debit_amount": "480000",
      "interpolated_credit_amount": "480000"
    }
  ],
  "validation_errors": [],
  "would_succeed": true,
  "handler_metadata": {
    "handler": "Journal Entry Recorded",
    "total_debit_cents": 480000,
    "total_credit_cents": 480000,
    "target_status": "classified",
    "entry_count": 1
  }
}
```

`would_succeed: false` comes with the reasons in `validation_errors` — a metadata field missing or of the wrong type, lines that don't balance, or a posting date in a closed period. `matched_handler` is `null` when a built-in handler matched; its name is in `handler_metadata.handler`.

```python
from robosystems_client.api.robo_ledger_ledger_events import preview_event_block
from robosystems_client.models import CreateEventBlockRequest

preview = client.unwrap(
  preview_event_block.sync_detailed(
    client.config.graph_id,
    client=client.sdk,
    body=CreateEventBlockRequest.from_dict(event),
  )
)["result"]
if not preview["would_succeed"]:
  raise ValueError(preview["validation_errors"])
```

## Step 5: Capture the Event

`create-event-block` records the event. Leave `apply_handlers` at its default, `false`, so the event lands in the inbox as `captured` for review — the recommended mode for a new integration.

```bash
curl -s -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: driftline-INV-10042-v1" \
  -H "Content-Type: application/json" \
  -d @driftline-inv-10042.json
```

The response is an [`OperationEnvelope`](Operations-Contract.md#the-operationenvelope) whose `result` is the event, with its `id` (`evt_…`) and `"status": "captured"`. Keep the `id` — the next step needs it.

**Re-sending is safe in two independent ways:**

- **`(source, external_id)`** — the graph refuses a second event with the same pair. A repeat delivery answers **409** with `"detail": "Event already ingested for this source and external_id"`; treat that as "already done". This protects you across runs, days apart, with or without any header.
- **`Idempotency-Key`** — a retry with the same key and body within 24 hours replays the original envelope (`idempotentReplay: true`) instead of reaching the duplicate check. Useful for retrying after a timeout, when you did not see the first response. See [Operations Contract](Operations-Contract.md#idempotency-key).

Capture does not validate the handler metadata — that happens at commit — which is why Step 4 is worth running first.

```python
from integration.emit.events import emit_event

created = emit_event(client, event, idempotency_key="driftline-INV-10042-v1")
event_id = created["result"]["id"]
```

`emit_event` stamps `source` from `INTEGRATION_SOURCE_NAME` if the payload has none and refuses a payload without `external_id`. It raises `IntegrationAPIError` on any HTTP error, including the 409 for an already-ingested record — catch it if your loop re-sends history.

**Skipping the inbox.** Sending `"apply_handlers": true` runs the handler at capture: the event lands `classified` with its draft entry already written, and that draft posts at the next close unless the event is voided first. Use it only once you trust the integration's output.

## Step 6: Classify and Commit

`update-event-block` moves an event through its lifecycle with `transition_to`. Committing the captured invoice runs its handler and writes the draft journal entry:

```bash
curl -s -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/update-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "evt_01K5Q3Z8X2M4P6R8T0V2W4Y6Z8", "transition_to": "committed"}'
```

The envelope's `result` is the event, now `"status": "committed"`. The handler's work and the status change are one transaction: if the handler refuses — metadata that fails validation, unbalanced lines, a closed period — the response is a **422** with the reason, and the event stays `captured`.

The allowed moves:

| From | To |
| --- | --- |
| `captured` | `classified`, `committed`, `voided`, `superseded` |
| `classified` | `committed`, `pending`, `fulfilled`, `voided`, `superseded` |
| `committed` | `pending`, `fulfilled`, `voided`, `superseded` |
| `pending` | `fulfilled`, `voided`, `superseded` |
| `fulfilled` | `voided`, `superseded` |

Any other move is a **422** that lists the allowed transitions. Notes on the ones an integration uses:

- **`classified`** records a decision without posting anything. It is the reviewer's "this is right" step, useful when approval and posting are separate; the handler runs on the later `committed`. For an event captured without its accounts (a bank line, for example), the account choice is patched into `metadata_patch` in the same call, and the handler refuses a classification it could not post.
- **`committed`** writes the draft entry. The draft becomes `posted` when the period is closed ([`close-period`](RoboLedger-Operations.md)).
- **`voided`** and **`superseded`** retract an event and are final. They are refused once the event's entries have posted — after the close, correct the books with a reversing entry instead. `superseded` requires `superseded_by_id`, the replacement event.
- A **409** from this call means another process holds the event's row (usually a sync); retry after a moment.

The same call can correct fields while it transitions: `description`, `effective_at`, and `metadata_patch` (merged into the existing metadata).

```python
from robosystems_client.api.robo_ledger_ledger_events import update_event_block
from robosystems_client.models import UpdateEventBlockRequest

committed = client.unwrap(
  update_event_block.sync_detailed(
    client.config.graph_id,
    client=client.sdk,
    body=UpdateEventBlockRequest.from_dict(
      {"event_id": event_id, "transition_to": "committed"}
    ),
  )
)
```

In most deployments the commit is a person's decision in the RoboLedger inbox rather than the integration's; the integration's job ends at capture. Commit from code when the business has agreed that this source posts without review.

## Step 7: The Closed-Period Gate

Once a fiscal period is closed, nothing writes into it. The gate applies to anything that would write ledger lines dated inside a closed period: committing an event, creating one with `apply_handlers: true`, and voiding one whose entries sit there. The refusal is a **422**:

```json
{
  "detail": "Cannot write to closed period '2026-08' (posting_date=2026-08-28). Reopen the period first if an adjustment is needed.",
  "request_id": "…"
}
```

Capturing an event dated in a closed period is still accepted — it is a record of what happened — but it cannot be committed there. Two ways forward, and it is the books' owner's call which:

- **Post it into the open period** as a catch-up entry. The commit is checked against both the event's accounting date (`effective_at`, or `occurred_at` when that is unset) and the entry's `posting_date`, so move both, in a call without a transition, then commit in a second call:

  ```json
  {"event_id": "evt_…", "effective_at": "2026-09-01T00:00:00Z", "metadata_patch": {"posting_date": "2026-09-01"}}
  ```
- **Reopen the period**, commit, and close again (`reopen-period`, then `close-period`; see [RoboLedger Operations](RoboLedger-Operations.md)).

`preview-event-block` reports the gate in `validation_errors` before anything is sent, so a nightly job can route late records to a person instead of failing.

## Step 8: Materialize and Verify

**GraphQL reads the ledger directly**, so the result of Steps 5–6 is visible immediately. List Driftline's events and the drafts they produced:

```bash
curl -s -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ eventBlocks(source: \"driftline\", limit: 20) { id externalId status amount } journalEntries(status: \"draft\", limit: 20) { entries { id postingDate memo triggeredByEventId totalDebit totalCredit balanced } } }"}' \
  | jq .data
```

`eventBlocks` shows each invoice with its status; `journalEntries` shows the balanced drafts, each pointing back at its event through `triggeredByEventId`. After the period closes, the same entries appear with `status: "posted"` and in `trialBalance`.

**The analytical graph is rebuilt by `materialize`.** Cypher queries, fact grids, financial statements, and the MCP analysis tools read the materialized knowledge graph rather than the ledger, and pick up new activity once it is rebuilt:

```bash
curl -s -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/operations/materialize" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: driftline-materialize-2026-09-15" \
  -H "Content-Type: application/json" \
  -d '{}'
```

For a RoboLedger graph the source defaults to the ledger. The call returns a `pending` envelope; follow its `operationId` over the progress stream until `operation_completed` ([Operations Contract](Operations-Contract.md#following-an-async-operation)). A **409** means a materialization is already running for this graph — wait for it rather than starting another.

```python
from integration.emit.graph import materialize

pending = materialize(client)  # follow pending["operationId"]
```

Then query the rebuilt graph — see [Querying the Analytical Graph](Querying-the-Analytical-Graph.md).

Materializing after every event is unnecessary. Run it once after a batch, or leave it to the schedule your deployment already runs.

## Putting It in the Template

In the template, Driftline-specific code goes in two files, and the emitter does the rest:

```python
# src/integration/collect.py — pull invoices issued since the last run
def collect(config):
  return driftline_api.list_invoices(issued_since=last_run_date())

# src/integration/transform.py — one event per invoice
AR, REVENUE = "elem_01K5N7A3B5C7D9E1F3G5H7J9K1", "elem_01K5N7A3B5C7D9E1F3G5H7J9M3"

def transform(invoices):
  return [
    {
      "event_type": "invoice_issued",
      "event_category": "sales",
      "occurred_at": f"{inv['issued_on']}T00:00:00Z",
      "external_id": inv["number"],
      "amount": inv["total_cents"],
      "description": f"{inv['title']} — {inv['customer_name']}",
      "metadata": {
        "posting_date": inv["issued_on"],
        "memo": f"Driftline invoice {inv['number']}",
        "line_items": [
          {"element_id": AR, "debit_amount": inv["total_cents"]},
          {"element_id": REVENUE, "credit_amount": inv["total_cents"]},
        ],
      },
    }
    for inv in invoices
  ]

# src/integration/main.py
emit_events(client, records)
```

`emit_events` sends one call per event, so each record is validated and deduplicated on its own. The template's scheduled GitHub Actions workflow runs it with the API key as a repository secret. Keep Driftline's raw responses in your own storage; a backfill is then one loop over history, and the `(source, external_id)` check makes re-running it harmless.

## Lane 2: Metric Series Instead of Ledger Events

Not every number is bookkeeping. Driftline's operational figures — invoices sent, active clients, average days to pay — are facts about the business but not journal entries. They go through lane 2: author a small vocabulary once, then assert each period's values.

```bash
# Once: the vocabulary — concepts plus a metric structure that presents them
curl -s -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-taxonomy-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" -H "Content-Type: application/json" \
  -d @driftline-metrics-taxonomy.json          # taxonomy_type "reporting_extension", one structure with block_type "metric"

# Each period: the observed values, keyed by concept qname
curl -s -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/assert-metrics" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" -H "Content-Type: application/json" \
  -d '{
        "structure_id": "…",
        "period_start": "2026-09-01",
        "period_end": "2026-09-30",
        "source_system": "driftline",
        "observations": [
          { "qname": "drf:InvoicesSent", "value": 37 },
          { "qname": "drf:ActiveClients", "value": 12 }
        ]
      }'
```

`structure_id` comes from the `structures` in `create-taxonomy-block`'s result. Re-asserting a period replaces that period's values, so the call is safe to re-run, and the series then appears in fact grids, GraphQL, and MCP with no further work. The template's [`emit/metrics.py`](https://github.com/RoboFinSystems/robosystems-integration-template/blob/main/src/integration/emit/metrics.py) builds the full `create-taxonomy-block` payload from a list of concepts (`author_metric_structure`) and wraps `assert-metrics` (`assert_metrics`); [`robosystems-marketing-integration`](https://github.com/RoboFinSystems/robosystems-marketing-integration) is a working example. See [Information Blocks](Information-Blocks.md) and [Taxonomy and Frameworks](Taxonomy-and-Frameworks.md) for the model behind it.

## Common Pitfalls

### "Unknown event source"

The event's `source` is not registered on this graph. Register it as an external connection (Step 1), and check the spelling matches `INTEGRATION_SOURCE_NAME`. The 422 lists the sources the graph accepts.

### Committed, But Nothing in the Trial Balance

`trialBalance` reports **posted** entries. A committed event's entry is a draft until the period closes — look for it with `journalEntries(status: "draft")`.

### Committed, But No Entry at All

An `event_type` with no handler records the event and nothing else. Check with `preview-event-block`: a matched handler shows its name in `handler_metadata.handler` and planned lines in `planned_transactions`.

### Amounts in Dollars

`amount`, `debit_amount`, and `credit_amount` are integer cents. `4800` is $48.00.

### Voiding After the Close

Refused: posted books are corrected, not rewritten. Record a reversing entry in the open period instead.

## Self-hosted deployments

Everything above works against a local stack: set `ROBOSYSTEMS_API_URL=http://localhost:8000` and use the key from `just demo-user`. `just demo-roboledger` provisions a RoboLedger graph with a chart of accounts to practise against. See [Local Development](Local-Development.md).

## Related Documentation

**Guides:**

- **[Building Custom Integrations](Building-Custom-Integrations.md)** - The three lanes and the integration template
- **[Event-Driven Ledger](Event-Driven-Ledger.md)** - Events, handlers, and the three-level ledger behind this tutorial
- **[RoboLedger Operations](RoboLedger-Operations.md)** - Every ledger operation, including `close-period` and `reopen-period`
- **[Operations Contract](Operations-Contract.md)** - The envelope, `Idempotency-Key`, and progress streaming
- **[Errors and Rate Limits](Errors-and-Rate-Limits.md)** - What each status code means and how to retry
- **[GraphQL Reads](GraphQL-Reads.md)** - The read surface used in Steps 2 and 8

**Repositories:**

- **[robosystems-integration-template](https://github.com/RoboFinSystems/robosystems-integration-template)** - The scaffold used here
- **[robosystems-client](https://pypi.org/project/robosystems-client/)** - The Python SDK

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
