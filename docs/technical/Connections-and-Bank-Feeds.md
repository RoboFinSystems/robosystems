# Connections & Bank Feeds

How a graph connects to an outside source of books over the API, end to end for a bank feed: discovering the providers a deployment offers, creating and authorizing a connection, routing a feed's accounts to the right company's chart, working the lines it captures through the Inbox, syncing, and disconnecting.

The product docs at roboledger.ai describe this for end users (*Connect a bank feed* and *Review events in the Inbox*). This page covers the calls underneath. QuickBooks shares the connection endpoints and is covered on its own page, [QuickBooks Sync & Write Policy](QuickBooks-Sync-and-Write-Policy.md). An integration that sends a ledger of its own through the API is [Build a Ledger Integration](Build-a-Ledger-Integration.md).

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. Turning the feeds on there is [Connecting Bank Feeds Locally](Connecting-Bank-Feeds-Locally.md).

## Table of Contents

- [Overview](#overview)
- [The Providers a Deployment Offers](#the-providers-a-deployment-offers)
- [Creating a Connection](#creating-a-connection)
- [Authorizing It](#authorizing-it)
- [Routing a Feed's Accounts](#routing-a-feeds-accounts)
- [Working the Inbox](#working-the-inbox)
- [Syncing](#syncing)
- [Disconnecting](#disconnecting)
- [Gotchas and Pitfalls](#gotchas-and-pitfalls)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

A connection is a platform row scoped to one graph. It holds the provider's credentials (encrypted, never returned by any read), a status, the timestamp and outcome of its last sync, and the entity whose books it serves. The code supports four providers:

| Provider | `auth_type` | What it is | Per graph |
| --- | --- | --- | --- |
| `quickbooks` | `oauth` | The synced ledger that keeps the group parent's books | One |
| `plaid` | `link` | A bank feed through Plaid Link: one connection is one bank login | One per bank login |
| `mercury` | `oauth`, or `api_key` | A bank feed from one Mercury organization | One |
| `external` | `none` | A source namespace for an integration that writes through the API | One per `source_name` |

A **bank feed** (`plaid`, `mercury`) captures every posted transaction on the accounts it reads as an event in the ledger's Inbox, with a suggested account. Pending transactions are left out until they post. Nothing reaches the books on capture: a line posts only once it is classified and committed. A feed is native accounting, so it books to a company whose books RoboLedger keeps, never one that QuickBooks or an external ledger keeps.

Every call on this page takes the `X-API-Key` header. Creating, authorizing and syncing a connection need a member or admin role on the graph; deleting one needs admin; the reads need access to the graph.

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...
```

## The Providers a Deployment Offers

Each provider is switched on per deployment, so ask the deployment rather than assume:

```bash
curl "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections/options" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

The response lists only the providers that are enabled, each with:

| Field | Meaning |
| --- | --- |
| `provider` | The value `POST /connections` takes |
| `display_name`, `description`, `auth_flow`, `setup_instructions` | Copy for a connect screen |
| `auth_type` | `oauth` (redirect to the provider), `link` (Plaid Link, an embedded widget), `api_key` (a pasted token), `none` |
| `required_config`, `optional_config` | Fields the provider's config takes. For Mercury, `api_key` in `required_config` means a pasted token is the only route on this deployment; in `optional_config`, a token is allowed beside OAuth |
| `features`, `data_types`, `sync_frequency` | What the provider brings and how often it syncs |

Mercury is listed only when at least one of its two credential routes works on the deployment. Its `api_key` route takes a personal read-only Mercury token and is for self-hosted deployments only; the hosted service never accepts one.

## Creating a Connection

`POST /v1/graphs/{graph_id}/connections` names the provider and its config. A bank feed's config takes:

| Field | Plaid | Mercury | Meaning |
| --- | --- | --- | --- |
| `since_date` | ✓ | ✓ | First day of the backfill (ISO 8601); Plaid pulls at most two years. Omitted, it is two years back. It is fixed at connect, and incremental syncs never look back before it. Lines dated in a closed month are never captured into it, whatever the window |
| `entity_id` | ✓ | ✓ | The entity the feed's accounts book to: a subsidiary's id, or omit for the group parent. Required when QuickBooks keeps the parent's books. Each account can move to another entity later with `link-bank-account` |
| `api_key` | | ✓ | A personal read-only Mercury token, on a deployment that allows the `api_key` route. Omit to connect over OAuth |

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "provider": "plaid",
    "plaid_config": {"since_date": "2025-01-01", "entity_id": "ent_01..."}
  }'
```

The response (`201`) is the connection, in status `pending_oauth`:

```json
{
  "connection_id": "0b6e…",
  "provider": "plaid",
  "entity_id": "ent_01...",
  "keeps_books": false,
  "status": "pending_oauth",
  "created_at": "2026-11-20T15:02:11",
  "last_sync": null,
  "write_policy": "native",
  "metadata": {
    "item_id": null,
    "institution_name": null,
    "auto_sync_enabled": true,
    "last_sync": null,
    "last_sync_result": null
  }
}
```

(Trimmed; `metadata` carries a few more keys.) Once Link completes, `metadata.institution_name` names the bank.

```bash
export CONN_ID=0b6e…
```

`keeps_books` is always `false` for a feed: a feed is not the ledger of record, it feeds one, and its `write_policy` is `native` because nothing is ever written back to a bank. Only the matching config may be sent: `plaid_config` with `plaid`, `mercury_config` with `mercury`, and both are optional.

**Repeats.** A second `plaid` create while one Plaid connection is still `pending_oauth` returns that row instead of opening another, with its `since_date` and `entity_id` replaced by any the new request names. A second `mercury` create returns the graph's existing Mercury connection.

**Mercury with a token.** With `api_key` set, the token is checked against Mercury before anything is stored. A good one makes the connection `connected` at once, records the consent and starts the first sync: there is nothing to authorize. A token Mercury rejects is `400`. On a deployment that does not allow the route the request is refused (`403`, "The requested provider is not available.").

### Refusals

A connection that would mix native and synced books of one entity is refused before it is created. The body is the typed error shape described in [Errors & Rate Limits](Errors-and-Rate-Limits.md#typed-error-codes): `detail.code` is the stable value to branch on.

| `code` | Status | Provider | Meaning |
| --- | --- | --- | --- |
| `QUICKBOOKS_ACTIVE` | 409 | feed | A connected ledger keeps that entity's books: QuickBooks (the group parent's) or an external ledger source. Connect the feed for another entity, or sever the synced connection first |
| `CHART_REQUIRED` | 409 | feed | The entity has no chart of accounts. Start one from a template (`initialize-chart-of-accounts`) |
| `CONSOLIDATION_ENTITY` | 409 | feed, external ledger | The consolidation entity's books hold the group's consolidating entries and take no feed |
| `ENTITY_NOT_FOUND` | 404 | feed, QuickBooks, external ledger | `entity_id` is not an entity of this graph |
| `PARENT_ONLY` | 409 | QuickBooks | QuickBooks keeps the group parent's books only |
| `NATIVE_BOOKS_PRESENT` | 409 | QuickBooks, external ledger | The entity already keeps books of its own, a feed account bound to it among them |
| `BOOKS_KEPT_ELSEWHERE` | 409 | external ledger | Another ledger already keeps that entity's books |
| `OTHER_COMPANY_BOOKS` | 409 | QuickBooks | The graph's books were built from a different QuickBooks company |

A provider the deployment has not enabled is `403`.

## Authorizing It

Authorization is two calls whichever the provider: `oauth/init` says where the user authorizes, and `oauth/callback/{provider}` hands back what the provider returned. Plaid reuses the same pair for Link.

### `POST /connections/oauth/init`

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections/oauth/init" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "connection_id": "'$CONN_ID'",
    "redirect_uri": "https://app.example.com/connections/plaid-callback"
  }'
```

| Field | Meaning |
| --- | --- |
| `connection_id` | The connection to authorize |
| `redirect_uri` | Where the provider sends the user back. It must be one the provider has registered for the deployment's client. Omitted, an OAuth provider uses the deployment's default, and Plaid's Link token carries none, which leaves out a bank that signs in through its own OAuth |

The response carries one of two ways in:

| Field | `quickbooks`, `mercury` | `plaid` |
| --- | --- | --- |
| `auth_url` | The provider's consent page; send the user there | `null` |
| `link_token` | `null` | The token that opens Plaid Link |
| `state` | Redeemed by the callback; tied to the calling user | Same |
| `expires_at` | Ten minutes out | Thirty minutes out: a bank's own sign-in steps happen inside Link |

For Plaid, init is answered with the same `link_token` while that token lives, when it is asked again by the same user with the same `redirect_uri`. That is what a bank which signs in through its own OAuth needs: it sends the user to `redirect_uri`, and the page there opens Link again with the token it started with. A connection that already holds a Plaid Item opens Link in update mode on that Item, which is how a broken bank login is repaired.

Init runs the same books checks as create, for the entity the feed was connected for, so a `QUICKBOOKS_ACTIVE` or `CHART_REQUIRED` that arose since is refused here too. A provider with no authorization step (`external`) is `400`.

### `POST /connections/oauth/callback/{provider}`

```bash
# Plaid: Link's public_token goes in as `code`
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections/oauth/callback/plaid" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"code": "public-…", "state": "…"}'
```

| Field | Meaning |
| --- | --- |
| `code` | The authorization code from the redirect; for Plaid, the `public_token` Link returned |
| `state` | The `state` from init |
| `realm_id` | QuickBooks only |
| `error`, `error_description` | What the provider returned when the user declined; the call is then `400` |

On success the credential is stored encrypted, the connection turns `connected`, and the first sync is started. For a bank feed, the consent (who connected which bank, over which credential and scope, when) is also written to the security audit log. The response names the sync it started:

```json
{
  "success": true,
  "message": "… connected through Plaid",
  "connection_id": "0b6e…",
  "auto_sync_task_id": "…"
}
```

A connection's first sync takes its full backfill from `since_date`. Re-authorizing a connection that has synced before keeps its incremental position, except when Plaid Link replaced the Item, which starts the history again (see [Syncing](#syncing)).

| Status | `code` | When |
| --- | --- | --- |
| 400 | `INVALID_INPUT` | `state` is invalid or expired, or the connection is another provider's |
| 403 | `FORBIDDEN` | `state` was issued to another user |
| 409 | `DUPLICATE_BANK_CONNECTION` | Plaid: the bank login is already connected to this graph. A still-pending connection is removed, and the Item Link just created is removed at Plaid. Reconnect the existing connection instead |
| 409 | `INVALID_INPUT` | Mercury: the consent is for a different Mercury organization from the one this connection feeds. The new grant is revoked |
| 409 | `OTHER_COMPANY_BOOKS` | QuickBooks; see [QuickBooks Sync & Write Policy](QuickBooks-Sync-and-Write-Policy.md) |
| 400 | `PROVIDER_ERROR` | The provider refused the exchange. For Plaid, start Link again |

A Plaid login is a duplicate when another Plaid connection on the graph is at the same institution and shares an account with it, matched on the account mask and subtype, or on name and subtype where the bank sends no mask.

### Statuses

| `status` | Meaning | Next step |
| --- | --- | --- |
| `pending_oauth` | Created, never authorized. Nothing syncs | Run init again and finish the provider's step, or delete it |
| `connected` | Authorized; syncs on request and once a day. A failed sync leaves it `connected`, with the failure in `metadata.last_sync_result` | — |
| `needs_reauth` | The provider ended the access: a changed bank password, an expired consent, a locked login. The daily sync skips it | Run init again (Plaid opens Link in update mode) and complete the callback. The connection keeps its id and history |

## Routing a Feed's Accounts

The first sync reads the accounts the feed exposes and links each one to a chart account on the entity the feed was connected for: an active account with the same name, or a new one it adds to that chart. An account another connection's feed already books to is never claimed by name. The chart the linked account sits in decides whose books each line goes into.

### Reading them: `bankAccounts`

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ bankAccounts { total accounts { id code name kind entityId entityName source connectionId institution feedAccountId feedAccountName connectionStatus lastSyncAt lastSyncStatus } } }"}'
```

`bankAccounts(entityId:)` lists every chart account a feed books to, or that QuickBooks types as a bank or card account, across the group or for one entity. `source` is the provider that writes to it (`plaid`, `mercury`, `quickbooks`), or null for an account kept by hand. `feedAccountId` is the provider's own id for the account, the value `link-bank-account` takes. On MCP this is `list-bank-accounts`.

### Moving one: `link-bank-account`

Point a feed's account at another chart account: an existing one with `element_id`, or a new one created in an entity's chart with `entity_id` alone. Because the chart decides the books, this is also how a feed's accounts are split across the companies of a group.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/link-bank-account" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"connection_id": "'$CONN_ID'", "account_id": "<feedAccountId>", "entity_id": "ent_01..."}'
```

| Field | Meaning |
| --- | --- |
| `connection_id`, `account_id` | The feed and its account (`feedAccountId`) |
| `element_id` | An existing active chart account to link; its chart's entity takes the feed |
| `entity_id` | With `element_id`, the entity the account must belong to. Alone, the entity in whose chart a new account is created |

One of `element_id` and `entity_id` is required. The envelope's `result`:

| Field | Meaning |
| --- | --- |
| `element_id`, `previous_element_id`, `entity_id` | Where the feed now books, and where it booked before |
| `account_created` | A new account was created in the entity's chart |
| `events_repointed` | Inbox lines moved to the new account. Posted entries stay where they were posted |
| `events_unclassified` | Lines returned to `captured`, because their classification named an account in the previous entity's chart. Across an entity change, the lines' suggestions are resolved again on the new chart |
| `pairs_across_entities` | Open transfers whose two legs now sit on two entities, because only one leg's account moved. The commit refuses them until the other leg follows |
| `changed` | `false` when the link already stood and nothing moved |

| Status | When |
| --- | --- |
| 404 | The connection has no linked account by that id (sync the feed first), the chart account or the entity is not on this graph |
| 409 | Another connection's feed already books to the target account |
| 422 | The entity has no chart; the target is not an active chart account, or not of the entity named; a connected ledger keeps the entity's books; the entity is the consolidation entity |

A sync already in flight when the link moves can still land a line or two on the old account; running the same call again moves them. An account the feed created and then left stays on its chart as an ordinary account. On MCP this is the `link-bank-account` tool.

## Working the Inbox

A feed's lines are event blocks with `source` `plaid` or `mercury`, captured with event type `bank_transaction`, `bank_fee`, `external_transfer` or `internal_transfer`. The metadata keys that classify one, and what each refusal means, are in [Event Block Reference](Event-Block-Reference.md#bank-feed-handlers); the capture, classify and commit steps are in [Event-Driven Ledger](Event-Driven-Ledger.md#capture-classify-commit). This section is the batch work around them.

### Finding the open lines

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ eventBlocks(source: \"plaid\", status: \"captured\", limit: 50) { id eventType occurredAt amount description agentId resourceElementId metadata } }"}'
```

Each line's `metadata` carries the suggestion the feed attached:

| Key | Meaning |
| --- | --- |
| `suggested_element_id`, `suggested_account_name` | The suggested account. A name with no id matched nothing on the chart, and needs a choice |
| `suggestion_source` | Where it came from, highest first: `open_item` (the line pays an open bill or invoice of the counterparty's for exactly its amount; taking it settles that item), `agent_default` (the counterparty's learned default), `tier0` (the bank's own category, matched to the chart) |
| `suggestion_basis` | Why, in words: the item it settles, or how often the default held |
| `suggestion_caution` | `unusual_amount`: the line is far larger than the counterparty's usual. A batch leaves it for a person |

### Taking suggestions in bulk: `accept-suggestions`

Takes the suggestion on every open line the filters select, oldest first, each through the same `update-event-block` a single line goes through, so the handler's checks, the closed-period gate and the row lock all apply. A line it cannot take comes back `skipped` with a reason; nothing is forced.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/accept-suggestions" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "source": "plaid",
    "suggestion_source": "agent_default",
    "occurred_from": "2026-10-01",
    "occurred_to": "2026-10-31",
    "commit": true,
    "dry_run": true
  }'
```

| Field | Default | Meaning |
| --- | --- | --- |
| `entity_id` | group parent | The entity whose Inbox to work |
| `event_ids` | — | Explicit lines, as from a multi-select. Each is answered, even one that cannot be taken; the other filters still narrow them |
| `source` | — | Only one feed's lines (`plaid`, `mercury`) |
| `agent_id` | — | Only one counterparty's lines |
| `suggestion_source` | — | Only lines whose suggestion came from there |
| `occurred_from`, `occurred_to` | — | A date range, inclusive |
| `commit` | `false` | Post each taken line as well (`captured` → `committed`). Off, lines are classified and wait for someone to commit them |
| `limit` | `200` | At most this many lines per call, 1 to 1000 |
| `dry_run` | `false` | Report what each line would do, writing nothing |

Only `bank_transaction`, `bank_fee` and `external_transfer` lines are considered: an `internal_transfer` knows both legs and is never classified. Without `event_ids`, only the `captured` ones that carry a suggested account are selected. The `result` counts `selected` (before the limit), `classified`, `committed` and `skipped`, sets `truncated` when more matched than the limit allowed, and lists each line with its `outcome` and, when skipped, a `reason`:

| `reason` | Meaning |
| --- | --- |
| `not_open` | Already classified, posted or retracted |
| `no_suggestion` | Nothing to take, or a suggested name that matches no account |
| `source_changed` | The feed changed the line after it was captured |
| `needs_review` | The suggestion carries a caution. Classify the line on its own |
| `suggestion_changed` | A line taken earlier in the batch moved the counterparty's default, so this line's suggestion is no longer the one selected |
| `closed_period` | The line is dated in a closed month |
| `locked` | Another process holds the line; retry |
| `refused` | The handler would not post the choice; `detail` carries its words |

A dry run finds a closed month only when a line actually posts, so a real run can still skip a line the dry run counted. On MCP this is `accept-suggestions`.

### Seeding counterparty defaults: `learn-classification-defaults`

Every commit teaches the counterparty's default account for that entity. Books classified before defaults were learned can seed them once from what is already committed: the account most of a counterparty's committed lines went to becomes its default, the agreeing lines count as confirmations, a default already there is kept, and the counterparty's still-open lines are suggested again from it.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/learn-classification-defaults" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"dry_run": true}'
```

The `result` reports `agents_learned`, `agents_kept`, `lines_read` and `open_lines_resuggested`. A default can also be set, cleared or switched to always ask by hand with `update-agent` (`classification_element_id`, `classification_mode`, `classification_entity_id`).

### Measuring the month: `classificationStats`

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ classificationStats(period: \"2026-10\") { lines accepted overridden unsuggested split unstamped humanChoiceShare openLines bySource { source lines accepted overridden unsuggested split } } }"}'
```

How one month's committed bank lines were classified: taken as suggested (`accepted`), posted to another account (`overridden`), chosen with no suggestion (`unsuggested`), or split. `humanChoiceShare` is everything but `accepted` over `lines`. Lines committed before outcomes were recorded are `unstamped` and outside the share; `openLines` are the month's lines still waiting. `entityId` picks the entity, the group parent by default. On MCP this is `get-classification-stats`.

### The cash flow row: `classified_flow_qname`

A bank line's cash flow row defaults to its account's. A loan drawn, a capital contribution or an equipment purchase belongs in investing or financing instead, so the line also takes `classified_flow_qname`, an rs-gaap flow concept. The rows the entity's cash flow statement offers, and the only ones the write accepts, are `flowCandidates`:

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ flowCandidates { qname name activity } }"}'
```

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/update-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "evt_01J...",
    "transition_to": "classified",
    "metadata_patch": {
      "classified_element_id": "elem_01...",
      "classified_flow_qname": "rs-gaap:ProceedsFromIssuanceOfLongTermDebt",
      "classified_by": "integration",
      "basis": "Term loan drawn"
    }
  }'
```

Patched on a line that has already posted, it re-tags the line in place, in a closed month too; an empty string clears it. A split takes `flow_qname` on each part of `classified_allocations`. A journal-entry line takes `flow_qname` the same way. On MCP the rows are `list-flow-candidates`.

## Syncing

`POST /connections/{connection_id}/sync` starts a sync and returns `202` with a `pending` [OperationEnvelope](Operations-Contract.md). It supports `Idempotency-Key`.

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections/$CONN_ID/sync" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Idempotency-Key: sync-$(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{}'
```

| Body | Plaid | Mercury |
| --- | --- | --- |
| `{}` | Continues from the stored cursor: what Plaid added, modified and removed since the last sync | Looks back 60 days from the last sync that succeeded, never before the connect-time `since_date` |
| `{"since_date": "2026-06-01"}` | Replays the history from that date | Pulls from that date |
| `{"full_rebuild": true}` | Replays the whole history from the connect-time `since_date` | Pulls from the connect-time `since_date` |

A sync already running on the connection is `409`. When the run finishes, `metadata.last_sync` on the connection advances and `metadata.last_sync_result` records the outcome, with `counts` such as `events_captured`, `events_updated`, `accounts_linked`, `accounts_created`, and for Plaid `events_removed` and `reconciling_items`. A failed run records its result without advancing `last_sync`, and the close reads `last_sync` (`sync_stale` in [Period Close](Period-Close.md#blockers)). On MCP this is `sync-connection`, and `get-graph-sync-status` reads the outcome.

The platform also syncs each connection once a day, with the default body; one whose last attempt failed is tried again a day later. A connection that is `pending_oauth` or `needs_reauth` is left alone until someone authorizes it.

**The first sync** of a feed sets up the fiscal calendar of each entity it brought lines for, when that entity has none, with the books opening in the month of its earliest line. Plaid can take several syncs to deliver a long history: a run captures what has landed, the rest arrives on a later sync, and the calendar waits until the history is complete. `last_sync_result.history_complete` says which.

**A line in a closed month** is not captured into it. The sync holds it out (`voided` with `void_reason: "feed_in_closed_month"`), and the next close is blocked by `held_bank_lines` until someone reopens the month or closes over it. See [Period Close](Period-Close.md#blockers).

**A line the bank changes or removes.** A line still `captured` or `classified` is edited in place, or deleted if the bank removed it. A classified line keeps its classification, and a split that no longer sums to the line is refused at commit. A line already committed is never touched: it is flagged as a reconciling item carrying what the bank now says, and is settled with `preview-reconciling-item` and `resolve-reconciling-item` like any other. A changed amount or date can be restated to the bank's figures; a removed line cannot be restated, and reverses through `catch_up`. See [QuickBooks Sync & Write Policy](QuickBooks-Sync-and-Write-Policy.md#reconciling-items) and [Event Block Reference](Event-Block-Reference.md#reconciling-items).

## Disconnecting

```bash
curl -X DELETE "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/connections/$CONN_ID" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

Deleting a feed needs the admin role on the graph. It gives up the credential first, a Plaid Item removed at Plaid or a Mercury OAuth grant revoked at Mercury, and then purges what the feed captured from the tenant's books:

- Lines not yet committed (`captured`, `classified`, and voided ones) are deleted, with any draft entry they left.
- Committed lines keep their accounting content and lose the provider's payload and link back to the bank.
- Counterparties the feed created that nothing else references are deleted.
- Chart accounts stay, and stop being linked to the feed.

The purge and its counts are written to the security audit log, as the consent was at connect. The `DELETE` response itself says only that the connection was removed. A pasted Mercury token is deleted from the platform but stays valid in Mercury until it is deleted there.

The call answers `409` while a sync is running for the connection, or while the feed's lines are being written; send it again once that finishes. `disposition=sever` is QuickBooks's cutover to native books and is `400` for a feed.

There is nothing to revive afterwards: connecting the bank again creates a new connection and starts from a fresh full sync. That sync links the existing chart accounts by name, and recognizes the lines already committed rather than capturing them a second time.

## Gotchas and Pitfalls

### A feed cannot book where a ledger keeps the books

The refusal arrives at create, at init, and at `link-bank-account`. While QuickBooks is connected, give a group parent's bank its own subsidiary, or sever QuickBooks to keep the books natively first.

### A Plaid backfill is fixed at connect

The backfill start is pinned when the connection is created, and Plaid is asked for that much history when the Item is made; it cannot be widened on the Item afterwards. A sync's `since_date` replays within that history, never before it. To reach further back, connect again with an earlier date, after a disconnect, which deletes the lines still waiting in the Inbox. A Mercury sync's `since_date` is not bound this way.

### Moving an account does not move posted entries

`link-bank-account` moves the Inbox lines only. An account moved to another entity after its lines posted leaves those entries in the first entity's books.

## Self-hosted deployments

Both feeds are off by default and are switched on per deployment, with the provider's credentials, encryption key and redirect URI. That setup, and Mercury's `api_key` route, are on [Connecting Bank Feeds Locally](Connecting-Bank-Feeds-Locally.md). Everything on this page applies unchanged once a feed is enabled. Use `http://localhost:8000` and the key from `just demo-user`.

## Related Documentation

- **[Event Block Reference](Event-Block-Reference.md)** - The bank-feed handlers, their metadata keys, and reconciling items
- **[Event-Driven Ledger](Event-Driven-Ledger.md)** - Capture, classify and commit
- **[QuickBooks Sync & Write Policy](QuickBooks-Sync-and-Write-Policy.md)** - The synced-ledger connection, write policy, and sever
- **[Period Close](Period-Close.md)** - The `sync_stale` and `held_bank_lines` blockers
- **[GraphQL Reads](GraphQL-Reads.md)** - `bankAccounts`, `eventBlocks`, `flowCandidates` and the other reads used here
- **[Operations Contract](Operations-Contract.md)** - The envelope, `Idempotency-Key`, and async operations
- **[Connecting Bank Feeds Locally](Connecting-Bank-Feeds-Locally.md)** - Turning the feeds on in a stack of your own

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
