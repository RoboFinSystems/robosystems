# Chart of Accounts Mapping

Mapping ties each account in a tenant's chart of accounts to an `rs-gaap` reporting concept. It is the join every statement depends on. Live statements, the statements stamped at close, saved reports, forecasts and metrics all read the ledger through it. This page covers the mapping model, how mappings are created (AI auto-map, deterministic suggestions, direct writes), how to measure a mapping, and the lock that keeps closed months consistent with it.

The product doc *Map your chart of accounts* at roboledger.ai describes the review workflow for end users. This page covers the model and the operations underneath.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development.md).

## Table of Contents

- [Overview](#overview)
- [The Mapping Model](#the-mapping-model)
- [Coverage vs. Renderability](#coverage-vs-renderability)
- [How Mappings Get Made](#how-mappings-get-made)
- [Working a Mapping by Hand](#working-a-mapping-by-hand)
- [The Closed-Period Mapping Lock](#the-closed-period-mapping-lock)
- [Gotchas and Pitfalls](#gotchas-and-pitfalls)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

A chart of accounts arrives in the graph in one of three ways: a QuickBooks sync, the `initialize-chart-of-accounts` operation (a shipped template for native books), or tenant authoring through a Taxonomy Block. However it arrives, each account is an `Element` row with a classification trait (asset, liability, equity, revenue, expense, gain or loss). The reporting vocabulary is the tenant's copy of `rs-gaap`. A mapping is a set of `mapping` associations from the first to the second, held in a `coa_mapping` Structure. Each mapping sits in its own `mapping` taxonomy, sourced from the chart and targeting one framework, and a chart holds one mapping per framework: a second mapping into the same framework is refused with `422`. A `coa_mapping` structure in a Taxonomy Block names its framework with `target_framework`, which defaults to the book framework, `rs-gaap`. The book mapping is the one statements read, and `list-mapping-structures` lists it first with each mapping's `framework`.

| Surface | Mapping reads | Mapping writes |
| --- | --- | --- |
| REST operations | — | `create-mapping-association`, `delete-mapping-association`, `auto-map-elements` |
| GraphQL (`/extensions/{graph_id}/graphql`) | `mappings`, `mapping`, `mappingCoverage`, `unmappedElements`, `mappingCandidates` | — |
| MCP | `list-mapping-structures`, `get-mapping-summary`, `get-unmapped-elements`, `suggest-mapping` | `create-mapping-association`, `delete-mapping-association` |

```bash
export ROBOSYSTEMS_API_KEY=rfs...
export GRAPH_ID=kg...
```

## The Mapping Model

**One arc per account, to a leaf.** A mapping association points from a chart-of-accounts element to one `rs-gaap` concept. The target must be a **leaf** of the calculation DAG: a concept whose value comes from facts, not from summing children. Statement-level subtotals such as `rs-gaap:Assets`, `rs-gaap:LiabilitiesCurrent` and gross profit are computed by the renderer from the leaves beneath them. An account mapped straight to a subtotal would place a leaf fact on a rollup and be counted twice. The suggester never offers subtotals, the auto-mapper never writes one, and the write path refuses one with `422`. See [Reporting and Rendering](Reporting-and-Rendering.md#the-calc-dag-model) for the calc DAG.

**Many accounts to one concept.** The standard Reporting Style aggregates to filing grade. Every bank account lands in one cash line, and many expense accounts land in selling, general and administrative expense. That is intended. Detail below the statement face belongs in supporting Information Blocks, not in extra statement lines.

**The Reporting Style bounds the targets.** Only concepts that render under the entity's active Reporting Style are offered as targets, so a mapping chosen from the candidates always shows up on a statement. The tenant library is trimmed to the same working set. See [Taxonomy and Frameworks](Taxonomy-and-Frameworks.md#the-json-ld-package-library).

**Mappings survive re-syncs.** A QuickBooks full rebuild upserts account elements in place and keeps their ids, so curated mappings stay attached. Associations copied in from the public library are immutable in the tenant, and deleting one returns `403`. Tenant-written arcs, including the template mappings `initialize-chart-of-accounts` writes, can be deleted and replaced.

## Coverage vs. Renderability

A mapping has two separate measurements, and a mapping can do well on one and badly on the other:

- **Coverage.** How many active, non-abstract chart-of-accounts elements have a mapping arc at all.
- **Renderability.** Whether each arc's target is reachable from a statement Network under the active Reporting Style. An arc whose target isn't reachable is **unreachable**. The mapping is durable, but the renderer never walks to it, so its balance is missing from the statements.

A chart can be 100% covered and still produce a thin income statement or an unbalanced balance sheet, because some accounts are mapped where no statement looks. Read both:

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ mappingCoverage(mappingId: \"'$MAPPING_ID'\") { totalCoaElements mappedCount unmappedCount coveragePercent highConfidence mediumConfidence lowConfidence manualConfidence unreachableCount unreachable { coaCode coaName targetQname } } }"}'
```

`{ mappings { structures { id name } } }` returns the mapping structure id. `manualConfidence` counts arcs written without a confidence score. The MCP `get-mapping-summary` returns the same numbers, with that count as its `manual` bucket.

An unreachable arc isn't always a mistake. A gross/contra pair (equipment at cost and its accumulated depreciation) mapped at a grain the Style presents net is a correct mapping that has no line of its own. `unreachable_count` makes such cases visible so you can decide which ones they are. The [RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough.md#the-mapping-view) shows a tenant at 27 of 27 mapped with 3 unreachable.

`live-financial-statement` reports an `unmapped_count` on every render. That is the quickest check for unmapped accounts that carry balances. An unmapped account with postings also holds the month's close with the `unmapped_accounts` blocker, since its balances are in no statement and no consolidating entry; see [Period Close](Period-Close.md#blockers). A group's own charts have a second requirement, the concepts the consolidation engine posts to; see [Group Consolidation](Group-Consolidation.md#charts-and-mapping).

## How Mappings Get Made

### AI auto-map (uses credits)

The **MappingOperator** is an AI Operator. It works through the unmapped accounts in batches grouped by classification. For each account it takes the renderable candidates for that classification and has the model pick one, with a confidence score. It then applies fixed thresholds:

| Confidence | Outcome |
| --- | --- |
| ≥ 0.90 | Mapped. The association is written. |
| 0.70 – 0.89 | Mapped and **flagged for review**. The association is written with its confidence recorded, and the review surfaces show it as medium confidence. |
| < 0.70 | Skipped. The account stays unmapped. |

Unambiguous account names can be pinned to a fixed concept, overriding the model. Today there is one such override: an account whose name contains "accumulated depreciation" maps to the accumulated-depreciation concept, never into net PP&E. An override is always applied as a confident match.

Auto-map runs in two ways:

- **On the first QuickBooks sync**, when the mapping structure has no associations yet. It never runs again on its own. Accounts added in QuickBooks later arrive unmapped.
- **On request**, with `auto-map-elements`. The operation dispatches to the background worker and returns `202` with a `pending` envelope. Follow it on `GET /v1/operations/{operation_id}/stream`.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/auto-map-elements" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"mapping_id": "'$MAPPING_ID'"}'
```

Auto-map calls a model, so it consumes AI credits. It is the only credit-consuming step in the mapping workflow. A run is bounded by the graph's credit balance, which is checked before each pass, and it stops when a pass maps nothing new. Each pass fetches only the accounts that are still unmapped, so an interrupted run resumes rather than repeats. See [Credits and Billing](Credits-and-Billing.md).

### Deterministic suggestions (no credits)

`suggest-mapping` (MCP) and `mappingCandidates` (GraphQL) return the renderable `rs-gaap` concepts that fit an account. The filter is the account's classification, narrowed by liquidity (current or noncurrent) for assets and liabilities. No model is involved, so suggestions consume no credits. The caller, whether a person or an AI assistant, picks among the candidates, usually by account name. That is how an assistant maps accounts in conversation without spending credits.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ unmappedElements { id code name trait liquidity balanceType } }"}'
```

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ mappingCandidates(classification: \"expense\") { id qname name } }"}'
```

Each candidate appears in at least one presentation Network, and statement subtotals are excluded.

### Templates (native books)

A graph with no chart can start from a shipped template with `initialize-chart-of-accounts`: `saas`, `services` or `product`. The operation creates the chart, the `coa_mapping` structure and the template's mappings in one transaction, and maps the equity accounts by the entity's legal form. It is one-time and returns `409` once a chart exists. A QuickBooks-synced graph never needs it.

## Working a Mapping by Hand

### Write one arc

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-mapping-association" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "mapping_id": "'$MAPPING_ID'",
    "from_element_id": "elem_coa_green_coffee",
    "to_element_id": "elem_rsgaap_inventory",
    "association_type": "mapping",
    "confidence": 1.0,
    "suggested_by": "user"
  }'
```

`from_element_id` is the chart-of-accounts element and `to_element_id` the `rs-gaap` concept's element id (from the candidates). `confidence` and `suggested_by` are recorded for audit. The renderer ignores them. A duplicate `(from, to, type)` returns `409`, an unknown element on either side returns `400`, and a target that is a statement subtotal returns `422` with a message pointing at the leaf concepts beneath it.

### Correct a wrong arc

Correcting an arc is a delete followed by a create. Find the association id on the mapping detail:

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ mapping(mappingId: \"'$MAPPING_ID'\") { associations { id fromElementName toElementId } } }"}'
```

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/delete-mapping-association" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"mapping_id": "'$MAPPING_ID'", "association_id": "assoc_01J..."}'
```

Then write the right arc with `create-mapping-association`. Both writes mark the graph stale, so the analytical graph picks up the change on its next materialization. `live-financial-statement` reflects the change immediately.

### The MCP sequence

An assistant working over MCP runs the same loop without leaving the tool surface:

```text
list-mapping-structures                       → the coa_mapping structure id
get-mapping-summary      mapping_id=…          → coverage, confidence buckets, unreachable
get-unmapped-elements    mapping_id=…          → accounts with no arc
suggest-mapping          element_id=…          → renderable candidates for one account
create-mapping-association mapping_id=… from_element_id=… to_element_id=…
delete-mapping-association mapping_id=… association_id=…
```

## The Closed-Period Mapping Lock

Closing a month stamps its statements from the ledger *through the mapping as it stood at that moment*. Changing an arc afterwards would leave the stamped statements disagreeing with a live render of the same month. So, after a close, `create-mapping-association` and `delete-mapping-association` both refuse any change to an account with activity in a closed month. They return `422`, and the message names the closed months affected and the earliest one to reopen back to:

```text
refusing to disturb facts that the ledger treats as immutable: 3 canonical statement set(s)
of closed period(s) 2026-06–2026-08 were computed through the mapping arcs or element
attributes this change alters (reopen latest-first down to 2026-06 — closed history is immutable)
```

Arcs for accounts with no closed-month activity are unaffected. So is any account in a graph that has never closed a month.

**Moving a locked account within its statement is allowed: a reclassification.** Pass `replaces_association_id` to `create-mapping-association` with the account's current arc, and the old arc is removed and the new one written in the same call. When the new concept is a leaf of the same primary statement as the old one under the entity's Reporting Style (balance sheet to balance sheet, income statement to income statement), no total the close asserted changes, so the write goes through. The stamped statements keep the answer as closed, a live render reads through the new arc, and the response names the months in `reclassified_periods`; the new arc's `metadata.reclassification` keeps the arc it replaced, the concept the account came from, the months and the `reason` given. A move across statements changes net income and every later carry-in, so it is refused with the same `422` naming the months. The same judgement applies to an `update-taxonomy-block` envelope that removes an account's arc and adds another in one call. `delete-mapping-association` alone stays refused for such an account: unmapping it drops its balance off the statement.

To change an arc the reclassification path cannot (a cross-statement move, or an account that was unmapped when the month closed), reopen the months back to the earliest one named (latest first, one at a time), change the mapping, then close forward again. Each re-close stamps the statements fresh through the new arc. See [Period Close](Period-Close.md#reopening-a-period). The practical rule is still **map first, close second**: a reclassification corrects the line an account sits on, not whether it was on the statements at all.

## Gotchas and Pitfalls

### Mapped is not the same as rendering

Check `unreachableCount` as well as `coveragePercent`. An unreachable arc is a balance missing from every statement.

### Never map to a subtotal

A mapping to a calc-DAG parent would double-count, so the write is refused with `422`. The candidate surfaces exclude subtotals, so pick targets from them instead of typing qnames.

### "Other" concepts render, but tell you little

An account mapped to a catch-all concept appears on the statement inside a broad line. Use a specific concept when one fits.

### Contra accounts go with the asset they reduce

Map accumulated depreciation and allowances to their contra concepts, never to the net asset line.

### Auto-map runs once

Accounts added after the first sync stay unmapped until someone maps them. Re-check coverage after any chart-of-accounts change.

## Self-hosted deployments

On your own stack, `just demo-roboledger` provisions a fully mapped tenant (pass `--ai` to have the MappingOperator produce the mappings, which needs Bedrock). See the [RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough.md). Every call on this page works against `http://localhost:8000` with the key from `just demo-user`.

## Related Documentation

- **[Taxonomy and Frameworks](Taxonomy-and-Frameworks.md)** - `rs-gaap`, traits, and the tenant library that mapping targets come from
- **[Reporting and Rendering](Reporting-and-Rendering.md)** - The calc DAG and the Reporting Style that decide what renders
- **[Period Close](Period-Close.md)** - Stamped statements and the reopen path the mapping lock points to
- **[QuickBooks Sync and Write Policy](QuickBooks-Sync-and-Write-Policy.md)** - The first sync that triggers auto-map
- **[AI Operators and MCP](AI-Operators-and-MCP.md)** - The MappingOperator and the MCP tool surface
- **[RoboLedger Operations](RoboLedger-Operations.md)** - The full operations catalog

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Technical docs](Home.md)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
