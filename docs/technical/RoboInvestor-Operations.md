# RoboInvestor Operations

This guide shows you how to drive the RoboInvestor command surface against a local RoboSystems stack — creating securities, building portfolio blocks with positions, updating and disposing positions, and reading the data back. It also explains the cross-graph issuer-linking concept that connects an investor's holdings to the companies that issue them.

RoboInvestor is focused on private-company portfolio management. This page documents what the command surface exposes: six write operations plus a GraphQL read surface. There is no synthetic-data demo recipe for RoboInvestor (unlike `just demo-roboledger`), so this is an API-first walkthrough.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [The Two Write Surfaces](#the-two-write-surfaces)
- [Core Concepts](#core-concepts)
- [Quick Start](#quick-start)
- [Walkthrough](#walkthrough)
- [Cross-Graph Issuer Linking](#cross-graph-issuer-linking)
- [Reading Data Back With GraphQL](#reading-data-back-with-graphql)
- [Operations Reference](#operations-reference)
- [Gotchas](#gotchas)
- [Surface Scope](#surface-scope)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

RoboInvestor is the holder's view of the financial universe: what you hold, in whom, and what it is worth. It is the counterpart to RoboLedger's issuer's view (what are my books). A `Security` on the investor side and a contract on the company side are two perspectives on one real-world instrument — the same way an invoice is accounts-receivable to the seller and accounts-payable to the buyer.

The RoboInvestor command surface lives under the extensions mount and is graph-scoped at the URL level:

```text
POST /extensions/roboinvestor/{graph_id}/operations/{op_name}
```

Like every extensions write surface, these operations follow CQRS: **writes go through REST command operations; reads go through GraphQL only.** There are no REST read routes. Each write returns an `OperationEnvelope` and accepts an `Idempotency-Key` header.

The surface is intentionally small. Portfolios and positions are written through a single molecule — the **Portfolio Block** — rather than through atom-level CRUD. Securities are the one standalone entity with their own create / update / delete operations.

## Prerequisites

- The local stack running:

  ```bash
  git clone https://github.com/RoboFinSystems/robosystems.git
  cd robosystems
  just start
  ```

- `ROBOINVESTOR_ENABLED=true` in your environment. This flag gates both the RoboInvestor operations and the GraphQL investor fields. If it is off, the REST operations return HTTP 404 "Investor module not initialized." and the GraphQL fields raise `INVESTOR_NOT_INITIALIZED`.
- A graph provisioned with the `roboinvestor` schema extension. The operations and the GraphQL investor fields only appear on graphs that carry this extension.
- An API key. Run `just demo-user` to create or reuse demo credentials, then read the key from `.local/config.json`.

## The Two Write Surfaces

RoboInvestor exposes exactly two write surfaces plus a read surface:

| Surface | Mechanism | Operations |
| --- | --- | --- |
| **Portfolio Block** | REST command operations | `create-portfolio-block`, `update-portfolio-block`, `delete-portfolio-block` |
| **Security Master Data** | REST command operations | `create-security`, `update-security`, `delete-security` |
| **Reads** | GraphQL only | `portfolios`, `securities`, `security`, `positions`, `position`, `holdings`, `portfolioBlock` |

All portfolio and position writes flow through the three Portfolio Block operations. There are no separate `create-portfolio`, `create-position`, or `update-position` endpoints — atom-level portfolio and position CRUD was retired in favor of the molecule.

## Core Concepts

**Two perspectives, one universe.** RoboInvestor is the holder's view; RoboLedger is the issuer's view. The same instrument is a `Security` you hold and a contract the issuer books.

**Two distinct entity links, deliberately named apart.** A portfolio's `entity_id` is the **owner** (the fund, trust, or person who holds the portfolio). A security's `entity_id` is the **issuer** (the company that issued the instrument). Do not conflate them — they point at entities from opposite sides of the holding relationship.

**The Portfolio Block molecule.** A portfolio plus its active positions, the securities those positions reference, and the issuer entities behind those securities are assembled into one `PortfolioBlockEnvelope`. This molecule is the primary read *and* write surface. It is the same "the molecule crosses the boundary" pattern as RoboLedger's Information Block.

**Money is stored in cents.** `cost_basis` and `current_value` are integer cents to avoid floating-point drift. The Portfolio Block envelope pre-converts these to `*_dollars` floats for display, but cents are authoritative. `"cost_basis": 1850000` means $18,500.00.

**Atomic, validate-up-front writes.** A portfolio-block create or update validates every referenced security and position before any row is written. If any reference is unknown, the whole operation is rejected and nothing is persisted.

**Cross-graph issuer linking.** A security can record the `graph_id` of the issuing company so an investor's holdings tie back to the issuer's own graph. This is covered in its own section below.

## Quick Start

Read your credentials once, then drive the surface with `curl`:

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=$(jq -r '.graphs | to_entries[0].value.graph_id' .local/config.json)
```

The `graph_id` is nested under `graphs.<slot>.graph_id` in `.local/config.json` — it is not a top-level key, and the slot name varies by which demo script created it, so read the id out of the nested structure rather than assuming a slot name. The graph must carry the `roboinvestor` extension; see [Prerequisites](#prerequisites).

The minimal end-to-end path is: create a security, create a portfolio block that references it, then read it back via GraphQL. The walkthrough below does exactly that.

## Walkthrough

### Step 1: Create a Security

Securities are master data. They must exist before any position can reference them — `create-portfolio-block` never mints securities.

```bash
curl -X POST "http://localhost:8000/extensions/roboinvestor/$GRAPH_ID/operations/create-security" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "entity_id": "ent_acme_holdings",
    "name": "Common Stock Class A",
    "security_type": "common_stock",
    "security_subtype": "class_a",
    "authorized_shares": 10000000,
    "outstanding_shares": 6500000
  }'
```

The response is an `OperationEnvelope` whose `result` is a `SecurityResponse`. It carries an `id` (shaped like `sec_...`), `is_active: true`, and `terms: {}` by default. `security_type` is an open vocabulary — common values include `common_stock`, `preferred_stock`, `warrant`, `convertible_note`, `safe`, `option`, `llc_unit`, `lp_interest`, and `restricted_stock_unit`. `security_subtype` is free text.

### Step 2: Create a Portfolio Block With Positions

A portfolio block creates the portfolio and its initial positions in one atomic operation. Every `security_id` is validated up front; an unknown one rejects the whole request.

```bash
curl -X POST "http://localhost:8000/extensions/roboinvestor/$GRAPH_ID/operations/create-portfolio-block" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "portfolio": {
      "name": "Q1 2026 Growth",
      "description": "Mid-cap public equities",
      "strategy": "growth",
      "inception_date": "2026-01-01",
      "base_currency": "USD",
      "entity_id": "ent_acme_holdings"
    },
    "positions": [
      {
        "security_id": "sec_aapl",
        "quantity": 100,
        "quantity_type": "shares",
        "cost_basis": 1850000,
        "currency": "USD",
        "current_value": 2010000,
        "valuation_date": "2026-04-30",
        "valuation_source": "broker_statement",
        "acquisition_date": "2026-01-15"
      }
    ]
  }'
```

`cost_basis` and `current_value` are integer cents — `1850000` is $18,500.00 and `2010000` is $20,100.00. The `result` is a `PortfolioBlockEnvelope` with dollar totals computed server-side (`total_cost_basis_dollars`, `total_current_value_dollars`) and an `active_position_count`.

### Step 3: Update the Portfolio Block

Updates to a portfolio block carry the portfolio's own field changes plus position deltas in three buckets: `add`, `update`, and `dispose`. Each delta targets a position by id, and every targeted id must belong to this portfolio.

```bash
curl -X POST "http://localhost:8000/extensions/roboinvestor/$GRAPH_ID/operations/update-portfolio-block" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "portfolio_id": "port_q1_growth_2026",
    "portfolio": {"description": "Pivoted toward defensive holdings"},
    "positions": {
      "add": [],
      "update": [
        {"id": "pos_aapl_lot_1", "current_value": 1980000, "valuation_date": "2026-05-06", "valuation_source": "broker_statement"}
      ],
      "dispose": [
        {"id": "pos_oldcorp_lot_3", "disposition_reason": "Liquidated; rotated capital"}
      ]
    }
  }'
```

The `update` bucket re-marks a position (new `current_value`, `valuation_date`, `valuation_source`). The `dispose` bucket retires a position; once disposed, it no longer counts toward the portfolio's active totals or the unique-active constraint. The `result` is the refreshed `PortfolioBlockEnvelope`.

### Step 4: Read It Back

Reads are GraphQL-only — see [Reading Data Back With GraphQL](#reading-data-back-with-graphql) below for the full query.

### Step 5: Delete a Portfolio Block

Deleting a portfolio that still has active positions requires explicit confirmation, so you do not accidentally drop a live portfolio.

```bash
curl -X POST "http://localhost:8000/extensions/roboinvestor/$GRAPH_ID/operations/delete-portfolio-block" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"portfolio_id": "port_legacy_2024", "confirm_active_positions": true}'
```

Without `confirm_active_positions: true`, a portfolio with active positions returns HTTP 409 with the active-position count in the message. A portfolio whose positions are all disposed deletes without the flag. The `result` is a `DeletePortfolioBlockResponse` carrying `deleted`, `portfolio_id`, and `positions_deleted` (the cascade count).

## Cross-Graph Issuer Linking

The cross-graph link is what ties an investor's holding back to the issuing company's own graph. It hinges on two fields on `Security`: `entity_id` (the issuer entity inside *this* graph) and `source_graph_id` (the `graph_id` of the issuing company's graph).

`source_graph_id` records a public identifier — a graph id, not access. Recording it does not grant the investor any read access to the company's graph.

When you call `create-security`, the issuer link is resolved in priority order:

1. If `entity_id` is provided, that issuer entity is used directly.
2. Otherwise, if `source_graph_id` is provided, the operation looks for an `Entity` already materialized in *this* graph whose metadata records a matching `source_graph_id` — that is, an issuer that has already shared a report into this graph. If found, it links the security to that entity.
3. Otherwise, the security is created with `entity_id` left NULL, to be linked later.

This is a **mutual handshake**: the investor records the company's `graph_id`, *and* the company shares a report into the investor's graph. Neither alone establishes the link. Authorization lives at the report-sharing boundary, not in the RoboInvestor OLTP layer — recording a `source_graph_id` for a company that has not shared a report simply leaves the security unlinked until that report arrives.

## Reading Data Back With GraphQL

The read side is GraphQL only. The endpoint is graph-scoped by URL and is **not** roboinvestor-prefixed:

```text
POST /extensions/{graph_id}/graphql
```

`graph_id` comes from the URL — there is no `graphId` query argument. In dev, this endpoint renders GraphiQL with introspection, which is the easiest way to confirm exact field names and casing.

The investor query fields are:

| Field | Arguments | Returns |
| --- | --- | --- |
| `portfolios` | `limit` (default 100), `offset` (default 0) | portfolio list |
| `securities` | `entityId`, `securityType`, `isActive`, `limit`, `offset` | security list |
| `security` | `securityId` (required) | a single security |
| `positions` | `portfolioId`, `securityId`, `status`, `limit`, `offset` | position list |
| `position` | `positionId` (required) | a single position |
| `holdings` | `portfolioId` (required) | holdings list |
| `portfolioBlock` | `portfolioId` (required) | the assembled Portfolio Block molecule |

Argument names are camelCased (the Python `entity_id` becomes `entityId`, `portfolio_id` becomes `portfolioId`, and so on). The example below reads a Portfolio Block back; verify the exact selection-set field names in GraphiQL, since the schema is generated from the response models:

```bash
curl -X POST "http://localhost:8000/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ portfolioBlock(portfolioId: \"port_q1_growth_2026\") { id name totalCostBasisDollars totalCurrentValueDollars activePositionCount positions { id quantity status security { name securityType issuer { name sourceGraphId } } } } }"}'
```

The Portfolio Block read returns **active positions only**. Its dollar totals are pre-converted from cents; `total_current_value_dollars` sums the marked positions and is null only when no active position has a mark at all. If you need cents-precision position values, read through the `positions` / `position` fields rather than the assembled block.

For agent-driven reads, the same GraphQL surface is reachable through the MCP `query-graphql` and `get-graphql-schema` tools; `graph_id` is taken from context, not passed as an argument.

## Operations Reference

The full request and response schemas are published in the live OpenAPI spec — see [API Documentation](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` locally) rather than re-tabulating every field here. The six operations are:

| Operation | Request model | Result in envelope |
| --- | --- | --- |
| `create-portfolio-block` | `CreatePortfolioBlockRequest` | `PortfolioBlockEnvelope` |
| `update-portfolio-block` | `UpdatePortfolioBlockOperation` | `PortfolioBlockEnvelope` |
| `delete-portfolio-block` | `DeletePortfolioBlockOperation` | `DeletePortfolioBlockResponse` |
| `create-security` | `CreateSecurityRequest` | `SecurityResponse` |
| `update-security` | `UpdateSecurityOperation` | `SecurityResponse` |
| `delete-security` | `DeleteSecurityOperation` | `DeleteResult` |

Every response is wrapped in an `OperationEnvelope` with these wire fields: `operation` (the kebab-case op name), `operationId` (an `op_`-prefixed ULID), `status` (`completed` for these synchronous operations, HTTP 200), `result` (the domain payload), `at` (ISO-8601 UTC), `createdBy`, and `idempotentReplay`. All six accept an `Idempotency-Key` header.

Common error responses:

| Condition | HTTP | Error |
| --- | --- | --- |
| Referenced security does not exist | 404 | `SecurityNotFoundError` |
| Referenced position does not exist | 404 | `PositionNotFoundError` |
| Position id belongs to a different portfolio | 409 | `PositionPortfolioMismatchError` |
| Second active position for same (portfolio, security) | 409 | `DuplicateActivePositionError` |
| Delete portfolio with active positions, no confirmation | 409 | `ActivePositionsRequireConfirmationError` |
| RoboInvestor module not initialized (REST) | 404 | "Investor module not initialized." |
| RoboInvestor module not initialized (GraphQL) | — | `INVESTOR_NOT_INITIALIZED` |

## Gotchas

**Money is cents, not dollars.** `"cost_basis": 1850000` means $18,500.00. This is the single most likely mistake. The envelope's `*_dollars` fields are display conveniences; the stored cents are authoritative.

**Securities are not minted by portfolio operations.** A position references an existing `security_id`. `create-portfolio-block` and `update-portfolio-block` never create securities. An unknown `security_id` returns HTTP 404 (`SecurityNotFoundError`), and because writes are atomic, the entire portfolio block is rejected.

**One active position per (portfolio, security).** A unique partial index blocks a second active position for the same security in the same portfolio. A violation returns HTTP 409 (`DuplicateActivePositionError`). Dispose the existing position first, or update it in place.

**Delete requires confirmation when active positions exist.** `delete-portfolio-block` returns HTTP 409 (`ActivePositionsRequireConfirmationError`, message includes the active count) unless you pass `confirm_active_positions: true`. Disposed-only portfolios delete without the flag.

**Position deltas must target the right portfolio.** In `update-portfolio-block`, a position id belonging to a different portfolio returns HTTP 409 (`PositionPortfolioMismatchError`); an unknown id returns HTTP 404 (`PositionNotFoundError`).

**Security delete is soft.** `delete-security` only sets `is_active=false`. Historical positions that reference the security stay valid.

**`source_graph_id` alone does not link an issuer.** It links only if the issuer has already shared a report into this graph, materializing the issuer `Entity` with a matching `source_graph_id` in its metadata. Otherwise the security is created with `entity_id` NULL and links once that report arrives.

**The GraphQL URL is not roboinvestor-prefixed.** Reads go to `/extensions/{graph_id}/graphql`, not `/extensions/roboinvestor/{graph_id}/graphql`. The graph is scoped by the URL path; there is no `graphId` query argument.

**Some vocabularies are open.** `quantity_type` (for example `shares`, `units`, `percentage`, `principal`), `valuation_source` (for example `manual`, `report_derived`, `market`, `formula`, `broker_statement`), `strategy`, and a position's `status` are treated as open vocabularies rather than fixed enums. Use the OpenAPI spec and GraphiQL for the values your deployment accepts.

## Surface Scope

The RoboInvestor surface covers private-company portfolio management:

- Security master data: create, update, soft-delete.
- Portfolio Block writes: create, update (with add / update / dispose position deltas), delete (with active-position confirmation).
- GraphQL reads across portfolios, securities, positions, holdings, and the assembled Portfolio Block.
- Cross-graph issuer linking via `source_graph_id` and the report-sharing handshake.

Atom-level portfolio and position CRUD endpoints do not exist — all portfolio and position writes go through the Portfolio Block. The surface is deliberately narrow; the molecule is the contract.

## Related Documentation

**Wiki Guides:**

- **[Extensions Surface Overview](Extensions-Surface-Overview)** - The shared `/extensions/{...}/operations/{op}` envelope, CQRS, and idempotency contract this surface inherits.
- **[GraphQL Reads](GraphQL-Reads)** - The typed read surface where the investor query fields live.
- **[RoboLedger Demo Walkthrough](RoboLedger-Demo-Walkthrough)** - The sibling issuer-side product, on the same extensions envelope contract.

**Codebase Documentation:**

- **[GraphQL README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - Strawberry extensions surface, Pydantic auto-derivation, and resolver patterns.
- **[Operations README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration and the operations kernel.
- **[Extensions Models README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/extensions/README.md)** - The schema-per-graph OLTP models behind securities, portfolios, and positions.

**API Reference:**

- **[API Documentation](https://api.robosystems.ai/docs)** - Full request and response schemas for all six operations (local equivalent at `http://localhost:8000/docs`).

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
