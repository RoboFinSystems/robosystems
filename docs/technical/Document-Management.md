# Document Management

Every graph you own carries a document store for policies, procedures and notes, written as markdown and indexed for search. This page covers managing it over each user graph — an entity graph or a custom graph alike — and feeding it into the search and retrieval layer your AI Operators read from. Documents live in PostgreSQL as the source of truth and are projected into OpenSearch as a sectioned, searchable index.

**Quick Start:** Create a markdown document with `POST /v1/graphs/{graph_id}/operations/index-document` (or the `create-document` MCP tool), and it is automatically split into sections and indexed for search. Creation, update, and deletion are **content operations**; the REST surface at `/v1/graphs/{graph_id}/documents` is read-only (list and get).

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Overview

The document store is a per-graph layer for unstructured, human- and agent-authored knowledge — the policies, procedures, working notes, and narrative context that complement the structured ledger and reporting data in the graph. It does four things end-to-end when you upload a document:

1. **Store** — The raw markdown is written to PostgreSQL as a `Document` row scoped to the graph. This is the canonical source of truth.
2. **Section** — On ingest, the markdown is split into sections on heading boundaries (`#` through `######`), with YAML frontmatter stripped and small sections merged.
3. **Embed and index** — Each section is projected into OpenSearch under a stable `udoc_` id, where it becomes discoverable via full-text (BM25) and semantic (KNN) search.
4. **Retrieve** — Agents and users find documents through the search surface (`POST /search`) and pull full section text via `GET /search/{document_id}`.

PostgreSQL holds the full document; OpenSearch holds a derived projection used only for search. Create and update write PostgreSQL first, then sync to OpenSearch; delete removes both. This split keeps the canonical record durable while letting the retrieval index be rebuilt at any time.

Documents are the foundation of the retrieval layer your AI agents read from — policies become searchable context an agent can cite. Agent recollections are a **separate** surface: semantic memory is now first-class, with its own per-graph vector store and its own operations (see [Semantic Memory Is a Separate Surface](#semantic-memory-is-a-separate-surface)). For the search and retrieval side, see [Search and AI Retrieval](Search-and-AI-Retrieval). For bulk tabular node and relationship data (parquet, CSV, JSON staged into the graph), see [File Uploads](File-Uploads).

## Prerequisites

- A RoboSystems account and an API key, sent as the `X-API-Key` header (see [Quick Start](Quick-Start)).
- A **non-shared user graph** — document operations are rejected on shared public repositories (SEC and other platform-managed repos).
- Document search enabled on the deployment. It is on robosystems.ai; a self-hosted deployment controls it with `SEMANTIC_SEARCH_ENABLED` (see [Self-hosted deployments](#self-hosted-deployments)).

## Quick Start

Upload a markdown document, then list what is in the graph:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector

# Upload a policy document
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/operations/index-document" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Revenue Recognition Policy",
    "folder": "policies",
    "tags": ["revenue", "asc-606"],
    "content": "# Scope\n\nThis policy governs how the company recognizes revenue under ASC 606.\n\n# Five-Step Model\n\nWe identify the contract, performance obligations, and transaction price.\n"
  }'

# List documents in the graph
curl "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/documents" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

The operation returns an `OperationEnvelope`; the indexing report sits inside `result`:

```json
{
  "operation": "index-document",
  "operationId": "op_01K2...",
  "status": "completed",
  "result": {
    "id": "doc_01J9XYZ...",
    "document_id": "udoc_doc_01J9XYZ...",
    "sections_indexed": 2,
    "total_content_length": 187,
    "section_ids": ["udoc_doc_01J9XYZ..._0", "udoc_doc_01J9XYZ..._1"]
  },
  "at": "2026-08-09T18:04:11.921Z",
  "createdBy": "user_01K2...",
  "idempotentReplay": false
}
```

Envelope keys are camelCase on the wire; the payload fields inside `result` keep their snake_case names.

The full endpoint and schema surface is published in the API reference at [robosystems.ai/docs/api](https://robosystems.ai/docs/api). This guide covers the concepts and tasks; the OpenAPI spec is the authoritative reference for request and response shapes.

## The Document Model

A document is a markdown payload plus a small set of metadata fields. The canonical record lives in PostgreSQL; the fields you work with are:

| Field | Purpose |
| --- | --- |
| **`id`** | Prefixed identifier (`doc_...`), the PostgreSQL primary key |
| **`title`** | Human-readable title (required, up to 500 chars) |
| **`content`** | Raw markdown body (required, up to 500,000 chars) |
| **`tags`** | Optional list of strings for categorization |
| **`folder`** | Optional free-text category (e.g. `"policies"`, `"memory"`) |
| **`external_id`** | Optional caller-supplied id, **unique per graph** — drives idempotent upsert |
| **`source_type`** | Origin classification; defaults to `"uploaded_doc"` |
| **`sections_indexed`** | Count of OpenSearch sections produced from the content |
| **`created_at` / `updated_at`** | Timestamps |

Every document is scoped to a `graph_id` (the tenant boundary) and carries the `user_id` of the uploader. `source_type` defaults to `"uploaded_doc"`; the document store is primarily the **user- and agent-authored** surface rather than a connection sync target.

The list response is a lighter shape than the detail response: it returns `document_title`, `section_count` (mapped from `sections_indexed`), `source_type`, `folder`, `tags`, and timestamps. Fetch a single document by id to get the full `content` back.

## CRUD via the REST Surface

The document surface is split the way the platform splits every content surface: **reads are REST GETs**, **writes are operations**. Both require the `X-API-Key` header, and each handler validates your API key and per-graph access before running; writes additionally require the member/admin write role on the graph.

| Kind | Method | Path | Purpose |
| --- | --- | --- | --- |
| Read | GET | `/v1/graphs/{graph_id}/documents` | List documents (optional `?source_type=` filter) |
| Read | GET | `/v1/graphs/{graph_id}/documents/{document_id}` | Get one document, full content |
| Write | POST | `/v1/graphs/{graph_id}/operations/index-document` | Create a document (omit `document_id`) or update one (supply it) |
| Write | POST | `/v1/graphs/{graph_id}/operations/delete-document` | Delete a document |

Both operations return an `OperationEnvelope` and accept an `Idempotency-Key` header — a retry with the same key inside 24 hours replays the original envelope with `idempotentReplay: true` rather than re-running the command.

### Get a Document

```bash
curl "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/documents/doc_01J9XYZ..." \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

Returns the full document including raw `content`, `tags`, `folder`, `external_id`, `source_type`, `sections_indexed`, and timestamps.

### Update a Document (Partial)

Update is the **same** `index-document` operation with a `document_id` supplied. It is a genuine partial update: only the fields you include in the request body are applied, and omitted fields are left untouched. Updating `content` re-sections and re-indexes the document.

```bash
# Update only the content — re-sections and re-indexes
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/operations/index-document" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"document_id": "doc_01J9XYZ...", "content": "# Scope\n\nUpdated scope language for FY2026.\n"}'
```

To **clear** a field rather than leave it untouched, pass `null` explicitly (e.g. `{"tags": null}`). The handler distinguishes "field omitted" from "field set to null", so this is the only way to remove existing tags or a folder via update.

### Delete a Document

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/operations/delete-document" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"document_id": "doc_01J9XYZ..."}'
```

Delete returns a **200 `OperationEnvelope`** whose `result` is `{"document_id": "doc_01J9XYZ...", "deleted": true}`; a document that does not exist returns 404. It removes the PostgreSQL row and the document's OpenSearch sections together, so the document disappears from search immediately.

## Markdown Frontmatter and Sectioning

Documents are markdown, and the way they are split into sections determines how they appear in search. Two ingest behaviors are worth understanding: frontmatter and sectioning.

### YAML Frontmatter

If the content opens with a YAML frontmatter block, it is stripped from the body and can fill **unset** metadata fields — `title`, `tags`, and `folder`. Explicit values in the request always win over frontmatter; frontmatter only fills gaps.

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/operations/index-document" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "content": "---\ntitle: Close Checklist\ntags: close, monthly\nfolder: procedures\n---\n\n# Pre-Close\n\nReconcile all bank accounts.\n\n# Close\n\nPost accruals and run the trial balance.\n"
  }'
```

Here the request body sets no `title`, `tags`, or `folder`, so they are taken from the frontmatter.

### Sectioning Rules

The body is split into sections on markdown heading boundaries (`#` through `######`). Each section gets a slugified `section_id` derived from its heading. The parser applies a few normalization rules:

- A document with **no headings** becomes a single `full-document` section.
- Sections under **20 words** are merged into a neighbor, so tiny stub sections do not fragment the index.
- Sections over **50,000 characters** are truncated.

`sections_indexed` in the response tells you exactly how many sections were produced — a useful sanity check that your headings split the way you expected.

## Upsert by `external_id`

`external_id` is unique per graph and drives idempotent re-ingestion. When you index a document with an `external_id` that already exists in the graph, the call routes to an update of the existing document instead of creating a duplicate.

This is the pattern for syncing documents from an external system where each source item has a stable id (for example, a Google Drive file id). Re-indexing the same `external_id` after the source changes updates the stored document in place:

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/operations/index-document" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Expense Policy",
    "external_id": "gdrive:1a2b3c4d5e",
    "folder": "policies",
    "content": "# Expense Policy\n\nReimbursable categories and limits.\n"
  }'
```

The first call creates the document; subsequent calls with the same `external_id` update it. The tier document limit is checked on create but **not** on the upsert path, so re-syncing an existing document never trips the limit.

## How Documents Feed Search

Documents become discoverable through the search and retrieval layer. The two surfaces are connected but distinct.

When a document is created or updated, its sections are indexed into OpenSearch under a stable base id. User-authored documents use a `udoc_` prefix (in contrast to SEC and pipeline documents, which use a `doc_` prefix and have no PostgreSQL row). Each section is indexed as `udoc_{base}_{index}`, and a search hit's document id maps back to the PostgreSQL document id.

The read side lives under `/v1/graphs/{graph_id}/search`:

- `POST /v1/graphs/{graph_id}/search` — search across indexed documents, with optional filters such as `source_type`.
- `GET /v1/graphs/{graph_id}/search/{document_id}` — fetch a single OpenSearch **section** by its id.

```bash
# Find the document via search, then delete it by its PostgreSQL id
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/search" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "revenue recognition ASC 606", "source_type": "uploaded_doc", "size": 5}'

curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/operations/delete-document" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"document_id": "doc_01J9XYZ..."}'
```

**Note:** `GET /documents/{id}` and `GET /search/{id}` return different things. The first returns the whole PostgreSQL document by its `doc_...` id; the second returns a single OpenSearch section by its `udoc_..._{index}` id. Do not conflate the two ids. The full search story — BM25 versus semantic search, ranking, and agent retrieval — is covered in [Search and AI Retrieval](Search-and-AI-Retrieval).

## The MCP Surface

The same document operations are exposed as MCP tools for agent-driven workflows. They enforce the same auth and tenant isolation as the REST API and return structured results an LLM can reason over.

| Tool | Purpose |
| --- | --- |
| `create-document` | Create a document |
| `update-document` | Update an existing document |
| `delete-document` | Delete a document |
| `get-document` | Fetch one document |
| `list-documents` | List documents in the active graph |
| `search-documents` | Search the document index (returns a `document_id` per hit) |
| `get-document-section` | Read a section by the id returned from search, a window at a time |

`search-documents` returns a `document_id` per hit; pass it to `get-document-section` to read the section. Semantic (KNN) search is opt-in; the default mode is BM25 full-text. The MCP retrieval tools are documented alongside the rest of the search surface in [Search and AI Retrieval](Search-and-AI-Retrieval).

## Semantic Memory Is a Separate Surface

Agent recollections used to be modelled as documents filed under `folder="memory"` — a convention that the `create-document` tool description still advertises. That is now the legacy pattern. **Semantic memory is first-class**: a per-graph LanceDB vector store with its own storage, its own tools, and its own feature flag.

| Surface | Operations |
| --- | --- |
| MCP tools | `remember`, `recall`, `update-memory`, `forget` |
| REST reads | `GET /v1/graphs/{g}/memory`, `GET /v1/graphs/{g}/memory/{memory_id}`, `POST /v1/graphs/{g}/memory/recall` |
| Write operations | `POST /v1/graphs/{g}/operations/{remember\|update-memory\|forget}` |

The gating is **different from documents**: a deployment controls memory with `SEMANTIC_MEMORY_ENABLED` (with `MCP_SEMANTIC_MEMORY_ENABLED` as an additional sub-gate on the MCP tools), not with `SEMANTIC_SEARCH_ENABLED`. Like documents, memory is blocked on shared repositories. Unlike documents, `remember` and `update-memory` are also blocked on subgraphs — a subgraph is meant to be cheap to create and throw away, so memories belong on the parent graph — while `forget` stays open so existing memories remain removable.

Memory recall is covered with the rest of the retrieval layer in [Search and AI Retrieval](Search-and-AI-Retrieval).

## Tier Limits

Each graph tier caps the number of uploaded documents. The limit counts only documents with `source_type="uploaded_doc"` and is enforced **on create only** — never on the `external_id` upsert path.

When a create would exceed the limit, the call returns **422** with a "Document limit reached" message. Because upsert by `external_id` skips the check, re-syncing existing documents is always safe regardless of the limit.

The cap itself is served rather than restated here. `GET /v1/graphs/{graph_id}/limits` returns a `documents` block with your graph's `current_count`, its `max_documents` for the tier, and an `approaching_limit` flag:

```bash
curl "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/limits" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" | jq .documents
```

## Troubleshooting

### Document Saves but `sections_indexed` Is 0

If the search service is unreachable (for example, OpenSearch is down but the feature flag is on), uploads still succeed in PostgreSQL — but the sync to OpenSearch is skipped. The response comes back with `sections_indexed: 0` and an empty `section_ids` list. The document is saved and retrievable by id, but it is not searchable until you re-index it (re-upload or update once the search service is healthy).

### 422 "Document Produced No Indexable Sections"

If parsing the content yields no indexable text — for example, content that is empty or contains only headings with no body — the upload fails with 422. Add real text under your headings, or check that the content is not blank.

### 403 on a Shared Repository

Document operations are blocked on shared public repositories (SEC and other platform-managed repos) and their subgraphs. You cannot store documents on a shared repo; use one of your own user graphs. The same restriction applies through the MCP tools.

### Update Changed More (or Less) Than Expected

Update via `index-document` is strictly partial: omitted fields are untouched, and a field is only cleared if you pass it explicitly as `null`. If an update did not clear `tags` or `folder`, you likely omitted the field — send `{"tags": null}` to clear it. If an update changed a field you did not mean to touch, you probably included it in the body.

### Content Rejected as Too Long

Content is capped at 500,000 characters at the request layer. Individual sections are additionally truncated at 50,000 characters during indexing. For very large bodies, split the source into multiple documents.

## Self-hosted deployments

Document search is gated by `SEMANTIC_SEARCH_ENABLED`, and the flag gates both halves differently. The read routers (`/documents`, `/search`) are conditionally mounted — with the flag off they do not exist and calls return **404** rather than a feature-disabled error. The `index-document` and `delete-document` operations are always mounted and return **503 "Document search is not enabled"** instead. Either way the fix is the same: confirm the flag in your environment and restart the stack if you changed it. See [Local Development](Local-Development).

## Related Documentation

**Wiki Guides:**

- **[Search and AI Retrieval](Search-and-AI-Retrieval)** - How documents become searchable and how AI agents retrieve them
- **[File Uploads](File-Uploads)** - Bulk tabular node and relationship data (parquet, CSV, JSON), a distinct surface from this markdown document store
- **[Custom Graph Schema](Custom-Graph-Schema)** - A custom-graph demo whose Step 6 indexes documents into this store

**Codebase Documentation:**

- **[Platform Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/core/README.md)** - Platform SQLAlchemy models, including the `Document` model
- **[API Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/api/README.md)** - Pydantic request and response models

**API Reference:**

- **[API reference](https://robosystems.ai/docs/api)** - Live OpenAPI spec with full request/response schemas

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
