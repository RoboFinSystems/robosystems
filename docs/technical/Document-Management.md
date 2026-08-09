# Document Management

This guide shows you how to manage the institutional-knowledge document store that lives over each entity graph — uploading policies, procedures, and notes as markdown, and feeding them into the search and retrieval layer your AI agents read from. Documents live in PostgreSQL as the source of truth and are projected into OpenSearch as a sectioned, searchable index.

**Quick Start:** Create a markdown document with the `create-document` MCP tool, and it is automatically split into sections and indexed for search. Document **creation is MCP-only today** — the REST surface at `/v1/graphs/{graph_id}/documents` is read-only (list and get); there is no REST upload endpoint.

## Overview

The document store is a per-graph layer for unstructured, human- and agent-authored knowledge — the policies, procedures, working notes, and narrative context that complement the structured ledger and reporting data in the graph. It does four things end-to-end when you upload a document:

1. **Store** — The raw markdown is written to PostgreSQL as a `Document` row scoped to the graph. This is the canonical source of truth.
2. **Section** — On ingest, the markdown is split into sections on heading boundaries (`#` through `######`), with YAML frontmatter stripped and small sections merged.
3. **Embed and index** — Each section is projected into OpenSearch under a stable `udoc_` id, where it becomes discoverable via full-text (BM25) and semantic (KNN) search.
4. **Retrieve** — Agents and users find documents through the search surface (`POST /search`) and pull full section text via `GET /search/{document_id}`.

PostgreSQL holds the full document; OpenSearch holds a derived projection used only for search. Create and update write PostgreSQL first, then sync to OpenSearch; delete removes both. This split keeps the canonical record durable while letting the retrieval index be rebuilt at any time.

Documents are the foundation of the retrieval layer your AI agents read from — policies become searchable context an agent can cite, and "memories" are simply documents filed under `folder="memory"`. For the search and retrieval side, see [Search and AI Retrieval](Search-and-AI-Retrieval). For binary file ingestion (which is a distinct surface), see [File Uploads](File-Uploads).

## Prerequisites

- A running local stack (`just start` brings up PostgreSQL, Valkey, LadybugDB, OpenSearch, the API, and Dagster).
- A **non-shared user graph** — document operations are rejected on shared public repositories (SEC and other platform-managed repos).
- An API key. Run `just demo-user` from the `robosystems/` directory; credentials are written to `.local/config.json`. Read the key with `jq -r .api_key .local/config.json`.
- `SEMANTIC_SEARCH_ENABLED=true`. The entire documents (and search) router is feature-flag-gated. When the flag is off, the routes are not mounted at all and calls return 404 rather than 503.

## Quick Start

Upload a markdown document, then list what is in the graph:

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=<your graph id>

# Upload a policy document
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/documents" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Revenue Recognition Policy",
    "folder": "policies",
    "tags": ["revenue", "asc-606"],
    "content": "# Scope\n\nThis policy governs how the company recognizes revenue under ASC 606.\n\n# Five-Step Model\n\nWe identify the contract, performance obligations, and transaction price.\n"
  }'

# List documents in the graph
curl "http://localhost:8000/v1/graphs/$GRAPH_ID/documents" \
  -H "X-API-Key: $API_KEY"
```

The upload response reports how many sections were indexed:

```json
{
  "id": "doc_01J9XYZ...",
  "document_id": "udoc_doc_01J9XYZ...",
  "sections_indexed": 2,
  "total_content_length": 187,
  "section_ids": ["udoc_doc_01J9XYZ..._0", "udoc_doc_01J9XYZ..._1"]
}
```

The full endpoint and schema surface is published in the live OpenAPI spec at [api.robosystems.ai/docs](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` locally). This guide covers the concepts and tasks; the OpenAPI spec is the authoritative reference for request and response shapes.

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

Every document is scoped to a `graph_id` (the tenant boundary) and carries the `user_id` of the uploader. `source_type` defaults to `"uploaded_doc"`; the document store is primarily the **user- and agent-authored** surface — uploads and memories — rather than a connection sync target.

The list response is a lighter shape than the detail response: it returns `document_title`, `section_count` (mapped from `sections_indexed`), `source_type`, `folder`, `tags`, and timestamps. Fetch a single document by id to get the full `content` back.

## CRUD via the REST Surface

All document operations live under `/v1/graphs/{graph_id}/documents` and require the `X-API-Key` header. Each handler validates your API key and per-graph access before running, and writes additionally require write permission on the graph.

The five operations are:

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/v1/graphs/{graph_id}/documents` | List documents (optional `?source_type=` filter) |
| GET | `/v1/graphs/{graph_id}/documents/{document_id}` | Get one document, full content |
| POST | `/v1/graphs/{graph_id}/documents` | Upload (create) a document |
| PUT | `/v1/graphs/{graph_id}/documents/{document_id}` | Update — partial; only provided fields change |
| DELETE | `/v1/graphs/{graph_id}/documents/{document_id}` | Delete (returns 204 No Content) |

### Get a Document

```bash
curl "http://localhost:8000/v1/graphs/$GRAPH_ID/documents/doc_01J9XYZ..." \
  -H "X-API-Key: $API_KEY"
```

Returns the full document including raw `content`, `tags`, `folder`, `external_id`, `source_type`, `sections_indexed`, and timestamps.

### Update a Document (Partial)

The PUT handler is a genuine partial update. Only the fields you include in the request body are applied; omitted fields are left untouched. Updating `content` re-sections and re-indexes the document.

```bash
# Update only the content — re-sections and re-indexes
curl -X PUT "http://localhost:8000/v1/graphs/$GRAPH_ID/documents/doc_01J9XYZ..." \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"content": "# Scope\n\nUpdated scope language for FY2026.\n"}'
```

To **clear** a field rather than leave it untouched, pass `null` explicitly (e.g. `{"tags": null}`). The handler distinguishes "field omitted" from "field set to null", so this is the only way to remove existing tags or a folder via update.

### Delete a Document

```bash
curl -X DELETE "http://localhost:8000/v1/graphs/$GRAPH_ID/documents/doc_01J9XYZ..." \
  -H "X-API-Key: $API_KEY" -i
```

Delete returns **204 No Content**. It removes the PostgreSQL row and the document's OpenSearch sections together, so the document disappears from search immediately.

## Markdown Frontmatter and Sectioning

Documents are markdown, and the way they are split into sections determines how they appear in search. Two ingest behaviors are worth understanding: frontmatter and sectioning.

### YAML Frontmatter

If the content opens with a YAML frontmatter block, it is stripped from the body and can fill **unset** metadata fields — `title`, `tags`, and `folder`. Explicit values in the request always win over frontmatter; frontmatter only fills gaps.

```bash
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/documents" \
  -H "X-API-Key: $API_KEY" \
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

`external_id` is unique per graph and drives idempotent re-ingestion. When you upload a document with an `external_id` that already exists in the graph, the upload routes to an update of the existing document instead of creating a duplicate.

This is the pattern for syncing documents from an external system where each source item has a stable id (for example, a Google Drive file id). Re-uploading the same `external_id` after the source changes updates the stored document in place:

```bash
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/documents" \
  -H "X-API-Key: $API_KEY" \
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
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/search" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "revenue recognition ASC 606", "source_type": "uploaded_doc", "size": 5}'

curl -X DELETE "http://localhost:8000/v1/graphs/$GRAPH_ID/documents/doc_01J9XYZ..." \
  -H "X-API-Key: $API_KEY" -i
```

**Note:** `GET /documents/{id}` and `GET /search/{id}` return different things. The first returns the whole PostgreSQL document by its `doc_...` id; the second returns a single OpenSearch section by its `udoc_..._{index}` id. Do not conflate the two ids. The full search story — BM25 versus semantic search, ranking, and agent retrieval — is covered in [Search and AI Retrieval](Search-and-AI-Retrieval).

## The MCP Surface

The same document operations are exposed as MCP tools for agent-driven workflows. They enforce the same auth and tenant isolation as the REST API and return structured results an LLM can reason over.

| Tool | Purpose |
| --- | --- |
| `create-document` | Create a document |
| `update-document` | Update an existing document |
| `get-document` | Fetch one document |
| `list-documents` | List documents in the active graph |
| `search-documents` | Search the document index (returns a `document_id` per hit) |
| `get-document-section` | Fetch a full section by the id returned from search |

These tools replaced the earlier `remember-text` / `recall-text` semantic-memory tools. The replacement is conceptual as well as mechanical: **memories are documents filed under `folder="memory"`**. To save an agent observation as a durable, searchable note, create a document in the memory folder:

```text
create-document --title "Q4 close observation" --folder memory \
  --content "# Note\n\nThe AR aging shows a stale invoice from Acme to follow up on."
```

`search-documents` returns a `document_id` per hit; pass it to `get-document-section` for the full section text. Semantic (KNN) search is opt-in; the default mode is BM25 full-text. The MCP retrieval tools are documented alongside the rest of the search surface in [Search and AI Retrieval](Search-and-AI-Retrieval).

## Tier Limits

Each graph tier caps the number of uploaded documents. The limit counts only documents with `source_type="uploaded_doc"` and is enforced **on create only** — never on the `external_id` upsert path.

| Tier | Document limit |
| --- | --- |
| `ladybug-standard` | 100 |
| `ladybug-large` | 1,000 |
| `ladybug-xlarge` | 10,000 |

When a create would exceed the limit, the upload returns **422** with a "Document limit reached" message. Because upsert by `external_id` skips the check, re-syncing existing documents is always safe regardless of the limit.

## Troubleshooting

### Calls Return 404 Instead of Working

The documents and search routers are only mounted when `SEMANTIC_SEARCH_ENABLED=true`. When the flag is off, the routes do not exist, so requests 404 rather than returning a feature-disabled error. Confirm the flag is set in your environment and restart the stack if you changed it.

### Document Saves but `sections_indexed` Is 0

If the search service is unreachable (for example, OpenSearch is down but the feature flag is on), uploads still succeed in PostgreSQL — but the sync to OpenSearch is skipped. The response comes back with `sections_indexed: 0` and an empty `section_ids` list. The document is saved and retrievable by id, but it is not searchable until you re-index it (re-upload or update once the search service is healthy).

### 422 "Document Produced No Indexable Sections"

If parsing the content yields no indexable text — for example, content that is empty or contains only headings with no body — the upload fails with 422. Add real text under your headings, or check that the content is not blank.

### 403 on a Shared Repository

Document operations are blocked on shared public repositories (SEC and other platform-managed repos) and their subgraphs. You cannot store documents on a shared repo; use one of your own user graphs. The same restriction applies through the MCP tools.

### Update Changed More (or Less) Than Expected

The PUT handler is strictly partial: omitted fields are untouched, and a field is only cleared if you pass it explicitly as `null`. If an update did not clear `tags` or `folder`, you likely omitted the field — send `{"tags": null}` to clear it. If an update changed a field you did not mean to touch, you probably included it in the body.

### Content Rejected as Too Long

Content is capped at 500,000 characters at the request layer. Individual sections are additionally truncated at 50,000 characters during indexing. For very large bodies, split the source into multiple documents.

## Related Documentation

**Wiki Guides:**

- **[Search and AI Retrieval](Search-and-AI-Retrieval)** - How documents become searchable and how AI agents retrieve them
- **[File Uploads](File-Uploads)** - Ingesting binary files, a distinct surface from this markdown document store
- **[Connecting QuickBooks Locally](Connecting-QuickBooks-Locally)** - Sync structured ledger data into a graph

**Codebase Documentation:**

- **[Platform Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/core/README.md)** - Platform SQLAlchemy models, including the `Document` model
- **[API Models](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/api/README.md)** - Pydantic request and response models

**API Reference:**

- **[API Documentation](https://api.robosystems.ai/docs)** - Live OpenAPI spec with full request/response schemas (local equivalent at `http://localhost:8000/docs`)

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
