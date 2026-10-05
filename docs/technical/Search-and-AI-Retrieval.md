# Search & AI Retrieval

Search gives an AI Operator hybrid keyword and semantic search over your own documents and SEC filing narratives, plus a memory store of its own. It is the institutional-knowledge layer over your entity graph: upload unstructured content (policies, procedures, notes), index it, and let an Operator ground its answers on the right document.

**Quick Start:** Upload a policy document into a writable graph with the `index-document` operation, then search it with `POST /v1/graphs/{graph_id}/search` (or the `search-documents` MCP tool) to see it surface.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Overview

Your entity graph holds the structured numbers — facts, accounts, periods. Search holds the *unstructured* context that explains them: depreciation policies, revenue-recognition procedures, accounting memos, and the narrative sections of SEC filings. With both in place, an AI Operator can reason about *why* and *how*, not just *what*.

The retrieval workflow has three stages:

1. **Upload** unstructured content into a graph (your own documents) or read from a shared repository (SEC filing narratives).
2. **Index** the content into OpenSearch as searchable sections, each carrying a local vector embedding for semantic matching.
3. **Query** the index through two MCP tools — `search-documents` returns ranked snippets, `get-document-section` returns the section's text, a window at a time — so an Operator finds and reads exactly the policy it needs.

There are three corpora available:

- **Your uploaded documents** — per-graph, private to that graph. Policies, procedures, internal notes. Indexed in OpenSearch.
- **SEC filing narratives** — on the shared `sec` repository: MD&A, risk factors, and iXBRL disclosure text, cross-referenced back to the structured XBRL graph. Indexed in OpenSearch.
- **Semantic memory** — short free-text recollections an Operator writes for itself, in a per-graph vector store that is **separate from OpenSearch**. See [Semantic Memory: The Second Retrieval Plane](#semantic-memory-the-second-retrieval-plane).

Indexing and semantic search run a local embedding model in-image. They make **no external API calls and consume no AI credits**.

## Prerequisites

Before starting, ensure you have:

- A RoboSystems account, an API key, and a writable graph of your own (see [Quick Start](Quick-Start))
- For SEC filing narratives, a subscription to the `sec` repository (from **Repositories** in the app)
- Document search enabled on the deployment. It is on robosystems.ai; a self-hosted deployment controls it with `SEMANTIC_SEARCH_ENABLED` (see [Self-hosted deployments](#self-hosted-deployments))

All `curl` examples below target `https://api.robosystems.ai` and read your API key from `$ROBOSYSTEMS_API_KEY`.

## Quick Start

The fastest path is to upload one policy document into a graph you own ([Step 1](#step-1-create-the-policy-document) of the worked example below) and search it.

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector

# Search documents in a graph (semantic ranking is opt-in; this call turns it on)
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/search" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "PP&E depreciation policy useful lives", "semantic": true, "size": 5}'
```

**Note:** A just-created document may take up to ~30 seconds to appear in search results — the index refresh interval is 30 seconds (60 seconds during bulk loads).

## The Two-Step Retrieval Pattern

Retrieval is deliberately split across two tools so an Operator reads only what it needs:

1. **`search-documents`** runs the query and returns a ranked list of *snippets* — small, scannable highlights with the matching `document_id`, score, and metadata. This is cheap and keeps token usage low.
2. **`get-document-section`** takes a `document_id` from a hit and returns that section's text — over MCP a window at a time, over REST the whole part. The Operator calls this only for the section it actually wants to read.

This keeps an Operator from pulling whole documents into context when a single paragraph answers the question. The snippet tells it *which* section is relevant; the section fetch gives it the words.

**Note:** Because document content is excluded from the search response body, a `snippet` falls back to the section label when there are no highlight fragments. Always call `get-document-section` to read the real text.

### Which filing, or where in this one

`search-documents` answers *which* filings say something, ranked across the corpus. Once you know the filing, `search-text` answers *where* in it, exhaustively, over the filing's whole text read from its public folder — the 10-K, 10-Q, 20-F or 40-F as filed, from any processed year, or an 8-K earnings release with its exhibits — so it finds what no indexed section carries: the cover page, a footnote to a table, an exhibit, a passage between Items. `read-text` pages the text from a hit's offset, and `describe-filing` maps the filing's Items with their offsets. Pass a hit's `accession_number` with the filer's ticker, or `form: "8-K"` with a ticker for the latest earnings release. The query is words matched in order (`|` between phrases, a trailing `*` for a stem), not a regular expression. Nothing in this lane goes through OpenSearch: the filing is read from the public data bucket and the built text cached.

## Worked Example: Ground an Operator on the PP&E Depreciation Policy

This is the canonical thread: you upload a depreciation policy, then an Operator searches it, drills into the right section, and answers from the actual policy text.

### Step 1: Create the Policy Document

Upload a markdown policy into your graph. The platform splits it on headings, embeds each section, and indexes the sections independently.

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector

curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/operations/index-document" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Property, Plant & Equipment Depreciation Policy",
    "content": "# PP&E Depreciation Policy\n\n## Method\nThe Company depreciates property, plant and equipment on a straight-line basis over the estimated useful lives of the assets.\n\n## Useful Lives\n- Buildings: 30 years\n- Machinery & equipment: 7 years\n- Computer hardware: 3 years\n\n## Capitalization Threshold\nIndividual assets with a cost of $5,000 or more are capitalized; smaller purchases are expensed.",
    "folder": "policies",
    "tags": ["depreciation", "fixed-assets", "ppe"]
  }'
```

The operation returns an `OperationEnvelope` (camelCase keys), with the indexing report nested under `result`:

```json
{
  "operation": "index-document",
  "operationId": "op_01K2...",
  "status": "completed",
  "result": {
    "id": "doc_abc123def456",
    "document_id": "udoc_doc_abc123def456",
    "sections_indexed": 4,
    "total_content_length": 512,
    "section_ids": ["udoc_doc_abc123def456_0", "udoc_doc_abc123def456_1", "..."]
  },
  "at": "2026-08-09T18:04:11.921Z",
  "createdBy": "user_01K2...",
  "idempotentReplay": false
}
```

The bare `id` (`doc_…`) is the PostgreSQL document id used by `get-document` and the REST `/documents/{id}` route. The `document_id` and `section_ids` (`udoc_doc_…`) are the OpenSearch ids; a per-section id (ending in `_N`) is what search hits return and what `get-document-section` takes.

**Note:** Sections under 20 words are merged into a neighboring section, so a bare one-line heading will not become its own searchable chunk.

### Step 2: Search the Documents

Search for the policy. By default search uses BM25 keyword matching; add `"semantic": true` to combine it with semantic vector ranking.

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/search" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "PP&E depreciation policy useful lives", "semantic": true, "size": 5}'
```

Each hit carries a `document_id`, a relevance `score`, a `section_label`, and a `snippet` — enough to pick the right section without reading the whole document.

### Step 3: Drill Into the Full Section

Take the `document_id` from the most relevant hit and fetch its complete text:

```bash
curl -X GET "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/search/udoc_doc_abc123def456_1" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

This returns the full section content (here, the "Useful Lives" list with the 7-year figure for machinery).

### Step 4: The Operator Does This Automatically

When you ask an AI Operator a policy question, it chains the same two tools on your behalf:

```text
You: Look up our depreciation policy for property and equipment, then tell me
     the useful life we use for machinery.

The Operator will:
1. search-documents { "query": "PP&E depreciation policy machinery useful life",
                      "semantic": true }
     -> top hit document_id: "udoc_doc_abc123def456_1", section_label: "Useful Lives"
2. get-document-section { "document_id": "udoc_doc_abc123def456_1" }
     -> the section text: "Machinery & equipment: 7 years"
3. Answers, grounded in the actual policy: "7 years, straight-line."
```

The Operator never guesses — its answer is anchored to the section text it retrieved.

## Accessing Search

You have two ways to upload documents and run searches:

1. **[MCP Tools](#option-1-mcp-tools-for-ai-operators)** — for AI Operators and any MCP-compatible client
2. **[REST API](#option-2-rest-api)** — `curl` with `X-API-Key` for direct integration

### Option 1: MCP Tools (For AI Operators)

AI Operators interact with the index through MCP tools. All of them require document search to be enabled on the deployment (`SEMANTIC_SEARCH_ENABLED`); if search is disabled or OpenSearch is unreachable, the tools return an error and are listed as unavailable.

**Search tools:**

- `search-documents` — hybrid BM25 + KNN search. Inputs: `query` (required), plus optional `entity`, `form_type`, `section`, `element`, `fiscal_year`, `semantic` (default `false`), `size` (default 10, max 50), and `snippet_chars` (an approximate snippet budget per hit: default 400, 80–1,500 — raise it when snippets are too short to choose between hits). Returns ranked hits with `document_id`s; see [Response Shapes](#response-shapes) for what the tool keeps and folds.
- `get-document-section` — input `document_id` (required), plus `offset` (default 0) and `length` (default 4,000 characters, max 8,000). Returns one window of the section: `content`, `content_length` (the whole part) and `offset`, with `next_offset` when more follows — call again with it rather than answering from a partial read. A long section is stored in parts of about 25,000 characters: the result is one part (`part` of `part_count`, sharing a `parent_document_id`) and carries `next_document_id` for the next part.

**Filing-text tools** (no OpenSearch; the filing is read whole from the public data bucket):

- `describe-filing`, `search-text`, `read-text` — one filing's layout, a word search inside it, and its text by offset, on shared repositories only. A `ticker` picks the filing (with `fiscal_year` / `period_type`, an `accession`, or `form: "8-K"` for the latest earnings release). A ledger files no document; its sections read through `disclosures` and `information-block`. See [AI Operators & MCP](AI-Operators-and-MCP#the-three-retrieval-planes).

**Document tools:**

- `create-document` — inputs `title`, `content` (required), plus optional `folder` and `tags`. Uploads and indexes a document.
- `get-document` — returns the full document from PostgreSQL (the source of truth).
- `list-documents` — browse documents by metadata; filter by `folder` or `source_type`.
- `update-document` — update an existing document.
- `delete-document` — remove a document from PostgreSQL and OpenSearch together.

**Note:** The document tools register only on your own graphs, not on shared repositories. The write tools (`create-document`, `update-document`, `delete-document`) additionally require a writable graph. The search tools (`search-documents`, `get-document-section`) work against both your graphs and shared repositories like `sec`.

For an iXBRL disclosure, `get-document-section` returns the XBRL elements tagged in that section (`xbrl_elements`); the MCP `search-documents` hits leave them out to stay small. An Operator can hand one to `resolve-element` to look up the XBRL element, then run `read-graph-cypher` to pull the structured values — pivoting from narrative to numbers.

For Operator setup and the broader MCP tool surface, see [AI Operators and MCP](AI-Operators-and-MCP).

### Option 2: REST API

Both surfaces live under `/v1/graphs/{graph_id}`. Send your API key in the `X-API-Key` header. Search and section-fetch are REST endpoints (`POST /search`, `GET /search/{document_id}`); document reads are REST GETs under `/documents`; document writes are the `index-document` and `delete-document` operations.

Indexing, search, and section-fetch are shown in the [worked example](#worked-example-ground-an-operator-on-the-ppe-depreciation-policy) above. The read side of the document lifecycle is list and get:

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector

# List documents in a graph (optionally filter by source type)
curl "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/documents" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"

# Get a single document with its metadata (use the bare PG id, e.g. doc_…)
curl "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/documents/doc_abc123def456" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY"
```

The full request/response schema for every endpoint is published in the live OpenAPI spec — see [API reference](https://robosystems.ai/docs/api) rather than re-deriving it here.

**Important:** The document write operations (`index-document`, `delete-document`) require write access and are blocked on shared-repository graphs. You can `search` and `get-document-section` against the `sec` repository, but you cannot index documents into it — index into your own graph. See [Document Management](Document-Management) for the full document lifecycle.

## Search Filters

`search-documents` and the REST `/search` endpoint accept filters that narrow the result set. The two surfaces overlap but are not identical.

| Filter | Type | Meaning |
| --- | --- | --- |
| `query` | string (required) | The search text, 1–500 characters |
| `semantic` | bool (default `false`) | `false` = BM25 keyword only; `true` = hybrid BM25 + KNN semantic ranking |
| `entity` | string | Restrict to a filer. Only a CIK selects exactly one; a ticker or name is a loose word match (`ON` also matches filers with "On" in their names) |
| `form_type` | string | SEC form type (e.g. `10-K`, `10-Q`) |
| `section` | string | Filing section (e.g. `item_1a` for risk factors) |
| `element` | string | An XBRL element associated with the section |
| `fiscal_year` | int | Restrict to a fiscal year |
| `size` | int (1–50, default 10) | Number of hits to return |
| `snippet_chars` | int (80–1,500) | Approximate snippet budget per hit. The MCP tool defaults to 400; REST defaults to three highlight fragments of about 200 characters |

The REST endpoint additionally accepts `source_type`, `date_from`, `date_to` (`YYYY-MM-DD`), `offset` for pagination, and `group`. **These five are not exposed by the `search-documents` MCP tool** — they are REST-only. The `size` cap is 50 on both surfaces.

`group` folds the same section from a filer's successive filings into its best-ranked (then newest) hit, with `also_in_filings` counting the others; it is ignored when `entity` is set, where a filer's filing history is the point, and a grouped page is drawn from the first 100 hits. REST defaults it to `false`; the MCP tool always groups.

## What Gets Indexed

A document is split into sections, each section is embedded and indexed independently, and search hits land on the section — not the whole document. This is why an Operator can retrieve "the Useful Lives paragraph" rather than the entire policy.

**Source types** carried on every indexed section:

- `uploaded_doc` — documents you upload into your own graph
- `narrative_section` — SEC filing narrative text (MD&A, risk factors)
- `ixbrl_disclosure` — inline-XBRL disclosure text, cross-referenced to the graph
- `xbrl_textblock` — XBRL text-block facts
- `connection_doc` — documents originating from a connected data source

Those five are the whole set. `memory` is **not** among them — semantic memories are never written to OpenSearch and never come back from `POST /search`. The value `source_type="memory"` does exist, but only as a field synthesized onto the hits that memory `recall` returns from its own vector store; see the next section.

**Markdown sectioning** (for uploaded documents):

- Content is split on markdown headings (`#` through `######`).
- YAML frontmatter (`title`, `tags`, `folder`) fills any field you didn't set on the request; an explicit request value always wins over frontmatter.
- Sections under 20 words are merged into a neighbor; the maximum is 50,000 characters per section, and 500,000 characters per document.

**The iXBRL bridge:** an SEC disclosure section carries the XBRL elements tagged in it (`xbrl_elements` on the section read, and on REST search hits). This lets an Operator move from the narrative ("goodwill impairment discussion") to the structured fact (`us-gaap:Goodwill`) by handing the element to `resolve-element`, then querying values with `read-graph-cypher`. For how the SEC corpus is built, see [SEC XBRL Pipeline](SEC-XBRL-Pipeline).

## How Search Works

**Hybrid BM25 + KNN.** BM25 is classic keyword matching over an inverted index — fast, exact, and the default. KNN is vector similarity over local embeddings, which matches on *meaning* rather than exact words. Setting `semantic=true` runs both and combines them through a normalization pipeline that weights the semantic signal slightly higher than the keyword signal, so conceptually-close sections surface even when they don't share the query's exact wording.

**Local embeddings, no credits.** Embeddings are produced in-image by a small open model (`BAAI/bge-small-en-v1.5`, 384 dimensions). There are no external API calls and **no AI credits are consumed** for indexing or semantic search.

**Tenant isolation.** Every search and document operation is filtered by `graph_id`. Your uploaded documents are scoped to their graph and never surface in another graph's results. The MCP search tools resolve a shared repository's subgraph to the parent repository's index — subgraphs are a storage split, not a search boundary.

## Semantic Memory: The Second Retrieval Plane

Alongside the document index there is a second retrieval plane: a **per-graph semantic memory store**, backed by LanceDB on the graph's writer instance rather than by OpenSearch. It is where an Operator files short free-text recollections — "the AR aging shows a stale invoice from Acme to follow up on" — and retrieves them later by meaning.

The two planes are deliberately kept apart. Documents are authored artifacts with titles, folders, sections, and a PostgreSQL record; memories are unstructured notes with no sectioning and no document row. Nothing an Operator remembers is written to OpenSearch, and nothing you index as a document shows up in recall.

**MCP tools:** `remember`, `recall`, `update-memory`, `forget`.

**REST surface:**

| Method | Path | Purpose |
| --- | --- | --- |
| POST | `/v1/graphs/{graph_id}/memory/recall` | Ranked semantic recall — body `{query, k, memory_type, source}` |
| GET | `/v1/graphs/{graph_id}/memory` | List memories (filter by `memory_type`, `source`; `limit`/`offset`) |
| GET | `/v1/graphs/{graph_id}/memory/{memory_id}` | Fetch one memory by its `mem_…` id |
| POST | `/v1/graphs/{graph_id}/operations/remember` | Store a memory |
| POST | `/v1/graphs/{graph_id}/operations/update-memory` | Partially update one; re-embeds when `text` changes |
| POST | `/v1/graphs/{graph_id}/operations/forget` | Delete one by id |

```bash
curl -X POST "https://api.robosystems.ai/v1/graphs/$GRAPH_ID/memory/recall" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "what did we decide about the Acme invoice", "k": 5}'
```

`recall` returns the **same `SearchResponse` shape as document search** — the same `SearchHit` objects, so an Operator can rank memory hits and document hits with one set of field names. What it does *not* mean is that memories live in the search index: `recall` reads LanceDB, `POST /search` reads OpenSearch, and the two never see each other's records. The `source_type: "memory"` you see on a recall hit is stamped on at read time, not stored in an index.

**Different flags gate it.** A deployment controls memory with `SEMANTIC_MEMORY_ENABLED`, with `MCP_SEMANTIC_MEMORY_ENABLED` as an additional sub-gate on the four MCP tools — **not** with `SEMANTIC_SEARCH_ENABLED`. A deployment can run document search with memory off, or memory with document search off. The memory reads, `recall` included, live under `/memory` and mount with `SEMANTIC_MEMORY_ENABLED`.

**Scope rules.** Memory is blocked on shared repositories. `remember` and `update-memory` are additionally blocked on subgraphs — a subgraph exists to be cheap to create and throw away, so memories belong on the parent graph — while `forget` stays open so existing memories remain removable.

## Response Shapes

A `search-documents` hit (`SearchHit`) includes the fields you need to rank and drill in:

- `document_id` — pass this to `get-document-section`
- `score`, `source_type`
- `section_label`, `section_id`, and, for a section stored in parts, `parent_document_id`, `part` / `part_count` and `next_document_id`
- `snippet` — an excerpt around the match (falling back to the section label), not the passage; read the section before quoting a figure from it
- `content_length`, and `content_url` for the CDN-hosted clean text when available
- `document_title`, `tags`, `folder` — your own documents
- SEC-specific: `entity_ticker`, `entity_name`, `form_type`, `fiscal_year`, `filing_date`, `accession_number`, `element_qname`, `xbrl_elements`
- `also_in_filings` — on a grouped search, how many other filings of the same section were folded into this hit

Around the hits, the response carries `query`, `graph_id` and `total` — the matching documents before any grouping, as OpenSearch counts them (it stops counting at 10,000), not the number of distinct results a grouped search can page through.

The MCP tool trims what the model reads: it drops null fields and `xbrl_elements`, and a filing field that every hit shares (`entity_ticker`, `entity_name`, `form_type`, `filing_date`, `fiscal_year`, `accession_number`) moves once to the result root; when the hits span filings it stays on each hit.

A `get-document-section` response (`DocumentSection`) adds the `content` string plus context such as `graph_id`, `entity_cik`, `fiscal_period`, `accession_number`, `xbrl_elements` and `content_url`. Over MCP, `content` is one window, with `offset`, and `next_offset` while more of the part follows.

The authoritative, machine-readable definition of every field lives in the OpenAPI spec — see [API reference](https://robosystems.ai/docs/api).

## Troubleshooting

### Search Returns 503 or "Text Search Is Not Available"

The search service is disabled or OpenSearch is unreachable.

```bash
# Confirm the search routes are mounted (they are absent when the flag is off)
curl -s https://api.robosystems.ai/openapi.json | jq '.paths | keys | map(select(test("/search")))'
```

On a self-hosted deployment, see [Self-hosted deployments](#self-hosted-deployments) for the flag and OpenSearch checks.

### Newly Uploaded Document Doesn't Appear in Search

The index refresh interval is 30 seconds (60 seconds during bulk loads), so a just-created document may not be searchable immediately.

**Solution:** Wait ~30 seconds and search again. To confirm the document was indexed, check that the `index-document` response reported a non-zero `sections_indexed`.

### 403 When Uploading to the `sec` Repository

Document write operations are blocked on shared-repository graphs.

**Solution:** Upload documents into your own graph, not into `sec`. You can still `search-documents` and `get-document-section` against `sec` — it is read-only for documents.

### 403 When Searching a Shared Repository

Searching `sec` runs a subscription/access check. Without access to the repository, the search returns 403.

**Solution:** Subscribe to the SEC repository from **Repositories** in the app, then retry.

### Snippet Is Just the Section Heading

Document content is excluded from the search response body, so when there are no highlight fragments the `snippet` falls back to the section label.

**Solution:** This is expected. Call `get-document-section` with the hit's `document_id` to read the full section text.

## Self-hosted deployments

**Flags and OpenSearch.** `SEMANTIC_SEARCH_ENABLED` gates document search, the documents router and all seven search/document MCP tools (`search-documents`, `get-document-section`, `create-document`, `update-document`, `delete-document`, `get-document`, `list-documents`) at once. Memory, `POST /memory/recall` included, mounts with `SEMANTIC_MEMORY_ENABLED` instead. OpenSearch must be reachable at `OPENSEARCH_URL` (default `http://localhost:9200`).

If search returns 503, confirm the routes are mounted with `curl -s http://localhost:8000/openapi.json | jq '.paths | keys | map(select(test("/search")))'` and that OpenSearch answers with `curl http://localhost:9200`. `grep SEMANTIC_SEARCH_ENABLED .env.local` is not a useful check — the flag defaults to `true` and ships commented out, so no output means the default applies, not that the feature is off. Set `SEMANTIC_SEARCH_ENABLED=true`, ensure OpenSearch is reachable, then `just restart`.

**Access to `sec`.** Grant a local user repository access with `just demo-user --repositories sec`.

**just commands.** Two recipes give you search from the command line:

```bash
# Search a graph; semantic ranking is ON by default in this recipe
just search <graph_id> "tariff exposure supply chain risk"

# Disable semantic ranking (BM25 only)
just search <graph_id> "depreciation policy" --no-semantic

# Scope an SEC narrative search with filters
just search sec "tariff exposure supply chain risk" --entity NVDA --form-type 10-K --fiscal-year 2025

# Count indexed documents and break down by source type
just search-count sec
```

The `just search` recipe forces semantic mode by default, while the REST and MCP surfaces default to BM25-only (`semantic=false`). Pass `--no-semantic` to match the REST/MCP default.

`just demo-user` then `just demo-roboledger` create a demo user and a writable RoboLedger graph to upload into, and `just demo-custom-graph` exercises both retrieval planes: step 6 indexes markdown documents, and step 7 creates a `knowledge` subgraph that pairs structured graph traversal on the subgraph with semantic recall on the parent. See [Local Development](Local-Development).

## Related Documentation

**Wiki Guides:**

- **[AI Operators and MCP](AI-Operators-and-MCP)** - How Operators use MCP tools to ground answers on your data
- **[Document Management](Document-Management)** - The full document upload, list, update, and delete lifecycle
- **[File Uploads](File-Uploads)** - Bulk tabular node and relationship data into a custom graph (a separate surface — it does not index documents)
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - The SEC narrative and iXBRL corpus, and the graph bridge

**Codebase Documentation:**

- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration, including the search service
- **[MCP Middleware](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/mcp/README.md)** - The MCP tool surface, including the search and document tools

**API Reference:**

- **[API reference](https://robosystems.ai/docs/api)** - API reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
