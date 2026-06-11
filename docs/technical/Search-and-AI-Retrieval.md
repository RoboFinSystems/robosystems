# Search & AI Retrieval

This guide shows you how to give an AI Operator the institutional-knowledge layer over your entity graph: upload unstructured content (policies, procedures, notes — plus SEC filing narratives on shared repositories), index it with hybrid keyword + semantic search, and let an Operator ground its answers on the right document.

**Quick Start:** Run `just demo-roboledger` to create a writable graph, upload a policy document, then run `just search <graph_id> "depreciation policy"` to see it surface.

## Overview

Your entity graph holds the structured numbers — facts, accounts, periods. Search holds the *unstructured* context that explains them: depreciation policies, revenue-recognition procedures, accounting memos, and the narrative sections of SEC filings. With both in place, an AI Operator can reason about *why* and *how*, not just *what*.

The retrieval workflow has three stages:

1. **Upload** unstructured content into a graph (your own documents) or read from a shared repository (SEC filing narratives).
2. **Index** the content into OpenSearch as searchable sections, each carrying a local vector embedding for semantic matching.
3. **Query** the index through two MCP tools — `search-documents` returns ranked snippets, `get-document-section` returns the full section text — so an Operator finds and reads exactly the policy it needs.

There are two corpora available:

- **Your uploaded documents** — per-graph, private to that graph. Policies, procedures, internal notes.
- **SEC filing narratives** — on the shared `sec` repository: MD&A, risk factors, and iXBRL disclosure text, cross-referenced back to the structured XBRL graph.

Indexing and semantic search run a local embedding model in-image. They make **no external API calls and consume no AI credits**.

## Prerequisites

Before starting, ensure you have:

- Docker running locally with the full stack up (`just start`)
- `SEMANTIC_SEARCH_ENABLED=true` in your environment (this single flag gates the search routers, the document routers, and all of the search/document MCP tools)
- OpenSearch running and reachable at `OPENSEARCH_URL` (default `http://localhost:9200`)
- A demo user and a writable graph — `just demo-user` then `just demo-roboledger`

Your API key is saved to `.local/config.json` after running `just demo-user`. All `curl` examples below read it from there.

## Quick Start

The fastest path is to create a graph, upload one policy document, and search it.

```bash
# Create a demo user and a writable RoboLedger graph
just demo-user
just demo-roboledger

# Search documents in a graph (semantic ranking is on by default in this recipe)
just search <your_graph_id> "PP&E depreciation policy useful lives"

# Count indexed documents and see the breakdown by source type
just search-count <your_graph_id>
```

**Note:** A just-created document may take up to ~30 seconds to appear in search results — the index refresh interval is 30 seconds (60 seconds during bulk loads).

## The Two-Step Retrieval Pattern

Retrieval is deliberately split across two tools so an Operator reads only what it needs:

1. **`search-documents`** runs the query and returns a ranked list of *snippets* — small, scannable highlights with the matching `document_id`, score, and metadata. This is cheap and keeps token usage low.
2. **`get-document-section`** takes a `document_id` from a hit and returns the *full* section text. The Operator calls this only for the section it actually wants to read.

This keeps an Operator from pulling whole documents into context when a single paragraph answers the question. The snippet tells it *which* section is relevant; the section fetch gives it the words.

**Note:** Because document content is excluded from the search response body, a `snippet` falls back to the section label when there are no highlight fragments. Always call `get-document-section` to read the real text.

## Worked Example: Ground an Operator on the PP&E Depreciation Policy

This is the canonical thread: you upload a depreciation policy, then an Operator searches it, drills into the right section, and answers from the actual policy text.

### Step 1: Create the Policy Document

Upload a markdown policy into your graph. The platform splits it on headings, embeds each section, and indexes the sections independently.

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=<your graph id>

curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/documents" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Property, Plant & Equipment Depreciation Policy",
    "content": "# PP&E Depreciation Policy\n\n## Method\nThe Company depreciates property, plant and equipment on a straight-line basis over the estimated useful lives of the assets.\n\n## Useful Lives\n- Buildings: 30 years\n- Machinery & equipment: 7 years\n- Computer hardware: 3 years\n\n## Capitalization Threshold\nIndividual assets with a cost of $5,000 or more are capitalized; smaller purchases are expensed.",
    "folder": "policies",
    "tags": ["depreciation", "fixed-assets", "ppe"]
  }'
```

The response reports how many sections were indexed:

```json
{
  "id": "doc_abc123def456",
  "document_id": "udoc_doc_abc123def456",
  "sections_indexed": 4,
  "total_content_length": 512,
  "section_ids": ["udoc_doc_abc123def456_0", "udoc_doc_abc123def456_1", "..."]
}
```

The bare `id` (`doc_…`) is the PostgreSQL document id used by `get-document` and the REST `/documents/{id}` route. The `document_id` and `section_ids` (`udoc_doc_…`) are the OpenSearch ids; a per-section id (ending in `_N`) is what search hits return and what `get-document-section` takes.

**Note:** Sections under 20 words are merged into a neighboring section, so a bare one-line heading will not become its own searchable chunk.

### Step 2: Search the Documents

Search for the policy. By default search uses BM25 keyword matching; add `"semantic": true` to combine it with semantic vector ranking.

```bash
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/search" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "PP&E depreciation policy useful lives", "semantic": true, "size": 5}'
```

Each hit carries a `document_id`, a relevance `score`, a `section_label`, and a `snippet` — enough to pick the right section without reading the whole document.

### Step 3: Drill Into the Full Section

Take the `document_id` from the most relevant hit and fetch its complete text:

```bash
curl -X GET "http://localhost:8000/v1/graphs/$GRAPH_ID/search/udoc_doc_abc123def456_1" \
  -H "X-API-Key: $API_KEY"
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
     -> full section text: "Machinery & equipment: 7 years"
3. Answers, grounded in the actual policy: "7 years, straight-line."
```

The Operator never guesses — its answer is anchored to the section text it retrieved.

## Accessing Search

You have three ways to upload documents and run searches:

1. **[MCP Tools](#option-1-mcp-tools-for-ai-operators)** — for AI Operators and any MCP-compatible client
2. **[REST API](#option-2-rest-api)** — `curl` with `X-API-Key` for direct integration
3. **[just Commands](#option-3-just-commands)** — developer convenience from the command line

### Option 1: MCP Tools (For AI Operators)

AI Operators interact with the index through MCP tools. All of them require `SEMANTIC_SEARCH_ENABLED=true`; if search is disabled or OpenSearch is unreachable, the tools return an error and are listed as unavailable.

**Search tools:**

- `search-documents` — hybrid BM25 + KNN search. Inputs: `query` (required), plus optional `entity`, `form_type`, `section`, `element`, `fiscal_year`, `semantic` (default `false`), and `size` (default 10, max 50). Returns ranked hits with `document_id`s.
- `get-document-section` — input `document_id` (required). Returns the full section text and metadata.

**Document tools:**

- `create-document` — inputs `title`, `content` (required), plus optional `folder` and `tags`. Uploads and indexes a document.
- `get-document` — returns the full document from PostgreSQL (the source of truth).
- `list-documents` — browse documents by metadata; filter by `folder` or `source_type`.
- `update-document` — update an existing document.

**Note:** The document tools register only on your own graphs, not on shared repositories. The write tools (`create-document`, `update-document`) additionally require a writable graph. The search tools (`search-documents`, `get-document-section`) work against both your graphs and shared repositories like `sec`.

For SEC narrative hits, the `search-documents` results include the XBRL elements referenced in that disclosure (`xbrl_elements`). An Operator can hand one to `resolve-element` to look up the XBRL element, then run `read-graph-cypher` to pull the structured values — pivoting from narrative to numbers.

For Operator setup and the broader MCP tool surface, see [AI Operators and MCP](AI-Operators-and-MCP).

### Option 2: REST API

Both the upload and search surfaces are plain REST under `/v1/graphs/{graph_id}`. Use the `X-API-Key` header and read the key from `.local/config.json`.

Upload, search, and section-fetch are shown in the [worked example](#worked-example-ground-an-operator-on-the-ppe-depreciation-policy) above. The document lifecycle also supports list, get, update, and delete:

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=<your graph id>

# List documents in a graph (optionally filter by source type)
curl "http://localhost:8000/v1/graphs/$GRAPH_ID/documents" \
  -H "X-API-Key: $API_KEY"

# Get a single document with its metadata (use the bare PG id, e.g. doc_…)
curl "http://localhost:8000/v1/graphs/$GRAPH_ID/documents/doc_abc123def456" \
  -H "X-API-Key: $API_KEY"
```

The full request/response schema for every endpoint is published in the live OpenAPI spec — see [API Documentation](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` locally) rather than re-deriving it here.

**Important:** Document write operations (`create`, `update`, `delete`) require write access and are blocked on shared-repository graphs. You can `search` and `get-document-section` against the `sec` repository, but you cannot upload documents into it — upload into your own graph. See [Document Management](Document-Management) for the full document lifecycle.

### Option 3: just Commands

Two recipes give you search from the command line during development:

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

**Note:** The `just search` recipe forces semantic mode by default, while the REST and MCP surfaces default to BM25-only (`semantic=false`). Pass `--no-semantic` to the recipe to match the REST/MCP default.

## Search Filters

`search-documents` and the REST `/search` endpoint accept filters that narrow the result set. The two surfaces overlap but are not identical.

| Filter | Type | Meaning |
| --- | --- | --- |
| `query` | string (required) | The search text, 1–500 characters |
| `semantic` | bool (default `false`) | `false` = BM25 keyword only; `true` = hybrid BM25 + KNN semantic ranking |
| `entity` | string | Restrict to a company (e.g. ticker `NVDA`) |
| `form_type` | string | SEC form type (e.g. `10-K`, `10-Q`) |
| `section` | string | Filing section (e.g. `item_1a` for risk factors) |
| `element` | string | An XBRL element associated with the section |
| `fiscal_year` | int | Restrict to a fiscal year |
| `size` | int (1–50, default 10) | Number of hits to return |

The REST endpoint additionally accepts `source_type`, `date_from`, `date_to` (`YYYY-MM-DD`), and `offset` for pagination. **These four are not exposed by the `search-documents` MCP tool** — they are REST-only. The `size` cap is 50 on both surfaces.

## What Gets Indexed

A document is split into sections, each section is embedded and indexed independently, and search hits land on the section — not the whole document. This is why an Operator can retrieve "the Useful Lives paragraph" rather than the entire policy.

**Source types** carried on every indexed section:

- `uploaded_doc` — documents you upload into your own graph
- `narrative_section` — SEC filing narrative text (MD&A, risk factors)
- `ixbrl_disclosure` — inline-XBRL disclosure text, cross-referenced to the graph
- `xbrl_textblock` — XBRL text-block facts
- `connection_doc` — documents originating from a connected data source
- `memory` — Operator memory entries

**Markdown sectioning** (for uploaded documents):

- Content is split on markdown headings (`#` through `######`).
- YAML frontmatter (`title`, `tags`, `folder`) fills any field you didn't set on the request; an explicit request value always wins over frontmatter.
- Sections under 20 words are merged into a neighbor; the maximum is 50,000 characters per section, and 500,000 characters per document.

**The iXBRL bridge:** SEC disclosure hits include the XBRL elements referenced in that disclosure. This lets an Operator move from the narrative ("goodwill impairment discussion") to the structured fact (`us-gaap:Goodwill`) by handing the element to `resolve-element`, then querying values with `read-graph-cypher`. For how the SEC corpus is built, see [SEC XBRL Pipeline](SEC-XBRL-Pipeline).

## How Search Works

**Hybrid BM25 + KNN.** BM25 is classic keyword matching over an inverted index — fast, exact, and the default. KNN is vector similarity over local embeddings, which matches on *meaning* rather than exact words. Setting `semantic=true` runs both and combines them through a normalization pipeline that weights the semantic signal slightly higher than the keyword signal, so conceptually-close sections surface even when they don't share the query's exact wording.

**Local embeddings, no credits.** Embeddings are produced in-image by a small open model (`BAAI/bge-small-en-v1.5`, 384 dimensions). There are no external API calls and **no AI credits are consumed** for indexing or semantic search.

**Tenant isolation.** Every search and document operation is filtered by `graph_id`. Your uploaded documents are scoped to their graph and never surface in another graph's results. Searches against the `sec` repository resolve subgraph IDs (such as `sec_historical`) back to the parent `sec` index — subgraphs are a storage split, not a search boundary.

## Response Shapes

A `search-documents` hit (`SearchHit`) includes the fields you need to rank and drill in:

- `document_id` — pass this to `get-document-section`
- `score` — relevance score
- `section_label`, `section_id`, `parent_document_id`
- `snippet` — highlighted match text (falls back to the section label)
- `source_type`, `document_title`, `tags`, `folder`
- SEC-specific: `entity_ticker`, `entity_name`, `form_type`, `fiscal_year`, `filing_date`, `element_qname`, `xbrl_elements`

A `get-document-section` response (`DocumentSection`) adds the full `content` string plus context such as `graph_id`, `entity_cik`, `fiscal_period`, and `accession_number`. A `content_url` is included when available.

The authoritative, machine-readable definition of every field lives in the OpenAPI spec — see [API Documentation](https://api.robosystems.ai/docs).

## Troubleshooting

### Search Returns 503 or "Text Search Is Not Available"

The search service is disabled or OpenSearch is unreachable.

```bash
# Confirm the feature flag is enabled
grep SEMANTIC_SEARCH_ENABLED .env.local

# Confirm OpenSearch is reachable at OPENSEARCH_URL
curl http://localhost:9200
```

**Solution:** Set `SEMANTIC_SEARCH_ENABLED=true`, ensure OpenSearch is running and reachable at `OPENSEARCH_URL`, then `just restart`. This one flag controls the search routers, the document routers, and all six search/document MCP tools (`search-documents`, `get-document-section`, `create-document`, `update-document`, `get-document`, `list-documents`) at once.

### Newly Uploaded Document Doesn't Appear in Search

The index refresh interval is 30 seconds (60 seconds during bulk loads), so a just-created document may not be searchable immediately.

**Solution:** Wait ~30 seconds and search again. To confirm the document was indexed, run `just search-count <graph_id>` and check the count and source-type breakdown.

### 403 When Uploading to the `sec` Repository

Document write operations are blocked on shared-repository graphs.

**Solution:** Upload documents into your own graph, not into `sec`. You can still `search-documents` and `get-document-section` against `sec` — it is read-only for documents.

### 403 When Searching a Shared Repository

Searching `sec` runs a subscription/access check. Without access to the repository, the search returns 403.

**Solution:** Grant repository access to your user (for example `just demo-user --repositories sec`), then retry.

### Snippet Is Just the Section Heading

Document content is excluded from the search response body, so when there are no highlight fragments the `snippet` falls back to the section label.

**Solution:** This is expected. Call `get-document-section` with the hit's `document_id` to read the full section text.

## Related Documentation

**Wiki Guides:**

- **[AI Operators and MCP](AI-Operators-and-MCP)** - How Operators use MCP tools to ground answers on your data
- **[Document Management](Document-Management)** - The full document upload, list, update, and delete lifecycle
- **[File Uploads](File-Uploads)** - Getting unstructured content into the platform before indexing
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - The SEC narrative and iXBRL corpus, and the graph bridge

**Codebase Documentation:**

- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration, including the search service
- **[MCP Middleware](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/mcp/README.md)** - The MCP tool surface, including the search and document tools

**API Reference:**

- **[API Documentation](https://api.robosystems.ai/docs)** - API reference with machine-readable OpenAPI spec
- **RoboSystems API (local)**: http://localhost:8000/docs

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
