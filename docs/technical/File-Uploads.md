# File Uploads

This guide shows you how to load bulk node and relationship data into a **custom (generic) graph** using the files router at `/v1/graphs/{graph_id}/files`. You upload parquet, CSV, or JSON files, stage them, and ingest them into your graph — all without your file bytes ever passing through the API server.

**Quick Start:** Run `just demo-custom-graph` to exercise the entire upload → stage → ingest → query flow end-to-end against a real schema.

## Table of Contents

- [Overview](#overview)
- [When to Use This](#when-to-use-this)
- [The Three-Layer Pipeline](#the-three-layer-pipeline)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [The Upload Flow](#the-upload-flow)
  - [Step 1: Request a Presigned Upload URL](#step-1-request-a-presigned-upload-url)
  - [Step 2: Upload the Bytes to S3](#step-2-upload-the-bytes-to-s3)
  - [Step 3: Mark Uploaded and Ingest](#step-3-mark-uploaded-and-ingest)
  - [Step 4: Verify the Data](#step-4-verify-the-data)
- [Auto-Ingest vs Batch](#auto-ingest-vs-batch)
- [Supported Formats and Limits](#supported-formats-and-limits)
- [Inspecting and Managing Files](#inspecting-and-managing-files)
- [Monitoring Async Work](#monitoring-async-work)
- [Worked Example (End to End)](#worked-example-end-to-end)
- [Gotchas and Pitfalls](#gotchas-and-pitfalls)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

The files router lets you load bulk data — nodes and relationships — into a custom graph by uploading flat files. Files are tracked as first-class, graph-scoped resources keyed by a `file_id`, and each one flows through a fixed pipeline:

1. **Upload** the file directly to S3 using a presigned URL.
2. **Stage** it into a DuckDB table (queryable immediately).
3. **Ingest** it into the LadybugDB graph, either per-file or in a single batch.

This is a different surface from document management. The files router moves **structured tabular data into the graph**; it does not store or index documents. For entity-graph document management (PDFs, policies, full-text and semantic search), see [Document Management](Document-Management) instead.

## When to Use This

Use the files router when you are loading data into a **custom / generic graph** — the kind you create with a `schema.json` template (see [Custom Graph Schema](Custom-Graph-Schema)).

Two graph types are explicitly **not** supported, and the router rejects them:

| Graph type | Result | Why |
| --- | --- | --- |
| **Entity graphs** | HTTP 400 | Entity graph data is managed through the extensions pipeline (connectors and OLTP APIs). Use `POST /materialize` with `source='extensions'` instead. |
| **Shared repositories** (and their subgraphs) | HTTP 403 | Shared repositories such as the SEC repo are read-only. File uploads and data ingestion are not allowed. |

If you are loading custom domain data — customers, products, orders, or any node/relationship model you defined yourself — this is the right surface.

## The Three-Layer Pipeline

Every file lives across three layers, each with its own independent status. The same `file_id` ties them together.

| Layer | Role | Mutability |
| --- | --- | --- |
| **S3** | Source of truth — the raw uploaded file | Immutable |
| **DuckDB** | Staging tables — queryable immediately after upload | Mutable |
| **LadybugDB** | The graph — a materialized view of staged data | Immutable view |

**Staging is the source of truth.** Because the graph is a materialized view of the DuckDB staging tables, it can always be rebuilt from staged data. That makes `materialize` (rebuild) a safe, repeatable operation.

**Tables auto-create on first upload.** You do not pre-declare tables. When you request an upload URL with a `table_name`, the table is created if it does not exist, and the table type (node vs relationship) is inferred from the name. You can review staging tables at any time with `GET /v1/graphs/{graph_id}/tables`.

## Prerequisites

Before starting, ensure you have:

- Docker running locally and services started with `just start`
- A user account and API key — run `just demo-user`, which writes credentials to `.local/config.json`
- A custom graph and its `graph_id` — `just demo-custom-graph` creates one for you (see [Custom Graph Schema](Custom-Graph-Schema))

All `curl` examples below read the API key from `.local/config.json` and target `http://localhost:8000`. Substitute your own `graph_id` (the examples use `kg1a2b3c4d`).

## Quick Start

The fastest way to see the full upload → stage → ingest → query flow is the bundled demo, which generates parquet node and relationship files, uploads them, stages them, materializes the graph, and runs a few verification queries:

```bash
just demo-custom-graph
```

To drive the flow yourself against an existing graph, the minimal path is three calls — request a presigned URL, `PUT` the bytes to S3, then `PATCH` the file as uploaded with `ingest_to_graph: true`:

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=kg1a2b3c4d

# 1. Request a presigned upload URL (auto-creates the Person table)
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/files" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"file_name": "Person.parquet", "content_type": "application/x-parquet", "table_name": "Person"}'
```

The sections below walk each step in detail. For the complete end-to-end script, see [Worked Example (End to End)](#worked-example-end-to-end).

## The Upload Flow

The flow is four steps: request a presigned URL, PUT the bytes to S3, mark the file as uploaded, then verify. The API never receives your file bytes — it hands back a time-limited S3 URL and you upload directly to S3.

### Step 1: Request a Presigned Upload URL

`POST /v1/graphs/{graph_id}/files` returns a presigned S3 URL and a `file_id`. It auto-creates the staging table named in `table_name` if it does not already exist.

```bash
curl -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d/files" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "Person.parquet",
    "content_type": "application/x-parquet",
    "table_name": "Person"
  }'
```

The response gives you everything you need for the next two steps:

```json
{
  "upload_url": "https://...",
  "expires_in": 3600,
  "file_id": "<uuid>",
  "s3_key": "user-staging/<user_id>/kg1a2b3c4d/Person/<file_id>/Person.parquet"
}
```

**Note:** `table_name` is required for this endpoint. The presigned URL expires in 3600 seconds (1 hour) — re-request it if it lapses.

### Step 2: Upload the Bytes to S3

`PUT` the file directly to the `upload_url` from step 1. This call goes to S3, not to the RoboSystems API.

```bash
curl -X PUT "<upload_url from step 1>" \
  -H "Content-Type: application/x-parquet" \
  --data-binary @Person.parquet
```

**Important:** The `Content-Type` on this `PUT` must match the `content_type` you sent in step 1. The presigned URL is signed with that content type, and S3 rejects the request if it does not match.

### Step 3: Mark Uploaded and Ingest

`PATCH /v1/graphs/{graph_id}/files/{file_id}` with `status=uploaded` tells the platform the bytes are in S3. The router validates the object exists (via a `head_object`), counts rows, and triggers DuckDB staging. Set `ingest_to_graph: true` to also push the staged data into the graph in the same call.

```bash
curl -X PATCH "http://localhost:8000/v1/graphs/kg1a2b3c4d/files/<file_id>" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{ "status": "uploaded", "ingest_to_graph": true }'
```

The response confirms the staged file and, for async work, hands you an `operation_id` and a `monitor_url`:

```json
{
  "status": "success",
  "file_id": "...",
  "upload_status": "uploaded",
  "file_size_bytes": 12345,
  "row_count": 50,
  "operation_id": "<uuid>",
  "monitor_url": "/v1/operations/<uuid>/stream"
}
```

### Step 4: Verify the Data

Once the data is in the graph, query it. Reads run through `POST /v1/graphs/{graph_id}/query`, or through any MCP-compatible AI tool using the `read-graph-cypher` tool (run `get-graph-schema` first). See [Search and AI Retrieval](Search-and-AI-Retrieval) for the MCP path.

```bash
curl -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d/query" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{ "query": "MATCH (p:Person) RETURN count(p) AS people" }'
```

You can also verify directly from the host with the `just` helpers:

```bash
just graph-query kg1a2b3c4d "MATCH (p:Person) RETURN count(p) AS people"
just graph-info kg1a2b3c4d
```

**Note:** Main graphs are read-only to Cypher — use `POST /query` to verify, not to write. You cannot create nodes via Cypher on a main graph (subgraphs are writable).

## Auto-Ingest vs Batch

There are two ingestion modes. They differ only in *when* staged data reaches the graph.

| Mode | How | When to use |
| --- | --- | --- |
| **Real-time (auto-ingest)** | `PATCH` with `ingest_to_graph: true` — each file is ingested incrementally as you mark it uploaded | A single file, or incremental updates |
| **Batch** | `PATCH` with `ingest_to_graph: false` (the default) for many files, then one `materialize` operation | Bulk loads of many files — cheaper and faster |

For batch loads, stage all your files first, then rebuild the graph from staging in a single operation:

```bash
curl -X POST "http://localhost:8000/v1/graphs/kg1a2b3c4d/operations/materialize" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Idempotency-Key: $(date +%s)" \
  -H "Content-Type: application/json" \
  -d '{}'
```

The `materialize` operation rebuilds the LadybugDB graph from the staging tables, returns an `OperationEnvelope` (HTTP 202), and supports an `Idempotency-Key` header and a `dry_run` flag. Because staging is the source of truth, this is safe to repeat.

## Supported Formats and Limits

Three file formats are supported. The file extension must match the content type.

| Content type | Format | Extension |
| --- | --- | --- |
| `application/x-parquet` | parquet | `.parquet` |
| `text/csv` | csv | `.csv` |
| `application/json` | json | `.json` |

Key constraints:

- **Maximum file size:** 100 MB per file.
- **Presigned URL expiry:** 3600 seconds (1 hour).
- **Size-based routing is automatic and invisible.** Files under 50 MB take a synchronous "direct staging" fast path; files 50 MB and larger go through an async Dagster job. Either way you may receive an `operation_id` for SSE monitoring.
- **Filename safety:** `file_name` is 1–255 characters and cannot contain `..`, `/`, or `\`.
- **Table name pattern:** `table_name` must match `^[A-Za-z_][A-Za-z0-9_]*$`.

## Inspecting and Managing Files

Files are managed independently of tables. You can list them, inspect per-layer status, and remove them.

**List all files in a graph** (optional `table_name` and `status` query filters):

```bash
curl "http://localhost:8000/v1/graphs/kg1a2b3c4d/files" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

**Get one file with its per-layer pipeline status.** The response includes a `layers` object with `s3`, `duckdb`, and `graph` entries — each carries its own `status`, `timestamp`, `row_count`, and `size_bytes`, so you can see exactly how far a file has progressed:

```bash
curl "http://localhost:8000/v1/graphs/kg1a2b3c4d/files/<file_id>" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

**Disable or archive a file** via `PATCH`. A file moves through `pending` → `uploaded` → `disabled` / `archived`. Once uploaded, it cannot be reset to `pending`.

```bash
curl -X PATCH "http://localhost:8000/v1/graphs/kg1a2b3c4d/files/<file_id>" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{ "status": "archived" }'
```

**Delete a file.** A plain delete removes the S3 object and the database record. Add `cascade=true` to also remove the file's rows from DuckDB and mark the graph stale (a rebuild is then recommended).

```bash
# Remove S3 object + DB record only (DuckDB rows excluded from queries, not physically removed)
curl -X DELETE "http://localhost:8000/v1/graphs/kg1a2b3c4d/files/<file_id>" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"

# Full cascade: also removes DuckDB rows and marks the graph stale
curl -X DELETE "http://localhost:8000/v1/graphs/kg1a2b3c4d/files/<file_id>?cascade=true" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

The delete response reports what happened: `cascade_deleted`, `tables_affected`, and `graph_marked_stale`.

## Monitoring Async Work

When staging or ingestion runs asynchronously (large files, or `ingest_to_graph: true`), the response carries an `operation_id` and a `monitor_url`. Stream progress over Server-Sent Events:

```bash
curl -N "http://localhost:8000/v1/operations/<operation_id>/stream" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

This is the same operation-monitoring stream used across the platform's write operations.

## Worked Example (End to End)

The example below loads a single `Person.parquet` node file and ingests it in one pass. For the complete multi-file flow against a real schema, `just demo-custom-graph` is the canonical runnable reference.

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=kg1a2b3c4d

# 1. Request a presigned upload URL (auto-creates the Person table)
RESPONSE=$(curl -s -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/files" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"file_name": "Person.parquet", "content_type": "application/x-parquet", "table_name": "Person"}')

UPLOAD_URL=$(echo "$RESPONSE" | jq -r .upload_url)
FILE_ID=$(echo "$RESPONSE" | jq -r .file_id)

# 2. Upload the bytes directly to S3 (Content-Type must match step 1)
curl -X PUT "$UPLOAD_URL" \
  -H "Content-Type: application/x-parquet" \
  --data-binary @Person.parquet

# 3. Mark uploaded and ingest into the graph
curl -X PATCH "http://localhost:8000/v1/graphs/$GRAPH_ID/files/$FILE_ID" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"status": "uploaded", "ingest_to_graph": true}'

# 4. Verify the data landed in the graph
curl -X POST "http://localhost:8000/v1/graphs/$GRAPH_ID/query" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (p:Person) RETURN count(p) AS people"}'
```

For the full request and response schemas (`FileUploadRequest`, `FileUploadResponse`, `FileStatusUpdate`, `GetFileInfoResponse`, `DeleteFileResponse`), see the live API reference at [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` locally).

## Gotchas and Pitfalls

- **Entity graphs are rejected (HTTP 400).** Their data is managed through the extensions pipeline; use `POST /materialize` with `source='extensions'` instead. This is the load-bearing scope boundary for this surface.
- **Shared repositories are rejected (HTTP 403)** on upload, status update, and delete — they are read-only.
- **`table_name` is required** in the `POST /files` body. Omitting it returns a 400.
- **Extension must match content type.** A `.csv` uploaded with `application/x-parquet` returns a 400.
- **Filename safety.** `..`, `/`, and `\` are rejected; the name is capped at 255 characters.
- **You must `PUT` to S3 before the `PATCH`.** `PATCH status=uploaded` runs a `head_object`; if the object is not in S3, it returns 404 ("File not found in S3"). An empty file (size 0 or less) returns 400 ("File is empty").
- **No reset to `pending`.** Once uploaded, only `uploaded`, `disabled`, and `archived` are valid statuses.
- **Storage tier limit.** The `PATCH` checks total staged bytes against your graph tier's storage limit; exceeding it returns HTTP 413.
- **Presigned URLs expire in 1 hour.** Re-request the URL if it lapses before you upload.
- **`Content-Type` on the S3 `PUT` must match** the `content_type` used to generate the URL, or S3 rejects the signature.
- **Delete without `cascade` leaves DuckDB rows.** A non-cascade delete removes the S3 object and DB record only; DuckDB excludes the file from queries but does not physically remove the data. Use `cascade=true` to remove DuckDB rows and mark the graph stale.
- **Main graphs are read-only to Cypher.** Verify ingested data with `POST /query` reads; you cannot write nodes via Cypher on a main graph (subgraphs are writable).

## Related Documentation

**Wiki Guides:**

- **[Custom Graph Schema](Custom-Graph-Schema)** - Design the schema and tables that file uploads populate; `just demo-custom-graph` runs this exact flow end-to-end
- **[Search and AI Retrieval](Search-and-AI-Retrieval)** - Query the loaded graph with MCP tools (`read-graph-cypher`, `get-graph-schema`) after ingest
- **[Document Management](Document-Management)** - For entity-graph documents (the contrasting surface; not the files router)

**Codebase Documentation:**

- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration, including OLTP→OLAP materialization
- **[Graph API](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graph_api/README.md)** - LadybugDB staging tables, query, and materialize endpoints
- **[API Documentation](https://api.robosystems.ai/docs)** - API reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
