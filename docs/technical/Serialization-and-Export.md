# Serialization & Export

This guide shows you how a published RoboLedger report is projected into portable file formats — JSON-LD, the dataset-form holon, XBRL 2.1 and the Tavi compiled model — and how to download those artifacts from the platform.

**Quick Start:** Publish a report with `create-report`, then query the `reportDownloadUrl` GraphQL field to get a presigned link to its bundle.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Overview

A published report in RoboLedger isn't a single rendered document — it's a structured object that can be projected into different portable file formats without re-querying the database. That object is the **StatementBundle**, an in-memory envelope assembled from the report's facts, periods, framework slice, and per-statement [Information Blocks](Information-Blocks). Two encoder families walk that one envelope:

- `serialize_to_rdf` produces **JSON-LD** in two flavors — the flat canonical artifact (`jsonld`), one file identified by global URIs that any JSON tool can read, and the dataset-form **holon** (`holon-jsonld`), which carries the same content as named graphs.
- `serialize_to_xbrl` produces an **XBRL 2.1** package — the filing-grade, standards-blessed format for interop with regulators and downstream tooling. Beside it, `serialize_to_tavi` produces the **Tavi** compiled model (`tavi`) through xbrlkit's writer, the same form the SEC pipeline publishes for each filing.

End to end, this page covers:

1. What a `StatementBundle` carries and why both encoders share it
2. How a bundle is produced and stamped at publish time
3. How generation-stamped bundles are stored in S3
4. How to download a bundle in each flavor
5. What each artifact contains
6. How someone without an account reads an export
7. How SHACL validation at publish and round-trip validation prove conformance
8. How to contribute the Block content that gets serialized

## Prerequisites

Before starting, ensure you have:

- A RoboSystems account and an API key — see [Quick Start](Quick-Start)
- The RoboLedger extension enabled — it is on robosystems.ai; a self-hosted deployment sets `ROBOLEDGER_ENABLED=true`
- A graph with ledger data and at least one published report

## The StatementBundle Envelope

Everything downstream walks a single in-memory object: the `StatementBundle`. Both encoders take a bundle and return bytes; **neither encoder touches the database**. This is the core design property — serialization is a pure projection of an already-assembled envelope, so adding a new output format means adding an encoder "flavor," not rewriting the pipeline.

A bundle is built by `build_report_bundle(session, graph_id, report_id)` and carries:

| Field | What it holds |
| --- | --- |
| `entity` | The reporting entity: `id, name, legal_name, ein, country` |
| `periods` | Reporting periods: `start, end, label, period_type` (`duration` or `instant`) |
| `reporting_style` | The entity's resolved `reporting_style_id` — a **UUID**, e.g. `025f5d48-12ce-5d65-b9eb-4f137a10ef06` |
| `framework_pins` | Each `{framework, version}` — three of them, since `rs-gaap@v1` depends on both `fac@v1` and `cm@v1` |
| `schema_concepts` | Concept declarations: `qname, name, label, balance_type, period_type, is_abstract, is_monetary, element_type, source` |
| `linkbases` | `presentation_links`, `calculation_links`, `definition_links` — each an ELR of `BundleArc[]` (`arc_type, arcrole, from_qname, to_qname, order_value, weight`) |
| `period_nodes` | `id, period_start, period_end, period_type` (`period_start` is `None` for instants) |
| `units` | `id, measure`, e.g. `iso4217:USD` |
| `facts` | `id, element_id, element_qname, value, period_ref, unit_ref, entity_ref, decimals, fact_set_id, structure_id` |
| `ib_envelopes` | One `InformationBlockEnvelope` per statement Network |
| `structure_display_order` | Per-structure sort key, published as `rs:structureOrder` |
| `mode` | `report` (a published, immutable report) or `live` (an ad-hoc snapshot) |
| `report_meta` | `report_id, generation_count, filing_status, filed_at, supersedes_id, source_graph_id, source_report_id, shared_at` |
| `live_meta` | Present instead of `report_meta` on a `live` bundle |

**`reporting_style` is an id, not a code.** The bundle carries `entities.reporting_style_id` verbatim and the JSON-LD emits it unchanged as `rs:reportingStyle`. The familiar four-segment code (`BSC-CORP-IS02-CF1`) is the Style Structure's own `reportingStyleCode` metadata — resolve the UUID against the Style to get it.

**Two modes, one shape.** `mode='report'` is the published, immutable path described throughout this page. `mode='live'` bundles are an ad-hoc snapshot: they are response-body-only and **cannot be imported as a Report**. The discriminator is a first-class JSON-LD type, not a flag — a report serializes as `rs:Report` and a live snapshot as `rs:LiveSnapshot`.

**Why one envelope.** The income statement, balance sheet, and cash flow statement are each an [Information Block](Information-Blocks). The bundle is the instance layer that exports those Blocks: it pairs each statement's structural skeleton (the linkbases and concept declarations) with the actual facts. JSON-LD and XBRL are two renderings of the same underlying content — keeping the assembly in one place guarantees the two formats can never drift apart.

## How a Bundle Gets Made (Publish-Time Stamping)

Bundles are produced as a side effect of publishing a report, not on demand. When you run the `create-report` or `regenerate-report` operation, the JSON-LD bundle is built and uploaded to S3 **inside the publish transaction**.

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector

curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-report" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "FY2025 Annual Report",
    "taxonomy_id": "rs-gaap",
    "mapping_id": "map_01K8...",
    "period_start": "2025-01-01",
    "period_end": "2025-12-31",
    "period_type": "annual",
    "comparative": true
  }'
```

The operation returns an `OperationEnvelope` wrapping a `ReportResponse`; the `id` field on that response is the `report_id` you use to download.

**Fail-loud.** If S3 is unavailable when the bundle is built, the publish fails. There is no such thing as a published report without a stored bundle — the artifact and the report row are committed together.

**Regeneration re-stamps.** Running `regenerate-report` re-runs the pipeline against current ledger state and writes a new generation. The report's facts come from the same `fact_grid` / report pipeline described in [Reporting & Rendering](Reporting-and-Rendering); serialization picks up wherever that pipeline leaves off.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/regenerate-report" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"report_id": "'"$REPORT_ID"'"}'
```

## Generation-Stamped S3 Storage

Every published generation of a report is stored as its own object. Bundle keys are stamped with the generation count:

```text
report-bundles/{graph_id}/{report_id}/g{generation_count}.jsonld
```

So the first publish writes `g1.jsonld`, a regenerate writes `g2.jsonld`, and so on. Older generations stay in S3 — they are not overwritten — and the `Report.bundle_url` column always points at the current generation's full `s3://` URI. This gives you an immutable history of every projection the platform ever published for a report.

The on-demand flavors cache beside it under the same generation stamp — `g{n}.zip` for the XBRL package, `g{n}.holon.jsonld` for the holon and `g{n}.tavi.json` for the Tavi model — written the first time each is requested and reused thereafter.

## Downloading a Bundle

The export surface is a **GraphQL read field**, not a REST resource. A download is a read of stored state, so it lives on the read surface alongside every other report read:

```text
POST /extensions/{graph_id}/graphql
{ reportDownloadUrl(reportId:, format:, expiresIn:) { … } }
```

| Argument | Default | Notes |
| --- | --- | --- |
| `reportId` | — | Required; the `id` from the `create-report` response |
| `format` | `JSONLD` | `JSONLD`, `HOLON_JSONLD`, `XBRL_2_1`, or `TAVI` |
| `expiresIn` | `300` | Presigned-URL TTL in seconds (min `60`, max `3600`); out-of-range values raise `INVALID_EXPIRES_IN` |

**Every flavor resolves to a presigned S3 URL** in the same `ReportBundleDownload` envelope — the API never streams bytes, because neither a GraphQL JSON response nor an `OperationEnvelope` can carry a raw binary zip. The resolver returns a URL; the client follows it to S3. What differs between flavors is only *when* the object was built:

| Flavor | Content type | Built |
| --- | --- | --- |
| `JSONLD` | `application/ld+json` | Stamped to S3 inside the publish transaction |
| `HOLON_JSONLD` | `application/ld+json` | Materialized and cached on first request |
| `XBRL_2_1` | `application/zip` | Materialized and cached on first request |
| `TAVI` | `application/json` | Materialized and cached on first request |

Each cached artifact is keyed by `generation_count`, and a generation is immutable — so the cache can never go stale.

### Requesting a Download URL

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ reportDownloadUrl(reportId: \"'"$REPORT_ID"'\", format: JSONLD) { downloadUrl expiresAt contentType format generationCount } }"}'
```

The response carries the envelope under `data.reportDownloadUrl`:

```json
{
  "downloadUrl": "https://...s3...",
  "expiresAt": "2026-06-11T19:05:00Z",
  "contentType": "application/ld+json",
  "format": "jsonld",
  "generationCount": 1
}
```

Follow the presigned URL to fetch the artifact:

```bash
curl -L "<downloadUrl from previous response>" -o report.jsonld
```

Swap `format: XBRL_2_1` for the filing package (`-o report.zip`), `format: HOLON_JSONLD` for the dataset-form holon, or `format: TAVI` for the Tavi model (`-o report.tavi.json`) — the query shape and the response envelope are identical.

**Note:** Reports published before serialization shipped have a `NULL` `bundle_url`. Any flavor requested for one of those raises `REPORT_BUNDLE_NOT_AVAILABLE` with a message to regenerate the report. There is no automatic backfill — run `regenerate-report` to stamp a current generation.

The field also raises `REPORT_BUNDLE_SIGNING_FAILED` on a signing or materialization fault, and returns `null` when the `reportId` does not resolve at all. Introspect the full schema over the API, or browse [https://robosystems.ai/docs/api](https://robosystems.ai/docs/api) for the surrounding operations.

## What's Inside Each Format

### JSON-LD

The JSON-LD artifact is a single document with one `@graph`. Concepts are identified by global URIs in the RoboSystems vocabulary namespace `https://robosystems.ai/vocab/`. Crucially, **facts carry their aspects directly** rather than referencing a separate context block:

- `rs:element` — the concept the fact reports
- `rs:period` — the reporting period
- `rs:unit` — the measurement unit
- `rs:numericValue` — the numeric value
- `rs:decimals` — declared precision (default `INF`)

There is no `xbrli:context` / `contextRef` indirection — period, unit, and entity are attached to each fact node. The reporting style appears as `rs:reportingStyle` carrying the Style's UUID (e.g. `"025f5d48-12ce-5d65-b9eb-4f137a10ef06"`), not its four-segment code. This shape mirrors how the framework itself (rs-gaap, fac, cm) already lives in the system: the bundle is the **instance layer** of the same canonical RDF ontology that defines the concepts.

### Holon JSON-LD

`holon-jsonld` is the same content in **dataset form**: instead of one `@graph`, the document carries named graphs for the report holon's scene, boundary, and projection. It is a derived projection of the very same `StatementBundle` the flat JSON-LD comes from — not a separate assembly — so the two can never disagree. Unlike the flat flavor it is not stamped at publish; it is materialized on first request and cached under a `.holon.jsonld` key alongside the other generation artifacts.

### XBRL 2.1

The XBRL package is a zip containing standard XBRL 2.1 artifacts. Two files are always present; the linkbase files are emitted **only when they have content**:

| File | Always present? | Contents |
| --- | --- | --- |
| `instance.xml` | Yes | The XBRL instance — facts with contexts and units |
| `report.xsd` | Yes | The schema declaring the report's concepts |
| `report-pre.xml` | When non-empty | Presentation linkbase |
| `report-cal.xml` | When non-empty | Calculation linkbase |
| `report-def.xml` | When non-empty | Definition linkbase |
| `report-lab.xml` | When non-empty | Label linkbase |

A report can therefore yield as few as two files (instance + schema) or as many as six.

### Tavi

`tavi` is the report as a Project Tavi compiled model: one compact JSON document, in the OIM-family form the SEC pipeline writes beside every filing's holon. The platform builds it from the same `StatementBundle` with xbrlkit's writer, so a RoboLedger report and an SEC filing come out in one shape and one renderer shows both. Like the holon, it is materialized on first request and cached as `g{n}.tavi.json`.

The compiled model carries the statements and notes but has nowhere to put some of what the bundle holds: `ib_envelopes`, `definition_links`, `reporting_style`, `framework_pins`, `fact_sets` and `filing_lifecycle`. The download response names them in `omittedContent` rather than dropping them silently. The flat JSON-LD and the holon keep all of it.

## Reading an Export Without the Platform

Someone who receives a report file needs no RoboSystems account and no graph to read it. The holon and Tavi flavors open in [xbrlkit](https://xbrlkit.com), the open-source XBRL toolkit RoboSystems publishes under the MIT license:

- **In a browser:** [xbrlkit.com/file](https://xbrlkit.com/file) renders the statements, notes and facts of a `.holon.jsonld` or `.tavi.json` file. The file stays on the reader's machine.
- **In an AI client:** `xbrlkit serve` is a local MCP server. Ask the client to load the file by path or URL, then ask about it. Setup for each client is at [xbrlkit.com/mcp](https://xbrlkit.com/mcp).

```bash
uvx --from "xbrlkit[mcp]@latest" xbrlkit serve
```

The RoboLedger guide [Reports and sharing](https://roboledger.ai/docs/reports-and-sharing) covers the same hand-off from the app.

## Validation: SHACL at Publish + Round-Trip

The platform makes a strong claim about its serialized output: **the JSON-LD conforms to the published RoboSystems ontology, and the XBRL is valid XBRL 2.1.** Two mechanisms back that claim.

### SHACL at Publish

The publish hook can run the ontology's SHACL shapes over the emitted JSON-LD and record conformance on the report. This is controlled by a deployment setting, `REPORT_BUNDLE_SHACL_VALIDATION`:

| Mode | Behavior |
| --- | --- |
| `off` (default) | SHACL validation does not run |
| `warn` | Non-conformance is recorded but does not block the publish |
| `strict` | Non-conformance fails the publish |

The mode is chosen by whoever operates the deployment; see [Self-hosted deployments](#self-hosted-deployments) for setting it on your own stack.

### Round-Trip Validation

The broader guarantee is verified end to end against a real reference dataset in the demos: the same published report is emitted in both flavors, and each is independently validated by an external, format-native tool — SHACL (via `pyshacl`) for the JSON-LD, and Arelle for the XBRL package. Because both projections come from one envelope, validating both proves the bundle is simultaneously a conformant ontology instance and a valid XBRL 2.1 filing.

A small in-repo harness runs each check on artifacts you have downloaded. It reads the files from disk, so it needs a checkout of the repository but no running stack:

```bash
# SHACL: does the JSON-LD conform to the ontology shapes?
uv run python -m examples._common.validate --jsonld report.jsonld --label fy2025

# Arelle: is the zip valid XBRL 2.1?
uv run python -m examples._common.validate --zip report.zip --label fy2025
```

## Contributing the Content That Gets Serialized

What ends up in a bundle is the content of your Blocks: the facts, the chart-of-accounts mapping, and the underlying economic events. You contribute that content through the extensions command surface — the same operations the demos use — and the next published report picks it up.

There are three Block write paths, all command operations under `/extensions/roboledger/{graph_id}/operations/`:

| Block | Write path | What it contributes |
| --- | --- | --- |
| **Information Block** | report operations (`create-report`, `regenerate-report`) | Publishes the statement Blocks that become `ib_envelopes` in the bundle |
| **Taxonomy Block** | mapping operations (`create-mapping-association`, `auto-map-elements`) | Determines which `schema_concepts` and `facts` appear, and how the chart of accounts rolls up to framework concepts |
| **Event Block** | `create-event-block` (e.g. `event_type='journal_entry_recorded'`) | Supplies the economic activity that facts are derived from |

A worked example — record a journal entry (Event Block), then republish so the new activity flows into the next bundle. Manual GL entries are written through `create-event-block` with `event_type='journal_entry_recorded'` and `apply_handlers=true`; the handler creates the balanced entry. Line items reference chart-of-accounts `element_id`s, and `debit_amount` / `credit_amount` are in **cents**:

```bash
# 1. Record a journal entry (Event Block) via the ledger command surface
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-event-block" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "journal_entry_recorded",
    "event_category": "recognition",
    "event_class": "economic",
    "occurred_at": "2025-12-15T00:00:00Z",
    "source": "manual",
    "description": "December consulting revenue",
    "apply_handlers": true,
    "metadata": {
      "posting_date": "2025-12-15",
      "memo": "December consulting revenue",
      "line_items": [
        {"element_id": "elem_cash", "debit_amount": 1200000},
        {"element_id": "elem_revenue", "credit_amount": 1200000}
      ]
    }
  }'

# 2. Regenerate the report so the new entry flows into a fresh bundle generation
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/regenerate-report" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"report_id": "'"$REPORT_ID"'"}'

# 3. Get a download URL for the new generation
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ reportDownloadUrl(reportId: \"'"$REPORT_ID"'\", format: JSONLD) { downloadUrl expiresAt generationCount } }"}'
```

All of these operations return an `OperationEnvelope` and accept an `Idempotency-Key` header. For the exact request schemas of each operation, see [https://robosystems.ai/docs/api](https://robosystems.ai/docs/api). For the mechanics of mapping a chart of accounts to framework concepts, see the [RoboLedger Operations](RoboLedger-Operations) guide.

## Troubleshooting

### `REPORT_BUNDLE_NOT_AVAILABLE`

The report has no stored bundle (`bundle_url` is `NULL`) — it was published before serialization shipped, or was never published.

**Solution:** Regenerate the report to stamp a current generation:

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/regenerate-report" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"report_id": "'"$REPORT_ID"'"}'
```

### An Unrecognized Format

`format` is a GraphQL enum, so an unknown name fails validation before the resolver runs.

**Solution:** Use `JSONLD`, `HOLON_JSONLD`, `XBRL_2_1`, or `TAVI`.

### The First XBRL Download Is Slow on Large Reports

Only the **first** download of a given generation pays for the build: it re-runs `build_report_bundle` plus `serialize_to_xbrl` and writes the zip to S3. Every later request for the same generation is a cache hit that just presigns the stored object. The same is true of `HOLON_JSONLD`.

**Solution:** This is expected on the first call and does not repeat. Regenerating the report starts a new generation, so the next download after a `regenerate-report` pays the build cost once again.

### Publish Fails on Bundle Upload

The JSON-LD bundle is uploaded inside the publish transaction. If S3 is unreachable, the publish fails by design rather than leaving a report without an artifact.

**Solution:** Re-run the publish operation. On a self-hosted deployment, verify S3 connectivity and storage configuration first.

## Self-hosted deployments

### SHACL Validation Mode

Set the mode in `.env.local`:

```bash
REPORT_BUNDLE_SHACL_VALIDATION=warn
```

Because validation is opt-in and defaults to `off`, you control whether conformance is enforced for your deployment.

### Seattle Method Demos

The Seattle Method demos exercise the full publish-to-validate path against a reference GL:

```bash
just demo-seattle-method
just demo-seattle-method-create-report
```

## Related Documentation

**Wiki Guides:**

- **[Information Blocks](Information-Blocks)** - The atomic/molecular Block model whose envelopes the bundle serializes
- **[Reporting & Rendering](Reporting-and-Rendering)** - How published reports and the `fact_grid` produce the content a bundle projects
- **[RoboLedger Operations](RoboLedger-Operations)** - The command surface for mapping, ledger writes, and report publishing
- **[Extensions Surface Overview](Extensions-Surface-Overview)** - How the GraphQL reads, command writes, and analytical views fit together
- **[Architecture Overview](Architecture-Overview)** - Platform architecture and storage model

**API & Codebase:**

- **[API reference](https://robosystems.ai/docs/api)** - Live OpenAPI spec for the report operations
- **[GraphQL Reads](GraphQL-Reads)** - The read surface `reportDownloadUrl` lives on
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration in codebase
- **[Models — Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/extensions/README.md)** - Extensions OLTP models in codebase

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
