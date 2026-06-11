# Serialization & Export

This guide shows you how a published RoboLedger report is projected into portable file formats — JSON-LD and XBRL 2.1 — and how to download those artifacts from the platform.

**Quick Start:** Publish a report with `create-report`, then `GET .../reports/{report_id}/download?format=jsonld` to get a presigned link to its bundle.

## Overview

A published report in RoboLedger isn't a single rendered document — it's a structured object that can be projected into different portable file formats without re-querying the database. That object is the **StatementBundle**, an in-memory envelope assembled from the report's facts, periods, framework slice, and per-statement [Information Blocks](Information-Blocks). Two encoder families walk that one envelope:

- `serialize_to_rdf` produces **JSON-LD** — the canonical, web-native artifact. One file, identified by global URIs, that any JSON tool can read.
- `serialize_to_xbrl` produces an **XBRL 2.1** package — the filing-grade, standards-blessed format for interop with regulators and downstream tooling.

End to end, this page covers:

1. What a `StatementBundle` carries and why both encoders share it
2. How a bundle is produced and stamped at publish time
3. How generation-stamped bundles are stored in S3
4. How to download a bundle in each format
5. What the JSON-LD and XBRL artifacts contain
6. How SHACL validation at publish and round-trip validation prove conformance
7. How to contribute the Block content that gets serialized

## Prerequisites

Before starting, ensure you have:

- Docker running locally with services started via `just start`
- The RoboLedger extension enabled (`ROBOLEDGER_ENABLED=true`)
- A demo user and API key (`just demo-user`) — the key is saved to `.local/config.json`
- A graph with ledger data and at least one published report (run `just demo-roboledger` to provision one end to end)

## The StatementBundle Envelope

Everything downstream walks a single in-memory object: the `StatementBundle`. Both encoders take a bundle and return bytes; **neither encoder touches the database**. This is the core design property — serialization is a pure projection of an already-assembled envelope, so adding a new output format means adding an encoder "flavor," not rewriting the pipeline.

A bundle is built by `build_report_bundle(session, graph_id, report_id)` and carries:

| Field | What it holds |
| --- | --- |
| `entity` | The reporting entity: `id, name, legal_name, ein, country` |
| `periods` | Reporting periods: `start, end, label, period_type` (`duration` or `instant`) |
| `reporting_style` | Resolved style id, e.g. `BSC-CORP-IS02-CF1` |
| `framework_pins` | Each `{framework, version}`, e.g. `rs-gaap` / `v1` |
| `schema_concepts` | Concept declarations: `qname, name, label, balance_type, period_type, is_abstract, is_monetary, element_type, source` |
| `linkbases` | `presentation_links`, `calculation_links`, `definition_links` — each an ELR of `BundleArc[]` (`arc_type, arcrole, from_qname, to_qname, order_value, weight`) |
| `period_nodes` | `id, period_start, period_end, period_type` (`period_start` is `None` for instants) |
| `units` | `id, measure`, e.g. `iso4217:USD` |
| `facts` | `id, element_id, element_qname, value, period_ref, unit_ref, entity_ref, decimals, fact_set_id, structure_id` |
| `ib_envelopes` | One `InformationBlockEnvelope` per statement Network |
| `mode` | `report` (a published, immutable report) |
| `report_meta` | `report_id, generation_count, filing_status, filed_at, supersedes_id, source_graph_id, source_report_id, shared_at` |

**Why one envelope.** The income statement, balance sheet, and cash flow statement are each an [Information Block](Information-Blocks). The bundle is the instance layer that exports those Blocks: it pairs each statement's structural skeleton (the linkbases and concept declarations) with the actual facts. JSON-LD and XBRL are two renderings of the same underlying content — keeping the assembly in one place guarantees the two formats can never drift apart.

## How a Bundle Gets Made (Publish-Time Stamping)

Bundles are produced as a side effect of publishing a report, not on demand. When you run the `create-report` or `regenerate-report` operation, the JSON-LD bundle is built and uploaded to S3 **inside the publish transaction**.

```bash
API_KEY=$(jq -r .api_key .local/config.json)
GRAPH_ID=<your graph id>

curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/create-report" \
  -H "X-API-Key: $API_KEY" \
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
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/regenerate-report" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"report_id": "'"$REPORT_ID"'"}'
```

## Generation-Stamped S3 Storage

Every published generation of a report is stored as its own object. Bundle keys are stamped with the generation count:

```text
report-bundles/{graph_id}/{report_id}/g{generation_count}.jsonld
```

So the first publish writes `g1.jsonld`, a regenerate writes `g2.jsonld`, and so on. Older generations stay in S3 — they are not overwritten — and the `Report.bundle_url` column always points at the current generation's full `s3://` URI. This gives you an immutable history of every projection the platform ever published for a report.

## Downloading a Bundle

The only external export surface is one REST endpoint:

```text
GET /extensions/roboledger/{graph_id}/reports/{report_id}/download
```

| Query param | Default | Notes |
| --- | --- | --- |
| `format` | `jsonld` | Accepted values: `jsonld`, `xbrl-2.1` |
| `expires_in` | `300` | Presigned-URL TTL in seconds (min `60`, max `3600`); ignored for XBRL |

The two formats behave differently: **JSON-LD returns a presigned link to the stored artifact; XBRL streams a freshly built zip.**

### Format `jsonld` — Presigned Link to the Stored Bundle

Because the JSON-LD bundle was already built and stored at publish time, the download endpoint returns a short-lived presigned S3 URL rather than the bytes:

```bash
curl "http://localhost:8000/extensions/roboledger/$GRAPH_ID/reports/$REPORT_ID/download?format=jsonld" \
  -H "X-API-Key: $API_KEY"
```

The response is a JSON envelope:

```json
{
  "download_url": "https://...s3...",
  "expires_at": "2026-06-11T19:05:00Z",
  "content_type": "application/ld+json",
  "format": "jsonld",
  "generation_count": 1
}
```

Follow the presigned URL to fetch the artifact:

```bash
curl -L "<download_url from previous response>" -o report.jsonld
```

### Format `xbrl-2.1` — On-Demand Zip Stream

XBRL packages are not stored — they are rebuilt on every request by re-running `build_report_bundle` plus `serialize_to_xbrl`. The endpoint streams the zip directly in the response body (no JSON wrapper):

```bash
curl "http://localhost:8000/extensions/roboledger/$GRAPH_ID/reports/$REPORT_ID/download?format=xbrl-2.1" \
  -H "X-API-Key: $API_KEY" \
  -o report.zip
```

The response carries the package as headers and body:

```text
Content-Type: application/zip
Content-Disposition: attachment; filename="<report_id>-g1.zip"
X-Bundle-Format: xbrl-2.1
X-Bundle-Generation: 1
```

**Note:** Reports published before serialization shipped have a `NULL` `bundle_url`. A `format=jsonld` download for one of those returns a 404 with a message to regenerate the report to produce a bundle. There is no automatic backfill — run `regenerate-report` to stamp a current generation.

For the full request/response schema, query parameter constraints, and error codes, see the live OpenAPI spec at [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` when running locally). The download `operation_id` is `getReportBundleDownloadUrl`.

## What's Inside Each Format

### JSON-LD

The JSON-LD artifact is a single document with one `@graph`. Concepts are identified by global URIs in the RoboSystems vocabulary namespace `https://robosystems.ai/vocab/`. Crucially, **facts carry their aspects directly** rather than referencing a separate context block:

- `rs:element` — the concept the fact reports
- `rs:period` — the reporting period
- `rs:unit` — the measurement unit
- `rs:numericValue` — the numeric value
- `rs:decimals` — declared precision (default `INF`)

There is no `xbrli:context` / `contextRef` indirection — period, unit, and entity are attached to each fact node. The reporting style appears as `rs:reportingStyle` (e.g. `"BSC-CORP-IS02-CF1"`). This shape mirrors how the framework itself (rs-gaap, fac) already lives in the system: the bundle is the **instance layer** of the same canonical RDF ontology that defines the concepts.

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

## Validation: SHACL at Publish + Round-Trip

The platform makes a strong claim about its serialized output: **the JSON-LD conforms to the published RoboSystems ontology, and the XBRL is valid XBRL 2.1.** Two mechanisms back that claim.

### SHACL at Publish

The publish hook can run the ontology's SHACL shapes over the emitted JSON-LD and record conformance on the report. This is controlled by an environment variable:

| Mode | Behavior |
| --- | --- |
| `off` (default) | SHACL validation does not run |
| `warn` | Non-conformance is recorded but does not block the publish |
| `strict` | Non-conformance fails the publish |

Set the mode in `.env.local`:

```bash
REPORT_BUNDLE_SHACL_VALIDATION=warn
```

Because validation is opt-in and defaults to `off`, you control whether conformance is enforced for your deployment.

### Round-Trip Validation

The broader guarantee is verified end to end against a real reference dataset in the demos: the same published report is emitted in both flavors, and each is independently validated by an external, format-native tool — SHACL (via `pyshacl`) for the JSON-LD, and Arelle for the XBRL package. Because both projections come from one envelope, validating both proves the bundle is simultaneously a conformant ontology instance and a valid XBRL 2.1 filing.

A small in-repo harness runs each check. After running a demo that publishes reports and emits both flavors:

```bash
# SHACL: does the JSON-LD conform to the ontology shapes?
uv run python -m examples._common.validate --jsonld report.jsonld --label fy2025

# Arelle: is the zip valid XBRL 2.1?
uv run python -m examples._common.validate --zip report.zip --label fy2025
```

The Seattle Method demos exercise this full publish-to-validate path against a reference GL:

```bash
just demo-seattle-method
just demo-seattle-method-create-report
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
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/create-event-block" \
  -H "X-API-Key: $API_KEY" \
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
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/regenerate-report" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"report_id": "'"$REPORT_ID"'"}'

# 3. Download the new generation
curl "http://localhost:8000/extensions/roboledger/$GRAPH_ID/reports/$REPORT_ID/download?format=jsonld" \
  -H "X-API-Key: $API_KEY"
```

All of these operations return an `OperationEnvelope` and accept an `Idempotency-Key` header. For the exact request schemas of each operation, see [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs). For the mechanics of mapping a chart of accounts to framework concepts, see the [RoboLedger Operations](RoboLedger-Operations) guide.

## Troubleshooting

### JSON-LD Download Returns 404

The report has no stored bundle (`bundle_url` is `NULL`) — it was published before serialization shipped, or was never published.

**Solution:** Regenerate the report to stamp a current generation:

```bash
curl -X POST "http://localhost:8000/extensions/roboledger/$GRAPH_ID/operations/regenerate-report" \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"report_id": "'"$REPORT_ID"'"}'
```

### Download Returns 400 for a Format

The endpoint accepts `jsonld` and `xbrl-2.1`. Any other value returns a 400 with the list of supported flavors in the error detail.

**Solution:** Use `format=jsonld` or `format=xbrl-2.1`.

### XBRL Download Is Slow on Large Reports

The XBRL package is built fresh on every request (it is not cached in S3), so it re-runs the full bundle build and encode each time.

**Solution:** This is expected. If you need a stable artifact to reference repeatedly, download once and persist the zip yourself.

### Publish Fails on Bundle Upload

The JSON-LD bundle is uploaded inside the publish transaction. If S3 is unreachable, the publish fails by design rather than leaving a report without an artifact.

**Solution:** Verify S3 connectivity and storage configuration, then re-run the publish operation.

## Related Documentation

**Wiki Guides:**

- **[Information Blocks](Information-Blocks)** - The atomic/molecular Block model whose envelopes the bundle serializes
- **[Reporting & Rendering](Reporting-and-Rendering)** - How published reports and the `fact_grid` produce the content a bundle projects
- **[RoboLedger Operations](RoboLedger-Operations)** - The command surface for mapping, ledger writes, and report publishing
- **[Extensions Surface Overview](Extensions-Surface-Overview)** - How the GraphQL reads, command writes, and analytical views fit together
- **[Architecture Overview](Architecture-Overview)** - Platform architecture and storage model

**API & Codebase:**

- **[API Documentation](https://api.robosystems.ai/docs)** - Live OpenAPI spec for the download endpoint and report operations
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - Business workflow orchestration in codebase
- **[Models — Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/models/extensions/README.md)** - Extensions OLTP models in codebase

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
