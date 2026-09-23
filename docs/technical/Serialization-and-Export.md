# Serialization & Export

A published RoboLedger report can be downloaded as a file in four formats: flat JSON-LD, the dataset-form holon, an XBRL 2.1 package, and the Tavi compiled model. This page covers what each format contains, what each leaves out, how to download one, and how someone without an account can read it.

**Quick Start:** Publish a report with `create-report`, then query the `reportDownloadUrl` GraphQL field for a short-lived link to the file.

> **Running your own stack?** Every example here works against a local deployment: use `http://localhost:8000` and the key from `just demo-user`. See [Local Development](Local-Development).

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [What a Report Bundle Carries](#what-a-report-bundle-carries)
- [When a Bundle Is Made](#when-a-bundle-is-made)
- [Downloading a Bundle](#downloading-a-bundle)
- [What's Inside Each Format](#whats-inside-each-format)
- [Reading an Export Without the Platform](#reading-an-export-without-the-platform)
- [Validation](#validation)
- [Changing What a Report Contains](#changing-what-a-report-contains)
- [Troubleshooting](#troubleshooting)
- [Self-hosted deployments](#self-hosted-deployments)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Overview

A published report is not a rendered document. It is a structured object: the report's facts, periods, concept declarations, presentation and calculation networks, and the [Information Block](Information-Blocks) for each statement. All four formats are projections of that one assembled object, so they cannot disagree with each other.

| Format | `format` value | What it is for |
| --- | --- | --- |
| **JSON-LD** | `JSONLD` | The canonical artifact: one document, every node identified by a global URI, readable by any JSON or RDF tool |
| **Holon JSON-LD** | `HOLON_JSONLD` | The same content in dataset form, as named graphs; opens in xbrlkit |
| **XBRL 2.1** | `XBRL_2_1` | A standard filing package for regulators and XBRL tooling |
| **Tavi** | `TAVI` | A compact compiled model, the same form the SEC pipeline publishes for each filing; opens in xbrlkit |

## Prerequisites

- A RoboSystems account and an API key; see [Quick Start](Quick-Start)
- A graph with the RoboLedger extension, ledger data, a chart-of-accounts mapping, and at least one published report

## What a Report Bundle Carries

Every format is built from the same contents:

| Content | What it holds |
| --- | --- |
| Entity | The reporting entity: name, legal name, tax id, country |
| Periods | The report's period columns, and each distinct period its facts use (instant or duration) |
| Reporting style | The entity's Reporting Style, by **id** (a UUID). The four-segment code such as `BSC-CORP-IS02-CF1` belongs to the Style itself; resolve the id against it to get the code. |
| Framework pins | The framework versions the report was built against. `rs-gaap@v1` depends on `fac@v1` and `cm@v1`, so there are three. |
| Concepts | A declaration for each concept the report uses: qname, label, balance type, period type, abstract or not, monetary or not |
| Networks | The presentation, calculation, and definition arcs, grouped by network |
| Units | For example `iso4217:USD` |
| Facts | Each fact with its concept, period, unit, entity, value, decimals, FactSet, and the structure it is pinned to |
| Information Blocks | One envelope per statement network, with its display order |
| Report metadata | Report id, generation number, filing status and date, the report it supersedes, and sharing provenance |

## When a Bundle Is Made

The JSON-LD bundle is built and stored as part of publishing. `create-report` and `regenerate-report` write it inside the publish itself: if the file cannot be stored, the publish fails, so there is no published report without a bundle.

Each publish is a **generation**. The first publish is generation 1, and each `regenerate-report` rebuilds the report from the current ledger and writes the next one. A generation never changes once written, and earlier generations are kept. `reportDownloadUrl` returns the current generation and tells you its number.

The JSON-LD file exists from the moment of publish. The holon, XBRL, and Tavi files are built from the same generation the first time someone asks for them, then kept, so later downloads of that generation are immediate. Because a generation is immutable, a cached file can never be stale.

```bash
export ROBOSYSTEMS_API_KEY=rfs...   # Settings → API keys at robosystems.ai
export GRAPH_ID=kg...               # from GET /v1/graphs or the app's graph selector

curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/create-report" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{
    "name": "FY2025 Annual Report",
    "taxonomy_id": "rs-gaap",
    "mapping_id": "struct_01K8...",
    "period_start": "2025-01-01",
    "period_end": "2025-12-31",
    "period_type": "annual",
    "comparative": true
  }'
```

`mapping_id` is the id of the graph's chart-of-accounts mapping structure, a `struct_…` id; see [Chart of Accounts Mapping](Chart-of-Accounts-Mapping). The `id` in the response's `result` is the `report_id` you download with.

## Downloading a Bundle

Downloads are a GraphQL read field, not a REST resource: a download is a read of stored state, so it lives with the other report reads.

```text
POST /extensions/{graph_id}/graphql
{ reportDownloadUrl(reportId:, format:, expiresIn:) { … } }
```

| Argument | Default | Notes |
| --- | --- | --- |
| `reportId` | | Required; the report's id |
| `format` | `JSONLD` | `JSONLD`, `HOLON_JSONLD`, `XBRL_2_1`, or `TAVI` |
| `expiresIn` | `300` | Lifetime of the link in seconds, 60 to 3600 |

Every format answers with the same envelope: a short-lived signed link that the client follows to fetch the file. The API never streams the bytes itself.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ reportDownloadUrl(reportId: \"'"$REPORT_ID"'\", format: JSONLD) { downloadUrl expiresAt contentType format generationCount omittedContent } }"}'
```

```json
{
  "downloadUrl": "https://…",
  "expiresAt": "2026-06-11T19:05:00Z",
  "contentType": "application/ld+json",
  "format": "jsonld",
  "generationCount": 1,
  "omittedContent": []
}
```

```bash
curl -L "<downloadUrl>" -o report.jsonld
```

Swap the `format` for `XBRL_2_1` (`-o report.zip`), `HOLON_JSONLD` (`-o report.holon.jsonld`), or `TAVI` (`-o report.tavi.json`); the query and the envelope are the same.

| Format | `contentType` |
| --- | --- |
| `JSONLD` | `application/ld+json` |
| `HOLON_JSONLD` | `application/ld+json` |
| `XBRL_2_1` | `application/zip` |
| `TAVI` | `application/json` |

`omittedContent` names report content the chosen format leaves out: `disclosure_notes` for XBRL 2.1, and the platform-specific items listed under [Tavi](#tavi) for Tavi. It is empty for the two JSON-LD formats, which carry everything. The field returns `null` for a `reportId` that does not resolve. Error codes are listed under [Troubleshooting](#troubleshooting).

## What's Inside Each Format

### JSON-LD

One document with one `@graph`, in the RoboSystems vocabulary (`https://robosystems.ai/vocab/`, compacted by the document's `@context`). Concepts, periods, units, the entity, and structures are nodes of their own, and each **fact points at its aspects directly**. There is no XBRL context and no `contextRef` indirection. A fact node:

```json
{
  "@id": "https://robosystems.ai/report/rpt_01M2…/fact/fact_01M2…",
  "@type": "rs:Fact",
  "element": "rs-gaap:ProceedsFromIssuanceOfCommonStock",
  "entity": "https://robosystems.ai/report/rpt_01M2…/entity/entity_kg1a…",
  "period": "https://robosystems.ai/report/rpt_01M2…/period/p_3",
  "unit": "https://robosystems.ai/report/rpt_01M2…/unit/u_USD",
  "factType": "numeric",
  "numericValue": "49800.0",
  "decimals": "INF",
  "factSet": "https://robosystems.ai/factset/fs_01M2…",
  "structure": "https://robosystems.ai/report/rpt_01M2…/structure/…",
  "internalId": "fact_01M2…"
}
```

and the period it points at:

```json
{
  "@id": "https://robosystems.ai/report/rpt_01M2…/period/p_3",
  "@type": "rs:Period",
  "periodType": "duration",
  "startDate": "2025-01-01",
  "endDate": "2025-12-31"
}
```

A text fact carries `factType: "nonnumeric"` and a `stringValue` instead of a unit, value, and decimals. The root node is typed `rs:Report` and carries `reportingStyle` (the Style's id) and `serializationVersion`. This is the instance layer of the same ontology that defines the rs-gaap, fac, and cm concepts, so a report and its framework read with one vocabulary.

### Holon JSON-LD

The same content in **dataset form**: instead of one `@graph`, the document holds named graphs for the report's scene, boundary, and projection. It is derived from the same assembled report as the flat JSON-LD, not assembled separately.

### XBRL 2.1

A zip of standard XBRL 2.1 files. Two are always present; the linkbases appear only when they have content, so a package has between two and six files:

| File | Always present | Contents |
| --- | --- | --- |
| `instance.xml` | Yes | Facts, with contexts and units derived from each fact's aspects |
| `report.xsd` | Yes | The schema declaring the report's concepts |
| `report-pre.xml` | When non-empty | Presentation linkbase |
| `report-cal.xml` | When non-empty | Calculation linkbase |
| `report-def.xml` | When non-empty | Definition linkbase |
| `report-lab.xml` | When non-empty | Label linkbase |

The package carries the statements only. Tenant-authored disclosure notes render in the app and ride the JSON-LD and holon formats, and the download names them in `omittedContent` as `disclosure_notes`.

### Tavi

The report as a Project Tavi compiled model: one compact JSON document in the OIM-family form the SEC pipeline writes beside every filing's holon. A RoboLedger report and an SEC filing come out in one shape, and one reader handles both.

The compiled model carries the statements and notes but has no place for some of the report's content: the Information Block envelopes, the definition arcs, the Reporting Style, the framework pins, the FactSet partition, and the filing lifecycle. `omittedContent` lists them (`ib_envelopes`, `definition_links`, `reporting_style`, `framework_pins`, `fact_sets`, `filing_lifecycle`).

### What each format keeps

XBRL 2.1 has no standard place for the platform-specific content, so the package leaves it out; only the disclosure notes are named in its `omittedContent`.

| Content | JSON-LD | Holon | XBRL 2.1 | Tavi |
| --- | --- | --- | --- | --- |
| Statement facts and networks | Yes | Yes | Yes | Yes |
| Disclosure notes | Yes | Yes | No | Yes |
| Information Block envelopes | Yes | Yes | No | No |
| Definition arcs | Yes | Yes | Yes, when present | No |
| Reporting Style, framework pins | Yes | Yes | No | No |
| FactSets, filing lifecycle | Yes | Yes | No | No |

## Reading an Export Without the Platform

Someone who receives a report file needs no RoboSystems account and no graph. The holon and Tavi formats open in [xbrlkit](https://xbrlkit.com), the open-source XBRL toolkit RoboSystems publishes under the MIT license:

- **In a browser:** [xbrlkit.com/file](https://xbrlkit.com/file) renders the statements, notes, and facts of a `.holon.jsonld` or `.tavi.json` file. The file stays on the reader's machine.
- **In an AI client:** `xbrlkit serve` is a local MCP server. Ask the client to load the file by path or URL, then ask about it. Setup for each client is at [xbrlkit.com/mcp](https://xbrlkit.com/mcp).

```bash
uvx --from "xbrlkit[mcp]@latest" xbrlkit serve
```

The flat JSON-LD reads with any JSON-LD or RDF library, and the XBRL package with any XBRL 2.1 processor, such as Arelle. The RoboLedger guide [Reports and sharing](https://roboledger.ai/docs/reports-and-sharing) covers the same hand-off from the app.

## Validation

Two claims are made about the output: the JSON-LD conforms to the published RoboSystems ontology, and the XBRL package is valid XBRL 2.1.

**SHACL at publish.** A deployment can run the ontology's SHACL shapes over the JSON-LD as each report publishes, and record the outcome on the report. The mode is a deployment setting: `off` (the default), `warn` (record non-conformance, publish anyway), or `strict` (non-conformance fails the publish). See [Self-hosted deployments](#self-hosted-deployments).

**Checking a download yourself.** Validate the files with format-native tools: `pyshacl` against the ontology's shapes for the JSON-LD, and Arelle for the XBRL package. The repository includes a small harness that runs both on downloaded files; it needs a checkout of the repository but no running stack:

```bash
# SHACL: does the JSON-LD conform to the ontology shapes?
uv run python -m examples._common.validate --jsonld report.jsonld --label fy2025

# Arelle: is the zip valid XBRL 2.1?
uv run python -m examples._common.validate --zip report.zip --label fy2025
```

Because every format comes from one assembled report, validating the JSON-LD and the XBRL package checks the same content from both sides.

## Changing What a Report Contains

A report's content is the ledger's facts, mapped through the chart of accounts to framework concepts. To change it, change those: post or correct events ([Event-Driven Ledger](Event-Driven-Ledger)), adjust the mapping ([Chart of Accounts Mapping](Chart-of-Accounts-Mapping)), then run `regenerate-report`. That writes a new generation, and the next download returns it.

```bash
curl -X POST "https://api.robosystems.ai/extensions/roboledger/$GRAPH_ID/operations/regenerate-report" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"report_id": "'"$REPORT_ID"'"}'
```

## Troubleshooting

### `REPORT_BUNDLE_NOT_AVAILABLE`

The report has no stored bundle: it was published before exports existed, or was never published. There is no automatic backfill.

**Solution:** Run `regenerate-report` to write a current generation.

### `INVALID_EXPIRES_IN`

`expiresIn` is outside 60 to 3600 seconds.

### `REPORT_BUNDLE_SIGNING_FAILED`

The link could not be signed, or a format could not be built. Retry; if it persists, report it.

### An Unrecognized Format

`format` is a GraphQL enum, so an unknown name fails validation before the request runs.

**Solution:** Use `JSONLD`, `HOLON_JSONLD`, `XBRL_2_1`, or `TAVI`.

### The First Download of a Format Is Slow

The first request for the holon, XBRL, or Tavi file of a generation builds it. Later requests for that generation return immediately. A `regenerate-report` starts a new generation, so the first download after it builds again.

### Publish Fails on Bundle Upload

The JSON-LD bundle is stored as part of the publish; if it cannot be stored, the publish fails rather than leave a report without its file.

**Solution:** Re-run the publish. On a self-hosted deployment, check the object-storage configuration first.

## Self-hosted deployments

### SHACL Validation Mode

Set the mode in `.env.local`:

```bash
REPORT_BUNDLE_SHACL_VALIDATION=warn
```

It defaults to `off`.

### Seattle Method Demos

The Seattle Method demos exercise the full publish-to-validate path against a reference general ledger:

```bash
just demo-seattle-method
just demo-seattle-method-create-report
```

## Related Documentation

**Wiki Guides:**

- **[Information Blocks](Information-Blocks)** - The blocks whose envelopes the bundle carries
- **[Reporting & Rendering](Reporting-and-Rendering)** - How published reports are produced
- **[Chart of Accounts Mapping](Chart-of-Accounts-Mapping)** - The mapping a report rolls up through
- **[RoboLedger Operations](RoboLedger-Operations)** - The command surface, including report publishing
- **[GraphQL Reads](GraphQL-Reads)** - The read surface `reportDownloadUrl` lives on

**API & Codebase:**

- **[API reference](https://robosystems.ai/docs/api)** - Request and response schemas for the report operations
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - The operations kernel

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
