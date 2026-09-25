# Serialization & Export

A published RoboLedger report can be downloaded as a file in three formats: the Tavi compiled model, the dataset-form holon, and an XBRL 2.1 package. This page covers what each format contains, what each leaves out, how to download one, and how someone without an account can read it.

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

A published report is not a rendered document. It is a structured object: the report's facts, periods, concept declarations, presentation and calculation networks, and the [Information Block](Information-Blocks) for each statement. All three formats are projections of that one assembled object, so they cannot disagree with each other.

| Format | `format` value | What it is for |
| --- | --- | --- |
| **Tavi** | `TAVI` | The report's anchor file, stored at publish: a compact compiled model in XBRL International's Project Tavi form, the same form the SEC pipeline publishes for each filing; opens in xbrlkit |
| **Holon JSON-LD** | `HOLON_JSONLD` | The same content as JSON-LD in dataset form, as named graphs; opens in xbrlkit |
| **XBRL 2.1** | `XBRL_2_1` | A standard filing package for regulators and XBRL tooling |

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

The Tavi file is built and stored as part of publishing. `create-report` and `regenerate-report` write it inside the publish itself: if the file cannot be stored, the publish fails, so there is no published report without its file.

Each publish is a **generation**. The first publish is generation 1, and each `regenerate-report` rebuilds the report from the current ledger and writes the next one. A generation never changes once written, and earlier generations are kept. `reportDownloadUrl` returns the current generation and tells you its number.

The Tavi file exists from the moment of publish. The holon and XBRL files are built from the same generation the first time someone asks for them, then kept, so later downloads of that generation are immediate. Because a generation is immutable, a cached file can never be stale.

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
| `format` | `TAVI` | `TAVI`, `HOLON_JSONLD`, or `XBRL_2_1` |
| `expiresIn` | `300` | Lifetime of the link in seconds, 60 to 3600 |

Every format answers with the same envelope: a short-lived signed link that the client follows to fetch the file. The API never streams the bytes itself.

```bash
curl -X POST "https://api.robosystems.ai/extensions/$GRAPH_ID/graphql" \
  -H "X-API-Key: $ROBOSYSTEMS_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query":"{ reportDownloadUrl(reportId: \"'"$REPORT_ID"'\", format: TAVI) { downloadUrl expiresAt contentType format generationCount omittedContent } }"}'
```

```json
{
  "downloadUrl": "https://…",
  "expiresAt": "2026-06-11T19:05:00Z",
  "contentType": "application/json",
  "format": "tavi",
  "generationCount": 1,
  "omittedContent": ["ib_envelopes", "definition_links", "framework_pins", "filing_lifecycle"]
}
```

```bash
curl -L "<downloadUrl>" -o report.tavi.json
```

Swap the `format` for `HOLON_JSONLD` (`-o report.holon.jsonld`) or `XBRL_2_1` (`-o report.zip`); the query and the envelope are the same.

| Format | `contentType` |
| --- | --- |
| `TAVI` | `application/json` |
| `HOLON_JSONLD` | `application/ld+json` |
| `XBRL_2_1` | `application/zip` |

`omittedContent` names report content the chosen format leaves out: `disclosure_notes` for XBRL 2.1, and the platform-specific items listed under [Tavi](#tavi) for Tavi. It is empty for the holon, which carries everything. The field returns `null` for a `reportId` that does not resolve. Error codes are listed under [Troubleshooting](#troubleshooting).

## What's Inside Each Format

### Tavi

The report as a Project Tavi compiled model: one compact JSON document in the OIM-family form XBRL International is drafting, the same form the SEC pipeline writes beside every filing. A RoboLedger report and an SEC filing come out in one shape, and one reader handles both. The document is a `documentInfo` header and an `xbrlModel` holding the concepts, labels, networks, groups, units and facts.

Every aspect of a fact, the concept included, is a dimension of it. A fact:

```json
{
  "name": "rpt:fact_01M2…",
  "factDimensions": {
    "xbrl:concept": "rs-gaap:AccountsPayableCurrent",
    "xbrl:period": "2026-09-01T00:00:00",
    "xbrl:entity": "entity:entity_kg1a…",
    "xbrl:unit": "iso4217:USD"
  },
  "factValues": [{ "value": "0", "decimals": 2 }],
  "properties": [{ "property": "rs:structureId", "value": "struct_…" }]
}
```

Periods are written as `xs:dateTime`, where the end is exclusive: the instant above is the close of 31 August 2026, and a fiscal year reads `2025-09-01T00:00:00/2026-09-01T00:00:00`. A text fact carries `xbrl:language` in place of a unit, and its value is the text. Ledger amounts are stated to the cent, so a numeric fact carries `decimals: 2`.

Platform content that Tavi has no built-in slot for rides as **model-defined properties** in the `rs:` namespace (`https://robosystems.ai/vocab/`), each declared in `propertyTypes`:

| Property | On | Holds |
| --- | --- | --- |
| `rs:reportingStyle` | the model | The Reporting Style id |
| `rs:legalName` | the entity | The legal name, when it differs from the name |
| `rs:blockType` | a network | The statement or note kind, such as `balance_sheet` |
| `rs:structureId` | a network, a fact | The structure the network presents, and the one a fact is pinned to |
| `rs:factSetId` | a network | The FactSet the structure's facts belong to |
| `rs:structureOrder` | a network | The structure's display order |

Four items still have no place in the file: the Information Block envelopes, the definition arcs, the framework pins, and the filing lifecycle. `omittedContent` lists them (`ib_envelopes`, `definition_links`, `framework_pins`, `filing_lifecycle`), and the holon carries all of them.

The format follows XBRL International's working draft, PWD-2026-09-01, and will track it as the draft moves.

### Holon JSON-LD

The same content as JSON-LD in **dataset form**: named graphs for the report's scene, boundary, and projection. It is derived from the same assembled report as the Tavi, not assembled separately, and it is the one format that keeps every platform-specific item.

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

The package carries the statements only. Tenant-authored disclosure notes render in the app and ride the Tavi and holon formats, and the download names them in `omittedContent` as `disclosure_notes`.

### What each format keeps

XBRL 2.1 has no standard place for the platform-specific content, so the package leaves it out; only the disclosure notes are named in its `omittedContent`.

| Content | Tavi | Holon | XBRL 2.1 |
| --- | --- | --- | --- |
| Statement facts and networks | Yes | Yes | Yes |
| Disclosure notes | Yes | Yes | No |
| Information Block envelopes | No | Yes | No |
| Definition arcs | No | Yes | Yes, when present |
| Reporting Style, FactSets | Yes | Yes | No |
| Framework pins, filing lifecycle | No | Yes | No |

## Reading an Export Without the Platform

Someone who receives a report file needs no RoboSystems account and no graph. The holon and Tavi formats open in [xbrlkit](https://xbrlkit.com), the open-source XBRL toolkit RoboSystems publishes under the MIT license:

- **In a browser:** [xbrlkit.com/file](https://xbrlkit.com/file) renders the statements, notes, and facts of a `.holon.jsonld` or `.tavi.json` file. The file stays on the reader's machine.
- **In an AI client:** `xbrlkit serve` is a local MCP server. Ask the client to load the file by path or URL, then ask about it. Setup for each client is at [xbrlkit.com/mcp](https://xbrlkit.com/mcp).

```bash
uvx --from "xbrlkit[mcp]@latest" xbrlkit serve
```

The holon also reads with any JSON-LD or RDF library, and the XBRL package with any XBRL 2.1 processor, such as Arelle. The RoboLedger guide [Reports and sharing](https://roboledger.ai/docs/reports-and-sharing) covers the same hand-off from the app.

## Validation

The claim made about the output is that the XBRL package is valid XBRL 2.1. The Tavi and the holon are checked by reading them back: xbrlkit loads either into the same in-memory model the tools run on.

**Checking a download yourself.** The repository includes a small harness that runs Arelle over a downloaded package; it needs a checkout of the repository but no running stack:

```bash
# Arelle: is the zip valid XBRL 2.1?
uv run python -m examples._common.validate --zip report.zip --label fy2025
```

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

**Solution:** Use `TAVI`, `HOLON_JSONLD`, or `XBRL_2_1`. The flat `JSONLD` format was retired in favor of the Tavi.

### The First Download of a Format Is Slow

The first request for the holon or XBRL file of a generation builds it. Later requests for that generation return immediately. A `regenerate-report` starts a new generation, so the first download after it builds again.

### Publish Fails on Bundle Upload

The Tavi file is stored as part of the publish; if it cannot be stored, the publish fails rather than leave a report without its file.

**Solution:** Re-run the publish. On a self-hosted deployment, check the object-storage configuration first.

## Self-hosted deployments

### Demos

The RoboLedger demos exercise the full publish-to-download path: each downloads its report's Tavi, holon and XBRL package through the published SDK and runs Arelle over the package.

```bash
just demo-roboledger
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
