# Report serialization

Turns a published report into the files a reader downloads. One assembled
report, the `StatementBundle`, is encoded into each format; nothing here reads
a format back or decides who may download one.

The reader's guide to the formats is
[Serialization & Export](/docs/technical/Serialization-and-Export.md). This
page is the map of the code.

## The flow

```
build_report_bundle (bundle.py)  →  StatementBundle
        │
        ├─ model.py  →  xbrlkit XbrlModel  ─┬─ rdf/holon.py   holon (dataset-form JSON-LD)
        │                                   └─ xbrl/tavi.py   Tavi compiled model
        │
        └─ xbrl/xbrl_21.py                     XBRL 2.1 package (lxml)

stored holon ─┬─ rdf/lineage.py    the same holon with its #lineage graph
              └─ rdf/databook.py   the DataBook (Markdown)
```

The holon is written when a report is published and stored. Every other format
is built on first download and cached beside it. Two of them are built from
the stored holon, not from the rows: the lineage holon splices a fourth named
graph into it, and the DataBook is a rendering of it.

## Modules

| Module | What it does |
| --- | --- |
| `bundle.py` | `build_report_bundle`: reads a report's structures, facts and rules into a `StatementBundle`, the envelope every encoder reads |
| `model.py` | `bundle_to_xbrl_model`: the bridge to xbrlkit's `XbrlModel`, so a ledger report and an SEC filing go through the same xbrlkit writers |
| `flavors.py` | `RdfFlavor` and `XbrlFlavor`, the format names the API models import |
| `rdf/holon.py` | The holon: the report as named graphs over one report IRI, written by xbrlkit's `to_holon` |
| `rdf/lineage.py` | The `#lineage` graph: the ledger lines behind each frozen figure, re-derived at download and checked against the hash frozen at close |
| `rdf/databook.py` | The DataBook: the stored holon as a Markdown document, written by xbrlkit's `databook_from_holon` |
| `xbrl/tavi.py` | The Tavi compiled JSON model, written by xbrlkit |
| `xbrl/xbrl_21.py` | The XBRL 2.1 package: instance, schema and linkbases as a flat zip |

`serialize_to_rdf` and `serialize_to_xbrl` dispatch a bundle by flavor. The
DataBook has its own entry point, `serialize_holon_to_databook`, because it
takes a holon and not a bundle.

## What is defined elsewhere

- **The holon format is xbrlkit's.** Its named graphs, its vocabulary and the
  SHACL shapes that check each graph are in xbrlkit
  ([`xbrlkit/holon`](https://github.com/RoboFinSystems/xbrlkit/blob/main/xbrlkit/holon/README.md)),
  as is the DataBook's layout. A change to either is a change there, picked up
  here by a version bump.
- **`#lineage` is written here.** xbrlkit describes the graph and checks it,
  but only the platform has the ledger, so the writer and its context
  (`LINEAGE_CONTEXT`) live in `rdf/lineage.py`.
- **Download, caching and access** are in
  `operations/roboledger/reads/reports.py`: which format a caller may have,
  where the built file is cached, and the presigned link.
- **Loading a report from a file** is `operations/roboinvestor`'s
  `ingest-report`, through xbrlkit's readers.

## Rules

- **Name what a format leaves out.** A format that cannot carry part of the
  bundle says so: the download response returns the list as `omittedContent`
  (`TAVI_OMITTED_CONTENT`, `DATABOOK_OMITTED_CONTENT`; XBRL 2.1's is set where
  the download is built). Nothing is dropped silently.
- **The DataBook is built from the stored holon,** never from the rows, so it
  always describes the file it names by digest.
- **Nothing here validates.** The platform does not run the holon shapes or
  Arelle at publish. `examples/_common/validate.py` runs both over a
  download, and `tests/operations/serialization/` holds the sample files to
  them.

## Adding a format

1. Add the flavor to `flavors.py` and the encoder under `rdf/` or `xbrl/`.
2. Name what it leaves out, for `omittedContent`.
3. Wire the download in `operations/roboledger/reads/reports.py`, and add the
   value to `ReportDownloadFormat` (`graphql/types/ledger.py`) and to the MCP
   tool's formats (`middleware/mcp/tools/report_tools.py`).
4. Add it to the tables in
   [Serialization & Export](/docs/technical/Serialization-and-Export.md). A
   new enum value is a minor release of both clients.
