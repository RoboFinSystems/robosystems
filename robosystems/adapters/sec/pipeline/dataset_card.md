---
license: cc-by-4.0
pretty_name: SEC XBRL Knowledge Graph (LadybugDB)
language:
  - en
tags:
  - finance
  - sec
  - edgar
  - xbrl
  - financial-statements
  - knowledge-graph
  - graph-database
  - cypher
  - ladybugdb
  - accounting
viewer: false
---

# SEC XBRL Knowledge Graph — one embedded LadybugDB file

The XBRL filings made with the SEC since January 2024 — annual and quarterly reports (10-K, 10-Q), foreign-filer annuals (20-F, 40-F), proxy statements (DEF 14A) and registration statements (S-1) — as a single
queryable property graph: **{{filers}} filers, {{filings}} filings, {{facts_millions}} million facts** on
**{{nodes_millions}} million nodes**, in one **{{uncompressed_gib}} GiB** [LadybugDB](https://github.com/LadybugDB/ladybug)
file that ships as a **{{compressed_gib}} GiB** zstd archive, written by **LadybugDB {{engine_version}}** — open it with that release
(see *Engine version* below).

It is the same file that backs the hosted `sec` graph on [robosystems.ai](https://robosystems.ai), built
from EDGAR by the open-source [RoboSystems](https://github.com/RoboFinSystems/robosystems) pipeline. Download
it, drop it into a local RoboSystems checkout, and you have the whole corpus under Cypher, the API, and MCP
on your own machine — no server, no pipeline run.

This is not a Parquet dataset: it is a database file. The dataset viewer is disabled on purpose.

## What is in the file

| | |
|---|---|
| **Snapshot** | {{snapshot_date}} (filings through {{coverage_end}}) — see the latest commit title for the current snapshot |
| **Coverage** | filings dated {{coverage_start}} → {{coverage_end}}; prior-period comparatives inside those filings reach further back |
| **Forms** | {{forms}} |
| **Filers** | {{filers}} entities ({{filers_with_ticker}} with a ticker), with CIK, SIC code and description, legal name, fiscal year end |
| **Facts** | {{facts_millions}} million XBRL facts ({{numeric_facts_millions}} million numeric), each linked to its element, entity, period, unit, dimensions, and the presentation/calculation structure it appears in |
| **Graph** | {{node_types}} node types · {{relationship_types}} relationship types · {{nodes_millions}} million nodes |
| **Engine** | LadybugDB **{{engine_version}}** (storage format {{storage_format}}) — the release that wrote the file and the one to open it with; see *Engine version* below |
| **Sizes** | `sec.lbug.zst` {{compressed_gib}} GiB ({{compressed_bytes}} bytes) → `sec.lbug` {{uncompressed_gib}} GiB ({{uncompressed_bytes}} bytes) |
| **Cadence** | monthly; the fixed path `sec.lbug.zst` is overwritten each snapshot, so `main` is always the latest and this URL never changes |

Node types: `Entity`, `Report`, `Fact`, `FactSet`, `Element`, `Label`, `Reference`, `Period`, `Unit`,
`Dimension`, `Structure`, `Association`, `Classification`, `Taxonomy`. The full schema — every
property and relationship — is one call away once the file is open (`CALL show_tables()`), and is
documented in [Querying the Analytical Graph](https://robosystems.ai/docs/technical/querying-the-analytical-graph).

## Don't need it on your own disk?

The same graph is hosted and kept current daily at [robosystems.ai](https://robosystems.ai), read through
MCP from Claude, ChatGPT or any MCP client, or through the API. Access is a subscription; see
[Analyze SEC filings](https://robosystems.ai/docs/guides/sec-filings) for what you can ask and how to connect.
This file is the free route: the whole graph, on your machine, under CC BY 4.0.

## Download and open

You need **~{{free_gib}} GiB free** while decompressing ({{compressed_gib}} + {{uncompressed_gib}}; the archive is deleted afterwards), a machine
with **16 GB RAM or more**, and [`uv`](https://docs.astral.sh/uv/) + [`just`](https://github.com/casey/just).
Budget real time: this is {{uncompressed_gib_whole}} GiB on disk, not a laptop-minutes toy.

```bash
git clone https://github.com/RoboFinSystems/robosystems.git
cd robosystems
just sec-dump          # downloads sec.lbug.zst (resumable), verifies it, decompresses to data/lbug-dbs/sec.lbug
```

`just sec-dump` checks the archive's checksum and recorded size as it streams, refuses to overwrite an
existing `sec.lbug` without `--force`, and warns if the engine that wrote the file differs from the one the
checkout pins.

Without `just` — the archive is plain zstd:

```bash
hf download robosystems/sec-xbrl-knowledge-graphs sec.lbug.zst --repo-type dataset --local-dir .
zstd -d sec.lbug.zst -o data/lbug-dbs/sec.lbug      # or: python -m zstandard, 7-Zip, any zstd
```

### Engine version

The file was written by **LadybugDB {{engine_version}}** and must be opened with a release that reads that storage
format ({{storage_format}}). An older release refuses the file; a newer one may open it, but a read-write open converts the
storage format in place — a one-way change — so pin the version unless that is what you want. A RoboSystems
checkout pins it for you (`ladybug=={{engine_version}}` in `pyproject.toml`); on your own:

```bash
pip install ladybug=={{engine_version}}
```

The version that wrote each snapshot is in that snapshot's commit title (`sec.lbug.zst: snapshot YYYY-MM-DD
(ladybug X.Y.Z)`), and it moves when the pipeline upgrades its engine — check it before opening a new
snapshot with an existing install. `just sec-dump` compares the two and warns on a mismatch.

## Query it

**Directly, no services** — the [`ladybug`](https://pypi.org/project/ladybug/) Python package
(`pip install ladybug=={{engine_version}}`) opens the file in-process:

```bash
just lbug-query sec "MATCH (e:Entity {ticker: 'NVDA'})-[:ENTITY_HAS_REPORT]->(r:Report) RETURN r.form, r.filing_date ORDER BY r.filing_date DESC"
```

```python
import ladybug as lbug

db = lbug.Database("data/lbug-dbs/sec.lbug")
conn = lbug.Connection(db)
result = conn.execute("MATCH (e:Entity) RETURN count(e)")
while result.has_next():
  print(result.get_next())
```

**Through the API and MCP** — start the stack, mint a local key, and subscribe your local user to the
`sec` repository; the graph is then served at `http://localhost:8000` exactly as the hosted one is:

```bash
just start                 # API, Graph API, PostgreSQL, Valkey, Dagster
just demo-user             # writes an API key to .local/config.json
just demo-sec-subscribe    # grants the local user access to the sec graph (no data loading)

curl -X POST "http://localhost:8000/v1/graphs/sec/query/cypher" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (e:Entity) RETURN count(e) AS filers"}'
```

MCP clients running on your machine (Claude Desktop, Cursor, any local MCP host) connect to `http://localhost:8000/v1/graphs/sec/mcp`
with the same key — see [AI Operators and MCP](https://robosystems.ai/docs/technical/ai-operators-and-mcp).
The MCP server carries the schema, example queries, a concept resolver, statement builders, and fact-grid pivots.

### Example queries

Consolidated annual revenue for one filer, robust across the different revenue elements filers use:

```cypher
MATCH (f:Fact {has_dimensions: false})-[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_ENTITY]->(ent:Entity {ticker: 'NVDA'}),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'})
WHERE e.canonical_concept = 'revenue' AND f.numeric_value IS NOT NULL
RETURN ent.ticker, e.qname, p.end_date, f.numeric_value AS revenue
ORDER BY p.end_date DESC LIMIT 10
```

A whole income statement via the presentation structure (anchor on the entity first — thousands of filings share each statement type):

```cypher
MATCH (ent:Entity {ticker: 'NVDA'})<-[:FACT_HAS_ENTITY]-(f:Fact {has_dimensions: false})-[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_PERIOD]->(p:Period {duration_type: 'annual'}),
      (fs:FactSet)-[:FACT_SET_CONTAINS_FACT]->(f),
      (s:Structure {canonical_type: 'income_statement'})-[:STRUCTURE_HAS_FACT_SET]->(fs)
WHERE f.numeric_value IS NOT NULL
RETURN DISTINCT e.qname, f.numeric_value AS value, p.end_date
ORDER BY p.end_date DESC LIMIT 40
```

Revenue by segment (dimensional breakdown):

```cypher
MATCH (f:Fact {has_dimensions: true})-[:FACT_HAS_ELEMENT]->(e:Element),
      (f)-[:FACT_HAS_DIMENSION]->(d:Dimension)
WHERE e.qname = 'us-gaap:Revenues' AND f.numeric_value IS NOT NULL
RETURN d.axis_uri, d.member_uri, f.numeric_value LIMIT 10
```

Two things every query should do: filter `Fact.has_dimensions = false` for consolidated totals (true when
you *want* segment breakdowns), and anchor on an entity, report, element, or date before reaching
`Structure` — an unanchored structure scan touches every filing.

## Versioning

- `sec.lbug.zst` is a fixed path. Each monthly snapshot overwrites it in a new commit whose title carries the
  snapshot date and engine version (`sec.lbug.zst: snapshot YYYY-MM-DD (ladybug X.Y.Z)`). `main` is
  always the latest.
- The engine version can move between snapshots. The commit title carries it; when it changes, install the
  matching `ladybug` before opening the new file (a RoboSystems checkout gets the new pin with the release
  that shipped it).
- Superseded snapshots are removed to keep the dataset one snapshot deep; pin a commit with
  `--revision` only while it is still listed.
- The hosted graph at robosystems.ai is rebuilt nightly; this file is that graph as of the snapshot date.

## Provenance and licence

This dataset — the compiled graph, its schema, and the cross-filer normalisation — is released under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/): use it for anything, including commercially, with
attribution (citation below). The source data is SEC EDGAR XBRL, public domain. The pipeline that parses,
normalises, classifies, and materialises it is [RoboSystems](https://github.com/RoboFinSystems/robosystems),
Apache-2.0. Facts are as filed: restatements appear as later
filings, not as corrections to earlier ones. Element names follow the filer's taxonomy (`us-gaap`,
`ifrs-full`, extensions); `canonical_concept` and `Structure.canonical_type` are RoboSystems' cross-filer
normalisation on top.

```
RoboSystems. SEC XBRL Knowledge Graph (LadybugDB). Hugging Face, snapshot {{snapshot_date}}.
https://huggingface.co/datasets/robosystems/sec-xbrl-knowledge-graphs
```

Issues and questions: [github.com/RoboFinSystems/robosystems/issues](https://github.com/RoboFinSystems/robosystems/issues).
