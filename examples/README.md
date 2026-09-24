# RoboSystems Examples

Runnable demos that load real and synthetic data into RoboSystems and query it
back. Each one is a working reference for a different slice of the platform:
double-entry accounting on a graph, a venture portfolio with a cross-graph
report share, SEC XBRL filings, a custom domain schema, and cross-taxonomy
financial reporting.

## Prerequisites

```bash
just start        # local stack: API, PostgreSQL, Valkey, LadybugDB
just demo-user    # provisions a user + API key into .local/config.json
```

Every demo reads its credentials from `.local/config.json`, so `just demo-user`
only needs to run once. Demos that call an AI Operator (`--ai`) additionally
need AWS Bedrock configured; without it, use the default hardcoded mappings.

## Quick Start

```bash
# Run the whole suite in sequence (long).
just demo

# Or pick one
just demo-roboledger
just demo-roboinvestor
just demo-custom-graph
just demo-sec --ticker NVDA --year 2025
```

`just demo` runs `demo-user` first, so it is self-contained on a fresh
checkout, and orders the rest so each demo can reuse what the previous one
built — `demo-saas-startup` runs before `demo-roboinvestor`, which adopts the
cached Cadence Labs graph as its issuer instead of provisioning a second one.
Every demo caches its graph in `.local/config.json` under its own slot, so
re-running is idempotent rather than additive.

**The SEC demo is deliberately not in `just demo`.** It drops and recreates the
whole local `sec` graph, which would silently discard a corpus that took hours
to build or 35 GiB to download. Populate that graph on its own terms —
`just sec-dump` for the prebuilt public dump, or the `sec-*` pipeline recipes to
build it from scratch — and run `just demo-sec` by itself when you want the
walkthrough.

## The Demos

### RoboLedger — end-to-end accounting workflow

`examples/roboledger_demo/` · [walkthrough](roboledger_demo/README.md)

The full RoboLedger arc on synthetic data for a boutique consulting firm
(Cascade Advisory Group LLC): bulk OLTP import, taxonomy and schedule blocks, a
fiscal calendar, a filed annual report, and an AI-driven month-end close. Data
is generated on a rolling 16-month window ending at the current month, so the
demo always covers recent history.

Everything loads through the same HTTP API the frontend UI and MCP tools use —
the demo deliberately does not write to the database directly, because the
point is to emulate data arriving from outside the system the way a real
customer integration would.

```bash
just demo-roboledger                  # create graph, load data, file the report
just demo-roboledger <graph_id>       # load into an existing graph
just demo-roboledger --skeleton       # user + empty graph only (connect QuickBooks by hand)
just demo-roboledger --ai             # map the CoA with the MappingOperator (needs Bedrock)
just demo-roboledger --dry-run        # validate the generated data, write nothing
```

You get a graph with 27 accounts, 17 REA agents, ~305 typed business events, 6
depreciation/amortization schedules, 4 policy documents, and a filed FY report
— plus exactly one period queued and ready for the AI close.

### Showcase scenarios — Driftline Coffee and Cadence Labs

`examples/coffee_roaster_demo/` · `examples/saas_startup_demo/`

Two synthetic companies built on the same scenario engine as the RoboLedger
demo, each authored to make one accounting story fall out of the numbers rather
than be asserted in prose:

- **Driftline Coffee Roasters** is *profitable but cash-poor*. The income
  statement glows while cash drains into green-coffee inventory and one
  slow-paying wholesale account. The working-capital squeeze emerges
  mechanically from the gap between revenue recognition and cash collection.
- **Cadence Labs** is a seed-funded B2B SaaS startup *burning cash behind a
  deferred-revenue float*. Customers pay annually up front, so the bank balance
  looks like comfortable runway until you net out the service still owed.

```bash
just demo-coffee-roaster              # Driftline: profitable-but-cash-poor
just demo-saas-startup                # Cadence: burn masked by deferred revenue
```

Both accept the same flags as the RoboLedger demo (`[graph_id]`, `--ai`,
`--dry-run`). Each also has an offline preview that renders the arc without the
platform running:

```bash
uv run python -m examples.coffee_roaster_demo.data
uv run python -m examples.saas_startup_demo.data
```

#### Loading an episode into a deployed environment

Because every step but the reset is an ordinary API call, an episode can load
into a deployed environment as easily as the local stack, rather than being
something that only exists on a laptop. Point `DEMO_API_URL` at the API and
pass the graph id of an already-provisioned graph:

```bash
DEMO_API_URL=https://<your-api-host> just demo-coffee-roaster kg…
```

Three things differ off-local, all deliberate:

- **Credentials and the demo-slot map** go to `.local/config.<host>.json`
  instead of `.local/config.json`. The slot map is keyed on the scenario slug
  and the runner reuses a graph on a hit, so a shared file would make a later
  local run silently operate on the remote graph.
- **The reset is skipped.** It issues raw `DELETE`s against whatever
  `EXTENSIONS_DATABASE_URL` names, which an SSM tunnel can make look local, so
  it is never run against a remote target. Give the episode a freshly
  provisioned graph — re-running it over one that already holds demo data will
  duplicate the data rather than replace it.
- **The graph must already exist.** Provision it the way a customer would
  (checkout, or `POST /v1/graphs` with a payment method on file) and pass its
  id; the runner will not create one on a deployed environment.

### RoboInvestor — a venture portfolio, and a report that crosses graphs

`examples/roboinvestor_demo/` · [walkthrough](roboinvestor_demo/README.md)

The investment side, and the only demo that spans two tenants. Meridian
Ventures Fund I holds five private instruments — a Series A preferred, a
bridge warrant, a post-money SAFE, LLC units, and a seed position that
gets disposed mid-run. Two of them are issued by Cadence Labs, which keeps
its books on this platform, so the fund declares the relationship with
`source_graph_id` before any link exists.

Cadence then shares its filed annual report into the fund's graph. That
share creates a linked entity, resolves both pre-associated securities,
and makes one query possible:

```
Portfolio → Position → Security → Entity → Report → Fact
```

A private holding joined to its issuer's reported financials — the thing
a single-tenant product cannot offer.

```bash
just demo-roboinvestor                  # provisions both graphs, runs everything
just demo-roboinvestor --skip-share     # portfolio surface only, no handshake
just demo-roboinvestor --revoke         # also withdraw the share at the end
just demo-roboinvestor --dry-run        # preview the portfolio, write nothing
```

The issuer is the `saas_startup` scenario, provisioned inline on the first
run and reused afterwards. The run ends with a hard validation pass and
exits non-zero if any invariant fails — including that every RoboInvestor
write marked its graph stale, which is the check that materialization
depends on.

### SEC — public company financial data

`examples/sec_demo/` · [walkthrough](sec_demo/README.md)

Loads real 10-K/10-Q XBRL filings from EDGAR into the shared SEC repository,
subscribes the demo user to it, and runs example queries over the resulting
facts. Works for any US public company with SEC filings (AAPL, MSFT, GOOGL,
TSLA, NVDA, …).

```bash
just demo-sec --ticker NVDA --year 2025          # load + query
just demo-sec --ticker NVDA --year 2025 --skip-queries
just demo-sec-subscribe                          # subscription only, no data load
just demo-sec-subscribe sec-advanced             # higher rate limits, more credits
```

Query the loaded data separately:

```bash
just demo-sec-query --list                       # show available presets
just demo-sec-query --preset <NAME>              # run one preset
just demo-sec-query --all                        # run them all
just demo-sec-query --search "revenue recognition"
```

The subscription must exist before queries will resolve — `just demo-sec`
creates it for you as part of the run.

### Custom Graph — your own schema

`examples/custom_graph_demo/` · [walkthrough](custom_graph_demo/README.md)

A domain-neutral example: people, companies, and projects defined by a custom
schema in `schema.json`. Use it as the template for modelling your own domain.
It generates 50 Person, 10 Company, and 15 Project nodes wired together by
employment, project-team, and sponsorship relationships, then queries them back
as org charts and collaboration graphs.

```bash
just demo-custom-graph                    # reuse existing user + graph
just demo-custom-graph --new-graph        # new graph for the existing user
just demo-custom-graph --new-user         # new user (implies --new-graph)
just demo-custom-graph --skip-queries     # load only, no verification queries
```

Edit `schema.json` to define your own node types and relationships.

## Credentials

```bash
just demo-user                                        # create or reuse
just demo-user --name "Your Name" --email you@example.com
just demo-user --force                                # discard and re-provision
```

Credentials live in `.local/config.json` and are shared by every demo, which
also records a graph ID per demo slot there so re-runs reuse the same graph.

`--force` provisions a *new* user: graphs created by the previous user stay
where they are and are no longer reachable with the new API key.

## The Ingestion Pipeline

The bulk-load demos all follow the same five-step path, which is the same one
production ingestion uses — this is why they stage through S3 rather than
inserting rows directly:

1. **Generate Parquet files** — node and relationship tables
2. **Upload to S3** — via presigned URLs issued by the API
3. **Create staging tables** — load the Parquet files into DuckDB staging
4. **Validate** — query the staging tables with SQL before committing
5. **Ingest to graph** — load from staging into the graph database

Step 4 is the reason for the detour: staging gives you a place to check the
data *before* it becomes graph state.

## Running Individual Steps

Each demo's `main.py` runs every step in order. To drive them one at a time:

```bash
cd examples/custom_graph_demo
uv run setup_credentials.py
uv run create_graph.py
uv run generate_data.py --regenerate
uv run upload_ingest.py
uv run query_graph.py --all
uv run upload_documents.py
uv run memory_subgraph.py
```

Data is regenerated on every run so the Parquet identifiers line up with the
current graph — that is why `generate_data.py` comes after `create_graph.py`.

The `just` recipes remain the recommended path — they set `UV_ENV_FILE` so the
scripts pick up `ROBOSYSTEMS_API_URL` and the other settings from `.env.local`.

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| "API connection failed" | The stack isn't up — run `just start` |
| "Permission denied" / 401 | Stale or missing API key in `.local/config.json` — re-run `just demo-user` |
| "User already exists" | Expected; demos reuse the existing user. Pass `--new-user` to force a fresh one |
| "Graph already exists" | Expected; demos reuse the recorded graph. Pass `--new-graph` for a fresh one |
| `--ai` step fails | Bedrock isn't configured. Drop `--ai` to use the hardcoded mappings |

Logs: `just logs api`, `just logs worker`.

## Related Documentation

- [Main README](../README.md) — platform overview and setup
- [API reference](https://robosystems.ai/docs/api) — every REST endpoint, with its schemas
- [Graph API README](../robosystems/graph_api/README.md) — graph database system
- [Schema System](../robosystems/schemas/README.md) — schema definitions
- [Documentation](https://robosystems.ai/docs/technical) — guides and tutorials
- SDKs: [Python](https://github.com/RoboFinSystems/robosystems-python-client) ·
  [TypeScript](https://github.com/RoboFinSystems/robosystems-typescript-client) ·
  [MCP](https://github.com/RoboFinSystems/robosystems-mcp-client)

## Support

- [GitHub Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Discussions](https://github.com/RoboFinSystems/robosystems/discussions)
