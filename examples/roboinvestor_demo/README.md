# RoboInvestor Demo — Meridian Ventures Fund I

The first demo that exercises the RoboInvestor surface, and the first that
crosses a graph boundary.

```bash
just start                  # local stack
just demo-user              # credentials → .local/config.json
just demo-roboinvestor      # provisions both graphs and runs everything
```

The run ends with a validation pass and exits non-zero if any invariant
fails, so it works as a pre-release gate as well as a walkthrough.

## What it builds

Two tenants, because the interesting part needs two:

| Graph | Who | Extensions | Role |
| --- | --- | --- | --- |
| Issuer | Cadence Labs, Inc. | `roboledger` | A seed-funded B2B SaaS company that keeps its books here and files an annual report |
| Investor | Meridian Ventures Fund I, LP | `roboinvestor`, `roboledger` | An early-stage venture fund holding five private instruments |

The issuer is the existing `saas_startup` showcase scenario, provisioned
inline on the first run and reused afterwards. Both graphs belong to the
same user and org — the boundary being crossed is the **graph** boundary,
which is where report-sharing authorization actually lives.

The fund holds private-company ownership, not listed tickers: a Series A
preferred, a bridge warrant, a post-money SAFE, LLC units, and a seed
position that gets disposed mid-run. Two of them — the Cadence
instruments — name an issuer that is also a tenant here.

## The arc

**1 · Pre-association.** The Cadence securities are registered with
`source_graph_id` and no `entity_id`. That is declared intent before any
link exists, and it is a first-class state: `holdings` reports the issuer
as unlinked rather than erroring.

**2 · The Portfolio Block is the write surface.** `create-portfolio-block`
validates the portfolio and its positions whole and writes them in one
transaction. `update-portfolio-block` then applies `add` / `update` /
`dispose` deltas in a single atomic call — two new positions, a 409A
re-mark, and an exit. Positions are never mutated as standalone atoms.
Deleting a portfolio that still holds active positions is refused with a
409 unless `confirm_active_positions` is set.

**3 · Every write marks its graph stale.** Read back through the graph
health endpoint before any materialization. Materialization is
sensor-driven on that flag alone; RoboInvestor shipped with zero of its
six operations setting it, which kept the entire domain out of LadybugDB.
The check asserts both that the graph is stale *and* that the reason came
from a RoboInvestor operation — a graph that also keeps books would
otherwise be marked by its ledger writes and mask the defect.

**4 · The handshake.** Cadence creates a publish list, adds the fund's
graph, and shares its filed report. Each share is an independent copy: the
report row, a cross-graph fact set, and every fact land in the fund's own
schema. The share also creates a linked `Entity` for Cadence in the fund's
graph and resolves **every** security that pre-associated to it — both
instruments, from one share.

**5 · The traversal.** After materialization, one Cypher query walks the
whole chain:

```
Portfolio → Position → Security → Entity → Report → Fact
```

A private holding joined to its issuer's reported financials, in one
query. Because SEC filings are a shared repository on the same platform, a
private position and public-company facts sit in the same query surface —
which is the thing that has no equivalent outside this platform.

**6 · Revocation** (`--revoke`, opt-in). The sender withdraws the copy.
The report leaves the fund's schema; the linked entity and the securities
pointing at it stay. A declared holding is a relationship, not an artifact
of one filing.

## Flags

```bash
just demo-roboinvestor                  # everything
just demo-roboinvestor <graph_id>       # reuse a specific investor graph
just demo-roboinvestor --issuer <id>    # use a specific issuer graph
just demo-roboinvestor --reload-issuer  # rebuild the issuer's ledger first
just demo-roboinvestor --skip-share     # portfolio surface only, no handshake
just demo-roboinvestor --revoke         # also withdraw the share at the end
just demo-roboinvestor --dry-run        # preview the portfolio, write nothing
```

Offline preview of the portfolio arc, no platform needed:

```bash
uv run python -m examples.roboinvestor_demo.data
```

## Why the investor graph carries `roboledger`

`add-publish-list-members` rejects any target graph whose
`schema_extensions` doesn't list `roboledger`. A fund keeps no books, so
that predicate really wants to be "target can receive reports" — until it
is, a graph that only wants to *receive* reporting has to declare the
ledger extension it will never write to. The demo provisions both rather
than working around it, so the constraint stays visible.

## Re-runs

`_reset.py` wipes the fund's portfolios, securities, positions, anything
the handshake delivered, and the issuer's publish lists — the only
direct-DB step in the demo, and deliberately not a product operation.
Everything else goes through the HTTP API via the SDK facades, the same
surface the frontends and MCP tools use.

Graph ids are cached in `.local/config.json` under the `roboinvestor_demo`
and `saas_startup` slots, so re-runs reuse both graphs.

## Related

- [`examples/saas_startup_demo/`](../saas_startup_demo/) — the issuer
