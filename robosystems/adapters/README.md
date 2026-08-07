# Adapters

External service integrations: API clients, data transformation, and — for
shared repositories — a manifest declaring the repository's configuration. Each
adapter is self-contained, and Dagster assets drive them.

## Two kinds

**Shared repository adapters** ([`sec/`](sec/README.md)) serve platform-wide
public data. They carry a `manifest.py` declaring identity, billing plans, rate
limits, endpoint access, and credit costs. The manifest is the single source of
truth; `config/shared_repositories.py` collects manifests and exposes the query
API used by billing, middleware, and operations.

**Private adapters** (`quickbooks/`) integrate a per-user external service. They
have a client and a Dagster ELT pipeline but no manifest, because they operate
on individual user graphs rather than shared platform data.

## The manifest pattern

A `SharedRepositoryManifest` (defined in `base.py`) declares:

| Field | Purpose |
|-------|---------|
| `id`, `name`, `description` | Identity — `id` doubles as the `graph_id` |
| `data_source_type`, `data_source_url`, `sync_frequency` | Data source metadata |
| `schema_type`, `schema_extensions` | Graph schema configuration |
| `has_semantic_enrichment` | MCP capability flag |
| `plans` | Billing plans: pricing, credits, features |
| `rate_limits` | Per-plan limits (queries, MCP, agent, downloads) |
| `allowed_endpoints`, `blocked_endpoints` | Endpoint access control |
| `credit_costs` | Per-operation credit costs |
| `graph_tier`, `graph_instance_id` | Infrastructure placement |

The import chain avoids circular dependencies, and it only works because
`base.py` imports nothing from the rest of the codebase:

```
config/shared_repositories.py → adapters/{name}/manifest.py → adapters/base.py
```

The registry loads manifests lazily, on first access. Importing a manifest at
module scope from anywhere in the registry's own import path will reintroduce
the cycle.

## The adapters here

**SEC EDGAR** — XBRL financial filings, a shared repository. Clients, XBRL
processors, inline semantic enrichment, offline knowledge artifacts, canonical
taxonomy mappings, and a full Dagster pipeline. See
[`sec/README.md`](sec/README.md).

```python
from robosystems.adapters.sec import SECClient, XBRLGraphProcessor

client = SECClient()
filings = client.get_filings(cik="0000320193", form_type="10-K")
```

```bash
just sec-load NVDA 2025
```

**QuickBooks** — small business accounting, structured as a dbt-on-DuckDB ELT
pipeline:

- `client/api.py` — `QBClient`, the OAuth-authenticated QuickBooks Online API
  client. The only symbol exported from the package `__init__.py`.
- `pipeline/` — Dagster ELT: `extract.py` (CDC plus SyncToken delta sync),
  `transform.py` (invokes the dbt models), `load.py` (materializes the resulting
  DuckDB tables into the user's LadybugDB graph), `event_action_mapping.py` (QB
  transaction types → REA event/action), `configs.py`, `jobs.py` (`qb_sync_job`),
  `utils.py`.
- `dbt/` — staging and ledger models transforming raw QuickBooks data into the
  RoboLedger schema (transactions, entries, line items, elements, agents,
  dimensions).

```python
from robosystems.adapters.quickbooks import QBClient  # the only export

client = QBClient(realm_id="123456", qb_credentials=credentials)
```

QuickBooks-connected graphs support outbound write-back governed by
`Connection.write_policy` (`native` / `qb_authoritative` / `hybrid`; QB defaults
to `qb_authoritative`). Locally-authored events push back through the
`execute-event-block` operation, which stamps `metadata.qb_external_id` so the
next CDC delta sync de-duplicates against the source. The close-review outbox
surfaces `will_publish_to_qb` on the period-drafts read, so a user sees what
closing a period will write to QuickBooks before committing.

## Adapter structure

1. **Client** — API connection and authentication
2. **Processors** — transformation for graph ingestion
3. **Manifest** — shared repositories only
4. **Enrichment** — optional, inline semantic enrichment during processing
5. **Knowledge** — optional, offline corpus-level artifact generation
6. **Taxonomy** — optional, canonical concept and structure mappings
7. **Models** — optional, service-specific data models

Adding a shared repository adapter: create `adapters/{name}/`, write
`manifest.py` with a `SharedRepositoryManifest`, add the import and `_register()`
call to `_load_manifests()` in `config/shared_repositories.py`, add
`client/{api}.py` and `processors/{type}.py`, add `pipeline/` exposing
`get_dagster_components()` (copy the shape from
[`sec/pipeline/`](sec/pipeline/README.md)), export from `__init__.py`, add tests
under `tests/adapters/{name}/`, and import the pipeline in
`dagster/definitions.py`.

A private adapter is the same minus the manifest and the registry registration;
the pipeline is optional.

## Custom data sources: integrations first

**The supported route for connecting your own data source is an integration, not
an in-core adapter.** An integration is a program in its own repository that
writes through the public API with an API key — start from
[`robosystems-integration-template`](https://github.com/RoboFinSystems/robosystems-integration-template),
and see
[`robosystems-marketing-integration`](https://github.com/RoboFinSystems/robosystems-marketing-integration)
for a working one. Integrations survive every platform release, work identically
against managed and self-hosted deployments, and hold their own source
credentials. The platform's own adapters prove the surface: the QuickBooks
adapter writes through the same public event envelope an external integration
would use.

This directory is maintained by the platform team. Both classes of adapter keep
expanding, but platform-operated deployments run an unmodified core, so it is not
a contribution surface for custom sources. To request native support for a
source, open a [discussion](https://github.com/orgs/RoboFinSystems/discussions).

## Custom adapters on self-hosted forks

If you fork this repository and run your own deployment on your own
infrastructure, this directory is a merge boundary. In-core additions belong in
the `custom_*` namespace, which upstream never touches, so updates merge without
conflict:

```
adapters/
├── sec/           # upstream maintains
├── quickbooks/    # upstream maintains
└── custom_*/      # yours; upstream never touches
```

Create `adapters/custom_myservice/` with the same client/processors/pipeline
shape, expose `get_dagster_components()` returning assets, jobs, sensors, and
schedules, and import the pipeline in `dagster/definitions.py` (see the
`# === FORK` comment). Then:

```bash
git remote add upstream https://github.com/RoboFinSystems/robosystems.git
git fetch upstream
git merge upstream/main
```

Even on a self-hosted deployment, consider the integration route first — it
needs no fork at all.

## Related

- [`sec/README.md`](sec/README.md) — SEC adapter
- [`sec/pipeline/README.md`](sec/pipeline/README.md) — SEC Dagster orchestration
- [`../dagster/README.md`](../dagster/README.md) — pipeline orchestration patterns
- [`../schemas/README.md`](../schemas/README.md) — graph schema definitions
- [`../config/README.md`](../config/README.md) — shared repository registry
