---
description: Start the local development stack and tail service logs.
---

Start the development environment — the full Docker stack under the `robosystems` profile.

## Start

```bash
just start
```

Always use the `robosystems` profile (the default), never an individual service profile. Add `just start robosystems build` to rebuild images as part of the start.

## Services

The `robosystems` profile brings up:

| Service | Port | Role |
| --- | --- | --- |
| `api` | 8000 | Main RoboSystems API |
| `graph-api` | 8001 | LadybugDB graph database API |
| `dagster-webserver` | 8002 | Orchestration UI |
| `dagster-daemon` | — | Schedules and sensors (runs migrations on boot) |
| `worker` | — | Background task processing |
| `pg` | 5432 | PostgreSQL (platform + extensions databases) |
| `valkey` | 6379 | Cache and queues |
| `localstack` | 4566 | Local AWS emulation |
| `opensearch` | 9200 | Full-text + semantic search |

The frontend apps (`robosystems-app`, `roboledger-app`, `roboinvestor-app`) are on the `apps` profile and are **not** started by `robosystems`. Observability (`otel-collector`, `prometheus`, `grafana`) is on the `observability` profile.

Confirm what actually came up rather than assuming:

```bash
docker compose -f compose.yaml ps
```

## Health checks

```bash
curl http://localhost:8000/v1/status     # API — liveness only
curl http://localhost:8001/health        # Graph API
```

`/v1/status` is a **liveness** probe: it reports healthy whenever the process is up and does not check PostgreSQL, Valkey, or the graph tier. Note the API path is `/v1/status` — `/health` and `/v1/health` do not exist on the main API.

## Logs

```bash
just logs api                 # follow one service
just logs dagster-daemon      # migrations + schedules land here
just logs-grep worker ERROR   # search a service's logs
```

Run log tailing as a background command rather than backgrounding it with `&` — a foreground follow will block.

## When it doesn't come up

- Code changes not picked up → `just restart`
- Dependency or Dockerfile changes → `just rebuild`
- Taxonomy/framework source edits → `just reset-local` (tears down, wipes data, rebuilds, reseeds)
