Start the development environment. This runs the full Docker stack with all services.

## Steps

1. Start Docker services with `just start`
2. Wait for services to be healthy
3. Show the API and Dagster logs

## Commands

```bash
just start
```

Wait 10 seconds for services to initialize, then tail the logs:

```bash
sleep 10 && just logs api &
```

## Services Started

- **API** (port 8000): Main RoboSystems API
- **Worker**: Background task processing
- **Graph API** (port 8001): LadybugDB graph database API
- **Dagster** (port 3001): Orchestration UI
- **PostgreSQL** (port 5432): Primary database
- **Valkey** (port 6379): Cache and queues

## Health Check

After starting, verify with:
```bash
curl http://localhost:8000/health
curl http://localhost:8001/status
```
