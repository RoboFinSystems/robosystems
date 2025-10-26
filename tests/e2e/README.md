# End-to-End (E2E) Tests

## Overview

These tests validate complete workflows through the entire RoboSystems stack by making **real HTTP requests** to a running API server at `localhost:8000`. They are NOT run by default in the standard test suite.

## Why Separate E2E Tests?

These tests require:
- Full Docker stack running (`just start robosystems`)
- Real Celery workers processing async tasks
- All services running (PostgreSQL, Valkey, S3, Graph API)
- Actual network calls and timing-dependent operations

Unlike integration tests (which use FastAPI's TestClient and mocks), these tests validate:
- Complete user workflows (registration → graph creation → data upload → querying)
- Async task processing with real Celery workers
- Multi-service coordination
- Real S3 uploads and database operations

## Running E2E Tests

### Prerequisites

1. **Start the full Docker stack:**
   ```bash
   just start robosystems
   ```

2. **Verify services are healthy:**
   ```bash
   just logs api
   just logs worker
   just graph-health
   ```

### Run E2E Tests

```bash
# Run only e2e tests
just test-e2e

# Or directly with pytest
uv run pytest -m e2e
```

### Skip E2E Tests

E2E tests are excluded by default from:
- `just test` - Unit and fast tests only
- `just test-all` - Includes integration but NOT e2e
- `just test-integration` - Integration tests only

To explicitly skip e2e tests:
```bash
uv run pytest -m "not e2e"
```

## Test Structure

### Current Tests

1. **`test_data_ingestion.py`** - Complete data ingestion workflow
   - User registration and authentication
   - Graph creation (async with SSE)
   - Parquet file upload to S3
   - DuckDB staging table creation
   - Data ingestion into Kuzu graph
   - Cypher query execution
   - Edge cases (empty files, schema validation, concurrent queries)

## When to Run E2E Tests

**Run locally when:**
- Testing complete workflows before deployment
- Validating async task processing
- Debugging multi-service issues
- Verifying Docker stack configuration

**Do NOT run in:**
- CI/CD pipelines (too slow, complex setup)
- Quick development iterations
- Unit test TDD cycles

## Development Notes

- These tests make real HTTP calls via `httpx.Client` (not TestClient)
- They mirror the behavior of `robosystems/scripts/e2e_workflow_demo.py`
- Timing-dependent (async operations, SSE polling)
- Cleanup is best-effort (graphs may need manual cleanup)

## Troubleshooting

**Tests timeout or fail:**
- Check Docker containers are running: `docker ps`
- Check API logs: `just logs api`
- Check worker logs: `just logs worker`
- Verify database: `just db-info`
- Check graph API: `just graph-health`

**Tests hang at graph creation:**
- Celery workers may not be processing tasks
- Check worker logs for errors
- Verify Valkey connection

**S3 upload fails:**
- LocalStack may not be running
- Check endpoint: `http://localhost:4566`
- Verify bucket exists
