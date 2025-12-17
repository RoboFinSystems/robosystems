# Celery Tasks Organization

This directory contains Celery tasks for real-time, user-triggered operations that require SSE (Server-Sent Events) feedback.

## Migration to Dagster

Most tasks have been migrated to Dagster for better orchestration and observability:

| Category | Old Location | New Location |
|----------|--------------|--------------|
| Billing | `tasks/billing/` | `dagster/jobs/billing.py` |
| Infrastructure | `tasks/infrastructure/` | `dagster/jobs/infrastructure.py` |
| Data Sync (SEC) | `tasks/sec_xbrl/` | `dagster/assets/sec.py` |
| Data Sync (Plaid) | `tasks/data_sync/plaid.py` | `dagster/assets/plaid.py` |
| Provisioning | Various | `dagster/jobs/provisioning.py` |

## Remaining Celery Tasks

The following tasks remain in Celery for real-time SSE feedback:

### `graph_operations/`

User-triggered graph database operations with SSE progress:

- `backup.py` - Graph backup creation
- `create_graph.py` - Graph database creation
- `create_subgraph.py` - Subgraph creation
- `create_entity_graph.py` - Entity graph creation

### `table_operations/`

User-triggered table operations with SSE progress:

- `duckdb_staging.py` - DuckDB staging table creation
- `graph_materialization.py` - Graph materialization from staging

### `agents/`

Real-time AI operations:

- `analyze.py` - AI-powered analysis with streaming responses

## Why Celery for These Tasks?

These tasks require:
1. **Real-time SSE feedback** - Progress updates streamed to UI
2. **User-triggered execution** - Not scheduled, but on-demand
3. **Immediate response** - User waiting for result

Dagster is better for:
- Scheduled/batch operations
- Data pipelines with dependencies
- Operations that don't need real-time UI feedback

## Task Best Practices

1. **Error Handling**: Use proper try/except blocks and log errors
2. **SSE Updates**: Send progress updates via SSE for long-running tasks
3. **Idempotency**: Tasks should be safe to retry
4. **Timeouts**: Set appropriate task timeouts
5. **Documentation**: Clear docstrings explaining task purpose
