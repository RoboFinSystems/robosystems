# Celery Tasks Organization

This directory contains all Celery tasks organized by functional domain for better maintainability and clear separation of concerns.

## Directory Structure

### Core Modules

#### `billing/`

Financial operations related to credits, usage tracking, and billing:

- `credit_allocation.py` - Monthly graph credit allocation and health checks
- `shared_credit_allocation.py` - Shared repository (SEC) credit allocation
- `credit_reservation_cleanup.py` - Cleanup expired credit reservations (runs every 15 min)
- `storage_billing.py` - Daily storage billing and monthly summaries
- `usage_collector.py` - Hourly storage usage data collection

#### `data_sync/`

External data source synchronization:

- `sec_filings.py` - SEC filing synchronization and processing
- `qb.py` - QuickBooks data synchronization
- `plaid.py` - Plaid financial data synchronization

#### `graph_operations/`

Kuzu graph database operations:

- `backup.py` - Graph backup creation and management
- `ingestion.py` - Kuzu database ingestion and data loading

#### `infrastructure/`

Infrastructure maintenance and monitoring:

- `auth_cleanup.py` - Expired API key and authentication cleanup (hourly)
- Note: All infrastructure monitoring has been migrated to Lambda functions:
  - Instance monitoring: `bin/lambda/kuzu_instance_monitor.py`
  - Worker monitoring: `bin/lambda/worker_monitor.py` (queue metrics, task protection)

#### `processing/`

Data processing and transformation:

- `entity.py` - Entity data processing tasks
- `agent.py` - Agent-related processing tasks
- `graph.py` - Graph processing and analytics operations

### Configuration Files

#### `schedule.py`

Centralized Celery Beat schedule configuration defining all periodic tasks, their schedules, and priorities.

## Task Organization Principles

### Queue Strategy

All tasks currently use a single queue (`WORKER_QUEUE` from environment) but the structure supports future multi-queue deployments:

- Standard queue for general tasks
- Priority queues for time-sensitive operations
- Dedicated queues for heavy processing

### Task Priorities

Tasks are assigned priorities (1-10, where 10 is highest):

- **9-10**: Critical infrastructure (queue monitoring for autoscaling)
- **8**: Billing and credit operations
- **7**: Credit reservation cleanup
- **6**: Regular monitoring and data collection
- **5**: Health checks
- **4**: Maintenance and cleanup tasks

## Scheduled Tasks

### Regular Intervals

- Credit reservation cleanup (15 minutes) [Note: Moving to Lambda]
- API key cleanup (hourly)
- Storage usage collection (hourly)

### Daily Tasks

- Storage billing (2 AM)
- Stale graph entry cleanup (1 AM)

### Weekly Tasks

- Credit allocation health checks (Mondays)

### Monthly Tasks

- Shared credit allocation (configured day/hour)
- Graph credit allocation (30 min after shared)
- Monthly storage summary (2nd of month)

## Adding New Tasks

1. **Choose the appropriate domain directory** based on task functionality
2. **Add task to the module** with proper decorators and error handling
3. **Update `__init__.py`** in the domain directory to export the task
4. **Add to schedule.py** if it's a periodic task
5. **Set appropriate priority** based on task importance
6. **Document the task** purpose and parameters

## Task Best Practices

1. **Error Handling**: Use proper try/except blocks and log errors
2. **Idempotency**: Tasks should be safe to retry
3. **Timeouts**: Set appropriate task timeouts
4. **Monitoring**: Emit metrics for task performance
5. **Documentation**: Clear docstrings explaining task purpose
6. **Testing**: Write unit tests for task logic

## Future Enhancements

The current structure supports future enhancements:

- Multi-queue deployment for task isolation
- Worker pool specialization
- Dynamic priority adjustment
- Task dependency management
- Advanced retry strategies
