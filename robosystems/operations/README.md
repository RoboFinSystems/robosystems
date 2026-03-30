# Operations Directory

This directory contains business workflow orchestration and high-level operations that coordinate multiple components to accomplish complex business tasks. Operations represent the business logic layer that ties together adapters and database operations.

## 📂 Directory Structure

```
operations/
├── README.md                          # This file
├── __init__.py                        # Exports core operations
├── connection_service.py              # External service connection management
├── user_limits_service.py             # User limits and quota management
├── graph/                             # Graph database business operations
│   ├── graph_creation_service.py      # Unified graph creation pipeline (entity + generic)
│   ├── subscription_service.py        # Graph subscription management
│   ├── credit_service.py              # Credit system operations
│   ├── pricing_service.py             # Pricing and billing logic
│   ├── metrics_service.py             # Graph metrics and analytics
│   └── repository_subscription_service.py # Repository access management
├── lbug/                              # LadybugDB database infrastructure operations
│   ├── backup_manager.py              # Database backup operations
│   ├── backup.py                      # Backup service interface
│   └── ingest.py                      # Data ingestion operations
└── providers/                         # External provider integrations
    └── registry.py                    # Provider registry and management
```

## 🎯 Purpose

Operations orchestrate complex business workflows by:

- **Coordinating Components** - Bringing together adapters and databases
- **Implementing Business Logic** - High-level business rules and workflows
- **Managing State** - Handling complex stateful operations
- **Enforcing Policies** - Credit limits, access controls, and business policies
- **Providing Abstractions** - Simple interfaces for complex operations

## 🏗️ Key Operation Categories

### **Graph Database Operations** (`graph/`)

High-level business operations for graph database management:

- **`GraphCreationService`** - Unified graph creation pipeline (entity and generic graphs)
- **`SubgraphService`** - Subgraph lifecycle management (create, list, delete)
- **`GraphSubscriptionService`** - Billing subscription creation for graphs (uses billing namespace)
- **`CreditService`** - Credit-based billing and consumption tracking
- **`GraphPricingService`** - Dynamic pricing calculations
- **`GraphMetricsService`** - Analytics and performance metrics
- **`RepositorySubscriptionService`** - Shared repository access management

### **Infrastructure Operations** (`lbug/`)

Low-level database infrastructure management:

- **`BackupManager`** - Database backup and restore operations
- **`LadybugDBGraphBackupService`** - Backup service coordination
- **Data Ingestion** - S3-based data ingestion pipelines using COPY operations

### **Core Business Services**

Foundation services for the platform:

- **`ConnectionService`** - External service connection lifecycle
- **`UserLimitsService`** - User quota and limit enforcement
- **`ProviderRegistry`** - External provider management

## 🔧 Architecture Patterns

### 1. **Service Layer Pattern**

Operations act as the service layer that:

- Encapsulates business logic
- Coordinates multiple data sources
- Provides transaction boundaries
- Handles error scenarios

### 2. **Orchestration vs Integration**

- **Operations** (this directory): Orchestrate workflows and business logic
- **Adapters** (`/adapters/`): Connect to external services and transform data

### 3. **Multi-Tenant Architecture**

Operations handle multi-tenancy concerns:

- Database allocation and routing
- User access control
- Resource isolation
- Per-tenant configuration

### 4. **Credit-Based Billing**

Many operations integrate with the credit system:

- Operation cost calculation
- Credit consumption tracking
- Limit enforcement
- Usage analytics

## 🚀 Usage Patterns

### Service Instantiation

```python
from robosystems.operations import (
    CreditService,
    GraphCreationService,
    GraphCreationConfig,
    ConnectionService,
)

# Initialize services
credit_service = CreditService(user_id="123", graph_id="entity_456")
creation_service = GraphCreationService()
```

### Workflow Orchestration

```python
# Graph creation via unified service
async def create_entity_graph(user_id: str, entity_data: dict):
    service = GraphCreationService()
    result = await service.create(GraphCreationConfig(
        user_id=user_id,
        tier="ladybug-standard",
        graph_name=entity_data["name"],
        graph_type="entity",
        schema_extensions=["roboledger"],
        entity_data=entity_data,
        create_entity=True,
    ))
    # Credits and billing are handled by the worker task handler
    return result.graph_id
```

### Credit-Aware Operations

```python
# Operation with credit consumption
credit_service = CreditService(user_id, graph_id)

# Check if operation is allowed
cost_info = await credit_service.check_operation_cost("query_execution")
if not cost_info["sufficient_credits"]:
    raise InsufficientCreditsError()

# Perform operation and consume credits
result = await graph_service.execute_query(query)
await credit_service.consume_credits("query_execution", cost_info["cost"])
```

## 🔄 Integration Points

### Database Integration

Operations work with multiple database types:

- **PostgreSQL** - User accounts, subscriptions, billing
- **LadybugDB** - Graph data storage and querying
- **DynamoDB** - Database allocation and instance management
- **Valkey** - Caching and session management

### External Services

Operations coordinate with external services via adapters:

- **SEC EDGAR API** - Financial filing data
- **QuickBooks API** - Accounting data integration
- **AWS S3** - File storage and data lakes
- **Payment Providers** - Billing and subscription management

### Background Processing

Operations often trigger background tasks:

- Data ingestion pipelines
- Backup operations
- Credit allocation jobs
- Analytics processing

## 🎯 Key Benefits

### For Business Logic

- **Centralized** - All business rules in one place
- **Consistent** - Standardized operation patterns
- **Reliable** - Comprehensive error handling and rollback
- **Auditable** - Full operation logging and tracking

### For Development

- **Testable** - Business logic isolated from infrastructure
- **Maintainable** - Clear separation of concerns
- **Reusable** - Operations can be composed into larger workflows
- **Scalable** - Designed for high-volume operations

### For Operations

- **Observable** - Comprehensive logging and metrics
- **Debuggable** - Clear operation boundaries and state
- **Recoverable** - Transaction rollback and error recovery
- **Monitorable** - Health checks and performance metrics

## 📚 Related Components

- **Routers** (`/routers/`) - HTTP endpoints that call operations
- **Adapters** (`/adapters/`) - External service connections and data transformation
- **Models** (`/models/`) - Data structure definitions
- **Tasks** (`/tasks/`) - Background processing jobs

## 🔧 Development Guidelines

When creating new operations:

1. **Focus on Business Logic** - Implement business rules and workflows
2. **Coordinate Don't Transform** - Use adapters for data transformation
3. **Handle Errors Gracefully** - Comprehensive error handling and rollback
4. **Log Extensively** - Detailed operation logs for debugging
5. **Consider Credits** - Integrate with credit system where appropriate
6. **Make It Testable** - Dependency injection and mocking support
7. **Document Thoroughly** - Clear operation contracts and examples

## 🎯 Examples

### Graph Operations

```python
# Graph creation (entity or generic) via unified pipeline
from robosystems.operations.graph.graph_creation_service import (
    GraphCreationConfig,
    GraphCreationService,
)

service = GraphCreationService()
result = await service.create(GraphCreationConfig(
    user_id="user_123",
    tier="ladybug-standard",
    graph_name="My Company",
    graph_type="entity",
    schema_extensions=["roboledger"],
))
```

### Subgraph Management

```python
# Subgraph operations (requires ladybug-large or ladybug-xlarge tier)
from robosystems.operations.graph.subgraph_service import SubgraphService

service = SubgraphService()

# Create a subgraph for development environment
result = await service.create_subgraph_database(
    parent_graph_id="kg1234567890abcdef",
    subgraph_name="dev",
    schema_extensions=["analytics"]
)

# List all subgraphs for a parent
subgraphs = await service.list_subgraph_databases("kg1234567890abcdef")

# Get info about a specific subgraph
info = await service.get_subgraph_info("kg1234567890abcdef_dev")

# Delete subgraph with backup
await service.delete_subgraph_database(
    subgraph_id="kg1234567890abcdef_dev",
    force=True,
    create_backup=True
)
```

### Credit Management

```python
# Credit-aware operation
service = CreditService(user_id="user_456", graph_id="kg1a2b3c")
if await service.has_sufficient_credits("backup_creation"):
    await service.create_backup()
    await service.consume_credits("backup_creation")
```

### Connection Management

```python
# External service integration
service = ConnectionService(user_id="user_789")
connection = await service.create_connection("quickbooks", credentials)
await service.sync_data(connection)
```

This directory represents the business logic backbone of the RoboSystems platform, orchestrating complex workflows while maintaining separation of concerns and providing clean abstractions for business operations.
