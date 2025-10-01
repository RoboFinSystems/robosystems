# IAM Models Directory

This directory contains **SQLAlchemy database models** that serve as the data layer for the RoboSystems platform. These models define the database schema and provide the foundation for **Alembic migrations** and all interactions between the application and **PostgreSQL** database.

## 📂 Directory Structure

```
models/iam/
├── README.md                     # This file
├── __init__.py                   # Exports all IAM models and enums
├── user.py                       # User authentication and profiles
├── user_api_key.py               # API key management
├── user_limits.py                # User quotas and limits
├── user_usage_tracking.py        # User-level usage analytics
├── graph.py                      # Graph database metadata
├── user_graph.py                 # User-graph relationships
├── graph_credits.py              # Credit-based billing system
├── graph_subscription.py         # Subscription management
├── graph_usage_tracking.py       # Graph-level usage analytics
├── graph_backup.py               # Database backup tracking
├── connection_credentials.py     # External service credentials
├── user_repository.py            # Shared repository access
└── user_repository_credits.py    # Repository-based credits
```

## 🎯 Purpose

This directory serves as the **single source of truth** for:

- **Database Schema Definition** - SQLAlchemy models that define PostgreSQL table structures
- **Alembic Migration Generation** - Models that drive automatic migration creation
- **Application Data Layer** - Python classes for database operations
- **Relationship Management** - Foreign keys and relationships between entities
- **Data Validation** - Column constraints and business rules
- **Query Interface** - Methods for common database operations

## 🏗️ Architecture Pattern

### SQLAlchemy ORM Integration

All models inherit from the shared `Base` class (aliased as `Model`) which provides:

```python
from robosystems.database import Model

class User(Model):
    __tablename__ = "users"
    # Column definitions...
```

### Alembic Migration System

These models are the **foundation for Alembic migrations**:

1. **Model Changes** → Update SQLAlchemy model definitions
2. **Generate Migration** → `alembic revision --autogenerate -m "description"`
3. **Review Migration** → Verify generated SQL in `/alembic/versions/`
4. **Apply Migration** → `alembic upgrade head` (or via `just migrate-up`)

### Database Connection

Models use the centralized database configuration:

- **Development**: Direct PostgreSQL connection (localhost:5432)
- **Production/Staging**: SSL-enabled RDS connections
- **Connection Pooling**: Managed via SQLAlchemy engine configuration
- **Session Management**: Scoped sessions for request isolation

## 📋 Model Categories

### 1. **User Management**

Core user authentication and profile management:

- **`User`** - Primary user accounts with authentication
- **`UserAPIKey`** - API key generation and management
- **`UserLimits`** - User quotas, rate limits, and restrictions

### 2. **Graph Database System**

Multi-tenant graph database infrastructure:

- **`Graph`** - Graph database metadata and schema configuration
- **`UserGraph`** - User ownership and permissions for graphs
- **`GraphCredits`** - Credit-based billing and consumption tracking
- **`GraphCreditTransaction`** - Individual credit transaction records
- **`GraphSubscription`** - Subscription tiers and billing plans
- **`GraphBackup`** - Database backup and restore operations

### 3. **Usage Analytics**

Comprehensive usage tracking and analytics:

- **`UserUsageTracking`** - User-level API usage and analytics
- **`GraphUsageTracking`** - Graph-level database usage metrics

### 4. **External Integrations**

Service connections and shared repositories:

- **`ConnectionCredentials`** - Encrypted storage for external API credentials
- **`UserRepository`** - Access to shared repositories (SEC, industry data)
- **`UserRepositoryCredits`** - Credit tracking for shared repository usage
- **`UserRepositoryCreditTransaction`** - Shared repository credit transactions

## 🔧 Key Features

### 1. **Multi-Tenant Architecture**

The schema supports complete multi-tenancy:

```python
# Each user can own multiple graphs
user = User.get_by_id("user_123", session)
user_graphs = user.user_graphs  # List of graphs owned by user

# Each graph has independent credit tracking
graph_credits = GraphCredits.get_by_graph_id("entity_456", session)
```

### 2. **Credit-Based Billing**

Sophisticated credit system for usage-based billing:

```python
# Reserve credits atomically
credits = GraphCredits.get_by_graph_id("kg1a2b3c", session)
result = credits.reserve_credits_atomic(
    amount=Decimal("10.5"),
    operation_type="query_execution",
    session=session,
    reservation_id="op_uuid"
)

# Confirm or cancel reservation
if operation_successful:
    credits.confirm_credit_reservation(reservation_id, operation_type, session)
else:
    credits.cancel_credit_reservation(reservation_id, session, "operation_failed")
```

### 3. **Relationship Management**

Comprehensive foreign key relationships:

```python
# User → Multiple Graphs → Multiple Subscriptions
user.user_graphs[0].graph.graph_subscriptions[0].plan.name

# Graph → Credit Tracking → Transaction History
graph.user_graphs[0].graph_credits.transactions[-1].description
```

### 4. **Audit Trail**

Complete audit tracking with timestamps:

- **`created_at`** - Record creation timestamp (UTC)
- **`updated_at`** - Last modification timestamp (UTC)
- **Transaction Records** - Full history of credit transactions
- **Metadata Storage** - JSONB fields for flexible data storage

## 🚀 Usage Patterns

### Model Import and Usage

```python
# Import from centralized location
from robosystems.models.iam import (
    User,
    Graph,
    GraphCredits,
    GraphCreditTransaction,
    UserGraph
)

# Create new user
user = User.create(
    email="user@example.com",
    name="John Doe",
    password_hash="hashed_password",
    session=session
)

# Create graph database
graph = Graph.create(
    graph_id="kg1a2b3c",
    graph_name="ACME Corp",
    graph_type="entity",
    session=session
)

# Link user to graph
user_graph = UserGraph.create(
    user_id=user.id,
    graph_id=graph.graph_id,
    entity_id="123",
    session=session
)
```

### Migration Workflow

When making model changes:

1. **Update Model Definition**

   ```python
   # Add new column to existing model
   class User(Model):
       # ... existing columns ...
       new_field = Column(String, nullable=True)
   ```

2. **Generate Migration**

   ```bash
   DATABASE_URL=postgresql://user:pass@localhost:5432/robosystems \
   uv run alembic revision --autogenerate -m "add user new_field"
   ```

3. **Review Generated Migration**

   ```python
   # Check /alembic/versions/xxxxx_add_user_new_field.py
   def upgrade() -> None:
       op.add_column('users', sa.Column('new_field', sa.String(), nullable=True))
   ```

4. **Apply Migration**
   ```bash
   just migrate-up
   ```

### Query Patterns

```python
# User queries
user = User.get_by_email("user@example.com", session)
all_users = User.get_all(session)

# Graph queries with relationships
graphs_with_credits = (
    session.query(Graph)
    .join(UserGraph)
    .join(GraphCredits)
    .filter(UserGraph.user_id == user_id)
    .all()
)

# Credit transaction history
transactions = GraphCreditTransaction.get_transactions_for_graph(
    graph_credits_id="gc_kg1a2b3c",
    transaction_type=CreditTransactionType.CONSUMPTION,
    limit=50,
    session=session
)
```

## 📚 Database Integration

### PostgreSQL Features Used

- **JSONB Columns** - Flexible metadata storage with indexing
- **Foreign Key Constraints** - Referential integrity
- **Unique Constraints** - Data consistency (emails, API keys)
- **Check Constraints** - Business rule enforcement
- **Composite Indexes** - Query performance optimization
- **Partial Indexes** - Conditional indexing for efficiency

### Performance Optimizations

- **Strategic Indexing** - Optimized for common query patterns
- **Connection Pooling** - Managed via SQLAlchemy
- **Lazy Loading** - Relationships loaded on-demand
- **Query Optimization** - Efficient joins and filters
- **Atomic Operations** - Race condition prevention in credit system

## 🔄 Alembic Integration

### Migration Commands

```bash
# Generate new migration after model changes
DATABASE_URL=postgresql://user:pass@localhost:5432/robosystems \
uv run alembic revision --autogenerate -m "description of changes"

# Apply all pending migrations
just migrate-up

# Check current migration status
uv run alembic current

# Rollback last migration (use with caution)
uv run alembic downgrade -1
```

### Migration Best Practices

1. **Always use autogenerate** - Don't manually create migrations
2. **Review generated SQL** - Verify migration correctness
3. **Test migrations** - Use development database first
4. **Backup before production** - Always backup before applying
5. **Handle data migrations** - Add custom logic when needed

## 🎯 Key Benefits

### For Database Operations

- **Type Safety** - SQLAlchemy provides Python type hints
- **Relationship Management** - Automatic foreign key handling
- **Query Builder** - Pythonic database queries
- **Transaction Support** - ACID compliance and rollback support

### For Development

- **Schema Validation** - Models enforce data constraints
- **Migration Management** - Automatic schema versioning
- **IDE Support** - Full autocomplete and type checking
- **Testing Support** - Easy database mocking and fixtures

### For Operations

- **Audit Trail** - Complete transaction history
- **Performance Monitoring** - Query optimization insights
- **Data Integrity** - Constraint enforcement
- **Scalability** - Connection pooling and optimization

## 📚 Related Components

- **Database Configuration** (`/robosystems/database.py`) - SQLAlchemy engine and session setup
- **Alembic Migrations** (`/alembic/versions/`) - Generated migration files
- **API Models** (`/robosystems/models/api/`) - Pydantic models for API validation
- **Operations Layer** (`/robosystems/operations/`) - Business logic using these models
- **Routers** (`/robosystems/routers/`) - HTTP endpoints that query these models

## 🔧 Development Guidelines

When working with IAM models:

1. **Always use sessions** - Database operations require session context
2. **Handle exceptions** - Use try/catch for SQLAlchemy operations
3. **Use transactions** - Group related operations in transactions
4. **Follow naming conventions** - Consistent table and column naming
5. **Add proper indexes** - Optimize for expected query patterns
6. **Document relationships** - Clear docstrings for complex relationships
7. **Test thoroughly** - Unit tests for model operations

## 🚨 Important Notes

- **Never modify models directly in production** - Always use migrations
- **Test migrations thoroughly** - Use development environment first
- **Backup before major changes** - Protect against data loss
- **Monitor migration performance** - Large tables may require maintenance windows
- **Review autogenerated migrations** - Ensure correctness before applying

This directory represents the **foundational data layer** of the RoboSystems platform, providing reliable, scalable, and maintainable database operations through SQLAlchemy ORM and Alembic migration management.
