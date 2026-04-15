# API Models Directory

This directory contains centralized Pydantic models for all REST API request and response structures in the RoboSystems service.

## 📂 Directory Structure

```
models/api/
├── README.md                   # This file
├── __init__.py                 # Exports all API models
├── agent.py                    # AI agent interaction models
├── auth.py                     # Authentication models (login, register, SSO)
├── billing.py                  # Billing and payment models
├── common.py                   # Shared models (errors, pagination, health)
├── connection.py               # External service connection models
├── credits.py                  # Credit system models
├── graph.py                    # Graph database operation models
├── mcp.py                      # Model Context Protocol models
├── oauth.py                    # OAuth integration models
├── subscription.py             # Subscription management models
├── task.py                     # Background task models
└── user.py                     # User profile and management models
```

## 🎯 Purpose

This directory centralizes all Pydantic models used for:

- **Request validation** - Ensuring API requests have correct structure and types
- **Response serialization** - Providing consistent API response formats
- **OpenAPI documentation** - Automatic generation of API documentation
- **Type safety** - Static type checking and IDE support
- **Reusability** - Sharing models across multiple router endpoints

## 📋 Model Categories

### Core Business Models

- **`user.py`** - User profiles, API keys, usage analytics
- **`graph.py`** - Cypher queries, backups, metrics, schema operations

### Authentication & Authorization

- **`auth.py`** - Login, registration, JWT tokens, SSO flows
- **`oauth.py`** - OAuth provider integrations (QuickBooks, etc.)

### Financial & Billing

- **`credits.py`** - Credit balances, transactions, storage limits
- **`subscription.py`** - Repository subscriptions, tier management
- **`billing.py`** - Payment processing, subscription upgrades

### Integration & Connectivity

- **`connection.py`** - External service connections (SEC, QuickBooks)
- **`mcp.py`** - Model Context Protocol for AI interactions
- **`agent.py`** - AI agent requests and responses

### Infrastructure & Operations

- **`task.py`** - Background task status and monitoring
- **`common.py`** - Error responses, pagination, health checks

## 🏗️ Architecture Principles

### 1. **Centralization**

All API models are centralized here instead of being scattered across router files. This provides:

- Single source of truth for API contracts
- Easier maintenance and updates
- Better reusability across endpoints

### 2. **Separation of Concerns**

- **Models** (this directory): Pydantic request/response data structures
- **Routers** (`/routers/`): Business logic and endpoint handlers
- **Operations** (`/operations/`): Core business operations
- **Platform SQLAlchemy Models** (`/models/core/`): Platform database entities (users, orgs, graphs, billing, connections, documents)
- **Extensions SQLAlchemy Models** (`/models/extensions/`): Per-graph OLTP entities for roboledger and roboinvestor (schema-per-graph tenancy)

### 3. **Consistency**

All models follow consistent patterns:

- Clear docstrings with purpose descriptions
- Proper Field(...) definitions with descriptions
- Type hints for all properties
- Validation where appropriate

### 4. **Documentation**

Models automatically generate OpenAPI documentation with:

- Request/response schemas
- Field descriptions and examples
- Validation constraints
- Deprecation notices

## 🔧 Usage Patterns

### Importing Models

```python
# Import from centralized location
from robosystems.models.api.credits import CreditSummaryResponse
from robosystems.models.api.subscription import SubscriptionRequest
from robosystems.models.api.common import ErrorResponse

# Use in router endpoints
@router.get("/summary", response_model=CreditSummaryResponse)
async def get_credit_summary(...):
    return CreditSummaryResponse(...)
```

### Creating New Models

When creating new API endpoints:

1. **Determine the category** - Which file should contain your model?
2. **Create the model** - Add it to the appropriate file
3. **Export it** - Add to `__init__.py` and `__all__` list
4. **Use in router** - Import and use in your endpoint
5. **Document it** - Add clear docstrings and field descriptions

### Model Naming Conventions

- **Request models**: `*Request` (e.g., `SubscriptionRequest`)
- **Response models**: `*Response` (e.g., `CreditSummaryResponse`)
- **Info models**: `*Info` (e.g., `RepositoryPlanInfo`)
- **Summary models**: `*Summary` (e.g., `CreditSummary`)

## 🚀 Benefits

### For Developers

- **Type Safety**: Full IDE support with autocomplete and type checking
- **Validation**: Automatic request/response validation
- **Documentation**: Self-documenting API contracts
- **Reusability**: Models can be shared across multiple endpoints

### For API Consumers

- **Consistent Responses**: All endpoints follow the same response patterns
- **Clear Documentation**: OpenAPI docs generated from model definitions
- **Predictable Structure**: Similar operations have similar response formats

### For Maintenance

- **Single Location**: All API changes happen in one place
- **Version Control**: Clear history of API contract changes
- **Refactoring**: Easy to update models across all usages

## 📚 Related Documentation

- **Router Implementation**: `/robosystems/routers/` - Business logic and endpoint handlers
- **Platform SQLAlchemy Models**: [`/robosystems/models/core/README.md`](../core/README.md) - Platform database entities
- **Extensions SQLAlchemy Models**: [`/robosystems/models/extensions/README.md`](../extensions/README.md) - Extensions OLTP entities with schema-per-graph tenancy
- **Operations Layer**: `/robosystems/operations/` - Core business operations
- **Extensions GraphQL**: [`/robosystems/graphql/README.md`](../../graphql/README.md) - How Pydantic response models in this directory get auto-derived into Strawberry GraphQL types
- **OpenAPI Schema**: Generated automatically from these models at runtime

## 🔄 Migration Notes

This directory was created by consolidating Pydantic models that were previously defined inline within router files. The migration provides:

- **Better organization** - Models grouped by functional area
- **Improved maintainability** - Centralized location for API contracts
- **Enhanced reusability** - Models can be shared across routers
- **Cleaner routers** - Router files focus on business logic, not data structures

When adding new API endpoints, always check this directory first to see if appropriate models already exist before creating new ones.
