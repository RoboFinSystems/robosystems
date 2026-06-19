# API Models Directory

This directory contains centralized Pydantic models for all REST API request and response structures in the RoboSystems service.

## 📂 Directory Structure

```
models/api/
├── README.md                   # This file
├── __init__.py                 # Exports all API models
├── auth.py                     # Authentication models (login, register, SSO)
├── common.py                   # Shared models (errors, pagination, health)
├── entity_graph.py             # Entity graph operation models
├── event_block.py              # Event Block envelope models (REA business events)
├── event_handler.py            # Event handler (DSL rule) models
├── fact_provenance.py          # Typed FactProvenance discriminated union (active surface)
├── information_block.py        # InformationBlock envelope models (active surface)
├── library.py                  # Taxonomy/framework library models
├── oauth.py                    # OAuth integration models
├── orgs.py                     # Organization models
├── search.py                   # Document search models
├── taxonomy_block.py           # Taxonomy Block envelope models (CoA, custom ontology)
├── user.py                     # User profile and management models
├── admin/                      # Admin CLI / dashboard models (cache, credits, graphs, invoice, orgs, subscription, users)
├── billing/                    # Billing models (checkout, credits, customer, invoice, offering, subscription)
├── graphs/                     # Graph platform models (core, backups, connections, health, limits, mcp, metrics, operations, operator, query, schema, subgraphs, tables, tier)
├── views/                      # Analytical view models (fact_grid, view_config, view_response)
└── extensions/                 # RoboLedger + RoboInvestor request/response models
    ├── account_rollups.py
    ├── accounts.py
    ├── agent.py
    ├── ar_ap.py
    ├── closing_book.py
    ├── entity.py
    ├── fiscal_calendar.py
    ├── investor.py
    ├── journal_entries.py
    ├── publish_lists.py
    ├── report_package.py
    ├── reports.py
    ├── rollforward.py
    ├── schedules.py
    ├── summary.py
    ├── taxonomies.py
    ├── transactions.py
    └── trial_balance.py
```

The `extensions/` subdirectory holds the RoboLedger/RoboInvestor Pydantic models. These are the same models the GraphQL read surface auto-derives Strawberry types from — the [GraphQL README](../../graphql/README.md) cross-links them as `models/api/extensions/*`. `fact_provenance.py` (the typed `FactProvenance` union) and `information_block.py` (the IB envelope) are the active cross-domain block surface.

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
- **`orgs.py`** - Organization models
- **`entity_graph.py`** - Entity graph operation models

### Authentication & Authorization

- **`auth.py`** - Login, registration, JWT tokens, SSO flows
- **`oauth.py`** - OAuth provider integrations (QuickBooks, etc.)

### Block Envelopes & Provenance (active surface)

- **`information_block.py`** - InformationBlock envelope (Structure + atoms + FactSet)
- **`fact_provenance.py`** - Typed `FactProvenance` discriminated union (pivot/schedule/derived/asserted)
- **`event_block.py`** - Event Block envelope (REA business events)
- **`taxonomy_block.py`** - Taxonomy Block envelope (CoA, custom ontology)
- **`event_handler.py`** - Event handler (DSL rule) models

### Domain (RoboLedger / RoboInvestor)

- **`extensions/`** - Per-graph request/response models (entity, accounts, reports, schedules, journal entries, trial balance, investor, etc.) — see the subdirectory listed above

### Integration & Infrastructure

- **`library.py`** - Taxonomy/framework library models
- **`search.py`** - Document search models
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
from robosystems.models.api.information_block import InformationBlockEnvelope
from robosystems.models.api.common import ErrorResponse

# Use in router endpoints
@router.get("/block", response_model=InformationBlockEnvelope)
async def get_block(...):
    return InformationBlockEnvelope(...)
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
