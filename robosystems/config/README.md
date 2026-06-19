# Configuration Module

This module provides centralized configuration management for the entire RoboSystems platform, serving as the single source of truth for all system settings, environment variables, and business rules.

## Overview

The configuration module:

- Manages all environment variables with validation and defaults
- Defines billing plans and credit allocations
- Configures rate limiting for burst protection
- Sets credit costs for all operations
- Validates configuration at startup
- Provides a config-as-code approach

## Architecture

```
config/
├── __init__.py              # Module exports
├── env.py                   # Environment variable management
├── constants.py             # Fixed operational constants (never change at runtime)
├── defaults.py              # Centralized tunable defaults
├── deprovisioning.py        # Graph deprovisioning policy
├── tuning.py                # SSM Parameter Store tuning accessors
├── parameter_store.py       # SSM Parameter Store client
├── secrets_manager.py       # AWS Secrets Manager client
├── logging.py               # Logging configuration
├── openapi_tags.py          # OpenAPI tag metadata
├── billing/                 # Billing plans and pricing
│   ├── core.py              # Subscription tiers and base pricing
│   └── ai.py                # AI/token-based pricing
├── rate_limits.py           # Burst-focused rate limiting
├── credits.py               # Credit costs and allocations
├── operators.py             # AI Operator configuration (Bedrock Claude models)
├── graph_tier.py            # Graph tier config from .github/configs/graph.yml
├── query_queue.py           # Query queue configuration
├── shared_repositories.py   # Shared repository registry
├── validation.py            # Startup validation
├── valkey_registry.py       # Valkey database allocation
└── storage/                 # S3 path configuration (see storage/README.md)
    ├── __init__.py          # Re-exports from shared and graph
    ├── shared.py            # Shared data sources (SEC, FRED, etc.)
    └── graph.py             # Graph database storage paths
```

## Configuration Tiers

Configuration is organized into three tiers based on how values are managed:

```
┌───────────────────┬───────────────────┬───────────────────┐
│    CONSTANTS      │     TUNABLES      │     SECRETS       │
│  (Never Change)   │ (Runtime Adjust)  │ (Sensitive Data)  │
├───────────────────┼───────────────────┼───────────────────┤
│ • Protocol limits │ • Cache TTLs      │ • DATABASE_URL    │
│ • Business rules  │ • Queue sizes     │ • JWT_SECRET_KEY  │
│ • Memory limits   │ • Thresholds      │ • API keys        │
│ • API versions    │ • Timeouts        │ • Passwords       │
├───────────────────┼───────────────────┼───────────────────┤
│ constants.py      │ SSM /tuning/      │ Secrets Manager   │
│                   │ + defaults.py     │                   │
└───────────────────┴───────────────────┴───────────────────┘
```

**Override Priority:** Environment Variable > SSM Parameter Store > Default Value

### SSM Parameter Store (Tunables)

Runtime-adjustable parameters stored in SSM Parameter Store (FREE tier):

```
/robosystems/{env}/
  features/                    # Boolean feature flags
    RATE_LIMIT_ENABLED
    BILLING_ENABLED
    ...
  tuning/                      # Runtime tunables
    cache/                     # Cache TTLs (BALANCE_TTL, JWT_TTL, etc.)
    admission/                 # Main API thresholds (MEMORY_THRESHOLD, CPU_THRESHOLD)
    lbug_admission/            # LadybugDB thresholds (MEMORY_THRESHOLD, CPU_THRESHOLD)
    queues/                    # Queue config (MAX_SIZE, MAX_CONCURRENT)
    circuits/                  # Circuit breakers (THRESHOLD, TIMEOUT)
    load_shedding/             # Load shedding (START_PRESSURE, STOP_PRESSURE)
    mcp/                       # MCP limits (MAX_RESULT_ROWS, MAX_RESULT_SIZE_MB)
```

**Management:**

```bash
just ssm-list prod tuning              # List all tuning parameters
just ssm-set prod tuning/cache/BALANCE_TTL 600
just ssm-get prod tuning/admission/MEMORY_THRESHOLD
```

Changes take effect within the application's cache TTL (typically 5 minutes).

## Key Components

### 1. Environment Configuration (`env.py`)

Centralized environment variable management with validation.

**Features:**

- **Type-Safe Access**: Automatic type conversion
- **Default Values**: Sensible defaults for development
- **Environment Detection**: is_production(), is_staging(), etc.
- **Validation**: Required variables checked at startup

**Usage:**

```python
from robosystems.config import env

# Access typed environment variables
database_url = env.DATABASE_URL
api_port = env.API_PORT  # Returns int
debug_mode = env.DEBUG   # Returns bool

# Environment checks
if env.is_production():
    setup_production_logging()

# AWS configuration
s3_bucket = env.USER_DATA_BUCKET
region = env.AWS_REGION
```

**Key Variables:**

```python
# Core Settings
ENVIRONMENT          # dev/staging/prod
DATABASE_URL         # PostgreSQL connection
JWT_SECRET_KEY       # JWT signing key

# Graph API Configuration
GRAPH_API_URL        # Graph API endpoint
LBUG_DATABASE_PATH   # Database file path
LBUG_MAX_DATABASES_PER_NODE  # Capacity limit

# AWS Settings
AWS_REGION           # AWS region
USER_DATA_BUCKET     # S3 bucket for user data storage
SHARED_RAW_BUCKET    # S3 bucket for shared raw data (SEC, FRED, etc.)
SHARED_PROCESSED_BUCKET  # S3 bucket for shared processed data

# Feature Flags
RATE_LIMIT_ENABLED   # Rate limiting toggle
BILLING_ENABLED      # Credit/billing system toggle
```

### 2. Billing Configuration (`billing/`)

Defines subscription plans, credit allocations, and AI token pricing.

**Features:**

- **Config-as-Code**: All plans defined in code
- **Single Source of Truth**: `billing/core.py` for tiers, `billing/ai.py` for token pricing
- **Plan Validation**: Ensures plan consistency
- **Stripe Integration**: Prices auto-created from config on first checkout

**Subscription Tiers** (per-graph):

| Tier | Price | Monthly Credits | ~Operator Calls |
|------|-------|----------------|-------------|
| ladybug-standard | $149/mo | 8,000 | ~200/mo |
| ladybug-large | $299/mo | 32,000 | ~800/mo |
| ladybug-xlarge | $699/mo | 100,000 | ~2,600/mo |

**Usage:**

```python
from robosystems.config.billing import BillingConfig

# Get all pricing information
pricing = BillingConfig.get_all_pricing_info()

# Get specific plan
plan = BillingConfig.get_subscription_plan("ladybug-large")
print(f"{plan['display_name']}: ${plan['base_price_cents']/100}/month")
```

### 3. Rate Limiting (`rate_limits.py`)

Burst-focused rate limiting for spike protection.

**Features:**

- **1-Minute Windows**: All limits use 60-second windows
- **Tier-Based**: Different limits per subscription
- **Category-Based**: Different limits per operation type
- **Burst Protection**: Prevents abuse without limiting volume

**Endpoint Categories:**

```python
class EndpointCategory(str, Enum):
    # Non-graph scoped endpoints
    AUTH = "auth"                          # Login, register
    USER_MANAGEMENT = "user_management"
    TASKS = "tasks"
    STATUS = "status"                      # Health checks
    SSE = "sse"                            # Server-Sent Events connections
    BILLING = "billing"                    # Checkout and payment flows

    # Graph-scoped endpoints
    GRAPH_READ = "graph_read"              # GET operations
    GRAPH_WRITE = "graph_write"            # POST/PUT/DELETE
    GRAPH_ANALYTICS = "graph_analytics"    # Heavy computations
    GRAPH_BACKUP = "graph_backup"
    GRAPH_SYNC = "graph_sync"
    GRAPH_MCP = "graph_mcp"                # MCP operations
    GRAPH_OPERATOR = "graph_operator"      # AI Operator operations
    GRAPH_SEARCH = "graph_search"          # OpenSearch full-text search

    # High-cost operations
    GRAPH_QUERY = "graph_query"            # Direct Cypher queries
    GRAPH_IMPORT = "graph_import"          # Bulk data imports

    # Extensions surface (OLTP on shared RDS)
    EXTENSIONS_GRAPHQL = "extensions_graphql"  # Typed GraphQL reads
    EXTENSIONS_WRITE = "extensions_write"      # Command writes + views

    # Table operations (DuckDB staging tables)
    TABLE_QUERY = "table_query"            # SQL queries on staging tables
    TABLE_UPLOAD = "table_upload"          # File uploads to staging tables
    TABLE_MANAGEMENT = "table_management"  # Table creation/deletion
```

**Rate Limits** (representative — see `rate_limits.py` for the authoritative table):

```python
# ladybug-standard tier (1-minute windows, base values)
GRAPH_READ: 120/min
GRAPH_WRITE: 30/min
GRAPH_QUERY: 60/min
GRAPH_OPERATOR: 15/min   # AI Operator operations

# Larger tiers apply graph.yml api_rate_multiplier (e.g. 1.5x large, higher for xlarge)
# on top of these base values.
```

**Usage:**

```python
from robosystems.config.rate_limits import RateLimitConfig, EndpointCategory

# Get limits for tier and operation — returns (limit, window_seconds) or None
result = RateLimitConfig.get_rate_limit("large", EndpointCategory.GRAPH_QUERY)
limit, window_seconds = result  # e.g. (300, 60)

# Or apply the tier-config multiplier
RateLimitConfig.get_rate_limit_with_multiplier("xlarge", EndpointCategory.GRAPH_READ)

# Classify a request path/method into a category
category = RateLimitConfig.get_endpoint_category(path, method)

# Apply in middleware
@rate_limit(
    calls=limit,
    period=window_seconds,
    key=lambda: f"user:{user.id}"
)
```

### 4. Credit Configuration (`credits.py` + `billing/ai.py`)

Defines what consumes credits and token-based pricing for AI operations.

**Credit Model:**

- Only AI Operator operations consume credits (token-based pricing)
- All database operations are included with the subscription (no credits)
- MCP tool access is unlimited (no credits)
- Storage is included in each tier (no metering)

**Token Pricing** (`billing/ai.py`):

```python
from robosystems.config.billing.ai import AIBillingConfig

# Single pricing tier for all Sonnet models
pricing = AIBillingConfig.TOKEN_PRICING["anthropic_claude_4_sonnet"]
# {"input": Decimal("3"), "output": Decimal("15")}  # credits per 1K tokens
```

**Operation Costs** (`credits.py`):

All non-AI operations return `Decimal("0")` — they are included with the subscription.

### 5. Configuration Validation (`validation.py`)

Validates all configuration at startup.

**Features:**

- **Environment-Specific**: Different requirements per environment
- **Clear Warnings**: Helpful messages for missing config
- **Fail-Fast**: Prevents startup with invalid config
- **Feature Detection**: Warns about disabled features

**Validation Rules:**

```python
# Production Requirements
- DATABASE_URL must be set
- JWT_SECRET_KEY must be secure
- AWS credentials configured
- LadybugDB endpoints defined

# Development Warnings
- Missing optional services
- Insecure defaults in use
- Feature flags disabled
```

**Usage:**

```python
from robosystems.config.validation import EnvValidator

# Automatic validation in main.py
validator = EnvValidator()
validator.validate_all()  # Raises on critical errors

# Manual validation
if not validator.validate_database():
    setup_fallback_database()
```

### 6. Operator Configuration (`operators.py`)

Centralized configuration for the AI Operator system.

**Features:**

- **Model Selection**: AWS Bedrock Claude model configuration
- **Execution Profiles**: Time/token limits per operator mode
- **Orchestrator Config**: Routing strategy and fallback settings
- **Operator-Specific Overrides**: Per-operator model customization

**Available Models:**

Each `BedrockModel` enum member's value is a short identifier (e.g. `SONNET_4_6.value == "claude-sonnet-4-6"`); it is mapped internally to a regional Bedrock inference profile id (`us.anthropic.*`) shown in the comments below.

```python
from robosystems.config import BedrockModel

# Sonnet 4.6 (default) → us.anthropic.claude-sonnet-4-6
BedrockModel.SONNET_4_6

# Sonnet 4.5 (fallback) → us.anthropic.claude-sonnet-4-5-20250929-v1:0
BedrockModel.SONNET_4_5

# Sonnet 4 (last resort) → us.anthropic.claude-sonnet-4-20250514-v1:0
BedrockModel.SONNET_4
```

**Note:** All models use regional inference profiles (`us.*`) for on-demand access without marketplace subscriptions.

**Token Pricing:** All Sonnet models use the same credit rates (3/15 credits per 1K tokens). See `billing/ai.py` for details.

**Execution Modes:**

```python
from robosystems.config import OperatorExecutionMode

# Quick: 2-5 seconds, 2 tool calls, 50k input tokens
OperatorExecutionMode.QUICK

# Standard: 5-15 seconds, 5 tool calls, 100k input tokens
OperatorExecutionMode.STANDARD

# Extended: 30-120 seconds, 12 tool calls, 150k input tokens
OperatorExecutionMode.EXTENDED

# Streaming: 5-60 seconds, 8 tool calls, SSE responses
OperatorExecutionMode.STREAMING
```

**Usage:**

```python
from robosystems.config import OperatorConfig, BedrockModel

# Get default model ID
model_id = OperatorConfig.get_bedrock_model_id()

# Get model for a specific operator with override
model_id = OperatorConfig.get_bedrock_model_id(
    model=BedrockModel.SONNET_4_5,
    operator_type="financial"
)

# Get execution profile
from robosystems.config import OperatorExecutionMode
profile = OperatorConfig.get_execution_profile(OperatorExecutionMode.STANDARD)
# profile.max_tool_calls = 5
# profile.timeout_seconds = 60
# profile.max_input_tokens = 100000

# Get mode limits (backward compatible)
limits = OperatorConfig.get_mode_limits("standard")
# limits = {"max_tools": 5, "timeout": 60, ...}

# Get orchestrator config
fallback_operator = OperatorConfig.ORCHESTRATOR_CONFIG["fallback_operator"]  # "cypher"
enable_rag = OperatorConfig.ORCHESTRATOR_CONFIG["enable_rag"]  # False

# Validate configuration
validation = OperatorConfig.validate_configuration()
if not validation["valid"]:
    print(f"Issues: {validation['issues']}")
```

**Customizing Operator Models:**

To use a different model for a specific operator, update `OPERATOR_MODEL_OVERRIDES`:

```python
# In robosystems/config/operators.py
OPERATOR_MODEL_OVERRIDES: Dict[str, BedrockModel] = {
    "financial": BedrockModel.SONNET_4_5,  # Use latest model for financial analysis
    "cypher": BedrockModel.SONNET_4,      # Use Sonnet 4 for Cypher queries
}
```

**Changing Default Model:**

To change the default model globally:

```python
# In robosystems/config/operators.py
DEFAULT_MODEL_CONFIG = ModelConfig(
    default_model=BedrockModel.SONNET_4_6,  # Current default: Sonnet 4.6
    fallback_model=BedrockModel.SONNET_4_5, # Current fallback: Sonnet 4.5
    region=env.AWS_BEDROCK_REGION,
    temperature=0.7,
)
```

### 7. Storage Configuration (`storage/`)

Centralized S3 path helpers for consistent bucket usage across the platform.

**Features:**

- **Shared Data Sources**: SEC, FRED, and future data repositories
- **Graph Storage**: User staging, backups, and instance-level database storage
- **Consistent Paths**: Centralized key generation for all S3 operations
- **Data Source Registry**: Config-driven data source management

**Bucket Variables:**

```python
SHARED_RAW_BUCKET        # Raw downloads (SEC filings, etc.)
SHARED_PROCESSED_BUCKET  # Processed parquet files
USER_DATA_BUCKET         # User uploads, graph backups
PUBLIC_DATA_BUCKET       # CDN-served public content
```

**Usage:**

```python
from robosystems.config.storage import shared, graph
from robosystems.config.storage.shared import DataSourceType

# Shared data paths (SEC, FRED, etc.)
raw_key = shared.get_raw_key(DataSourceType.SEC, "year=2024", "320193", "filing.zip")
processed_key = shared.get_processed_key(DataSourceType.SEC, "year=2024", "nodes", "Entity.parquet")

# Graph storage paths
staging_key = graph.get_staging_key("user123", "kg456", "Entity", "file789", "data.parquet")
backup_key = graph.get_instance_backup_key("prod", "kg456", timestamp)
```

See `storage/README.md` for complete documentation.

## Configuration Philosophy

### Config-as-Code

All business configuration lives in code, not database:

- Version controlled
- Code reviewed
- Tested in CI/CD
- No runtime surprises

### Burst vs Volume

- **Rate Limits**: Handle burst protection (1-minute windows)
- **Credits**: Control volume usage (monthly allocations)
- **Clear Separation**: Different concerns, different solutions

### Rate Multipliers vs Credit Costs

**IMPORTANT**: These are two separate systems that serve different purposes:

**`api_rate_multiplier`** (Rate Limiting):

- Scales API request rate limits based on subscription tier
- Examples: 1.0x for standard, 2.5x for large, 5.0x for xlarge
- Affects how many requests per minute you can make
- Provides burst protection without limiting total volume
- Does NOT affect credit costs

**Credit Costs** (Billing):

- Only AI Operator operations consume credits (token-based pricing)
- All database operations are included (queries, imports, backups, etc.)
- No multipliers applied to credit costs — same rate for all tiers
- Credits are billed based on actual AI token usage (input + output tokens)

**Example:**

- An xlarge tier customer gets 5.0x more API requests per minute
- But pays the same credit rate per AI operation as a standard tier customer
- Database queries don't consume any credits regardless of tier

### Environment-Aware

- **Development**: Permissive defaults, helpful warnings
- **Staging**: Production-like with safety nets
- **Production**: Strict validation, no compromises

## Best Practices

1. **Use Type-Safe Access**: Always use env.VARIABLE, not os.getenv()
2. **Define Defaults**: Provide sensible defaults for development
3. **Validate Early**: Check configuration at startup
4. **Document Variables**: Add comments explaining each variable
5. **Group Related Config**: Keep related settings together
6. **Avoid Magic Numbers**: Use named constants

## Testing Configuration

```python
# Test environment setup
import os
os.environ["ENVIRONMENT"] = "test"
os.environ["DATABASE_URL"] = "postgresql://test"

# Test configuration access
from robosystems.config import env
assert env.is_test()
assert env.DATABASE_URL == "postgresql://test"

# Test billing config
from robosystems.config.billing import BillingConfig
plan = BillingConfig.get_subscription_plan("ladybug-standard")
assert plan["monthly_credit_allocation"] == 8000
```

## Troubleshooting

### Common Issues

1. **Missing Environment Variables**

   - Check .env file exists
   - Verify Docker Compose environment
   - Review validation warnings

2. **Configuration Not Loading**

   - Ensure config module imported early
   - Check for circular imports
   - Verify environment detection

3. **Rate Limit Confusion**

   - Remember: all limits are per minute
   - Credits control volume, not rate limits
   - Check tier multipliers

4. **Validation Failures**
   - Review error messages carefully
   - Check environment-specific requirements
   - Ensure secrets are properly set

## Security Considerations

1. **Secret Management**: Never commit secrets to code
2. **Environment Isolation**: Use different secrets per environment
3. **Validation**: Always validate configuration at startup
4. **Least Privilege**: Only expose needed configuration
5. **Audit Trail**: Log configuration changes
