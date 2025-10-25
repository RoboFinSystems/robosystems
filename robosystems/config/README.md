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
├── billing.py               # Billing plans and pricing
├── rate_limits.py           # Burst-focused rate limiting
├── credits.py               # Credit costs and allocations
├── external_services.py     # External API configurations
├── validation.py            # Startup validation
└── repositories.py          # Repository configuration
```

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
from robosystems.config.env import env

# Access typed environment variables
database_url = env.DATABASE_URL
api_port = env.API_PORT  # Returns int
debug_mode = env.DEBUG   # Returns bool

# Environment checks
if env.is_production():
    setup_production_logging()

# AWS configuration
s3_bucket = env.AWS_S3_BUCKET
region = env.AWS_REGION
```

**Key Variables:**

```python
# Core Settings
ENVIRONMENT          # dev/staging/prod
DATABASE_URL         # PostgreSQL connection
CELERY_BROKER_URL    # Valkey/Redis connection
JWT_SECRET_KEY       # JWT signing key

# Graph API Configuration
GRAPH_API_URL        # Graph API endpoint
KUZU_DATABASE_PATH   # Database file path
KUZU_MAX_DATABASES_PER_NODE  # Capacity limit

# AWS Settings
AWS_REGION           # AWS region
AWS_S3_BUCKET        # S3 bucket for storage

# Feature Flags
ENABLE_RATE_LIMITING # Rate limiting toggle
ENABLE_CREDITS       # Credit system toggle
```

### 2. Billing Configuration (`billing.py`)

Defines all subscription plans and pricing.

**Features:**

- **Config-as-Code**: All plans defined in code
- **Unified Pricing**: Single source for all tiers
- **Plan Validation**: Ensures plan consistency
- **Marketing Info**: Display names and descriptions

**Subscription Tiers:**

```python
SUBSCRIPTION_PLANS = {
    "standard": {
        "display_name": "Standard",
        "credits_per_month": 100_000,
        "base_price_cents": 4999,  # $49.99
        "max_graphs": 5,
        "rate_limit_multiplier": 2.0,
    },
    "enterprise": {
        "display_name": "Enterprise",
        "credits_per_month": 1_000_000,
        "base_price_cents": 19999,  # $199.99
        "max_graphs": 25,
        "rate_limit_multiplier": 5.0,
    },
    "premium": {
        "display_name": "Premium",
        "credits_per_month": 3_000_000,
        "base_price_cents": 49999,  # $499.99
        "max_graphs": 100,
        "rate_limit_multiplier": 10.0,
    }
}
```

**Usage:**

```python
from robosystems.config.billing import BillingConfig

# Get all pricing information
pricing = BillingConfig.get_all_pricing_info()

# Get specific plan
plan = BillingConfig.get_subscription_plan("enterprise")
print(f"{plan['display_name']}: ${plan['base_price_cents']/100}/month")

# Check plan features
if plan['max_graphs'] >= 10:
    enable_advanced_features()
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
    AUTH = "auth"                # Login, register
    GRAPH_READ = "graph_read"    # GET operations
    GRAPH_WRITE = "graph_write"  # POST/PUT/DELETE
    GRAPH_QUERY = "graph_query"  # Cypher queries
    ANALYTICS = "analytics"      # Heavy computations
    AI = "ai"                    # AI/MCP operations
    PUBLIC = "public"            # Health checks
```

**Rate Limits:**

```python
# Standard tier (1-minute windows)
GRAPH_READ: 500/min   # 30k/hour possible
GRAPH_WRITE: 100/min  # 6k/hour possible
GRAPH_QUERY: 60/min   # 3.6k/hour possible
AI: 10/min            # 600/hour possible

# Enterprise tier (5x multiplier)
GRAPH_READ: 2500/min  # 150k/hour possible
GRAPH_WRITE: 500/min  # 30k/hour possible
```

**Usage:**

```python
from robosystems.config.rate_limits import get_rate_limit_for_tier

# Get limits for tier and operation
limit, period = get_rate_limit_for_tier("enterprise", EndpointCategory.GRAPH_QUERY)
# Returns: (300, RateLimitPeriod.MINUTE)

# Apply in middleware
@rate_limit(
    calls=limit,
    period=period.value,
    key=lambda: f"user:{user.id}"
)
```

### 4. Credit Configuration (`credits.py`)

Defines credit costs for all operations.

**Features:**

- **Operation Costs**: Fixed costs per operation type
- **Dynamic Pricing**: Result-based pricing for queries
- **Storage Costs**: GB-hour based storage pricing
- **Tier Discounts**: Multipliers for different tiers

**Operation Costs:**

```python
BASE_COSTS = {
    "api_call": 1,          # Standard API calls
    "query": 10,            # Base query cost
    "analytics": 25,        # Analytics queries
    "backup": 50,           # Backup operations
    "ai_operation": 100,    # AI/MCP operations
}

# Dynamic costs
QUERY_RESULT_COST = 0.01   # Per result row
STORAGE_COST_PER_GB_HOUR = 0.1
```

**Usage:**

```python
from robosystems.config.credits import CreditConfig

# Calculate query cost
cost = CreditConfig.get_query_cost(result_count=1000)
# Base: 10 + (1000 * 0.01) = 20 credits

# Calculate storage cost
storage_cost = CreditConfig.get_storage_cost(gb_hours=24)
# 24 * 0.1 = 2.4 credits

# Apply tier discount
enterprise_cost = cost * 0.8  # 20% discount
```

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
- Kuzu endpoints defined

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
from robosystems.config.env import env
assert env.is_test()
assert env.DATABASE_URL == "postgresql://test"

# Test billing config
from robosystems.config.billing import BillingConfig
plan = BillingConfig.get_subscription_plan("standard")
assert plan["credits_per_month"] == 100_000
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
