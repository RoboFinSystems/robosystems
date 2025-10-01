# Authentication Middleware

This middleware provides comprehensive authentication, authorization, and rate limiting for the RoboSystems platform.

## Overview

The authentication middleware:

- Handles JWT token and API key authentication
- Implements sophisticated caching for performance
- Provides credit-based and subscription-based rate limiting
- Manages multi-tenant graph access control
- Supports Single Sign-On (SSO) across RoboSystems applications

## Architecture

```
auth/
├── __init__.py                  # Module exports
├── dependencies.py              # FastAPI dependency injection
├── utils.py                     # Authentication utilities
├── cache.py                     # Redis/Valkey caching layer
├── cache_validator.py           # Cache validation and refresh
├── rate_limiting.py             # Rate limiting middleware
├── credit_rate_limiting.py      # Credit-based rate limiting
├── subscription_rate_limits.py  # Subscription tier limits
├── distributed_lock.py          # Distributed locking for cache
└── maintenance.py               # Maintenance mode handling
```

## Authentication Methods

### 1. JWT Token Authentication

Used by frontend applications for user sessions.

**Features:**

- **Storage**: HTTP-only, Secure, SameSite cookies
- **Cookie Name**: `auth-token`
- **Algorithm**: HS256
- **Expiration**: 30 days with auto-refresh
- **Blacklist Support**: Tokens can be revoked

**Cookie Configuration:**

```python
response.set_cookie(
    key="auth-token",
    value=token,
    httponly=True,
    secure=True,  # HTTPS only
    samesite="lax",
    max_age=30 * 24 * 60 * 60,  # 30 days
    domain=".robosystems.ai"  # Cross-subdomain
)
```

### 2. API Key Authentication

Used for programmatic access and integrations.

**Features:**

- **Header**: `X-API-Key`
- **Format**: `rfs[32 random characters]`
- **Storage**: SHA-256 hashed in database
- **Graph Scoping**: Keys can be limited to specific graphs
- **Activity Tracking**: Last used timestamp

**Example:**

```bash
curl -H "X-API-Key: rfs*" \
     https://api.robosystems.ai/v1/kg1a2b3c/data
```

### 3. Single Sign-On (SSO)

Seamless authentication across RoboSystems applications.

**Flow:**

1. Generate SSO token (60-second TTL)
2. Exchange token for session
3. Complete handoff with cookie

**Supported Applications:**

- RoboLedger (roboledger.ai)
- RoboInvestor (roboinvestor.ai)
- RoboSystems (app.robosystems.ai)

## Key Components

### 1. Dependencies (`dependencies.py`)

FastAPI dependency injection for authentication.

**Core Dependencies:**

#### `get_current_user`

Requires authenticated user:

```python
@router.get("/protected")
async def protected_route(
    user: User = Depends(get_current_user)
):
    return {"user_id": user.id}
```

#### `get_optional_user`

Optional authentication:

```python
@router.get("/public")
async def public_route(
    user: Optional[User] = Depends(get_optional_user)
):
    if user:
        return {"message": f"Hello {user.name}"}
    return {"message": "Hello anonymous"}
```

#### `get_current_user_with_graph`

Validates graph access:

```python
@router.get("/v1/graphs/{graph_id}/data")
async def get_graph_data(
    graph_id: str,
    auth: Tuple[User, str] = Depends(get_current_user_with_graph)
):
    user, validated_graph_id = auth
    # User has access to this graph
    return {"data": "..."}
```

### 2. Authentication Cache (`cache.py`)

High-performance caching using Redis/Valkey.

**Cache Types:**

#### API Key Cache

- **TTL**: 5 minutes
- **Key Format**: `api_key:{key_prefix}`
- **Invalidation**: On key update/deletion

#### JWT Cache

- **TTL**: 30 minutes
- **Key Format**: `jwt:{user_id}`
- **Blacklist**: `jwt_blacklist:{user_id}`

#### User Session Cache

- **TTL**: 24 hours
- **Key Format**: `user_session:{user_id}`
- **Content**: User profile, permissions, limits

**Cache Operations:**

```python
cache = APIKeyCache()

# Cache API key validation
await cache.cache_api_key_user(
    api_key="rbs_xxx",
    user_id="user_123",
    user_data={"email": "user@example.com"}
)

# Get cached user
user_data = await cache.get_cached_api_key_user("rbs_xxx")

# Invalidate on changes
await cache.invalidate_user_cache("user_123")
```

### 3. Rate Limiting (`rate_limiting.py`)

Sliding window rate limiting with Redis backend.

**Default Limits:**

```python
RATE_LIMITS = {
    "api_key": 10000,      # per hour
    "jwt": 1000,           # per hour
    "anonymous": 100,      # per hour
    "login": 5,            # per 15 minutes
    "register": 3,         # per 15 minutes
}
```

**Headers Returned:**

- `X-RateLimit-Limit`: Request limit
- `X-RateLimit-Remaining`: Requests remaining
- `X-RateLimit-Reset`: Reset timestamp

**Usage:**

```python
limiter = RateLimitMiddleware()

# Check rate limit
allowed = await limiter.check_rate_limit(
    key="user_123",
    limit=1000,
    window=3600
)

if not allowed:
    raise HTTPException(429, "Rate limit exceeded")
```

### 4. Credit-Based Rate Limiting (`credit_rate_limiting.py`)

Integrates with the credit system for usage-based limiting.

**Features:**

- **Credit Deduction**: Automatically deducts credits
- **Pre-flight Checks**: Validates credits before operations
- **Graceful Degradation**: Returns 402 when credits exhausted

**Credit Costs by Operation:**

```python
OPERATION_CREDITS = {
    "api_call": 1,
    "query": 10,
    "analytics": 25,
    "ai_operation": 100,
}
```

### 5. Subscription Rate Limits (`subscription_rate_limits.py`)

Tier-based rate limiting.

**Tiers:**

```python
SUBSCRIPTION_LIMITS = {
    "standard": {
        "requests_per_hour": 1000,
        "queries_per_hour": 100,
        "ai_calls_per_day": 50,
    },
    "enterprise": {
        "requests_per_hour": 10000,
        "queries_per_hour": 1000,
        "ai_calls_per_day": 500,
    },
    "premium": {
        "requests_per_hour": None,  # Unlimited
        "queries_per_hour": None,
        "ai_calls_per_day": None,
    }
}
```

### 6. Cache Validator (`cache_validator.py`)

Ensures cache consistency with database.

**Features:**

- **Periodic Validation**: Checks cache accuracy
- **Lazy Refresh**: Updates cache on access
- **Batch Operations**: Validates multiple entries

**Validation Flow:**

1. Check cache staleness
2. Compare with database
3. Update if different
4. Track validation metrics

### 7. Distributed Lock (`distributed_lock.py`)

Prevents cache stampedes and race conditions.

**Usage:**

```python
lock = DistributedLock(redis_client)

async with lock.acquire("user_update:123", timeout=5):
    # Critical section
    await update_user_cache()
    await update_database()
```

### 8. Maintenance Mode (`maintenance.py`)

Handles system maintenance gracefully.

**Features:**

- **Selective Access**: Admin users can still access
- **Custom Messages**: Configurable maintenance messages
- **Scheduled Maintenance**: Set start/end times

## Security Features

### Password Security

- **Algorithm**: Bcrypt with cost factor 12
- **Validation**: Minimum 8 characters
- **History**: Prevents reuse of last 5 passwords

### Token Security

- **JWT Secret**: Strong random key (minimum 32 bytes)
- **Rotation**: Automatic token refresh every 10 minutes
- **Revocation**: Immediate blacklisting on logout

### API Key Security

- **Generation**: Cryptographically secure (32 bytes)
- **Prefix**: Identifiable prefix for key type
- **Hashing**: SHA-256 before storage
- **Rotation**: Support for key rotation

### Multi-Tenant Security

- **Graph Isolation**: Complete data isolation
- **Role-Based Access**: Admin, Member, Viewer
- **Permission Caching**: 5-minute TTL
- **Audit Logging**: All access logged

## Configuration

### Required Environment Variables

```bash
# JWT Configuration
JWT_SECRET_KEY=your-secret-key-minimum-32-bytes
JWT_ALGORITHM=HS256
JWT_EXPIRATION_DAYS=30

# API Key Configuration
API_KEY_PREFIX=rbs_
API_KEY_LENGTH=32

# Redis/Valkey Cache
VALKEY_URL=redis://localhost:6379
API_KEY_CACHE_TTL=300
JWT_CACHE_TTL=1800
USER_SESSION_CACHE_TTL=86400

# Rate Limiting
RATE_LIMIT_ENABLED=true
RATE_LIMIT_REDIS_DB=3
RATE_LIMIT_WINDOW=3600

# SSO Configuration
SSO_TOKEN_TTL=60
SSO_SESSION_TTL=300
```

### Optional Configuration

```bash
# Security
PASSWORD_MIN_LENGTH=8
PASSWORD_REQUIRE_SPECIAL=true
PASSWORD_REQUIRE_NUMBERS=true
MAX_LOGIN_ATTEMPTS=5
LOCKOUT_DURATION=900

# Performance
CACHE_WARM_ON_STARTUP=true
CACHE_BATCH_SIZE=100
DISTRIBUTED_LOCK_TIMEOUT=5

# Development
AUTH_DEBUG_MODE=false
BYPASS_AUTH_ENDPOINTS=[]
```

## Integration Examples

### FastAPI Application Setup

```python
from fastapi import FastAPI
from robosystems.middleware.auth import (
    AuthenticationMiddleware,
    RateLimitMiddleware,
    MaintenanceMiddleware
)

app = FastAPI()

# Add middleware in order
app.add_middleware(MaintenanceMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(AuthenticationMiddleware)
```

### Protected Endpoint

```python
from robosystems.middleware.auth.dependencies import (
    get_current_user,
    require_graph_access
)

@router.post("/v1/graphs/{graph_id}/expensive-operation")
async def expensive_operation(
    graph_id: str,
    user: User = Depends(get_current_user),
    graph_access = Depends(require_graph_access)
):
    # User is authenticated and has access to graph
    return {"status": "success"}
```

### Custom Rate Limiting

```python
from robosystems.middleware.auth.decorators import rate_limit

@router.get("/api/search")
@rate_limit(calls=10, period=60)  # 10 calls per minute
async def search(query: str):
    return {"results": [...]}
```

## Monitoring

### Key Metrics

1. **Authentication Metrics**

   - Login success/failure rates
   - Token refresh frequency
   - API key usage by key
   - SSO token exchanges

2. **Cache Metrics**

   - Hit/miss ratios
   - Eviction rates
   - Validation frequency
   - Lock contention

3. **Rate Limit Metrics**

   - Limit exceeded events
   - Usage by tier
   - Burst patterns
   - Credit exhaustion

4. **Security Metrics**
   - Failed authentication attempts
   - Suspicious activity patterns
   - Token blacklist size
   - Permission changes

### Health Checks

```python
@router.get("/health/auth")
async def auth_health():
    return {
        "cache": await check_cache_health(),
        "rate_limiter": await check_rate_limiter_health(),
        "database": await check_auth_db_health(),
    }
```

## Troubleshooting

### Common Issues

1. **"Invalid token" Errors**

   ```bash
   # Check if token is blacklisted
   redis-cli -n 2 GET "jwt_blacklist:user_xxx"

   # Clear user cache
   redis-cli -n 2 DEL "jwt:user_xxx"
   ```

2. **Rate Limit Issues**

   ```bash
   # Check current usage
   redis-cli -n 3 GET "rate_limit:user:123"

   # Reset rate limit (emergency)
   redis-cli -n 3 DEL "rate_limit:user:123"
   ```

3. **Cache Inconsistency**

   ```python
   # Force cache refresh
   await cache.invalidate_user_cache(user_id)

   # Validate all user caches
   await validator.validate_all_users()
   ```

4. **SSO Failures**

   ```bash
   # Check SSO token
   redis-cli GET "sso_token:xxx"

   # Verify session
   redis-cli GET "sso_session:xxx"
   ```

## Best Practices

1. **Security**

   - Rotate JWT secrets regularly
   - Monitor failed authentication attempts
   - Implement IP-based blocking for attackers
   - Use secure cookie settings in production

2. **Performance**

   - Enable caching for all authentication checks
   - Use connection pooling for Redis
   - Implement cache warming for hot users
   - Monitor cache hit rates

3. **Reliability**

   - Implement circuit breakers for cache
   - Have fallback authentication paths
   - Monitor rate limit effectiveness
   - Test failover scenarios

4. **Maintenance**
   - Regular cache cleanup
   - Monitor blacklist size
   - Review rate limit settings
   - Audit authentication logs
