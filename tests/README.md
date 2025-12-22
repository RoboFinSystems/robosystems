# Testing in RoboSystems Service

This directory contains comprehensive tests for the RoboSystems Service application. With 194+ test files covering all major components, the test suite ensures reliability across the platform.

## Quick Start

```bash
# Run standard test suite (unit tests only)
just test

# Run with coverage report
just test-cov

# Run all tests including linting and formatting
just test-all

# Run integration tests
just test-integration
```

## End-to-End Validation

For complete end-to-end workflow validation, use the **examples** directory instead of traditional e2e tests:

```bash
# Run complete accounting workflow
cd examples/accounting_demo
uv run main.py

# Run custom graph workflow
cd examples/custom_graph_demo
uv run main.py

# Run SEC data workflow
cd examples/sec_demo
uv run main.py
```

These examples validate the entire stack (authentication, graph creation, data upload, ingestion, queries) in a production-like environment while also serving as user documentation.

## Test Structure

The test suite is organized by component, mirroring the application structure:

### Core Components

- **`adapters/`** - External service integrations (Arelle/XBRL, OpenFIGI, QuickBooks, S3, SEC)
- **`config/`** - Configuration validation and billing plan tests
- **`middleware/`** - Request/response middleware layers
  - `auth/` - Authentication, cache validation, distributed locks (20+ test files)
  - `billing/` - Credit consumption and subscription billing middleware
  - `graph/` - Graph database routing and multi-tenancy
  - `mcp/` - Model Context Protocol integration
  - `otel/` - OpenTelemetry metrics and tracing
  - `rate_limits/` - Rate limiting and burst protection
  - `robustness/` - Circuit breakers, retries, health checks
  - `sse/` - Server-sent events for real-time updates
- **`models/`** - Database models and schemas
  - `api/` - API request/response models
  - `billing/` - Billing and subscription models (customer, subscription, invoice, audit log)
  - `iam/` - Identity and access management models
- **`operations/`** - Business logic services
  - `agents/` - AI agent operations and orchestration
  - `graph/` - Graph database operations (credit service, entity service)
  - `lbug/` - LadybugDB-specific operations (backup, health monitoring)
  - `pipeline/` - Data processing pipelines
  - `providers/` - External provider integrations

### Adapters

- **`adapters/`** - External service integrations
  - SEC adapter (XBRL processing, filings, taxonomies)
  - QuickBooks adapter (transaction processing)

### API Layer

- **`routers/`** - HTTP endpoint tests
  - `auth/` - Authentication and authorization endpoints
  - `graphs/` - Graph database CRUD operations
  - `user/` - User management and subscription endpoints

### Background Tasks

- **`dagster/`** - Dagster pipeline tests
  - `assets/` - Asset tests (SEC, Plaid, etc.)
  - `jobs/` - Job tests (billing, infrastructure)

### Infrastructure

- **`graph_api/`** - Graph API cluster services (multi-backend support)
  - `client/` - Graph API client functionality
  - `routers/` - Graph API HTTP endpoints
- **`schemas/`** - Dynamic schema management
- **`security/`** - Security implementations and validators
- **`utils/`** - Utility functions and helpers

### Test Types

- **`integration/`** - Cross-component integration tests
- **`unit/`** - Isolated unit tests for specific components

## Test Categories

Tests are marked with pytest markers to categorize them. Use these markers to run specific test subsets:

### Primary Markers

- **`@pytest.mark.unit`** - Fast, isolated unit tests (no external dependencies)
- **`@pytest.mark.integration`** - Integration tests (may use databases, create LadybugDB instances)
- **`@pytest.mark.slow`** - Long-running tests (XBRL processing, large datasets)
- **`@pytest.mark.security`** - Security-focused tests
- **`@pytest.mark.asyncio`** - Async operation tests (handled automatically)

### Running Tests by Category

```bash
# Only unit tests (fast, no external services)
uv run pytest -m unit

# Only integration tests
uv run pytest -m integration

# Only security tests
uv run pytest -m security

# Exclude slow tests
uv run pytest -m "not slow"

# Exclude slow tests (default)
uv run pytest -m "not slow"
```

## Running Tests

### By Directory

```bash
# All task tests
uv run pytest tests/tasks/

# Specific task category
uv run pytest tests/tasks/billing/
uv run pytest tests/tasks/data_sync/

# Business operations
uv run pytest tests/operations/

# Middleware components
uv run pytest tests/middleware/auth/
uv run pytest tests/middleware/credits/

# Adapters
uv run pytest tests/adapters/

# API endpoints
uv run pytest tests/routers/

# Graph API
uv run pytest tests/graph_api/
```

### By Specific Test

```bash
# Specific test file
uv run pytest tests/tasks/billing/test_storage_billing.py

# Specific test class
uv run pytest tests/tasks/billing/test_storage_billing.py::TestDailyStorageBilling

# Specific test function
uv run pytest tests/tasks/billing/test_storage_billing.py::TestDailyStorageBilling::test_successful_billing

# Parametrized test case
uv run pytest tests/routers/auth/test_login.py::test_login[success]
```

### Advanced Test Selection

```bash
# Run tests matching a pattern
uv run pytest -k "storage and billing"
uv run pytest -k "test_auth or test_login"

# Exclude specific patterns
uv run pytest -k "not slow"

# Verbose output with test names
uv run pytest -v

# Stop on first failure
uv run pytest -x

# Show local variables on failure
uv run pytest -l

# Run last failed tests
uv run pytest --lf

# Run failed tests first, then others
uv run pytest --ff

# Parallel execution (requires pytest-xdist)
uv run pytest -n auto
```

## Test Fixtures

Common test fixtures provide reusable test components. Fixtures are defined at multiple levels:

### Global Fixtures (`tests/conftest.py`)

- **`test_db`** (session scope) - Test PostgreSQL database, auto-migrated
- **`client`** (module scope) - FastAPI TestClient with database
- **`client_with_mocked_auth`** (function scope) - TestClient with mocked authentication
- **`mock_get_current_user`** (module scope) - Mock authentication dependency
- **`test_user`** (function scope) - Test user with API key
- **`sample_graph`** (function scope) - Sample graph database
- **`test_user_graph`** (function scope) - Graph owned by test user
- **`test_graph_with_credits`** (function scope) - Graph with credit allocation
- **`db_session`** (function scope) - Database session for direct queries

### Model Fixtures (`tests/models/conftest.py`)

- Database model factories
- Sample model instances
- Relationship fixtures

### Using Fixtures

```python
def test_with_database(test_db):
    """Use test database directly."""
    # test_db is already migrated and ready
    pass

def test_with_client(client):
    """Make HTTP requests to the API."""
    response = client.get("/api/health")
    assert response.status_code == 200

def test_with_auth(client_with_mocked_auth):
    """Make authenticated requests."""
    response = client_with_mocked_auth.get("/v1/user/profile")
    assert response.status_code == 200

def test_with_user(test_user):
    """Use a test user with API key."""
    assert test_user.api_key is not None
```

## Test Environment

Tests run in an isolated environment with specific configuration:

### Database Configuration

- **Test Database**: `robosystems_test` on `localhost:5432`
- **Auto-Migration**: Alembic migrations run automatically on `test_db` fixture
- **Isolation**: Each test using `db_session` gets a rolled-back transaction
- **Cleanup**: Database state is cleaned between test modules

### External Services

- **LocalStack**: AWS services (S3, etc.) on `http://localhost:4566`
- **Valkey/Redis**: Cache and queues on `localhost:6379`
- **Graph API**: LadybugDB service on `localhost:8001`
- **LadybugDB Databases**: Test databases in `./data/lbug-dbs`

### Mock Services

Tests mock external services by default:

- SEC EDGAR API
- QuickBooks API
- Plaid API
- Anthropic/Claude API
- OpenFIGI API

### Environment Variables

Key test environment variables (from `pytest.ini`):

```bash
ENVIRONMENT=test
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/robosystems_test
GRAPH_API_URL=http://localhost:8001
LBUG_DATABASE_PATH=./data/lbug-dbs

# Feature flags (mostly enabled for testing)
RATE_LIMIT_ENABLED=false           # Disabled for easier testing
BILLING_ENABLED=true
SECURITY_AUDIT_ENABLED=true
SUBGRAPH_CREATION_ENABLED=true
BACKUP_CREATION_ENABLED=true

# Mock API keys
ANTHROPIC_API_KEY=test-anthropic-key
INTUIT_CLIENT_ID=test-intuit-client-id
OPENFIGI_API_KEY=test-openfigi-key
```

## Test Organization Best Practices

### General Principles

1. **Unit tests** should be fast (<100ms) and isolated (no external dependencies)
2. **Integration tests** can use databases but should clean up after themselves
3. **E2E tests** require full Docker stack and test complete user workflows
4. **Async tests** use `@pytest.mark.asyncio` (auto-detected)
5. **Slow tests** should be marked `@pytest.mark.slow` for selective exclusion

### Test File Organization

```python
"""Tests for [component name]."""

import pytest
from unittest.mock import MagicMock, patch

# Import code under test
from robosystems.module import function_to_test


class TestComponentName:
    """Test cases for [specific component]."""

    def test_success_case(self):
        """Test successful operation."""
        # Arrange
        # Act
        # Assert
        pass

    def test_error_case(self):
        """Test error handling."""
        # Arrange
        # Act
        # Assert
        pass


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_input(self):
        """Test handling of empty input."""
        pass

    def test_invalid_input(self):
        """Test handling of invalid input."""
        pass
```

### Writing Good Tests

```python
# ✓ GOOD: Descriptive test name
def test_user_cannot_access_other_users_graphs():
    pass

# ✗ BAD: Vague test name
def test_graphs():
    pass

# ✓ GOOD: Test one thing
def test_credit_consumption_decrements_balance():
    # Tests only credit balance change
    pass

# ✗ BAD: Test multiple things
def test_credit_system():
    # Tests consumption, refills, limits, history...
    pass

# ✓ GOOD: Clear assertions
def test_authentication_returns_jwt_token():
    response = login(username, password)
    assert "access_token" in response
    assert response["token_type"] == "bearer"

# ✗ BAD: Unclear assertions
def test_authentication():
    response = login(username, password)
    assert response
```

### Common Testing Scenarios

#### Testing Error Handling

```python
def test_error_handling(self, mock_engine, mock_sessionmaker, mock_func):
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_func.side_effect = RuntimeError("Something failed")

    with pytest.raises(RuntimeError) as exc_info:
        your_task.apply(kwargs={}).get()  # type: ignore[attr-defined]

    assert "Something failed" in str(exc_info.value)
```

#### Testing Database Connection Failures

```python
def test_database_connection_failure(self, mock_engine, mock_sessionmaker, mock_func):
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    from sqlalchemy.exc import OperationalError
    mock_session.execute.side_effect = OperationalError("Connection failed", None, None)

    with pytest.raises(OperationalError):
        your_task()  # type: ignore[call-arg]
```

#### Testing Logging

```python
def test_logging_on_success(self, mock_engine, mock_sessionmaker, mock_func):
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_func.return_value = {"status": "success"}

    with patch("path.to.task.logger") as mock_logger:
        your_task()  # type: ignore[call-arg]

        mock_logger.info.assert_any_call("Starting task")
        assert any("completed" in str(call) for call in mock_logger.info.call_args_list)
```

#### Testing Retry Behavior

```python
def test_retry_behavior(self, mock_engine, mock_sessionmaker, mock_func):
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_func.side_effect = RuntimeError("Temporary error")

    with patch.object(your_task, "retry") as mock_retry:
        mock_retry.side_effect = RuntimeError("Temporary error")

        with pytest.raises(RuntimeError):
            your_task.apply(kwargs={}).get()  # type: ignore[attr-defined]
```

### Testing Async Tasks

For tasks using `asyncio.run()`:

```python
@patch("path.to.task.asyncio")
def test_async_task(self, mock_asyncio):
    mock_asyncio.run.return_value = {"status": "success"}

    result = your_async_task()  # type: ignore[call-arg]

    assert result is None
    mock_asyncio.run.assert_called_once()
```

### Troubleshooting

#### "Expected 'commit' to have been called once. Called 0 times."

**Solution:** Your mock session isn't being used by the task. Make sure you're mocking the full chain:

```python
# Wrong - only mocks one level
mock_sessionmaker.return_value = mock_session

# Correct - mocks the full chain
mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
```

#### Task hangs or times out

**Solution:** Make sure you're properly mocking the `__exit__` method:

```python
mock_sessionmaker.return_value.return_value.__exit__.return_value = False
```

## Coverage and Quality

### Running with Coverage

```bash
# Coverage report in terminal
just test-cov

# Generate HTML coverage report
uv run pytest --cov=robosystems --cov-report=html
open htmlcov/index.html

# Show missing lines
uv run pytest --cov=robosystems --cov-report=term-missing

# Fail if coverage below threshold
uv run pytest --cov=robosystems --cov-fail-under=80
```

### Code Quality Checks

```bash
# Run all quality checks (includes tests)
just test-all

# Individual checks
just lint           # Ruff linting
just format         # Ruff formatting
just typecheck      # Pyright type checking
```

## Continuous Integration

Tests run automatically in GitHub Actions on:

- Every pull request
- Every push to `main` or `staging`
- Manual workflow dispatch

CI runs:

1. Linting and formatting checks
2. Type checking
3. Unit tests (fast)
4. Integration tests (with PostgreSQL)
5. Coverage reporting

E2E tests run separately as they require the full Docker stack.

## Debugging Tests

### Common Issues

#### Import Errors

```bash
# Ensure you're using uv run
uv run pytest  # ✓ Correct
pytest         # ✗ Wrong - may use system Python
```

#### Database Connection Errors

```bash
# Check PostgreSQL is running
docker ps | grep postgres

# Verify test database exists
psql -h localhost -U postgres -l | grep robosystems_test
```

#### Fixture Not Found

```python
# Check fixture scope and location
# Fixtures must be in conftest.py or imported
pytest --fixtures  # List all available fixtures
```

### Debug Mode

```bash
# Drop into debugger on failure
uv run pytest --pdb

# Drop into debugger on first failure
uv run pytest -x --pdb

# Print output even on success
uv run pytest -s

# Very verbose output
uv run pytest -vv

# Show local variables on failure
uv run pytest -l --tb=long
```

## Performance Optimization

### Speeding Up Tests

```bash
# Run only fast tests
uv run pytest -m "unit and not slow"

# Run in parallel (requires pytest-xdist)
uv run pytest -n auto

# Run with minimal output
uv run pytest -q

# Skip slow fixtures
uv run pytest --no-cov  # Skip coverage collection
```

### Test Isolation

- Each test using `db_session` gets a rolled-back transaction
- Integration tests should clean up created resources
- Use unique identifiers (UUIDs) to avoid conflicts
- Parallel tests should not share mutable state

## Contributing New Tests

When adding new tests:

1. **Choose the right location** - Mirror the source code structure
2. **Add appropriate markers** - `@pytest.mark.unit`, `@pytest.mark.integration`, etc.
3. **Follow naming conventions** - `test_*.py`, `Test*` classes, `test_*` functions
4. **Write descriptive docstrings** - Explain what the test verifies
5. **Use existing fixtures** - Don't duplicate fixture setup
6. **Clean up resources** - Integration tests should clean up after themselves
7. **Run locally first** - Ensure tests pass before committing
8. **Check coverage** - New code should have corresponding tests

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [pytest-asyncio documentation](https://pytest-asyncio.readthedocs.io/)
- [Dagster testing guide](https://docs.dagster.io/concepts/testing)
- [FastAPI testing guide](https://fastapi.tiangolo.com/tutorial/testing/)
- [Coverage.py documentation](https://coverage.readthedocs.io/)
