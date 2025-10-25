# Testing in RoboSystems Service

This directory contains tests for the RoboSystems Service application. The tests are organized by module and separated into different categories.

## Test Structure

- `adapters/` - Tests for external service adapters (MCP, S3)
- `processors/` - Tests for data transformation processors (XBRL, QuickBooks, schedules)
- `integration/` - End-to-end integration tests
- `graph_api/` - Tests for Kuzu database cluster services
- `middleware/` - Tests for middleware components (OpenTelemetry metrics)
- `models/` - Tests for database models (IAM, graph, financial entities)
- `operations/` - Tests for business logic services
- `routers/` - Tests for API endpoints and HTTP request handling
- `schema/` - Tests for dynamic schema management
- `tasks/` - Tests for Celery tasks and asynchronous operations

## Test Categories

Tests are marked with pytest markers to categorize them:

- **Unit Tests** (`@pytest.mark.unit`): Fast tests that don't rely on external services
- **Integration Tests** (`@pytest.mark.integration`): Tests that interact with databases or external services
- **Kuzu Integration Tests** (`@pytest.mark.kuzu_integration`): Tests that create real Kuzu databases (excluded by default)
- **Celery Tests** (`@pytest.mark.celery`): Tests that involve Celery task functionality
- **Async Tests** (`@pytest.mark.asyncio`): Tests for asynchronous operations
- **Slow Tests** (`@pytest.mark.slow`): Long-running tests (XBRL processing, large data sets)

## Running Tests

### Quick Test Commands (using justfile)

```bash
# Run standard test suite (excludes kuzu_integration tests)
just test

# Run tests with coverage report
just test-cov

# Run all tests including linting and formatting
just test-all

# Run Kuzu integration tests
just test-integration
```

### Running All Tests

```bash
# Standard test run (excludes kuzu_integration by default)
uv run pytest

# Include all tests
uv run pytest -m ""
```

### Running Only Unit Tests

```bash
uv run pytest -m unit
```

### Running Only Integration Tests

```bash
uv run pytest -m integration
```

### Running Only Celery Tests

```bash
uv run pytest -m celery
```

### Excluding Certain Test Types

```bash
# Run all tests except integration tests
uv run pytest -m "not integration"
```

### Combining Test Types

```bash
# Run tests that are both unit tests and related to celery
uv run pytest -m "unit and celery"
```

### Running Tests in a Specific Directory

```bash
# Test all task-related functionality
uv run pytest tests/tasks/

# Test business operations (entity service, user limits, etc.)
uv run pytest tests/operations/

# Test data processing processors (XBRL, trial balance, etc.)
uv run pytest tests/processors/

# Test API endpoints
uv run pytest tests/routers/
```

### Running a Specific Test File

```bash
uv run pytest tests/tasks/test_filings.py
```

### Running a Specific Test Function

```bash
uv run pytest tests/tasks/test_filings.py::test_add_entity
```

### Running a Specific Parametrized Test Case

```bash
uv run pytest tests/tasks/test_filings.py::test_add_entity[success]
```

## Test Fixtures

Common test fixtures are defined in:

- `tests/conftest.py` - Global fixtures available to all tests
- `tests/tasks/conftest.py` - Fixtures specific to task tests
- `tests/models/conftest.py` - Fixtures for model tests

## Test Environment

Tests use a separate environment from the production application:

- A test PostgreSQL database (`robosystems_test`) using port 5432
- Mock objects for external services
- Environment variables specific to testing (defined in `pytest.ini`)
- LocalStack for AWS services simulation
- Test-specific Kuzu databases in `./data/kuzu-dbs`

## Key Test Configuration

### pytest.ini Settings

- **Default Exclusions**: `kuzu_integration` tests are excluded by default
- **Async Mode**: `asyncio_mode = auto` for automatic async test handling
- **Console Output**: Progress style with INFO level logging
- **Warnings**: Deprecation and user warnings are ignored

### Environment Variables

Key test environment variables include:

- `ENVIRONMENT=test`
- `DATABASE_URL` points to test database
- Mock API keys for external services
- LocalStack endpoint for AWS services

## Test Organization Best Practices

1. **Unit tests** should be fast and isolated
2. **Integration tests** should test real interactions but use test databases
3. **Kuzu integration tests** create real database files and should be run separately
4. **Async tests** are automatically handled by pytest-asyncio
5. **Celery tests** use a test broker configuration
