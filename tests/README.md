# Testing

The test suite mirrors the application layout: `tests/<package>/` holds the tests for `robosystems/<package>/`. Put a new test where its source lives.

## Quick Start

```bash
just test                   # unit tests, excluding slow ones — the everyday loop
just test adapters          # only tests under tests/adapters/
just test-cov               # with a coverage report
just test-code              # lint, format, typecheck, CloudFormation lint (no tests)
just test-all               # test-code plus the test suite — what CI runs
just test-full              # everything, including tests marked slow
```

`just test <module>` takes a path *relative to* `tests/`, so `just test adapters` and `just test middleware/billing` work; `just test tests/adapters` does not. To run an arbitrary path, file, or node id, call pytest directly.

The recipes run the suite in parallel (`-n auto --dist loadfile`); see [Running in parallel](#running-in-parallel) for what that isolates and the two rules it imposes on tests.

Always go through `uv`:

```bash
uv run pytest    # correct
pytest           # wrong: may resolve to the system Python
```

## Markers

Declared in `pytest.ini`:

| Marker                        | Meaning                                                                 |
| ----------------------------- | ----------------------------------------------------------------------- |
| `@pytest.mark.unit`           | Fast and isolated — no databases, no network                            |
| `@pytest.mark.integration`    | May use PostgreSQL, Valkey, or create LadybugDB instances               |
| `@pytest.mark.slow`           | Long-running (XBRL processing, large datasets); excluded from `just test` |
| `@pytest.mark.security`       | Security-focused assertions                                             |
| `@pytest.mark.real_retry_delay` | Opts out of the zeroed Graph API client backoff (see `tests/graph_api/conftest.py`) |

Async tests need no marker — `asyncio_mode = auto` is set in `pytest.ini`, so `async def test_*` functions run as-is.

For a test that legitimately needs more time than the default, use `@pytest.mark.timeout(300)` alongside `@pytest.mark.slow`.

```bash
uv run pytest -m unit
uv run pytest -m integration
uv run pytest -m security
uv run pytest -m "unit and not slow"
```

## Selecting tests

```bash
# File, class, function, parametrized case
uv run pytest tests/middleware/billing/test_enforcement.py
uv run pytest tests/middleware/billing/test_enforcement.py::TestCheckCanProvisionGraph
uv run pytest tests/middleware/billing/test_enforcement.py::TestCheckCanProvisionGraph::test_can_provision_with_valid_subscription
uv run pytest "tests/routers/auth/test_login.py::test_login[success]"

# By name pattern
uv run pytest -k "storage and billing"
uv run pytest -k "test_auth or test_login"

# Reruns
uv run pytest --lf     # last failed
uv run pytest --ff     # failed first, then the rest
```

`pytest.ini` sets `addopts = -xv`, so runs are verbose and stop at the first failure by default. Pass `-p no:cacheprovider` or override with `--maxfail=N` when you want to see more than one failure at a time.

## Fixtures

### Global (`tests/conftest.py`)

| Fixture                   | Scope    | What it gives you                                        |
| ------------------------- | -------- | -------------------------------------------------------- |
| `test_db`                 | session  | The test PostgreSQL database, already migrated            |
| `db_session`              | function | A session whose transaction is rolled back after the test |
| `client`                  | module   | FastAPI `TestClient` wired to `test_db`                   |
| `client_with_mocked_auth` | function | `TestClient` with authentication stubbed out              |
| `async_client`            | function | `httpx` async client for async endpoint tests             |
| `mock_get_current_user`   | module   | Mocked authentication dependency                          |
| `test_user`               | function | A user with an API key                                    |
| `test_user_token`         | function | A JWT for `test_user`                                     |
| `other_user`              | function | A second user, for cross-tenant isolation tests           |
| `test_org`                | function | An organization owning `sample_graph`                     |
| `sample_graph`            | function | A graph record                                            |
| `test_user_graph`         | function | A graph owned by `test_user`                              |
| `test_graph_with_credits` | function | A graph with a credit allocation                          |
| `temp_lbug_db`            | function | A throwaway on-disk LadybugDB database                    |
| `lbug_repository`         | function | A repository bound to `temp_lbug_db`                      |
| `lbug_repository_with_schema` | function | The same, with the base schema installed                 |
| `mock_sec_client`         | function | Stubbed SEC EDGAR client                                  |

Packages add their own `conftest.py` — check the nearest one before writing a new fixture, and reuse rather than duplicate.

```python
def test_with_client(client):
    response = client.get("/v1/status")
    assert response.status_code == 200


def test_with_auth(client_with_mocked_auth):
    response = client_with_mocked_auth.get("/v1/user/profile")
    assert response.status_code == 200


def test_with_user(test_user):
    assert test_user.api_key is not None
```

## Running in parallel

`just test`, `test-full`, `test-cov` and CI run `pytest -n auto --dist loadfile` (pytest-xdist): one worker per core, each file's tests kept on one worker. The full non-slow suite is ~2 minutes on a 14-core machine and ~6 on the 2-core CI runner, against 12+ serially.

**What is isolated per worker, and what is not** (`tests/xdist_workers.py`, invoked from `tests/__init__.py`):

- **The platform test database — per worker.** `test_db` drops and recreates `public`, and the autouse cleanup truncates every table after each test; two workers on one database would race on that. Each worker gets `robosystems_test_<gwN>`, created on demand, and both `TEST_DATABASE_URL` (the fixtures) and `DATABASE_URL` (the app engine) move to it. The rewrite runs from the test package's `__init__` because `robosystems.config.env` reads the URL at import and `robosystems.db.platform` binds its engine from it at import — a later hook would leave the app on the shared database while the fixtures truncate a private one. The uuid-named throwaway extensions schemas some tests build live in this database, so they follow it.
- **The extensions database — shared.** Its tests use those throwaway schemas or a module-scoped tenant schema with a per-file name; `--dist loadfile` keeps a file on one worker, so a file never runs concurrently against itself. The taxonomy library it holds is read-only to tests.
- **Valkey and LocalStack — shared.** Tests that touch them live must key by their own ids.

Nothing here runs outside xdist: a plain `uv run pytest path/to/test.py` still uses `robosystems_test` directly.

**Two rules the parallel order enforces.** Both were latent for months; a new file order surfaced them in a day.

1. **Every table with a plain (non-CASCADE) foreign key into a truncated table must be in `setup_database`'s delete list, before the table it references.** One missing table makes the `users` (or `graph_credits`) delete fail, and the cleanup then rolls *everything* back — every row of that test stays alive for the rest of the worker's session, and the failure shows up later as a duplicate key in an unrelated file. The cleanup now warns with the offending constraint instead of hiding it; if you see `Test database cleanup failed`, add the table it names. To audit the list, walk `Base.metadata.sorted_tables` for FK constraints without `ondelete="CASCADE"` whose referred table is in the list and whose own table is not.
2. **A `robosystems.config.env` attribute set in a test must be set with `monkeypatch.setattr(env, ...)`.** `env` reads its variables at import, so `monkeypatch.setenv` changes nothing the code sees, and a direct `env.X = ...` assignment leaks into every later test on the session — the S3 adapter's bucket probe failing in CI was one such leak.

A third class: tests that consult a real resource whose answer depends on host load (the Graph API admission controller reads CPU and memory headroom) must mock it, or they fail only when the machine is busy — which under xdist is always.

**Verifying a change to the harness.** Run the suite twice at `-n 2` — the runner's worker count, which is a different file order from `-n auto` on a big machine — and look for both failures and cleanup warnings. A test that passes alone and fails in the suite is order-dependent: reproduce it serially by naming the earlier file and the failing file in that order on the command line.

## Test environment

Configuration comes from the `env` block in `pytest.ini`, so tests get a deterministic environment without touching `.env`.

| Dependency | Where tests expect it                                          |
| ---------- | -------------------------------------------------------------- |
| PostgreSQL | `robosystems_test` on `localhost:5432` (`robosystems_test_<gwN>` per xdist worker), schema built by `test_db` |
| Valkey     | `localhost:6379`                                               |
| Graph API  | `localhost:8001`                                               |
| LadybugDB  | `./data/lbug-dbs`                                              |
| LocalStack | `http://localhost:4566` for S3 and other AWS services          |

Rate limiting, OpenTelemetry, and CAPTCHA are disabled for tests; billing, security auditing, subgraph creation, and backup creation are enabled so their code paths are exercised. External APIs — SEC EDGAR, QuickBooks, Anthropic, OpenFIGI — are mocked; tests never make live calls.

## Writing a test

Mirror the source layout, name the file `test_*.py`, group related cases in a `Test*` class, and name each test after the behavior it asserts.

```python
"""Tests for [component]."""

import pytest
from unittest.mock import MagicMock, patch

from robosystems.module import function_to_test


class TestComponentName:
    def test_success_case(self):
        # Arrange / Act / Assert
        ...

    def test_error_case(self):
        with pytest.raises(ValueError):
            function_to_test(invalid_input)
```

A test name should say what is protected, so a failure is self-explanatory:

```python
# Good — names the behavior
def test_user_cannot_access_other_users_graphs(): ...
def test_credit_consumption_decrements_balance(): ...

# Poor — names the subject only
def test_graphs(): ...
def test_credit_system(): ...
```

Assert on specifics rather than truthiness:

```python
# Good
response = login(username, password)
assert "access_token" in response
assert response["token_type"] == "bearer"

# Poor
assert response
```

Guidelines that keep the suite fast and reliable:

1. Unit tests should run in well under 100 ms and touch nothing external.
2. Integration tests may use the database but must clean up what they create; prefer `db_session`, whose transaction is rolled back automatically.
3. Mock at the boundary — the external client or the session factory — not deep inside the code under test.
4. Use unique identifiers (UUIDs) so tests can run in any order or in parallel.
5. Mark anything long-running `@pytest.mark.slow` so the default loop stays fast.

### Mocking a session factory

Session-scoped code usually goes through a context manager, so the mock has to cover the whole chain:

```python
def test_error_handling(self, mock_engine, mock_sessionmaker, mock_func):
    mock_session = MagicMock()
    mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session
    mock_sessionmaker.return_value.return_value.__exit__.return_value = False

    mock_func.side_effect = RuntimeError("Something failed")

    with pytest.raises(RuntimeError) as exc_info:
        your_task()

    assert "Something failed" in str(exc_info.value)
```

### Asserting on logs

```python
def test_logs_completion(self, mock_func):
    mock_func.return_value = {"status": "success"}

    with patch("path.to.task.logger") as mock_logger:
        your_task()

        mock_logger.info.assert_any_call("Starting task")
        assert any("completed" in str(c) for c in mock_logger.info.call_args_list)
```

`log_cli` is off, so logs are captured rather than streamed; pytest prints them as part of the failure context, and `caplog` works normally.

### Testing code that calls `asyncio.run`

```python
@patch("path.to.task.asyncio")
def test_async_task(self, mock_asyncio):
    mock_asyncio.run.return_value = {"status": "success"}

    your_async_task()

    mock_asyncio.run.assert_called_once()
```

## End-to-end validation

Full-stack workflows are validated by the runnable demos in [`examples/`](/examples/README.md) rather than by e2e tests. They exercise authentication, graph creation, upload, ingestion, and querying against a running stack, and double as user-facing documentation.

```bash
just demo-custom-graph
just demo-sec
just demo-roboledger
```

## Coverage

```bash
just test-cov                                              # terminal report
uv run pytest --cov=robosystems --cov-report=term-missing  # with missing lines
uv run pytest --cov=robosystems --cov-report=html && open htmlcov/index.html
```

## Debugging

```bash
uv run pytest --pdb          # debugger on failure
uv run pytest -s             # don't capture stdout
uv run pytest -l --tb=long   # locals plus full tracebacks
uv run pytest --fixtures     # list every fixture visible from here
```

### Common failures

**`Expected 'commit' to have been called once. Called 0 times.`** — the code under test is not using your mock session. Mock the full chain, not just one level:

```python
mock_sessionmaker.return_value = mock_session                                  # wrong
mock_sessionmaker.return_value.return_value.__enter__.return_value = mock_session  # right
```

**A test hangs or times out** — the context manager never exits. Set `mock_sessionmaker.return_value.return_value.__exit__.return_value = False`.

**Database connection errors** — check the stack is up (`docker ps | grep postgres`) and the test database exists (`psql -h localhost -U postgres -l | grep robosystems_test`).

**Fixture not found** — fixtures must live in a `conftest.py` at or above the test's directory. `uv run pytest --fixtures` lists what is in scope.

## Continuous integration

`test-ci.yml` runs the gate on every pull request and on pushes to `main`; the staging and production deploy workflows call the same reusable `test.yml` before deploying, but they are manual dispatches, not push triggers.

The gate is close to `just test-all` but not identical. CI runs the test suite (`pytest -m "not slow"`), the dbt tests, `ruff check`, `basedpyright`, CloudFormation lint, workflow lint, and a Trivy dependency scan. It does **not** run a formatting step — `just test-all` calls `just format`, which rewrites files rather than checking them, so unformatted code passes CI and gets reformatted on your next local run. Run `just test-all` locally before opening a pull request; the suite runs in parallel and takes a few minutes. CI's `-n auto` on its 2-core runner brings the pytest step to ~6 minutes against a 15-minute step timeout.

## Reference

- [pytest](https://docs.pytest.org/) · [pytest-asyncio](https://pytest-asyncio.readthedocs.io/) · [Coverage.py](https://coverage.readthedocs.io/)
- [FastAPI testing](https://fastapi.tiangolo.com/tutorial/testing/) · [Dagster testing](https://docs.dagster.io/concepts/testing)
