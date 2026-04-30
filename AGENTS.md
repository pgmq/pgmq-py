# AGENTS.md — pgmq-py

This file contains project-specific context for AI coding agents. The project is a Python client library for the [PGMQ](https://github.com/pgmq/pgmq) PostgreSQL extension.

---

## Project Overview

`pgmq` is the official Python client for PGMQ (Postgres Message Queue). It exposes synchronous and asynchronous APIs, with both raw driver (psycopg/asyncpg) and SQLAlchemy-based backends. The public API surface is intentionally identical across all four client variants so users can swap implementations with minimal changes.

- **Package name:** `pgmq`
- **Version:** `1.0.6` (pyproject.toml) — note that `src/pgmq/__init__.py` hard-codes `__version__ = "1.1.0"`; keep these in sync when bumping.
- **License:** Apache-2.0
- **Python support:** `>=3.9`
- **Repository:** https://github.com/pgmq/pgmq-py

### Technology Stack

| Layer | Choices |
|-------|---------|
| Build / package manager | `uv` (build backend `uv_build`) |
| Sync driver | `psycopg[binary,pool]>=3.2.10` |
| Async driver | `asyncpg>=0.30.0` (optional extra `[async]`) |
| SQLAlchemy | `sqlalchemy>=2.0.0` (optional extras `[sqlalchemy]` / `[sqlalchemy-async]`) |
| JSON | `orjson>=3.11.3` |
| Lint / format | `ruff>=0.12.12` |
| Logging | stdlib `logging` with optional `loguru` fallback |
| Benchmarks | `locust`, `pandas`, `scipy`, `typer` |
| Documentation | `mkdocs`, `mkdocs-material`, `mkdocstrings`, `mike` |

---

## Project Structure

```
src/pgmq/
  __init__.py               # Public exports, backward-compat aliases, version
  base.py                   # PGMQConfig dataclass + BaseQueue (shared init/logging)
  queue.py                  # Sync psycopg-based PGMQueue
  async_queue.py            # Async asyncpg-based PGMQueue
  sqlalchemy_queue.py       # Sync SQLAlchemy-based PGMQueue
  sqlalchemy_async_queue.py # Async SQLAlchemy-based PGMQueue
  _sql.py                   # All SQL templates + conversion helpers (%s → $N, %s → :param_N)
  messages.py               # Dataclasses mapping PGMQ composite types (Message, QueueMetrics, ...)
  decorators.py             # Transaction decorators (transaction, async_transaction, sqlalchemy_transaction, sqlalchemy_async_transaction)
  logger.py                 # LoggingManager with dual stdlib/loguru backend
  notify_listener.py        # SyncNotificationListener + AsyncNotificationListener (PostgreSQL NOTIFY/LISTEN)

tests/
  utils.py                  # PGMQTestCase base class + env-driven PG_* constants
  test_integration.py       # Sync psycopg integration tests
  test_async_integration.py # Async asyncpg integration tests
  test_sqlalchemy_integration.py       # Sync SQLAlchemy integration tests
  test_sqlalchemy_async_integration.py # Async SQLAlchemy integration tests
  test_features.py          # Partitioning, notifications, validation utilities
  test_routing.py           # Topic routing (bind/send/unbind/test_routing)
  test_notify_listener.py   # NOTIFY/LISTEN tests for all four backends
  test_sql_conversion.py    # Pure unit tests for _sql.py conversions (no DB required)
  test_logger.py            # Logger unit tests

example/
  example_app_sync.py       # Transaction decorator usage examples
  example_app_async.py      # Async transaction usage examples

benches/
  bench.py / runner.py / ... # Locust-based load testing (dependency-group "bench")

docs/
  index.md                    # Documentation homepage
  getting_started.md          # Installation & quick start
  configuration.md            # PGMQConfig reference
  clients.md                  # Four backend clients
  queue_management.md         # Create, drop, list, purge queues
  messages.md                 # Dataclasses (Message, QueueMetrics, ...)
  sending_messages.md         # send, send_batch, headers, delay
  reading_messages.md         # read, read_with_poll, FIFO variants
  deleting_and_archiving.md   # delete, archive, pop, purge
  visibility_timeout.md       # set_vt
  topic_routing.md            # Topic-based routing
  metrics.md                  # Queue statistics
  notifications.md            # NOTIFY/LISTEN & listeners
  transactions.md             # Decorators & manual transactions
  logging.md                  # Logging configuration
  utilities.md                # Validation, FIFO indexes
  backward_compatibility.md   # Migration notes
  development.md              # Tests, contributing, MkDocs/Mike

mkdocs.yml                    # MkDocs configuration (Material theme, Mike versioning)
```

---

## Build, Install, and Test Commands

All common tasks are wrapped in the `Makefile`:

```bash
# Install everything (dev + all extras + bench)
uv sync --all-groups --all-extras

# Format code and auto-fix lint issues
make format
#  → uv run ruff format src/
#  → uv run ruff check --fix --exit-zero src/

# Run lint checks without modifying files
make lint
#  → uv run ruff check src/
#  → uv run ruff format --check src/

# Run the full test suite (spins up Docker PostgreSQL automatically)
make test
#  → docker rm -f pgmq-postgres
#  → docker run -d --name pgmq-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/pgmq/pg18-pgmq:latest
#  → sleep 10
#  → uv run python -m unittest discover -s tests -p "test_*.py"
```

### Manual test run (without Makefile)

If you already have a Postgres with PGMQ running:

```bash
uv run python -m unittest discover -s tests -p "test_*.py"
```

### Docker helper

```bash
# Start a local PGMQ-enabled Postgres
make run-pgmq-postgres
#  → docker run -d --name pgmq-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/pgmq/pg18-pgmq:latest

# Tear it down
make clear-postgres

### Documentation

```bash
# Serve docs locally with live reload
make docs-serve
#  → uv run --group docs mkdocs serve

# Build static site
make docs-build
#  → uv run --group docs mkdocs build

# Deploy a versioned release (uses mike)
make docs-deploy VERSION=1.1.0
#  → uv run --group docs mike deploy 1.1.0 latest
#  → uv run --group docs mike set-default latest
```

---

## Code Style Guidelines

- **Formatter / Linter:** `ruff` only (no black, no isort, no flake8).
- **Line length:** default ruff line length (88).
- **Import style:** `ruff` handles import sorting; do not add `# isort: skip` unless absolutely necessary.
- **Docstrings:** Every module and public class/method should have a docstring. Use imperative mood ("Create a queue", not "Creates a queue").
- **Type hints:** Use modern `typing` imports (`Optional`, `List`, `Dict`, `Union`, `Any`). The codebase targets Python 3.9+.
- **Logging:** Never use bare `print()` in library code. Use `log_with_context(self.logger, logging.DEBUG, "...", key=value)`.
- **Section dividers:** In large classes (e.g., `PGMQueue`), use `====` comment blocks to group methods by feature area (Queue Management, Sending Messages, Reading Messages, etc.).

---

## Testing Instructions

### Requirements

- A running PostgreSQL instance with the `pgmq` extension installed.
- Default connection falls back to `localhost:5432`, `postgres/postgres`/`postgres`.
- Override via environment variables: `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USERNAME`, `PG_PASSWORD`, `DATABASE_URL`.

### Test Conventions

- `tests/utils.py` defines `PGMQTestCase` (sync base class). It creates a uniquely-named queue per class, purges it in `setUp`, and drops it in `tearDownClass`.
- Async tests extend `unittest.IsolatedAsyncioTestCase` and use `asyncSetUp` / `asyncTearDown`.
- Tests that depend on newer PGMQ features (topic routing, validation functions, advanced notify) **must** gracefully skip when `UndefinedFunction` is raised, because the CI image may lag behind the bleeding-edge extension.
- `test_sql_conversion.py` is the only test file that does **not** need a database.

### Adding a New Test

1. If it is sync + psycopg, add to `tests/test_integration.py` or create a new file and inherit from `tests.utils.PGMQTestCase`.
2. If it is async + asyncpg, add to `tests/test_async_integration.py` and inherit from `unittest.IsolatedAsyncioTestCase`.
3. If it tests SQLAlchemy variants, add to the corresponding `test_sqlalchemy_*_integration.py`.
4. If it tests pure Python logic (no DB), add to `tests/test_sql_conversion.py` or a new file.

---

## Architecture & Design Patterns

### Four Client Implementations, One API

The library maintains four `PGMQueue` classes with **identical public method signatures**:

| Module | Class | Backend | Transaction decorator |
|--------|-------|---------|----------------------|
| `pgmq.queue` | `PGMQueue` | psycopg + `ConnectionPool` | `@transaction` |
| `pgmq.async_queue` | `PGMQueue` | asyncpg + `Pool` | `@async_transaction` |
| `pgmq.sqlalchemy_queue` | `PGMQueue` | SQLAlchemy `Engine` (sync) | `@sqlalchemy_transaction` |
| `pgmq.sqlalchemy_async_queue` | `PGMQueue` | SQLAlchemy `AsyncEngine` | `@sqlalchemy_async_transaction` |

`pgmq/__init__.py` re-exports aliases:
- `PGMQueue` → sync psycopg version (backward compatible)
- `SyncPGMQueue`, `AsyncPGMQueue`
- `SQLAlchemyPGMQueue`, `SQLAlchemyAsyncPGMQueue`

### Configuration (`base.py`)

`PGMQConfig` is a dataclass that reads from environment variables by default but allows explicit overrides. It supports:
- Individual fields (`host`, `port`, `database`, `username`, `password`)
- Full connection strings (`conn_string` arg or `DATABASE_URL` env var) in URI or libpq format

`BaseQueue` handles logger creation via `LoggingManager.get_logger(...)`.

### SQL Centralization (`_sql.py`)

All SQL lives in `src/pgmq/_sql.py`:
- Constants use psycopg `%s` placeholders.
- `get_send_sql`, `get_send_batch_sql`, etc. return the correct template based on flags (`has_headers`, `has_delay`, `delay_is_timestamp`).
- `_convert_psycopg_to_asyncpg` converts `%s` → `$1`, `$2`, etc. The results are pre-computed into `ASYNC_SQL_MAP` at import time.
- `convert_sql_params` converts `%s` + tuple params into SQLAlchemy `:param_N` + dict form, with special JSONB handling for dicts/lists.

### Transactions (`decorators.py`)

Decorators check whether `conn` is already provided in kwargs. If not, they acquire a connection/transaction from the pool or engine, inject it as `conn`, and commit/rollback automatically. This design lets users compose multiple operations into a single transaction by passing the same `conn` around, or by using the decorator.

### Dataclasses (`messages.py`)

Row-to-object mapping uses `from_row(cls, row, json_parser=None)` classmethods. Rows can be tuples or mappings (psycopg/asyncpg/SQLAlchemy return different types). `_get_value` helper abstracts over both, preferring name-based access with index fallback.

---

## Security Considerations

- **No SQL string interpolation of user data.** All queries in `_sql.py` use placeholders (`%s`, `$N`, or `:param_N`). Queue names, routing keys, and patterns are passed as bound parameters to the PGMQ extension functions.
- **Connection string privacy:** `PGMQConfig.conn_string` has `repr=False` to avoid leaking credentials in logs or tracebacks.
- **Queue name validation:** `validate_queue_name` delegates to `pgmq.validate_queue_name()` in the database.
- **Notification channels:** The listener code quotes channel names (`LISTEN "{channel}"`) to avoid identifier injection.

---

## CI / CD

- Workflow file: `.github/workflows/pgmq_python.yml`
- Triggers on PR/push to `main` when `src/**`, `tests/**`, `pyproject.toml`, or the workflow itself changes.
- Jobs:
  1. `lints` — `make lint`
  2. `tests` — `make test` (requires Docker)
  3. `publish` — `uv build` + `uv publish` (only on `pgmq/pgmq-py` `main` branch, uses OIDC token)
- Pre-commit hooks (`.pre-commit-config.yaml`) run `ruff` with `--fix` and `ruff-format`.

### Documentation CI

- Workflow file: `.github/workflows/docs.yml`
- Triggers on push to `main` and on tags (`v*`).
- Uses `mike` to deploy versioned documentation to the `gh-pages` branch.
- Jobs:
  1. Push to `main` — deploys as `main` version, updates `latest` alias.
  2. Tag push (`v1.1.0`) — deploys as `1.1.0` version, updates `latest` alias.
  3. Sets `latest` as the default version on every deploy.
- GitHub Pages source must be set to the `gh-pages` branch (`/root`) in repo settings.

---

## Common Gotchas for Agents

1. **Async clients require explicit initialization.**
   ```python
   queue = AsyncPGMQueue(...)
   await queue.init()  # required before any operation
   ```
   The sync psycopg client initializes automatically in `__post_init__`.

2. **SQLAlchemy async client also requires `await queue.init()`.**

3. **Do not duplicate SQL strings.** If you need to add a new query, add it to `src/pgmq/_sql.py`, add it to `_ALL_SQL_CONSTANTS`, and use `get_*_sql` helpers if there are parameter variants.

4. **Keep `ASYNC_SQL_MAP` in mind.** Any new SQL constant added to `_sql.py` must be included in `_ALL_SQL_CONSTANTS` so the asyncpg pre-computed map picks it up.

5. **Backward compatibility:**
   - `tz` is an alias for `delay` in `send()`.
   - `list_queues()` returns `List[QueueRecord]` (it used to return `List[str]`). A `UserWarning` is emitted.
   - `PGMQueue` in `pgmq/__init__.py` is aliased to the sync version.

6. **Version drift:** `pyproject.toml` version and `__init__.py`'s `__version__` may be out of sync. Update both when releasing.

7. **Tests assume feature availability:** Newer PGMQ features (topic routing, validation utilities, advanced notify) are not guaranteed in all DB versions. Tests should `self.skipTest(...)` or catch `UndefinedFunction` / `RaiseException`.

---

## Useful Links

- PGMQ Extension: https://github.com/pgmq/pgmq
- Python Client Repo: https://github.com/pgmq/pgmq-py
- PyPI: https://pypi.org/project/pgmq/
