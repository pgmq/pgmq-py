# AGENTS.md — pgmq-py

This file contains project-specific context for AI coding agents. The project is a Python client library for the [PGMQ](https://github.com/pgmq/pgmq) PostgreSQL extension.

---

## Project Overview

`pgmq` is the official Python client for PGMQ (Postgres Message Queue). It exposes synchronous and asynchronous APIs, with both raw driver (psycopg/asyncpg) and SQLAlchemy-based backends. The public API surface is intentionally identical across all four client variants so users can swap implementations with minimal changes.

- **Package name:** `pgmq`
- **Version:** `1.1.2` (`pyproject.toml`); runtime `__version__` is read from installed package metadata via `importlib.metadata.version("pgmq")` (falls back to `"unknown"`).
- **License:** Apache-2.0
- **Python support:** `>=3.10` (classifiers: 3.10–3.14)
- **Authors:** Adam Hendel, Ali Tavallaie
- **Repository:** https://github.com/pgmq/pgmq-py
- **Documentation:** https://pgmq.github.io/pgmq-py/

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
| Benchmarks | `locust`, `pandas`, `pyyaml`, `scipy`, `typer` |
| Documentation | `mkdocs`, `mkdocs-material`, `mkdocstrings`, `mike` |

---

## Project Structure

```
src/pgmq/
  __init__.py               # Public exports, backward-compat aliases, dynamic version
  base.py                   # PGMQConfig, resolve_pgmq_config(), BaseQueue (shared init/logging)
  install.py                # Bundled SQL install (install_pgmq_from_sql, PGMQInstallError)
  sql/pgmq.sql              # Bundled PGMQ SQL-only schema (version marker in first line)
  _client_fields.py         # Shared dataclass fields for all PGMQueue clients
  sync_operations.py        # SyncPGMQueueOperationsMixin — all sync public methods (write once)
  async_operations.py       # AsyncPGMQueueOperationsMixin — all async public methods (write once)
  queue.py                  # Thin sync psycopg backend (pool + _execute + JSON encoding)
  async_queue.py            # Thin async asyncpg backend
  sqlalchemy_queue.py       # Thin sync SQLAlchemy backend
  sqlalchemy_async_queue.py # Thin async SQLAlchemy backend
  _sql.py                   # All SQL templates + conversion helpers (%s → $N, %s → :param_N)
  messages.py               # Dataclasses mapping PGMQ composite types
  decorators.py             # Transaction decorators (transaction, async_transaction, sqlalchemy_transaction, sqlalchemy_async_transaction)
  logger.py                 # LoggingManager with dual stdlib/loguru backend
  notify_listener.py        # SyncNotificationListener + AsyncNotificationListener (PostgreSQL NOTIFY/LISTEN)

tests/
  utils.py                  # PGMQTestCase base class + env-driven PG_* constants via PGMQConfig
  test_integration.py       # Sync psycopg integration tests
  test_async_integration.py # Async asyncpg integration tests
  test_sqlalchemy_integration.py       # Sync SQLAlchemy integration tests
  test_sqlalchemy_async_integration.py # Async SQLAlchemy integration tests
  test_features.py          # Partitioning, notifications, validation utilities
  test_routing.py           # Topic routing (bind/send/unbind/test_routing)
  test_notify_listener.py   # NOTIFY/LISTEN tests for all four backends
  test_sql_conversion.py    # Pure unit tests for _sql.py conversions (no DB required)
  test_logger.py            # Logger unit tests
  test_install.py           # SQL install unit + integration tests (plain Postgres on 5433)

example/
  example_app_sync.py       # Transaction decorator usage examples
  example_app_async.py      # Async transaction usage examples

benches/
  bench.py / runner.py / ... # Locust-based load testing (dependency-group "bench")

docs/
  index.md                    # Documentation homepage
  getting_started.md          # Installation & quick start
  sql_installation.md         # Bundled SQL install from Python
  configuration.md            # PGMQConfig reference
  clients.md                  # Four backend clients
  queue_management.md         # Create, drop, list, purge queues
  messages.md                 # Dataclasses (Message, QueueMetrics, ...)
  sending_messages.md         # send, send_batch, headers, delay
  reading_messages.md         # read, read_with_poll, FIFO variants, conditional reads
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
#  → starts PGMQ Postgres on 5432 and plain Postgres on 5433
#  → sleep 10
#  → make test-env (full suite)
#  → make test-sql-install-env (tests.test_install only)

# Run tests against an existing Postgres (no Docker)
make test-env
#  → uv run python -m unittest discover -s tests -p "test_*.py"

# SQL install tests only (plain Postgres; default PG_SQL_INSTALL_PORT=5433)
make test-sql-install-env

# Install bundled PGMQ SQL on plain Postgres (default localhost:5433)
make install-pgmq-sql
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
```

### Documentation

```bash
# Serve docs locally with live reload
make docs-serve
#  → uv run --group docs mkdocs serve

# Build static site
make docs-build
#  → uv run --group docs mkdocs build

# Deploy a versioned release (uses mike)
make docs-deploy VERSION=1.1.1
#  → uv run --group docs mike deploy 1.1.1 latest
#  → uv run --group docs mike set-default latest
```

---

## Code Style Guidelines

- **Formatter / Linter:** `ruff` only (no black, no isort, no flake8).
- **Line length:** default ruff line length (88).
- **Import style:** `ruff` handles import sorting; do not add `# isort: skip` unless absolutely necessary.
- **Docstrings:** Every module and public class/method should have a docstring. Use imperative mood ("Create a queue", not "Creates a queue").
- **Type hints:** Use modern `typing` imports (`Optional`, `List`, `Dict`, `Union`, `Any`). The codebase targets Python 3.10+.
- **Logging:** Never use bare `print()` in library code. Use `log_with_context(self.logger, logging.DEBUG, "...", key=value)`.
- **Section dividers:** In large classes (e.g., `PGMQueue`), use `====` comment blocks to group methods by feature area (Queue Management, Sending Messages, Reading Messages, etc.).

---

## Testing Instructions

### Requirements

- A running PostgreSQL instance with the `pgmq` extension installed.
- Default connection falls back to `localhost:5432`, `postgres/postgres`/`postgres`.
- Override via environment variables: `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USERNAME`, `PG_PASSWORD`, `DATABASE_URL`.
- `tests/utils.py` uses `PGMQConfig()` to resolve connection settings the same way production code does.

### Test Conventions

- `tests/utils.py` defines `PGMQTestCase` (sync base class). It creates a uniquely-named queue per class, purges it in `setUp`, and drops it in `tearDownClass`.
- Async tests extend `unittest.IsolatedAsyncioTestCase` and use `asyncSetUp` / `asyncTearDown`.
- Tests that depend on newer PGMQ features (topic routing, conditional reads, validation functions, partitioning, advanced notify) **must** gracefully skip when `UndefinedFunction`, `RaiseException`, or related errors are raised, because the CI image may lag behind the bleeding-edge extension.
- `test_sql_conversion.py` is the only test file that does **not** need a database.

### Adding a New Test

1. If it is sync + psycopg, add to `tests/test_integration.py` or create a new file and inherit from `tests.utils.PGMQTestCase`.
2. If it is async + asyncpg, add to `tests/test_async_integration.py` and inherit from `unittest.IsolatedAsyncioTestCase`.
3. If it tests SQLAlchemy variants, add to the corresponding `test_sqlalchemy_*_integration.py`.
4. If it tests pure Python logic (no DB), add to `tests/test_sql_conversion.py` or a new file.

---

## Architecture & Design Patterns

### Four Client Implementations, One API

The library maintains four `PGMQueue` classes with **identical public method signatures**. Public methods are implemented **once** in shared operation mixins; each backend file only provides connection execution and JSON encoding.

| Layer | Module | Responsibility |
|-------|--------|----------------|
| Operations (sync) | `sync_operations.py` | `SyncPGMQueueOperationsMixin` — all sync PGMQ methods |
| Operations (async) | `async_operations.py` | `AsyncPGMQueueOperationsMixin` — all async PGMQ methods |
| Shared fields | `_client_fields.py` | `PGMQClientFields` — connection/config dataclass fields |
| Backend adapter | `queue.py`, etc. | `_execute*`, `_encode_jsonb`, pool/engine init |

Transaction decorators are applied automatically via `__init_subclass__` when a backend sets `_transaction_decorator` (sync) or `_async_transaction_decorator` (async).

The four concrete clients:

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

Optional backends are imported inside `try/except ImportError` blocks — `AsyncPGMQueue`, `SQLAlchemyPGMQueue`, and `SQLAlchemyAsyncPGMQueue` are set to `None` when their dependencies are not installed.

### Public API Surface (all four clients)

Each `PGMQueue` exposes the same methods, grouped by feature area:

| Area | Methods |
|------|---------|
| Queue management | `create_queue`, `create_partitioned_queue`, `drop_queue`, `list_queues`, `validate_queue_name` |
| Sending | `send`, `send_batch`, `send_topic`, `send_batch_topic` |
| Topic routing | `bind_topic`, `unbind_topic`, `list_topic_bindings`, `test_routing` |
| Reading | `read`, `read_batch`, `read_with_poll`, `read_grouped`, `read_grouped_with_poll`, `read_grouped_rr`, `read_grouped_rr_with_poll` |
| Pop | `pop` |
| Delete / archive | `delete`, `delete_batch`, `archive`, `archive_batch`, `purge` |
| Visibility timeout | `set_vt` (single or batch `msg_id`; `int` or `datetime` vt) |
| Metrics | `metrics`, `metrics_all` |
| Notifications | `enable_notify`, `disable_notify`, `update_notify`, `list_notify_throttles` |
| Utilities | `validate_routing_key`, `validate_topic_pattern`, `create_fifo_index`, `create_fifo_indexes_all`, `convert_archive_partitioned`, `detach_archive` |

Async-only lifecycle methods: `init()` (required before operations), `close()` (asyncpg pool teardown).

### Configuration (`base.py`)

`PGMQConfig` is a dataclass that reads from environment variables by default but allows explicit overrides. It supports:
- Individual fields (`host`, `port`, `database`, `username`, `password`)
- Full connection strings (`conn_string` arg or `DATABASE_URL` env var) in URI or libpq format
- Robust URI parsing: splits on the *last* `@` so passwords containing `@` work; uses `psycopg.conninfo.conninfo_to_dict` for libpq strings
- `init_extension: bool = True` — clients auto-run `CREATE EXTENSION IF NOT EXISTS pgmq CASCADE` on init
- `dsn` property (libpq format) and `async_dsn` property (URI format with URL-encoded credentials)

Clients always connect via the re-assembled `config.dsn` / `config.async_dsn`, not the raw `conn_string`, to fix malformed URIs.

`BaseQueue` handles logger creation via `LoggingManager.get_logger(...)`.

### SQL Centralization (`_sql.py`)

All SQL lives in `src/pgmq/_sql.py`:
- Constants use psycopg `%s` placeholders.
- `get_send_sql`, `get_send_batch_sql`, `get_send_topic_sql`, `get_send_batch_topic_sql`, `get_set_vt_sql` return the correct template based on flags (`has_headers`, `has_delay`, `delay_is_timestamp`, `is_batch`, `vt_is_timestamp`).
- `_iter_placeholders` skips `%s` inside SQL string literals before conversion.
- `_convert_psycopg_to_asyncpg` converts `%s` → `$1`, `$2`, etc. Results are pre-computed into `ASYNC_SQL_MAP` at import time.
- `convert_sql_params` converts `%s` + tuple params into SQLAlchemy `:param_N` + dict form, with special JSONB handling for dicts/lists and `::` cast escaping.

### Transactions (`decorators.py`)

Decorators check whether `conn` is already provided in kwargs. If not, they acquire a connection/transaction from the pool or engine, inject it as `conn`, and commit/rollback automatically. This design lets users compose multiple operations into a single transaction by passing the same `conn` around, or by using the decorator.

- psycopg: `with conn.transaction()`
- asyncpg: explicit `txn.start()` / `commit()` / `rollback()`
- SQLAlchemy: `engine.begin()` (sync and async)

### Dataclasses (`messages.py`)

| Class | PGMQ type | Notes |
|-------|-----------|-------|
| `Message` | `message_record` | `from_row` with optional `json_parser` |
| `QueueRecord` | `queue_record` | `list_queues()` return type |
| `QueueMetrics` | `metrics_result` | Handles old (6-col) and new (7-col) schemas via `queue_visible_length` |
| `TopicBinding` | topic binding row | |
| `RoutingResult` | `test_routing` result | |
| `BatchTopicResult` | `send_batch_topic` result | Used internally, not in `__all__` |
| `NotificationThrottle` | notify throttle row | |

Row-to-object mapping uses `from_row(cls, row, json_parser=None)` classmethods. Rows can be tuples or mappings (psycopg/asyncpg/SQLAlchemy return different types). `_get_value` helper abstracts over both, preferring name-based access with index fallback.

### Notification Listeners (`notify_listener.py`)

- `SyncNotificationListener` — dedicated psycopg connection with `autocommit=True`, listens on channel `pgmq.q_{queue_name}.INSERT`
- `AsyncNotificationListener` — asyncpg connection with `add_listener`, dispatches via `asyncio.Queue`
- Both quote channel names and handle empty/malformed JSON payloads gracefully

### Logging (`logger.py`)

- `LoggingManager.get_logger()` — library-safe by default (no handlers injected unless `verbose`, `log_filename`, etc. are set)
- Dual backend: stdlib `logging` or `loguru` (when installed)
- `LoggingManager.configure_global_logging()` for app-wide setup
- `log_with_context()`, `log_performance()` decorator, `PGMQLogger` alias for backward compat
- `LoggingManager._test_mode` disables loguru `enqueue` for synchronous test assertions

---

## Security Considerations

- **No SQL string interpolation of user data.** All queries in `_sql.py` use placeholders (`%s`, `$N`, or `:param_N`). Queue names, routing keys, and patterns are passed as bound parameters to the PGMQ extension functions.
- **Connection string privacy:** `PGMQConfig.conn_string` has `repr=False` to avoid leaking credentials in logs or tracebacks.
- **Queue name validation:** `validate_queue_name` delegates to `pgmq.validate_queue_name()` in the database.
- **Notification channels:** The listener code quotes channel names (`LISTEN "{channel}"`) to avoid identifier injection.

---

## CI / CD

### Main CI (`.github/workflows/pgmq_python.yml`)

- Triggers on PR/push to `main` when `src/**`, `tests/**`, `pyproject.toml`, or the workflow itself changes.
- Python 3.13 via `uv python install 3.13`.
- Jobs:
  1. `lints` — `uv sync --all-groups` + `make lint`
  2. `tests` — `uv sync --all-groups --all-extras` + `make test` (requires Docker)

### Release (`.github/workflows/release.yml`)

- Triggers on tag push (`v*`).
- Runs full test suite, `uv build`, `uv publish` to PyPI (OIDC), and creates a GitHub Release with generated notes.
- Only runs on `pgmq/pgmq-py` repository.

### Pre-commit

`.pre-commit-config.yaml` runs `ruff` with `--fix` and `ruff-format`.

### Documentation CI (`.github/workflows/docs.yml`)

- Triggers on push to `main` and on tags (`v*`).
- Uses `mike` to deploy versioned documentation to the `gh-pages` branch.
- Jobs:
  1. Push to `main` — deploys as `main` version, updates `latest` alias.
  2. Tag push (`v1.1.1`) — deploys as `1.1.1` version, updates `latest` alias.
  3. Sets `latest` as the default version on every deploy.
- GitHub Pages source must be set to the `gh-pages` branch (`/root`) in repo settings.
- Live site: https://pgmq.github.io/pgmq-py/

---

## Common Gotchas for Agents

1. **Async clients require explicit initialization.**
   ```python
   queue = AsyncPGMQueue(...)
   await queue.init()  # required before any operation
   ```
   The sync psycopg and sync SQLAlchemy clients initialize automatically in `__post_init__`.

2. **SQLAlchemy async client also requires `await queue.init()`.** It can accept an external `AsyncEngine` via the `engine` kwarg.

3. **Do not duplicate SQL strings.** If you need to add a new query, add it to `src/pgmq/_sql.py`, add it to `_ALL_SQL_CONSTANTS`, and use `get_*_sql` helpers if there are parameter variants.

4. **Keep `ASYNC_SQL_MAP` in mind.** Any new SQL constant added to `_sql.py` must be included in `_ALL_SQL_CONSTANTS` so the asyncpg pre-computed map picks it up.

5. **Backward compatibility:**
   - `tz` is an alias for `delay` in `send()`.
   - `list_queues()` returns `List[QueueRecord]` (it used to return `List[str]`). A `UserWarning` is emitted.
   - `PGMQueue` in `pgmq/__init__.py` is aliased to the sync version.
   - `read_batch()` is a backward-compat alias for `read(..., qty=batch_size)`.

6. **Version bumping:** Update `pyproject.toml` version when releasing. Runtime `__version__` is derived from installed package metadata automatically.

7. **Tests assume feature availability:** Newer PGMQ features (topic routing, conditional reads, validation utilities, partitioning, advanced notify) are not guaranteed in all DB versions. Tests should `self.skipTest(...)` or catch `UndefinedFunction` / `RaiseException`.

8. **`init_extension=False`:** Set on `PGMQConfig` or client kwargs when the extension is pre-installed (e.g., hosted Postgres with SQL-only PGMQ install) to skip `CREATE EXTENSION`.

9. **Conditional reads:** `read()` and `read_with_poll()` accept an optional `conditional: Dict[str, Any]` parameter for JSONB-based message filtering. Requires a recent PGMQ extension version.

10. **Connection strings with special characters:** Always rely on `PGMQConfig` parsing — clients use the re-built `dsn`/`async_dsn`, not the raw input string.

---

## Public Exports (`pgmq/__init__.py`)

Everything users import from `from pgmq import ...`:

| Category | Names |
|----------|-------|
| Clients | `PGMQueue` (sync alias), `SyncPGMQueue`, `AsyncPGMQueue`, `SQLAlchemyPGMQueue`, `SQLAlchemyAsyncPGMQueue` |
| Dataclasses | `Message`, `QueueMetrics`, `QueueRecord`, `TopicBinding`, `RoutingResult`, `NotificationThrottle` |
| Decorators | `transaction`, `async_transaction`, `sqlalchemy_transaction`, `sqlalchemy_async_transaction` |
| Logging | `PGMQLogger`, `create_logger`, `log_performance` |
| SQL install | `install_pgmq_from_sql`, `install_pgmq_sql`, `get_embedded_install_sql`, `get_embedded_sql_version`, `PGMQInstallError` |
| Version | `__version__` |

Not exported but available via submodules: `PGMQConfig`, `resolve_pgmq_config`, `BaseQueue`, `SyncNotificationListener`, `AsyncNotificationListener`, `BatchTopicResult`.

Optional clients (`AsyncPGMQueue`, `SQLAlchemyPGMQueue`, `SQLAlchemyAsyncPGMQueue`) are `None` when their extra is not installed.

### Optional Extras

```bash
pip install pgmq[async]              # asyncpg
pip install pgmq[sqlalchemy]         # SQLAlchemy sync
pip install pgmq[sqlalchemy-async]   # SQLAlchemy async + asyncpg
```

### Dependency Groups (dev)

| Group | Purpose |
|-------|---------|
| `dev` | `ruff`, `pre-commit`, `loguru`, `python-dotenv` |
| `docs` | `mkdocs`, `mkdocs-material`, `mkdocstrings`, `mike` |
| `bench` | `locust`, `pandas`, `pyyaml`, `scipy`, `typer` |

---

## Per-Backend Execution Patterns

All four clients share the same public method signatures but differ in how they run SQL internally. **Add or change public methods in the operation mixins** (`sync_operations.py` / `async_operations.py`), not in each backend file.

### Sync psycopg (`queue.py`)

- Pool: `psycopg_pool.ConnectionPool`, auto-opened in `__post_init__`
- Execute: `conn.execute(sql, params)` with raw `%s` SQL from `_sql.py`
- JSONB params: wrap dicts/lists in `psycopg.types.json.Jsonb`
- Row parsing: `Message.from_row(row, lambda x: x)` — psycopg returns dicts directly
- Transaction: `@transaction` → `with conn.transaction()`

### Async asyncpg (`async_queue.py`)

- Pool: `asyncpg.create_pool(config.async_dsn)`, requires `await init()`
- Execute: looks up `_sql.ASYNC_SQL_MAP[sql]` for `$1` placeholders, passes `*params`
- JSONB params: `orjson.dumps(m).decode("utf-8")` for message arrays; asyncpg handles encoding
- Row parsing: `Message.from_row(row, _parse_jsonb)` — may need `orjson.loads` on strings
- Transaction: `@async_transaction` → explicit `txn.start()` / `commit()` / `rollback()`
- Cleanup: `await close()` shuts down the pool

### Sync SQLAlchemy (`sqlalchemy_queue.py`)

- Engine: `create_engine(config.async_dsn.replace("postgresql://", "postgresql+psycopg://"))`
- Execute: `_sql.convert_sql_params(sql, params)` → `text(converted_sql)` with `:param_N` dict
- JSONB: `convert_sql_params` auto-`json.dumps` dicts/lists when `::jsonb` cast follows placeholder
- Row parsing: `Message.from_row(row, _parse_jsonb)`
- Transaction: `@sqlalchemy_transaction` → `with engine.begin() as conn`
- External engine: pass `engine=` kwarg to skip auto-creation

### Async SQLAlchemy (`sqlalchemy_async_queue.py`)

- Engine: `create_async_engine` with `postgresql+asyncpg://` driver prefix
- Execute: same `convert_sql_params` + `text()` pattern as sync SQLAlchemy
- Requires `await init()`; accepts external `AsyncEngine` via `engine=` kwarg
- Transaction: `@sqlalchemy_async_transaction` → `async with engine.begin() as conn`

---

## Agent Workflows

### Adding a New PGMQ API Method

1. **Add SQL** to `src/pgmq/_sql.py` as a constant with `%s` placeholders.
2. **Register** the constant in `_ALL_SQL_CONSTANTS` (required for asyncpg).
3. **Add a `get_*_sql` helper** if the method has parameter variants (headers, delay, batch, etc.).
4. **Implement once in each operations mixin**:
   - Sync method → `src/pgmq/sync_operations.py` (`SyncPGMQueueOperationsMixin`)
   - Async method → `src/pgmq/async_operations.py` (`AsyncPGMQueueOperationsMixin`)
   - Use `self._encode_jsonb()` / `self._encode_jsonb_list()` for JSONB params
   - Use `Message.from_row(row, self._json_parser)` for message rows
5. **If the method needs a transaction**, add its name to `TRANSACTIONAL_SYNC_METHODS` or `TRANSACTIONAL_ASYNC_METHODS` in the same file (decorators are applied automatically).
6. **Do not duplicate logic** in `queue.py`, `async_queue.py`, `sqlalchemy_queue.py`, or `sqlalchemy_async_queue.py` — those files only implement `_execute*` and encoding.
7. **Add tests** in the matching integration test files (all four backends if applicable).
8. **Gracefully skip** in tests if the PGMQ function may not exist yet (`UndefinedFunction`).
9. **Update user docs** in `docs/` and nav entry in `mkdocs.yml` if the feature is user-facing.

### Adding a New Dataclass

1. Define in `src/pgmq/messages.py` with `from_row(cls, row)` using `_get_value`.
2. Export in `pgmq/__init__.py` and `__all__` if part of the public API.
3. Document in `docs/messages.md`.

### Modifying an Existing SQL Query

- Edit only `_sql.py` — never inline SQL in client files.
- If placeholders change, verify `convert_sql_params` and `ASYNC_SQL_MAP` still work (run `test_sql_conversion.py`).
- Check all `get_*_sql` helpers that reference the modified constant.

---

## Return Types & Parameter Conventions

| Method | Return type | Notes |
|--------|-------------|-------|
| `send` | `int` | Message ID; `-1` if no result |
| `send_batch` | `List[int]` | Empty list if `messages` is empty |
| `send_topic` | `int` | Routed message ID |
| `send_batch_topic` | `List[BatchTopicResult]` | Per-queue routing results |
| `read` | `Message \| List[Message] \| None` | Single if `qty=1`, list otherwise |
| `read_with_poll` | `List[Message]` | Always returns a list |
| `pop` | `Message \| List[Message] \| None` | Same qty semantics as `read` |
| `delete` / `archive` | `bool` | |
| `delete_batch` / `archive_batch` | `List[int]` | IDs successfully processed |
| `drop_queue` | `bool` | |
| `purge` | `int` | Count of purged messages |
| `set_vt` | `Message \| List[Message] \| None` | Batch if `msg_id` is a list |
| `list_queues` | `List[QueueRecord]` | Emits `UserWarning` |
| `metrics` | `QueueMetrics` | Raises `ValueError` if queue not found |
| `metrics_all` | `List[QueueMetrics]` | |
| `validate_queue_name` | `bool` | Raises on invalid name |
| `validate_routing_key` / `validate_topic_pattern` | `bool` | Returns `False` on invalid, no raise |

Common kwargs on almost every method: `conn=None` (for manual transaction composition).

Delay parameters accept `int` (seconds) or `datetime` (timestamptz). `send()` also accepts `tz` as a `delay` alias.

---

## Benchmarks (`benches/`)

Load-testing harness using Locust. Not part of CI.

```bash
uv sync --group bench --all-extras
uv run python -m benches.runner benches/config.yaml
```

Key files: `bench.py` (core bench logic), `runner.py` (YAML-driven multi-bench), `locustfile.py`, `payloads/` (sample JSON sizes).

---

## Release Checklist

1. Bump `version` in `pyproject.toml`.
2. Run `make lint` and `make test`.
3. Tag `vX.Y.Z` on `main` — triggers `release.yml` (PyPI publish + GitHub Release) and `docs.yml` (mike deploy).
4. Verify https://pgmq.github.io/pgmq-py/ shows the new version.

---

## Scope Rules for Agents

- **Only modify `src/`** for library changes; keep SQL in `_sql.py`.
- **Update the operation mixins** when changing the public API — backend files should stay thin adapters.
- **Do not add new dependencies** to `[project.dependencies]` without explicit user request.
- **Do not use bare `print()`** — use `log_with_context`.
- **Do not create markdown files** the user did not ask for (except `AGENTS.md` updates).
- **Prefer extending existing patterns** over introducing new abstractions.
- **Run `make lint`** after code changes; run `make test` or `make test-env` when DB is available.

---

## Useful Links

- PGMQ Extension: https://github.com/pgmq/pgmq
- Python Client Repo: https://github.com/pgmq/pgmq-py
- Documentation: https://pgmq.github.io/pgmq-py/
- PyPI: https://pypi.org/project/pgmq/