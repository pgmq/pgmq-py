# Project rules

This file is the project layer for agents that change this repository.
Consumers and external LLMs should start at `llms.txt`.
The task protocol lives in the installed kit.

## Stack

- Language: Python 3.10 or newer (classifiers 3.10–3.14)
- Framework: Official PGMQ Python client (`pgmq`). Build with `uv` (`uv_build`). Four backends share one public API: psycopg, asyncpg, SQLAlchemy sync, SQLAlchemy async.
- Test runner: `unittest` through `uv run python -m unittest`

## Verify commands

Run these commands in the Verify step. Report the result of each one. Show the output.

| Command | Gate |
|---------|------|
| `make lint` | Zero ruff check and format failures. |
| `make test-env` | Zero test failures against an existing Postgres with PGMQ. |
| `make test` | Zero test failures. Starts Docker PGMQ Postgres on 5432 and plain Postgres on 5433. |
| `make test-sql-install-env` | Zero SQL-install test failures against plain Postgres (default port 5433). |
| `make build` | Vendors pinned `pgmq.sql`, runs `uv build`, and fails if the wheel/sdist omit the script. |

| Layer | Choices |
|-------|---------|
| Build / package manager | `uv` (build backend `uv_build`) |
| Sync driver | `psycopg[binary,pool]>=3.2.10` |
| Async driver | `asyncpg>=0.30.0` (optional extra `[async]`) |
| SQLAlchemy | `sqlalchemy>=2.0.0` (optional extras `[sqlalchemy]` / `[sqlalchemy-async]`) |
| HTTP API | FastAPI + uvicorn + PyYAML (optional extra `[fastapi]`) |
| JSON | `orjson>=3.11.3` |
| Lint / format | `ruff>=0.12.12` |
| Logging | stdlib `logging` with optional `loguru` fallback |
| Benchmarks | `locust`, `pandas`, `pyyaml`, `scipy`, `typer` |
| Documentation | `mkdocs`, `mkdocs-material`, `mkdocstrings`, `mike` |
In a fresh checkout, run `make vendor-pgmq-sql` before SQL-install tests. Use `make build` (not bare `uv build`) so the wheel includes `pgmq.sql`. `src/pgmq/sql/pgmq.sql` is not in git.

Other Makefile targets: `make format`, `make install-pgmq-sql`, `make run-pgmq-postgres`, `make run-plain-postgres`, `make docs-serve`, `make docs-build`.

## Integration branches

```
src/pgmq/
  __init__.py               # Public exports, backward-compat aliases, dynamic version
  base.py                   # PGMQConfig dataclass + BaseQueue (shared init/logging)
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
  api/                      # Optional HTTP extra (pgmq[fastapi]): factory, routes, YAML config, worker keys

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
  test_api.py               # HTTP API (HAS_API guard; skip when extra or DB is missing)

example/
  example_app_sync.py       # Transaction decorator usage examples
  example_app_async.py      # Async transaction usage examples
  pgmq-api.yaml             # Sample HTTP API config (no secrets)

benches/
  bench.py / runner.py / ... # Locust-based load testing (dependency-group "bench")

docs/
  index.md                    # Documentation homepage
  getting_started.md          # Installation & quick start
  configuration.md            # PGMQConfig reference
  clients.md                  # Four backend clients
  http_api.md                 # Optional FastAPI /v1 REST adapter
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
Do not push directly to a branch in this table.

| Branch | Note |
|--------|------|
| `main` | Default integration branch (`origin/HEAD`). |
| `gh-pages` | Mike documentation deploy only. |

## Ticket and branch

- Ticket key format: GitHub issue or pull-request number when one exists (`25`, `#40`)
- Branch name format: `{issue}-{short-description}` when an issue exists (`25-install-pgmq-sql-from-python`); otherwise a short name
- Commit message format: `type(scope): description` or `type: description`. A pull-request number may follow as `(#N)`.

## Task protocol

Follow the `task-protocol` skill for a feature, a bug fix, and a ticket.

Use the plan mode of the current harness. Wait for the user to accept the plan. Do not write product code before that.

# Run tests against an existing Postgres (no Docker)
make test-env
#  → uv run python -m unittest discover -s tests -p "test_*.py"

# HTTP API only
uv run python -m unittest tests.test_api
# Serve locally (requires pgmq[fastapi])
uv run python -m pgmq.api --host 127.0.0.1 --port 8080
#  → uvicorn pgmq.api.asgi:app, timeout_keep_alive = poll cap + 10
```

### Manual test run (without Makefile)
Use the `coder` agent for Build and Verify when the harness has that agent.

Use the `reviewer` agent for Review and the pull request draft when the harness has that agent.

## Optional gates

Uncomment a line that this project needs.

- The user must write the word `Approved` before Build.
- After Commit, write an explanation document in `plans/<TICKET>/explained/`.

## Adopted project rules

### Compatibility contract

The public API of all four `PGMQueue` classes must stay identical. Do not add a method to one backend only. Do not change a return type, rename a public export, or remove a backward-compat alias without an explicit user request.

`pgmq/__init__.py` exports:

| Category | Names |
|----------|-------|
| Clients | `PGMQueue` (sync alias), `SyncPGMQueue`, `AsyncPGMQueue`, `SQLAlchemyPGMQueue`, `SQLAlchemyAsyncPGMQueue` |
| Dataclasses | `Message`, `QueueMetrics`, `QueueRecord`, `TopicBinding`, `RoutingResult`, `NotificationThrottle` |
| Decorators | `transaction`, `async_transaction`, `sqlalchemy_transaction`, `sqlalchemy_async_transaction` |
| Logging | `PGMQLogger`, `create_logger`, `log_performance` |
| SQL install | `install_pgmq_from_sql`, `install_pgmq_sql`, `get_embedded_install_sql`, `get_embedded_sql_version`, `PGMQInstallError` |
| Version | `__version__` |

Optional clients are `None` when their extra is not installed. Not exported but available via submodules: `PGMQConfig`, `resolve_pgmq_config`, `BaseQueue`, `SyncNotificationListener`, `AsyncNotificationListener`, `BatchTopicResult`.

Backward-compat aliases that must remain:

- `tz` is an alias for `delay` in `send()`.
- `list_queues()` returns `List[QueueRecord]` (it used to return `List[str]`). A `UserWarning` is emitted.
- `PGMQueue` in `pgmq/__init__.py` is the sync psycopg client.
- `read_batch()` is an alias for `read(..., qty=batch_size)`.

Runtime `__version__` comes from `importlib.metadata.version("pgmq")`. Bump `version` in `pyproject.toml` on release. Tag `vX.Y.Z` on `main`.

### Architecture

Public methods live once in the operation mixins. Backend files only run SQL and encode JSON.

| Layer | Module | Responsibility |
|-------|--------|----------------|
| Operations (sync) | `sync_operations.py` | `SyncPGMQueueOperationsMixin` |
| Operations (async) | `async_operations.py` | `AsyncPGMQueueOperationsMixin` |
| Shared fields | `_client_fields.py` | `PGMQClientFields` |
| Backend adapter | `queue.py`, `async_queue.py`, `sqlalchemy_queue.py`, `sqlalchemy_async_queue.py` | `_execute*`, `_encode_jsonb`, pool or engine |

| Module | Backend | Transaction decorator |
|--------|---------|----------------------|
| `pgmq.queue` | psycopg + `ConnectionPool` | `@transaction` |
| `pgmq.async_queue` | asyncpg + `Pool` | `@async_transaction` |
| `pgmq.sqlalchemy_queue` | SQLAlchemy `Engine` | `@sqlalchemy_transaction` |
| `pgmq.sqlalchemy_async_queue` | SQLAlchemy `AsyncEngine` | `@sqlalchemy_async_transaction` |

Keep every query string in `src/pgmq/_sql.py` with `%s` placeholders. Register each new constant in `_ALL_SQL_CONSTANTS`. Do not interpolate user data into SQL.

Connect through `PGMQConfig.dsn` / `async_dsn`, not the raw connection string. `PGMQConfig.conn_string` has `repr=False`.

Pin the SQL-only snapshot in `src/pgmq/sql/VERSION`. `src/pgmq/sql/pgmq.sql` is downloaded at CI and build time. SQL-only install does not support extension versioning or upgrades.

### Public API surface

Each `PGMQueue` exposes the same methods:

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

Async-only lifecycle: `init()` (required before operations) and `close()`.

| Method | Return type | Notes |
|--------|-------------|-------|
| `send` | `int` | Message ID; `-1` if no result |
| `send_batch` | `List[int]` | Empty list if `messages` is empty |
| `send_topic` | `int` | Routed message ID |
| `send_batch_topic` | `List[BatchTopicResult]` | Per-queue routing results |
| `read` | `Message \| List[Message] \| None` | Single if `qty=1`, list otherwise |
| `read_with_poll` | `List[Message]` | Always a list |
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

Common kwarg: `conn=None` for manual transaction composition. Delay accepts `int` seconds or `datetime`. `send()` also accepts `tz` as a `delay` alias. `read()` and `read_with_poll()` accept optional `conditional: Dict[str, Any]` on recent extension versions.

Dataclasses in `messages.py` use `from_row` and `_get_value` so rows may be tuples or mappings.

### Adding a public method

1. Add SQL to `src/pgmq/_sql.py` and register it in `_ALL_SQL_CONSTANTS`.
2. Add a `get_*_sql` helper when the method has parameter variants.
3. Implement once in `SyncPGMQueueOperationsMixin` and once in `AsyncPGMQueueOperationsMixin`.
4. Add the name to `TRANSACTIONAL_SYNC_METHODS` or `TRANSACTIONAL_ASYNC_METHODS` when it needs a transaction.
5. Do not duplicate logic in the four backend files.
6. Add tests for each backend that should support it. Skip with `UndefinedFunction` / `RaiseException` when the extension may lack the function.
7. Export new public names from `pgmq/__init__.py`. Document them in `docs/` and `llms.txt`.

### Style and scope

- Use `ruff` only. Line length is 88. Write docstrings in the imperative mood. Use `typing` names (`Optional`, `List`, `Dict`, `Union`, `Any`).
- Do not use bare `print()` in library code. Use `log_with_context`.
- Do not add a dependency to `[project.dependencies]` unless the user asks.
- Do not create a markdown file unless the user asks, except updates to `AGENTS.md` and `llms.txt`.
- Change library code only under `src/`. Prefer existing patterns over new abstractions.
- `tests/test_sql_conversion.py` does not need a database. Other tests need Postgres. Default connection is `localhost:5432` with `postgres`/`postgres`/`postgres`. Override with `PG_HOST`, `PG_PORT`, `PG_DATABASE`, `PG_USERNAME`, `PG_PASSWORD`, or `DATABASE_URL`.
- Set `init_extension=False` when PGMQ is already installed as SQL-only.

### Layout (source of change)

```
src/pgmq/
  __init__.py               # Public exports and aliases
  base.py                   # PGMQConfig, resolve_pgmq_config(), BaseQueue
  install.py                # SQL-only install
  sql/VERSION               # Pinned PGMQ extension semver
  sql/pgmq.sql              # Not committed; fetched from the pin
  _client_fields.py
  sync_operations.py / async_operations.py
  queue.py / async_queue.py / sqlalchemy_queue.py / sqlalchemy_async_queue.py
  _sql.py
  messages.py / decorators.py / logger.py / notify_listener.py
```

User docs live in `docs/` (`mkdocs.yml`). CI lives in `.github/workflows/`.

## Hard rules

Obey `protocol/HARD-RULES.md` in the task-protocol kit. A rule in this file may add a project constraint. It must not weaken a hard rule about secrets, data, or verify evidence.
