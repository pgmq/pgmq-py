# Development

## Project Setup

This project uses `uv` as its build system and package manager.

```bash
# Install everything (dev + all extras + bench)
uv sync --all-groups --all-extras
```

## Running Tests

Tests require a running PostgreSQL instance with the PGMQ extension.

### Automated (Docker)

```bash
make test
```

This command:
1. Tears down any existing `pgmq-postgres` and `pgmq-plain-postgres` containers.
2. Starts a fresh PGMQ-enabled Postgres container on port `5432`.
3. Starts a plain Postgres container (no PGMQ extension) on port `5433` for SQL install tests.
4. Waits for them to be ready.
5. Runs the full test suite, including SQL install tests against plain Postgres on port `5433`.

### Manual

If you already have PGMQ installed:

```bash
make test-env
```

Override connection parameters with environment variables:

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_USERNAME=postgres
export PG_PASSWORD=postgres
export PG_DATABASE=postgres
```

### SQL-only install tests

Run SQL install tests against plain Postgres (defaults to `localhost:5433`):

```bash
make run-plain-postgres
sleep 10
make install-pgmq-sql
make test-sql-install-env
```

`make install-pgmq-sql` and `make test-sql-install-env` both target plain Postgres
via `PG_SQL_INSTALL_*` variables (see [SQL Installation](sql_installation.md)).

CI also runs this path in `.github/workflows/sql_install_tests.yml`.

## Docker Helpers

```bash
# Start a local PGMQ-enabled Postgres
make run-pgmq-postgres

# Start plain Postgres for SQL-only install tests
make run-plain-postgres

# Tear them down
make clear-postgres
make clear-plain-postgres
```

## Lint and Format

```bash
# Auto-fix and format
make format

# Check only
make lint
```

These wrap `ruff` — the only linter/formatter used in this project.

## Writing Tests

- Sync + psycopg tests go in `tests/test_integration.py`.
- Async + asyncpg tests go in `tests/test_async_integration.py`.
- SQLAlchemy sync tests go in `tests/test_sqlalchemy_integration.py`.
- SQLAlchemy async tests go in `tests/test_sqlalchemy_async_integration.py`.
- Pure Python unit tests (no DB) go in `tests/test_sql_conversion.py`.

Tests that depend on bleeding-edge PGMQ features must gracefully skip when `UndefinedFunction` or `RaiseException` is raised, because CI images may lag behind the extension.

## Building Documentation

This documentation is designed for [MkDocs](https://www.mkdocs.org/) with the [Material theme](https://squidfunk.github.io/mkdocs-material/).

### Install MkDocs

```bash
pip install mkdocs mkdocs-material mike
```

### Serve Locally

```bash
mkdocs serve
```

### Build

```bash
mkdocs build
```

### Versioned Deployment (Mike)

This project uses `mike` to deploy versioned documentation.

```bash
# Deploy the current version
mike deploy 1.1.0 latest

# Set the default version
mike set-default latest
```

`mike` stores versions as separate commits on the `gh-pages` branch and generates a version switcher in the UI.

## Contributing Guidelines

- Follow the existing code style (`ruff` enforces this).
- Add docstrings to all public methods.
- Use `log_with_context` instead of bare `print()` in library code.
- Keep SQL centralized in `src/pgmq/_sql.py`.
- Update `pyproject.toml` version when releasing.
- Add tests for new features.
