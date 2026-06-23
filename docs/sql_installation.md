# SQL Installation

Install PGMQ objects directly into the `pgmq` schema when the PostgreSQL
extension cannot be created (for example on managed Postgres without custom
extensions).

## Install from Python

```python
from pgmq import install_pgmq_from_sql, PGMQueue

version = install_pgmq_from_sql(
    host="localhost",
    port="5432",
    username="postgres",
    password="postgres",
    database="postgres",
)
print(f"Installed PGMQ SQL version {version}")

queue = PGMQueue(
    host="localhost",
    port="5432",
    username="postgres",
    password="postgres",
    database="postgres",
    init_extension=False,
)
```

Or pass a libpq connection string when you need extra options such as
`sslmode` or `connect_timeout`:

```python
install_pgmq_from_sql(
    dsn=(
        "host=db.example.com port=5432 dbname=postgres "
        "user=postgres password=secret sslmode=require connect_timeout=10"
    )
)
```

## Connection parameters

`install_pgmq_from_sql()` and `install_pgmq_sql()` accept
[`PGMQConfig`][pgmq.base.PGMQConfig] fields as keyword arguments (`host`,
`port`, `database`, `username`, `password`, and client options such as
`verbose`).

Libpq-only connection options (`sslmode`, `connect_timeout`, `keepalives`,
and similar) are **not** accepted as keyword arguments. Pass them inside
`dsn` or `conn_string` instead. Unknown keyword arguments raise
`ValueError`.

When you pass explicit connection fields, `DATABASE_URL` is ignored for the
install connection (same behavior as [`PGMQueue`][pgmq.queue.PGMQueue]).

## Exceptions

| Exception | When |
|-----------|------|
| [`PGMQInstallError`][pgmq.install.PGMQInstallError] | Connection failure, missing bundled SQL, or SQL execution failure |
| `ValueError` | Conflicting `config` + kwargs, or unsupported keyword arguments |

## After installation

Create clients with `init_extension=False` so the library does not run
`CREATE EXTENSION`. You can also set the environment variable:

```bash
export PG_INIT_EXTENSION=false
```

## Versioning

The bundled SQL script includes a version marker
(`-- pgmq-py bundled SQL version: ...`). `install_pgmq_from_sql()` returns
that version string.

Version upgrades are not supported yet. Re-running the installer on a database
that already has the `pgmq` schema is not supported and will raise
`PGMQInstallError` (for example when composite types already exist).

## Makefile helpers

```bash
# Start plain Postgres on port 5433 (no PGMQ extension)
make run-plain-postgres

# Install bundled SQL on plain Postgres (defaults to localhost:5433)
make install-pgmq-sql

# Run SQL install tests only
make test-sql-install-env
```

Override the plain Postgres target with `PG_SQL_INSTALL_HOST`,
`PG_SQL_INSTALL_PORT`, `PG_SQL_INSTALL_DATABASE`, `PG_SQL_INSTALL_USERNAME`,
and `PG_SQL_INSTALL_PASSWORD`.

## Low-level API

[`install_pgmq_sql()`][pgmq.install.install_pgmq_sql] executes arbitrary SQL
(typically from [`get_embedded_install_sql()`][pgmq.install.get_embedded_install_sql]).
When passing an existing psycopg connection, use `autocommit=False` so the
installer can run the script inside a transaction.