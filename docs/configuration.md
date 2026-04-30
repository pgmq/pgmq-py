# Configuration

All PGMQ clients share a common configuration layer centered around the `PGMQConfig` dataclass. You can provide settings via environment variables, a connection string, explicit keyword arguments, or a combination of all three.

## Priority of Configuration Sources

1. **Explicit keyword arguments** passed to the client constructor.
2. **Environment variables** (`PG_HOST`, `DATABASE_URL`, etc.).
3. **Built-in defaults** (`localhost`, `5432`, `postgres`, etc.).

## Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `PG_HOST` | Database host | `localhost` |
| `PG_PORT` | Database port | `5432` |
| `PG_DATABASE` | Database name | `postgres` |
| `PG_USERNAME` | Username | `postgres` |
| `PG_PASSWORD` | Password | `postgres` |
| `DATABASE_URL` | Full connection string (URI or libpq) | `None` |

## Using a Connection String

You can pass a full connection string via the `conn_string` argument or the `DATABASE_URL` environment variable. Both URI and libpq formats are supported.

### URI Format

```python
queue = PGMQueue(conn_string="postgresql://user:pass@host:5432/dbname")
```

### Libpq Format

```python
queue = PGMQueue(conn_string="host=localhost port=5432 dbname=mydb user=postgres password=secret")
```

When `conn_string` is provided, individual fields (`host`, `port`, etc.) are automatically populated from it. You can still override specific fields by passing them explicitly.

## Explicit Configuration

```python
from pgmq import PGMQueue

queue = PGMQueue(
    host="0.0.0.0",
    port="5432",
    username="postgres",
    password="postgres",
    database="postgres",
    pool_size=20,
    verbose=True,
    log_filename="pgmq.log",
)
```

## Configuration Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `conn_string` | `str \| None` | `DATABASE_URL` or `None` | Full PostgreSQL connection string. Hidden from `repr`. |
| `host` | `str` | `localhost` | Database host. |
| `port` | `str` | `5432` | Database port. |
| `database` | `str` | `postgres` | Database name. |
| `username` | `str` | `postgres` | Login username. |
| `password` | `str` | `postgres` | Login password. |
| `delay` | `int` | `0` | Default message delay (seconds). |
| `vt` | `int` | `30` | Default visibility timeout (seconds). |
| `pool_size` | `int` | `10` | Connection pool size. |
| `verbose` | `bool` | `False` | Enable `DEBUG` level logging. |
| `log_filename` | `str \| None` | `None` | Optional log file path. |
| `structured_logging` | `bool` | `False` | Output logs as JSON. |
| `log_rotation` | `bool` | `False` | Enable log file rotation. |
| `log_rotation_size` | `str` | `10 MB` | Rotation threshold (loguru format). |
| `log_retention` | `str` | `1 week` | Log retention period (loguru format). |
| `init_extension` | `bool` | `True` | Automatically run `CREATE EXTENSION IF NOT EXISTS pgmq`. |

## Reusing a Config Object

```python
from pgmq.base import PGMQConfig
from pgmq import PGMQueue, AsyncPGMQueue

config = PGMQConfig(
    host="pg.example.com",
    port="5432",
    database="app",
    username="app_user",
    password="secret",
    verbose=True,
)

sync_queue = PGMQueue(config=config)
async_queue = AsyncPGMQueue(config=config)
await async_queue.init()
```

## Security Note

`PGMQConfig.conn_string` uses `repr=False` to prevent credentials from leaking into logs or tracebacks when the config object is printed.
