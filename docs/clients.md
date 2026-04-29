# Clients

`pgmq` provides four client implementations with **identical public method signatures**. This design lets you swap backends with minimal code changes.

## Client Overview

| Import Alias | Module | Backend | Transaction Decorator |
|--------------|--------|---------|-----------------------|
| `PGMQueue` | `pgmq.queue` | `psycopg` + `ConnectionPool` | `@transaction` |
| `SyncPGMQueue` | `pgmq.queue` | `psycopg` + `ConnectionPool` | `@transaction` |
| `AsyncPGMQueue` | `pgmq.async_queue` | `asyncpg` + `Pool` | `@async_transaction` |
| `SQLAlchemyPGMQueue` | `pgmq.sqlalchemy_queue` | SQLAlchemy `Engine` | `@sqlalchemy_transaction` |
| `SQLAlchemyAsyncPGMQueue` | `pgmq.sqlalchemy_async_queue` | SQLAlchemy `AsyncEngine` | `@sqlalchemy_async_transaction` |

> **Backward compatibility**: `PGMQueue` is aliased to the sync `psycopg` version by default.

## Sync psycopg Client

Auto-initializes in `__post_init__`. No explicit `init()` call is required.

```python
from pgmq import PGMQueue  # or SyncPGMQueue

queue = PGMQueue()
queue.create_queue("my_queue")
```

## Async asyncpg Client

Requires an explicit `await queue.init()` before use. Remember to `await queue.close()` on shutdown.

```python
from pgmq import AsyncPGMQueue

queue = AsyncPGMQueue()
await queue.init()

await queue.create_queue("my_queue")
msg_id = await queue.send("my_queue", {"hello": "world"})

await queue.close()
```

## Sync SQLAlchemy Client

Uses SQLAlchemy Core. You can pass an existing `Engine` or let the client build one from connection parameters.

```python
from pgmq import SQLAlchemyPGMQueue
from sqlalchemy import create_engine

# From parameters
queue = SQLAlchemyPGMQueue(
    host="localhost",
    port="5432",
    username="postgres",
    password="postgres",
    database="postgres",
)

# Or with an existing engine
engine = create_engine("postgresql+psycopg://...")
queue = SQLAlchemyPGMQueue(engine=engine)

queue.create_queue("my_queue")
```

You can also obtain an ORM `Session` from the queue:

```python
session = queue.session()
```

## Async SQLAlchemy Client

Requires `await queue.init()` like the asyncpg client.

```python
from pgmq import SQLAlchemyAsyncPGMQueue

queue = SQLAlchemyAsyncPGMQueue(
    host="localhost",
    port="5432",
    username="postgres",
    password="postgres",
    database="postgres",
)
await queue.init()

await queue.create_queue("my_queue")

# AsyncSession support
async with queue.session() as session:
    ...

await queue.close()
```

## Choosing a Backend

| Use Case | Recommended Client |
|----------|--------------------|
| Simple sync scripts | `PGMQueue` (psycopg) |
| High-throughput async services | `AsyncPGMQueue` (asyncpg) |
| Existing SQLAlchemy application | `SQLAlchemyPGMQueue` or `SQLAlchemyAsyncPGMQueue` |
| Need both PGMQ + ORM in one transaction | SQLAlchemy variant |

## Unified API

Regardless of backend, all clients expose the same methods:

```python
# Queue management
.create_queue(name)
.create_partitioned_queue(name, ...)
.drop_queue(name)
.list_queues()

# Messaging
.send(queue, msg)
.send_batch(queue, msgs)
.read(queue, vt=30)
.read_with_poll(queue, vt=30, qty=1, ...)
.pop(queue)

# Lifecycle
.delete(queue, msg_id)
.archive(queue, msg_id)
.purge(queue)

# And more...
```

See the remaining documentation sections for detailed usage of each feature.
