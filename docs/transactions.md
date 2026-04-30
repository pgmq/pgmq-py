# Transactions

All write operations in `pgmq` are transactional by default thanks to the built-in decorators. You can also compose multiple operations into a single transaction by passing a connection object manually.

## Transaction Decorators

Four decorators are provided, one for each client backend:

```python
from pgmq.decorators import (
    transaction,              # psycopg sync
    async_transaction,        # asyncpg
    sqlalchemy_transaction,   # SQLAlchemy sync
    sqlalchemy_async_transaction,  # SQLAlchemy async
)
```

## How Decorators Work

1. Check if `conn` is already provided in `kwargs`.
2. If not, acquire a connection (and start a transaction) from the pool or engine.
3. Inject `conn` into the function.
4. Commit on success, rollback on exception.

## Example: Automatic Transaction

```python
from pgmq import PGMQueue
from pgmq.decorators import transaction

queue = PGMQueue()

@transaction
def send_and_read(queue, conn=None):
    queue.create_queue("tx_queue", conn=conn)
    msg_id = queue.send("tx_queue", {"hello": "world"}, conn=conn)
    msg = queue.read("tx_queue", vt=30, conn=conn)
    return msg

msg = send_and_read(queue)
```

## Example: Async Transaction

```python
from pgmq import AsyncPGMQueue
from pgmq.decorators import async_transaction

queue = AsyncPGMQueue()
await queue.init()

@async_transaction
async def send_and_read(queue, conn=None):
    await queue.create_queue("tx_queue", conn=conn)
    msg_id = await queue.send("tx_queue", {"hello": "world"}, conn=conn)
    msg = await queue.read("tx_queue", vt=30, conn=conn)
    return msg

msg = await send_and_read(queue)
```

## Manual Connection Passing

You can bypass decorators and manage transactions yourself by passing `conn` to each operation:

```python
with queue.pool.connection() as conn:
    with conn.transaction():
        queue.send("my_queue", {"a": 1}, conn=conn)
        queue.send("my_queue", {"b": 2}, conn=conn)
```

For SQLAlchemy:

```python
with queue.engine.begin() as conn:
    queue.send("my_queue", {"a": 1}, conn=conn)
    queue.send("my_queue", {"b": 2}, conn=conn)
```

## Nested Transactions

Decorators detect when `conn` is already present and will not start a new transaction, allowing safe nesting:

```python
@transaction
def outer(queue, conn=None):
    inner(queue, conn=conn)  # reuses the same connection

@transaction
def inner(queue, conn=None):
    queue.send("my_queue", {"nested": True}, conn=conn)
```

## Read-Only Operations

Methods that do not modify data (e.g., `list_queues`, `metrics`, `test_routing`) typically do not use the transaction decorator. You can still pass `conn` if you want them to run inside your managed transaction scope.
