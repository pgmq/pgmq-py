# PGMQ Python Client

[![PyPI](https://img.shields.io/pypi/v/pgmq)](https://pypi.org/project/pgmq/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pgmq)](https://pypi.org/project/pgmq/)
[![PyPI Downloads](https://static.pepy.tech/personalized-badge/pgmq?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/pgmq)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The official Python client for [PGMQ](https://github.com/pgmq/pgmq), a message queue built on Postgres.

**Documentation:** [pgmq.github.io/pgmq-py](https://pgmq.github.io/pgmq-py/)

---

## Installation

```bash
pip install pgmq
```

**Optional extras:**

```bash
pip install pgmq[async]            # asyncpg backend
pip install pgmq[sqlalchemy]       # sync SQLAlchemy backend
pip install pgmq[sqlalchemy-async] # async SQLAlchemy backend
```

Requires Postgres with the [PGMQ extension](https://github.com/pgmq/pgmq) installed.

---

## Quick Start

```python
from pgmq import PGMQueue

queue = PGMQueue()  # reads PG_* env vars by default

# Create a queue
queue.create_queue("my_queue")

# Send messages
msg_id = queue.send("my_queue", {"hello": "world"})
batch_ids = queue.send_batch("my_queue", [{"foo": "bar"}, {"baz": "qux"}])

# Read a message (hidden for 30 seconds)
msg = queue.read("my_queue", vt=30)
print(msg.message)  # {'hello': 'world'}

# Archive or delete when done
queue.archive("my_queue", msg.msg_id)
# queue.delete("my_queue", msg.msg_id)
```

**Async version:**

```python
from pgmq import AsyncPGMQueue

queue = AsyncPGMQueue()
await queue.init()

await queue.create_queue("my_queue")
msg = await queue.read("my_queue", vt=30)
await queue.archive("my_queue", msg.msg_id)
```

---

## Features

- **Lightweight** — No background workers or external dependencies. Just Postgres SQL objects.
- **Exactly-once delivery** — Guaranteed delivery to a single consumer within a visibility timeout.
- **Four identical APIs** — Swap between sync (`psycopg`), async (`asyncpg`), sync SQLAlchemy, and async SQLAlchemy with minimal changes.
- **Queue management** — Create, drop, list, purge, and partition queues.
- **Message operations** — Send, read, archive, delete, pop. Batch operations for high throughput.
- **FIFO queues** — Ordered processing with message group keys.
- **Topic routing** — Pattern-based bindings for publish-subscribe and content-based routing.
- **Visibility timeouts** — Control how long a message stays hidden after reading.
- **Notifications** — PostgreSQL `NOTIFY`/`LISTEN` for real-time message arrival events.
- **Transactions** — Decorators and manual connection injection for complex workflows.
- **Structured logging** — stdlib logging with optional `loguru` backend.

See the [full documentation](https://pgmq.github.io/pgmq-py/) for detailed guides on [configuration](https://pgmq.github.io/pgmq-py/configuration/), [clients](https://pgmq.github.io/pgmq-py/clients/), [transactions](https://pgmq.github.io/pgmq-py/transactions/), and more.

---

## Development

```bash
# Install dependencies
uv sync --all-groups --all-extras

# Run tests (spins up Docker Postgres automatically)
make test

# Run lints
make lint

# Serve docs locally
make docs-serve
```

---

## License

Apache-2.0
