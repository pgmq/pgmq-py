<div align="center">

<h1>PGMQ Python Client</h1>

<p>
  <a href="https://pypi.org/project/pgmq/">
    <img src="https://img.shields.io/pypi/v/pgmq" alt="PyPI">
  </a>
  <a href="https://pypi.org/project/pgmq/">
    <img src="https://img.shields.io/pypi/pyversions/pgmq" alt="Python Versions">
  </a>
  <a href="https://pepy.tech/projects/pgmq">
    <img src="https://static.pepy.tech/personalized-badge/pgmq?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads" alt="Downloads">
  </a>
  <a href="https://opensource.org/licenses/Apache-2.0">
    <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License">
  </a>
</p>

<p>The official Python client for <a href="https://github.com/pgmq/pgmq">PGMQ</a>.</p>

<p><strong><a href="https://pgmq.github.io/pgmq-py/">📖 Documentation</a></strong> &nbsp;&middot;&nbsp; <strong><a href="https://github.com/pgmq/pgmq-py">💻 Source</a></strong></p>

</div>

---

## What is PGMQ?

> A lightweight message queue. Like AWS SQS and RSMQ but on Postgres.

PGMQ (Postgres Message Queue) is a message queue built on Postgres. It provides reliable, transactional message processing with the familiarity of SQL. The `pgmq` Python library exposes a clean, unified API for interacting with PGMQ across four different database backends.

---

## Prerequisites

A running PostgreSQL instance with the [PGMQ extension](https://github.com/pgmq/pgmq) installed.

### Docker (recommended)

The fastest way to get started is with the pre-built Docker image:

```bash
docker run -d --name pgmq-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  ghcr.io/pgmq/pg18-pgmq:latest
```

Then connect and enable PGMQ:

```bash
psql postgres://postgres:postgres@localhost:5432/postgres -c "CREATE EXTENSION pgmq;"
```

### SQL Only

You can also install PGMQ's objects directly into the `pgmq` schema. Use this on hosted Postgres services that do not support custom extensions.

```bash
git clone https://github.com/pgmq/pgmq.git
cd pgmq
psql -f pgmq-extension/sql/pgmq.sql postgres://postgres:postgres@localhost:5432/postgres
```

Or install from Python using the bundled SQL script:

```python
from pgmq import install_pgmq_from_sql

install_pgmq_from_sql(
    host="localhost",
    port="5432",
    username="postgres",
    password="postgres",
    database="postgres",
)
```

---

## Installation

```bash
pip install pgmq
```

**Optional backends:**

<div align="center">

| Extra | Backend |
|-------|---------|
| `pgmq[async]` | asyncpg |
| `pgmq[sqlalchemy]` | SQLAlchemy (sync) |
| `pgmq[sqlalchemy-async]` | SQLAlchemy (async) |

</div>

---

## Features

**Lightweight** — No background workers or external dependencies. Just Postgres SQL objects.

**Exactly-once delivery** — Guaranteed delivery to a single consumer within a visibility timeout.

**Four identical APIs** — Swap between sync (`psycopg`), async (`asyncpg`), sync SQLAlchemy, and async SQLAlchemy with minimal changes.

**Queue management** — Create, drop, list, purge, and partition queues.

**Message operations** — Send, read, archive, delete, pop. Batch operations for high throughput.

**FIFO queues** — Ordered processing with message group keys.

**Topic routing** — Pattern-based bindings for publish-subscribe and content-based routing.

**Visibility timeouts** — Control how long a message stays hidden after reading.

**Notifications** — PostgreSQL `NOTIFY`/`LISTEN` for real-time message arrival events.

**Transactions** — Decorators and manual connection injection for complex workflows.

**Structured logging** — stdlib logging with optional `loguru` backend.

---
## Documentation

- [Getting Started](https://pgmq.github.io/pgmq-py/getting_started/) — Installation, Docker setup, and first messages
- [Configuration](https://pgmq.github.io/pgmq-py/configuration/) — Environment variables and connection strings
- [Clients](https://pgmq.github.io/pgmq-py/clients/) — Choosing and initializing backends
- [Transactions](https://pgmq.github.io/pgmq-py/transactions/) — Transaction decorators and manual connections
- [Topic Routing](https://pgmq.github.io/pgmq-py/topic_routing/) — Pattern-based message routing
- [Notifications](https://pgmq.github.io/pgmq-py/notifications/) — Real-time NOTIFY/LISTEN listeners

---

## Quick Start

**Sync (psycopg):**

```python
from pgmq import PGMQueue

queue = PGMQueue()  # reads PG_* env vars by default

# Create a queue
queue.create_queue("my_queue")

# Send a message
msg_id = queue.send("my_queue", {"hello": "world"})

# Send a batch
batch_ids = queue.send_batch("my_queue", [{"foo": "bar"}, {"baz": "qux"}])

# Read with 30s visibility timeout
msg = queue.read("my_queue", vt=30)
print(msg.message)  # {'hello': 'world'}

# Archive when done
queue.archive("my_queue", msg.msg_id)
```

**Async (asyncpg):**

```python
from pgmq import AsyncPGMQueue

queue = AsyncPGMQueue()
await queue.init()

# Create a queue
await queue.create_queue("my_queue")

# Send a message
msg_id = await queue.send("my_queue", {"hello": "world"})

# Send a batch
batch_ids = await queue.send_batch("my_queue", [{"foo": "bar"}, {"baz": "qux"}])

# Read with 30s visibility timeout
msg = await queue.read("my_queue", vt=30)
print(msg.message)  # {'hello': 'world'}

# Archive when done
await queue.archive("my_queue", msg.msg_id)
```

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
