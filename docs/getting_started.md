# Getting Started

## Prerequisites

- Python 3.9 or newer.
- A running PostgreSQL instance with the [PGMQ extension](https://github.com/pgmq/pgmq) installed.

## Installation

Install the base package from PyPI:

```bash
pip install pgmq
```

### Optional Extras

| Extra | Command | Backend |
|-------|---------|---------|
| Async | `pip install pgmq[async]` | `asyncpg` |
| SQLAlchemy (sync) | `pip install pgmq[sqlalchemy]` | SQLAlchemy + `psycopg` |
| SQLAlchemy (async) | `pip install pgmq[sqlalchemy-async]` | SQLAlchemy + `asyncpg` |

You can also install everything at once:

```bash
pip install pgmq[async,sqlalchemy,sqlalchemy-async]
```

## Start a Local Postgres with PGMQ

### Docker (recommended)

The fastest way to get started is with the pre-built Docker image, where PGMQ comes pre-installed as an extension in Postgres:

```bash
docker run -d --name pgmq-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  ghcr.io/pgmq/pg18-pgmq:v1.10.0
```

Then connect and enable PGMQ:

```bash
psql postgres://postgres:postgres@localhost:5432/postgres -c "CREATE EXTENSION pgmq;"
```

### SQL Only

You can also use `psql` to install PGMQ's objects directly into the `pgmq` schema in Postgres. Use this method if you are running someplace that does not natively support the PGMQ Extension.

```bash
git clone https://github.com/pgmq/pgmq.git
cd pgmq
psql -f pgmq-extension/sql/pgmq.sql postgres://postgres:postgres@localhost:5432/postgres
```

## Your First Queue

Create a file named `quickstart.py`:

```python
from pgmq import PGMQueue

# Connect using environment variables or defaults
queue = PGMQueue()

# Create a queue
queue.create_queue("my_queue")

# Send a message
msg_id = queue.send("my_queue", {"task": "send_email", "to": "user@example.com"})
print(f"Sent message {msg_id}")

# Read a message (invisible for 30 seconds)
msg = queue.read("my_queue", vt=30)
print(f"Received: {msg.message}")

# Archive it when done
queue.archive("my_queue", msg.msg_id)
```

Run it:

```bash
python quickstart.py
```

## Environment Variables

If you prefer not to hard-code credentials, set these variables before running your script:

```bash
export PG_HOST=127.0.0.1
export PG_PORT=5432
export PG_USERNAME=postgres
export PG_PASSWORD=postgres
export PG_DATABASE=postgres
# Or use a full connection string:
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres
```

## Next Steps

- Learn how to [configure](configuration.md) the client programmatically.
- Explore the four available [client backends](clients.md).
- Understand [transactions](transactions.md) for atomic multi-operation workflows.
