# PGMQ Python Client Documentation

Welcome to the official documentation for `pgmq`, the Python client library for the [PGMQ](https://github.com/pgmq/pgmq) PostgreSQL extension.

## What is PGMQ?

PGMQ (Postgres Message Queue) is a message queue built on Postgres. It provides reliable, transactional message processing with the familiarity of SQL. The `pgmq` Python library exposes a clean, unified API for interacting with PGMQ across four different database backends.

## Key Features

- **Four Identical APIs**: Swap between sync (`psycopg`), async (`asyncpg`), sync SQLAlchemy, and async SQLAlchemy with minimal code changes.
- **Queue Management**: Create, drop, list, and purge queues. Support for partitioned and unlogged queues.
- **Message Operations**: Send, read, archive, delete, and pop messages. Batch operations for high throughput.
- **Topic Routing**: Route messages to multiple queues using pattern-based bindings.
- **FIFO Support**: Grouped and round-robin reads for ordered processing.
- **Notifications**: Built-in PostgreSQL `NOTIFY`/`LISTEN` support for real-time message arrival events.
- **Transactions**: First-class decorators and manual connection injection for complex transactional workflows.
- **Flexible Configuration**: Environment variables, connection strings, or explicit parameters.
- **Structured Logging**: stdlib `logging` with optional `loguru` backend, rotation, and structured JSON output.

## Documentation Structure

| Document | Description |
|----------|-------------|
| [Getting Started](getting_started.md) | Installation, Docker setup, and your first message. |
| [Configuration](configuration.md) | `PGMQConfig`, connection strings, and environment variables. |
| [Clients](clients.md) | Choosing and initializing the four client backends. |
| [Queue Management](queue_management.md) | Creating, dropping, listing, and partitioning queues. |
| [Messages](messages.md) | Dataclasses: `Message`, `QueueRecord`, `QueueMetrics`, and more. |
| [Sending Messages](sending_messages.md) | `send`, `send_batch`, headers, and delayed delivery. |
| [Reading Messages](reading_messages.md) | `read`, `read_with_poll`, conditional reads, and FIFO variants. |
| [Deleting and Archiving](deleting_and_archiving.md) | `delete`, `archive`, `pop`, and `purge`. |
| [Visibility Timeout](visibility_timeout.md) | Extending or changing message visibility with `set_vt`. |
| [Topic Routing](topic_routing.md) | Pattern-based message routing across queues. |
| [Metrics](metrics.md) | Queue statistics and monitoring. |
| [Notifications](notifications.md) | Real-time notifications and listeners. |
| [Transactions](transactions.md) | Transaction decorators and manual connection passing. |
| [Logging](logging.md) | Configuring loggers, structured output, and performance tracking. |
| [Utilities](utilities.md) | Validation helpers, FIFO indexes, and archive conversion. |
| [Backward Compatibility](backward_compatibility.md) | Migration notes and legacy behavior. |
| [Development](development.md) | Running tests and contributing to the library. |

## Quick Example

```python
from pgmq import PGMQueue

queue = PGMQueue()
queue.create_queue("my_queue")
msg_id = queue.send("my_queue", {"hello": "world"})
msg = queue.read("my_queue", vt=30)
print(msg.message)  # {'hello': 'world'}
```

For async usage:

```python
from pgmq import AsyncPGMQueue

queue = AsyncPGMQueue()
await queue.init()
await queue.create_queue("my_queue")
msg_id = await queue.send("my_queue", {"hello": "world"})
```

## Backward Compatibility

This library is actively developed while maintaining strong backward compatibility. See [Backward Compatibility](backward_compatibility.md) for details on deprecated aliases, behavior changes, and migration paths.
