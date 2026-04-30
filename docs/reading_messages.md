# Reading Messages

## Basic Read

Read one or more messages from a queue. The messages become invisible to other readers for the duration of the visibility timeout (`vt`).

```python
# Read a single message
msg = queue.read("my_queue", vt=30)

# Read multiple messages
msgs = queue.read("my_queue", vt=30, qty=5)
```

| Parameter | Description |
|-----------|-------------|
| `vt` | Visibility timeout in seconds. Defaults to the client's `config.vt` (30). |
| `qty` | Number of messages to read. Defaults to `1`. |

### Return Type

- If `qty == 1`, returns `Message | None`.
- If `qty > 1`, returns `list[Message]`.

## Read Batch (Backward Compatibility)

`read_batch` is an alias for `read` with `batch_size` mapped to `qty`:

```python
msgs = queue.read_batch("my_queue", vt=30, batch_size=5)
```

Always returns a `list[Message]` (empty if nothing found).

## Read with Polling

Repeatedly poll the queue until messages are found or the timeout expires.

```python
msgs = queue.read_with_poll(
    "my_queue",
    vt=30,
    qty=5,
    max_poll_seconds=5,
    poll_interval_ms=100,
)
```

| Parameter | Description |
|-----------|-------------|
| `max_poll_seconds` | Maximum total time to keep polling. |
| `poll_interval_ms` | Wait time between successive polls. |

## Conditional Reads

Read messages matching a JSONB condition. This is useful when messages contain a `headers` field or specific payload shapes.

```python
msgs = queue.read(
    "my_queue",
    vt=30,
    qty=10,
    conditional={"status": "pending"},
)
```

Conditional reads also work with `read_with_poll`.

## FIFO Reads

PGMQ supports FIFO (First-In-First-Out) semantics for ordered processing.

### Grouped Read

SQS-style batch filling that groups related messages:

```python
msgs = queue.read_grouped("my_queue", vt=30, qty=10)
```

### Grouped Read with Poll

```python
msgs = queue.read_grouped_with_poll(
    "my_queue", vt=30, qty=10, max_poll_seconds=5, poll_interval_ms=100
)
```

### Round-Robin Read

Interleave messages across partitions for balanced consumption:

```python
msgs = queue.read_grouped_rr("my_queue", vt=30, qty=10)
```

### Round-Robin Read with Poll

```python
msgs = queue.read_grouped_rr_with_poll(
    "my_queue", vt=30, qty=10, max_poll_seconds=5, poll_interval_ms=100
)
```

> FIFO operations require the queue to have a FIFO index. See [Utilities](utilities.md) for `create_fifo_index`.

## Handling the Result

```python
msg = queue.read("my_queue", vt=30)
if msg is None:
    print("No messages available")
else:
    print(f"ID: {msg.msg_id}, Body: {msg.message}")
    # Process the message, then archive or delete it
    queue.archive("my_queue", msg.msg_id)
```
