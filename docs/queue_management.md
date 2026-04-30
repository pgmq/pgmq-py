# Queue Management

## Creating a Queue

```python
queue.create_queue("my_queue")
```

### Unlogged Queues

Unlogged queues are faster but do not survive a database crash. Use them for transient, high-throughput data.

```python
queue.create_queue("temp_queue", unlogged=True)
```

### Partitioned Queues

Partitioned queues split data across multiple tables, improving performance for very large queues.

```python
queue.create_partitioned_queue(
    "big_queue",
    partition_interval=10000,
    retention_interval=100000,
)
```

| Parameter | Description |
|-----------|-------------|
| `partition_interval` | Number of messages per partition. |
| `retention_interval` | Number of messages to retain in the active partition set. |

## Dropping a Queue

```python
dropped: bool = queue.drop_queue("my_queue")
```

Dropping a queue removes the queue table and its archive table. Returns `True` on success.

## Listing Queues

```python
queues = queue.list_queues()
for q in queues:
    print(q.queue_name, q.is_partitioned, q.created_at)
```

`list_queues()` returns a list of `QueueRecord` objects:

| Attribute | Type | Description |
|-----------|------|-------------|
| `queue_name` | `str` | Name of the queue. |
| `is_partitioned` | `bool` | Whether the queue is partitioned. |
| `is_unlogged` | `bool` | Whether the queue is unlogged. |
| `created_at` | `datetime` | Creation timestamp. |

> **Backward compatibility note**: Older versions returned `List[str]`. The current version returns `List[QueueRecord]` and emits a `UserWarning` on the sync client. Use `.queue_name` to get the string name.

## Purging a Queue

Remove all messages from a queue without deleting the queue itself:

```python
purged_count: int = queue.purge("my_queue")
```

## Validating a Queue Name

```python
queue.validate_queue_name("my_queue")  # Raises if invalid
```

This delegates validation to `pgmq.validate_queue_name()` in the database, ensuring your queue name complies with PGMQ naming rules.
