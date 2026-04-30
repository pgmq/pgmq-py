# Deleting and Archiving

## Archive a Message

Archiving moves a message from the queue to the archive table. Archived messages are retained for auditing but are no longer visible to readers.

```python
archived: bool = queue.archive("my_queue", msg.msg_id)
```

## Archive a Batch

```python
archived_ids: list[int] = queue.archive_batch("my_queue", [msg_id_1, msg_id_2])
```

## Delete a Message

Deleting permanently removes a message. It cannot be recovered.

```python
deleted: bool = queue.delete("my_queue", msg.msg_id)
```

## Delete a Batch

```python
deleted_ids: list[int] = queue.delete_batch("my_queue", [msg_id_1, msg_id_2])
```

## Pop a Message

`pop` reads and immediately deletes a message in a single atomic operation. This is useful when you want to process a message exactly once without worrying about visibility timeouts.

```python
msg = queue.pop("my_queue")
print(msg.message)
```

You can also pop multiple messages at once:

```python
msgs = queue.pop("my_queue", qty=5)
```

### Return Type

- If `qty == 1`, returns `Message | None`.
- If `qty > 1`, returns `list[Message]`.

## Purge a Queue

Remove all messages from a queue without dropping the queue itself:

```python
purged_count: int = queue.purge("my_queue")
```

## Detach Archive

Detach the archive table from PGMQ management. This is deprecated in PGMQ v2.0 but retained for backward compatibility.

```python
queue.detach_archive("my_queue")
```

## Convert Archive to Partitioned

If your archive table has grown very large, you can convert it to a partitioned table:

```python
queue.convert_archive_partitioned(
    "my_queue",
    partition_interval=10000,
    retention_interval=100000,
    leading_partition=10,
)
```

| Parameter | Description |
|-----------|-------------|
| `partition_interval` | Rows per partition. |
| `retention_interval` | Retained partitions. |
| `leading_partition` | Number of future partitions to create. |
