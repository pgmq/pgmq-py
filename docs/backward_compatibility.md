# Backward Compatibility

The `pgmq` library is actively developed while maintaining strong backward compatibility. This document tracks known aliases, deprecations, and behavior changes.

## Import Aliases

| Alias | Points To | Status |
|-------|-----------|--------|
| `PGMQueue` | `SyncPGMQueue` (psycopg sync) | Stable alias |
| `SyncPGMQueue` | `pgmq.queue.PGMQueue` | Explicit sync name |
| `AsyncPGMQueue` | `pgmq.async_queue.PGMQueue` | Explicit async name |
| `SQLAlchemyPGMQueue` | `pgmq.sqlalchemy_queue.PGMQueue` | Explicit sync SQLAlchemy name |
| `SQLAlchemyAsyncPGMQueue` | `pgmq.sqlalchemy_async_queue.PGMQueue` | Explicit async SQLAlchemy name |

All aliases are exported from `pgmq` top-level package.

## Parameter Aliases

### `tz` → `delay`

The `send()` method accepts both `delay` and `tz` for specifying message delay. `tz` is a backward-compatible alias:

```python
queue.send("q", {"a": 1}, delay=60)
queue.send("q", {"a": 1}, tz=60)      # identical behavior
queue.send("q", {"a": 1}, delay=60, tz=30)  # tz wins if both provided
```

## `list_queues()` Return Type

`list_queues()` now returns `List[QueueRecord]` instead of `List[str]`.

```python
queues = queue.list_queues()
for q in queues:
    print(q.queue_name)  # access the string name
```

A `UserWarning` is emitted on the sync client to alert callers of the change.

## `read_batch()` Alias

`read_batch(queue, vt=30, batch_size=5)` is a convenience wrapper around `read(queue, vt=30, qty=5)` that always returns a list.

## Detach Archive

`detach_archive()` is deprecated in PGMQ v2.0 but remains available in the client for compatibility with older extension versions.

## SQLAlchemy Client Initialization

Sync SQLAlchemy clients initialize automatically in `__post_init__`, similar to the psycopg client. Async SQLAlchemy clients require `await queue.init()`, consistent with the asyncpg client.

## Version Synchronization

`pyproject.toml` declares the version. When releasing, ensure it is updated; `src/pgmq/__init__.py` will fetch the new version automatically.
