# Utilities

## Validation Helpers

### Validate Routing Key

```python
is_valid = queue.validate_routing_key("orders.created")
```

Returns `True` if the routing key conforms to PGMQ rules, `False` otherwise.

### Validate Topic Pattern

```python
is_valid = queue.validate_topic_pattern("orders.*")
```

Returns `True` if the pattern is valid for topic bindings.

## FIFO Indexes

FIFO indexes improve performance for ordered read operations (`read_grouped`, `read_grouped_rr`).

### Create Index for a Single Queue

```python
queue.create_fifo_index("my_queue")
```

### Create Indexes for All Queues

```python
queue.create_fifo_indexes_all()
```

## Archive Conversion

### Convert Archive to Partitioned

For queues with very large archive tables, you can convert the archive to a partitioned table:

```python
queue.convert_archive_partitioned(
    "my_queue",
    partition_interval=10000,
    retention_interval=100000,
    leading_partition=10,
)
```

### Detach Archive

Detach the archive table from PGMQ management. This is deprecated in PGMQ v2.0 but retained for backward compatibility.

```python
queue.detach_archive("my_queue")
```

## SQL Conversion Helpers

The internal `_sql.py` module provides utilities used by the async and SQLAlchemy backends. These are generally not needed by end users, but are documented here for contributors:

- `_convert_psycopg_to_asyncpg(sql)`: Converts `%s` placeholders to `$1`, `$2`, etc.
- `convert_sql_params(sql, params)`: Converts psycopg-style SQL + tuple params to SQLAlchemy `:param_N` + dict form, with special JSONB handling.
- `ASYNC_SQL_MAP`: Pre-computed dictionary of asyncpg-compatible SQL templates.

## Client Lifecycle

### Async Clients

Always close async clients to release pool resources:

```python
await queue.close()
```

### SQLAlchemy Sync Client

Dispose of the engine and close all connections:

```python
queue.dispose()
```

> If you passed an external engine, calling `dispose()` will also dispose it. Skip this call if the engine lifecycle is managed elsewhere.
