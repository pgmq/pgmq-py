# src/pgmq/_sql.py
"""
Centralized SQL templates for PGMQ operations.

This module contains all SQL queries used by the PGMQ client, ensuring
consistency between sync and async implementations and making the code
easier to maintain and audit.

It also provides ASYNC_SQL_MAP, a pre-computed dictionary of asyncpg-compatible
queries (converting %s to $1, $2, etc.) to avoid runtime string manipulation.
"""

from typing import Dict


# ============================================================================
# Queue Management
# ============================================================================

CREATE_QUEUE = "SELECT pgmq.create(queue_name=>%s);"
CREATE_UNLOGGED_QUEUE = "SELECT pgmq.create_unlogged(queue_name=>%s);"
CREATE_PARTITIONED_QUEUE = "SELECT pgmq.create_partitioned(queue_name=>%s, partition_interval=>%s, retention_interval=>%s);"
CREATE_NON_PARTITIONED = "SELECT pgmq.create_non_partitioned(queue_name=>%s);"
DROP_QUEUE = "SELECT pgmq.drop_queue(queue_name=>%s);"
LIST_QUEUES = "SELECT queue_name, created_at, is_partitioned, is_unlogged FROM pgmq.list_queues();"
VALIDATE_QUEUE_NAME = "SELECT pgmq.validate_queue_name(queue_name=>%s);"

# ============================================================================
# Sending Messages
# ============================================================================

SEND = "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb);"
SEND_WITH_DELAY_INT = (
    "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, delay=>%s::integer);"
)
SEND_WITH_DELAY_TZ = "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, delay=>%s::timestamptz);"
SEND_WITH_HEADERS = (
    "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, headers=>%s::jsonb);"
)
SEND_WITH_HEADERS_DELAY_INT = "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, headers=>%s::jsonb, delay=>%s::integer);"
SEND_WITH_HEADERS_DELAY_TZ = "SELECT * FROM pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, headers=>%s::jsonb, delay=>%s::timestamptz);"

SEND_BATCH = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[]);"
SEND_BATCH_WITH_DELAY_INT = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], delay=>%s::integer);"
SEND_BATCH_WITH_DELAY_TZ = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], delay=>%s::timestamptz);"
SEND_BATCH_WITH_HEADERS = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], headers=>%s::jsonb[]);"
SEND_BATCH_WITH_HEADERS_DELAY_INT = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], headers=>%s::jsonb[], delay=>%s::integer);"
SEND_BATCH_WITH_HEADERS_DELAY_TZ = "SELECT * FROM pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], headers=>%s::jsonb[], delay=>%s::timestamptz);"

# ============================================================================
# Topic-Based Routing
# ============================================================================

SEND_TOPIC = "SELECT pgmq.send_topic(topic=>%s::text, msg=>%s::jsonb);"
SEND_TOPIC_WITH_HEADERS = (
    "SELECT pgmq.send_topic(topic=>%s::text, msg=>%s::jsonb, headers=>%s::jsonb);"
)
SEND_TOPIC_WITH_DELAY_INT = (
    "SELECT pgmq.send_topic(topic=>%s::text, msg=>%s::jsonb, delay=>%s::integer);"
)
SEND_TOPIC_WITH_HEADERS_DELAY_INT = "SELECT pgmq.send_topic(topic=>%s::text, msg=>%s::jsonb, headers=>%s::jsonb, delay=>%s::integer);"

SEND_BATCH_TOPIC = (
    "SELECT * FROM pgmq.send_batch_topic(topic=>%s::text, msgs=>%s::jsonb[]);"
)
SEND_BATCH_TOPIC_WITH_HEADERS = "SELECT * FROM pgmq.send_batch_topic(topic=>%s::text, msgs=>%s::jsonb[], headers=>%s::jsonb[]);"
SEND_BATCH_TOPIC_WITH_DELAY_INT = "SELECT * FROM pgmq.send_batch_topic(topic=>%s::text, msgs=>%s::jsonb[], delay=>%s::integer);"
SEND_BATCH_TOPIC_WITH_DELAY_TZ = "SELECT * FROM pgmq.send_batch_topic(topic=>%s::text, msgs=>%s::jsonb[], delay=>%s::timestamptz);"
SEND_BATCH_TOPIC_WITH_HEADERS_DELAY_INT = "SELECT * FROM pgmq.send_batch_topic(topic=>%s::text, msgs=>%s::jsonb[], headers=>%s::jsonb[], delay=>%s::integer);"
SEND_BATCH_TOPIC_WITH_HEADERS_DELAY_TZ = "SELECT * FROM pgmq.send_batch_topic(topic=>%s::text, msgs=>%s::jsonb[], headers=>%s::jsonb[], delay=>%s::timestamptz);"

# Python API is bind_topic(pattern, queue_name) — SQL matches argument order
BIND_TOPIC = "SELECT pgmq.bind_topic(pattern=>%s, queue_name=>%s);"
UNBIND_TOPIC = "SELECT pgmq.unbind_topic(pattern=>%s, queue_name=>%s);"
LIST_TOPIC_BINDINGS = "SELECT pattern, queue_name, bound_at, compiled_regex FROM pgmq.list_topic_bindings();"
LIST_TOPIC_BINDINGS_FOR_QUEUE = "SELECT pattern, queue_name, bound_at, compiled_regex FROM pgmq.list_topic_bindings(queue_name=>%s);"
TEST_ROUTING = "SELECT pattern, queue_name, compiled_regex FROM pgmq.test_routing(routing_key=>%s);"

# ============================================================================
# Reading Messages
# ============================================================================

READ = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
          FROM pgmq.read(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer);"""

READ_WITH_POLL = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                    FROM pgmq.read_with_poll(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer, 
                    max_poll_seconds=>%s::integer, poll_interval_ms=>%s::integer);"""

READ_CONDITIONAL = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                      FROM pgmq.read(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer, conditional=>%s::jsonb);"""

READ_WITH_POLL_CONDITIONAL = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                                FROM pgmq.read_with_poll(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer, 
                                max_poll_seconds=>%s::integer, poll_interval_ms=>%s::integer, conditional=>%s::jsonb);"""

# ============================================================================
# FIFO Reading
# ============================================================================

READ_GROUPED = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                  FROM pgmq.read_grouped(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer);"""

READ_GROUPED_WITH_POLL = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                            FROM pgmq.read_grouped_with_poll(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer,
                            max_poll_seconds=>%s::integer, poll_interval_ms=>%s::integer);"""

READ_GROUPED_RR = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                     FROM pgmq.read_grouped_rr(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer);"""

READ_GROUPED_RR_WITH_POLL = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                               FROM pgmq.read_grouped_rr_with_poll(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer,
                               max_poll_seconds=>%s::integer, poll_interval_ms=>%s::integer);"""

# ============================================================================
# Pop
# ============================================================================

POP = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
         FROM pgmq.pop(queue_name=>%s::text, qty=>%s::integer);"""

# ============================================================================
# Deleting/Archiving
# ============================================================================

DELETE = "SELECT pgmq.delete(queue_name=>%s::text, msg_id=>%s::bigint);"
DELETE_BATCH = "SELECT * FROM pgmq.delete(queue_name=>%s::text, msg_ids=>%s::bigint[]);"
ARCHIVE = "SELECT pgmq.archive(queue_name=>%s::text, msg_id=>%s::bigint);"
ARCHIVE_BATCH = (
    "SELECT * FROM pgmq.archive(queue_name=>%s::text, msg_ids=>%s::bigint[]);"
)
PURGE_QUEUE = "SELECT pgmq.purge_queue(queue_name=>%s);"

# ============================================================================
# Visibility Timeout
# ============================================================================

# Explicit templates to avoid ambiguous function errors
SET_VT_INT = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                FROM pgmq.set_vt(queue_name=>%s::text, msg_id=>%s::bigint, vt=>%s::integer);"""

SET_VT_TZ = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
               FROM pgmq.set_vt(queue_name=>%s::text, msg_id=>%s::bigint, vt=>%s::timestamptz);"""

SET_VT_BATCH_INT = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                      FROM pgmq.set_vt(queue_name=>%s::text, msg_ids=>%s::bigint[], vt=>%s::integer);"""

SET_VT_BATCH_TZ = """SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers 
                     FROM pgmq.set_vt(queue_name=>%s::text, msg_ids=>%s::bigint[], vt=>%s::timestamptz);"""

# ============================================================================
# Metrics
# ============================================================================

METRICS = "SELECT * FROM pgmq.metrics(queue_name=>%s);"
METRICS_ALL = "SELECT * FROM pgmq.metrics_all();"

# ============================================================================
# Notifications
# ============================================================================

ENABLE_NOTIFY = "SELECT pgmq.enable_notify_insert(queue_name=>%s::text, throttle_interval_ms=>%s::integer);"
DISABLE_NOTIFY = "SELECT pgmq.disable_notify_insert(queue_name=>%s::text);"
UPDATE_NOTIFY = "SELECT pgmq.update_notify_insert(queue_name=>%s::text, throttle_interval_ms=>%s::integer);"
LIST_NOTIFY_THROTTLES = "SELECT queue_name, throttle_interval_ms, last_notified_at FROM pgmq.list_notify_insert_throttles();"

# ============================================================================
# Utilities
# ============================================================================

VALIDATE_ROUTING_KEY = "SELECT pgmq.validate_routing_key(routing_key=>%s);"
VALIDATE_TOPIC_PATTERN = "SELECT pgmq.validate_topic_pattern(pattern=>%s);"
CREATE_FIFO_INDEX = "SELECT pgmq.create_fifo_index(queue_name=>%s);"
CREATE_FIFO_INDEXES_ALL = "SELECT pgmq.create_fifo_indexes_all();"
CONVERT_ARCHIVE_PARTITIONED = "SELECT pgmq.convert_archive_partitioned(table_name=>%s, partition_interval=>%s, retention_interval=>%s, leading_partition=>%s);"
DETACH_ARCHIVE = "SELECT pgmq.detach_archive(queue_name=>%s);"


def get_send_sql(
    headers: bool = False,
    delay: bool = False,
    delay_is_timestamp: bool = False,
) -> str:
    """Get appropriate send SQL based on parameters."""
    if headers and delay:
        return (
            SEND_WITH_HEADERS_DELAY_TZ
            if delay_is_timestamp
            else SEND_WITH_HEADERS_DELAY_INT
        )
    elif headers:
        return SEND_WITH_HEADERS
    elif delay:
        return SEND_WITH_DELAY_TZ if delay_is_timestamp else SEND_WITH_DELAY_INT
    return SEND


def get_send_batch_sql(
    headers: bool = False,
    delay: bool = False,
    delay_is_timestamp: bool = False,
) -> str:
    """Get appropriate send_batch SQL based on parameters."""
    if headers and delay:
        return (
            SEND_BATCH_WITH_HEADERS_DELAY_TZ
            if delay_is_timestamp
            else SEND_BATCH_WITH_HEADERS_DELAY_INT
        )
    elif headers:
        return SEND_BATCH_WITH_HEADERS
    elif delay:
        return (
            SEND_BATCH_WITH_DELAY_TZ
            if delay_is_timestamp
            else SEND_BATCH_WITH_DELAY_INT
        )
    return SEND_BATCH


def get_send_topic_sql(
    headers: bool = False,
    delay: bool = False,
) -> str:
    """Get appropriate send_topic SQL based on parameters."""
    if headers and delay:
        return SEND_TOPIC_WITH_HEADERS_DELAY_INT
    elif headers:
        return SEND_TOPIC_WITH_HEADERS
    elif delay:
        return SEND_TOPIC_WITH_DELAY_INT
    return SEND_TOPIC


def get_send_batch_topic_sql(
    headers: bool = False,
    delay: bool = False,
    delay_is_timestamp: bool = False,
) -> str:
    """Get appropriate send_batch_topic SQL based on parameters."""
    if headers and delay:
        return (
            SEND_BATCH_TOPIC_WITH_HEADERS_DELAY_TZ
            if delay_is_timestamp
            else SEND_BATCH_TOPIC_WITH_HEADERS_DELAY_INT
        )
    elif headers:
        return SEND_BATCH_TOPIC_WITH_HEADERS
    elif delay:
        return (
            SEND_BATCH_TOPIC_WITH_DELAY_TZ
            if delay_is_timestamp
            else SEND_BATCH_TOPIC_WITH_DELAY_INT
        )
    return SEND_BATCH_TOPIC


def get_set_vt_sql(is_batch: bool = False, vt_is_timestamp: bool = False) -> str:
    """Get appropriate set_vt SQL based on parameters."""
    if is_batch:
        return SET_VT_BATCH_TZ if vt_is_timestamp else SET_VT_BATCH_INT
    return SET_VT_TZ if vt_is_timestamp else SET_VT_INT


# ============================================================================
# Asyncpg Optimization: Pre-computed SQL Map
# ============================================================================


def _convert_psycopg_to_asyncpg(sql: str) -> str:
    """
    Convert psycopg style SQL (%s) to asyncpg style ($1, $2...).
    Used once at module load time to create ASYNC_SQL_MAP.
    """
    count = sql.count("%s")
    if count == 0:
        return sql

    parts = sql.split("%s")
    result = []
    for i, part in enumerate(parts[:-1]):
        result.append(part)
        result.append(f"${i + 1}")
    result.append(parts[-1])
    return "".join(result)


# Collect all SQL constants from this module to build the map automatically
# This ensures we don't miss any new SQL strings added in the future.
_ALL_SQL_CONSTANTS = [
    CREATE_QUEUE,
    CREATE_UNLOGGED_QUEUE,
    CREATE_PARTITIONED_QUEUE,
    CREATE_NON_PARTITIONED,
    DROP_QUEUE,
    LIST_QUEUES,
    VALIDATE_QUEUE_NAME,
    SEND,
    SEND_WITH_DELAY_INT,
    SEND_WITH_DELAY_TZ,
    SEND_WITH_HEADERS,
    SEND_WITH_HEADERS_DELAY_INT,
    SEND_WITH_HEADERS_DELAY_TZ,
    SEND_BATCH,
    SEND_BATCH_WITH_DELAY_INT,
    SEND_BATCH_WITH_DELAY_TZ,
    SEND_BATCH_WITH_HEADERS,
    SEND_BATCH_WITH_HEADERS_DELAY_INT,
    SEND_BATCH_WITH_HEADERS_DELAY_TZ,
    SEND_TOPIC,
    SEND_TOPIC_WITH_HEADERS,
    SEND_TOPIC_WITH_DELAY_INT,
    SEND_TOPIC_WITH_HEADERS_DELAY_INT,
    SEND_BATCH_TOPIC,
    SEND_BATCH_TOPIC_WITH_HEADERS,
    SEND_BATCH_TOPIC_WITH_DELAY_INT,
    SEND_BATCH_TOPIC_WITH_DELAY_TZ,
    SEND_BATCH_TOPIC_WITH_HEADERS_DELAY_INT,
    SEND_BATCH_TOPIC_WITH_HEADERS_DELAY_TZ,
    BIND_TOPIC,
    UNBIND_TOPIC,
    LIST_TOPIC_BINDINGS,
    LIST_TOPIC_BINDINGS_FOR_QUEUE,
    TEST_ROUTING,
    READ,
    READ_WITH_POLL,
    READ_CONDITIONAL,
    READ_WITH_POLL_CONDITIONAL,
    READ_GROUPED,
    READ_GROUPED_WITH_POLL,
    READ_GROUPED_RR,
    READ_GROUPED_RR_WITH_POLL,
    POP,
    DELETE,
    DELETE_BATCH,
    ARCHIVE,
    ARCHIVE_BATCH,
    PURGE_QUEUE,
    SET_VT_INT,
    SET_VT_TZ,
    SET_VT_BATCH_INT,
    SET_VT_BATCH_TZ,
    METRICS,
    METRICS_ALL,
    ENABLE_NOTIFY,
    DISABLE_NOTIFY,
    UPDATE_NOTIFY,
    LIST_NOTIFY_THROTTLES,
    VALIDATE_ROUTING_KEY,
    VALIDATE_TOPIC_PATTERN,
    CREATE_FIFO_INDEX,
    CREATE_FIFO_INDEXES_ALL,
    CONVERT_ARCHIVE_PARTITIONED,
    DETACH_ARCHIVE,
]

ASYNC_SQL_MAP: Dict[str, str] = {
    sql: _convert_psycopg_to_asyncpg(sql) for sql in _ALL_SQL_CONSTANTS
}
