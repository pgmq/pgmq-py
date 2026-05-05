# src/pgmq/sqlalchemy_queue.py
"""
Synchronous PGMQ client implementation using SQLAlchemy.

This module provides the SQLAlchemyPGMQueue class for synchronous database operations
using SQLAlchemy Core, with full support for all PGMQ extension features including
topics, FIFO, and notifications.
"""

from dataclasses import dataclass, field, fields
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
import os
import logging
import warnings
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from pgmq.base import BaseQueue
from pgmq import _sql
from pgmq.decorators import sqlalchemy_transaction
from pgmq.logger import log_with_context
from pgmq.messages import (
    Message,
    QueueMetrics,
    QueueRecord,
    TopicBinding,
    RoutingResult,
    BatchTopicResult,
    NotificationThrottle,
)


def _parse_jsonb(val) -> Any:
    """Parse JSONB value from SQLAlchemy/psycopg result."""
    if val is None:
        return None
    # psycopg returns JSONB as dict/list directly
    return val


@dataclass
class PGMQueue(BaseQueue):
    """
    Synchronous PGMQueue client using SQLAlchemy for PostgreSQL Message Queue operations.
    """

    # --- Backward Compatible Fields ---
    conn_string: Optional[str] = field(
        default_factory=lambda: os.getenv("DATABASE_URL")
    )
    host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    port: str = field(default_factory=lambda: os.getenv("PG_PORT", "5432"))
    database: str = field(default_factory=lambda: os.getenv("PG_DATABASE", "postgres"))
    username: str = field(default_factory=lambda: os.getenv("PG_USERNAME", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "postgres"))
    delay: int = 0
    vt: int = 30
    pool_size: int = 10
    verbose: bool = False
    log_filename: Optional[str] = None
    init_extension: bool = True
    structured_logging: bool = False
    log_rotation: bool = False
    log_rotation_size: str = "10 MB"
    log_retention: str = "1 week"

    # --- SQLAlchemy Specific Fields ---
    engine: Optional[Engine] = field(default=None)  # type: ignore

    def __post_init__(self):
        """Initialize configuration and engine after dataclass construction."""
        super().__init__(
            **{f.name: getattr(self, f.name) for f in fields(self.__class__)}
        )
        if self.engine is None:
            self._init_engine()
            if self.config.init_extension:
                self._init_extensions()
        else:
            # External engine provided — ensure it's the right type
            if not isinstance(self.engine, Engine):
                raise TypeError(
                    f"Expected sqlalchemy.Engine, got {type(self.engine).__name__}"
                )
            if self.config.init_extension:
                self._init_extensions()
        self._session_factory = sessionmaker(bind=self.engine)

    def _init_engine(self) -> None:
        """Initialize the SQLAlchemy engine."""
        log_with_context(self.logger, logging.DEBUG, "Creating SQLAlchemy engine")
        # Use the re-assembled and quoted URI from config, swapping the driver prefix.
        # This handles both URI and libpq input formats and fixes malformed credentials.
        connection_url = self.config.async_dsn.replace(
            "postgresql://", "postgresql+psycopg://", 1
        )
        self.engine = create_engine(
            connection_url,
            poolclass=QueuePool,
            pool_size=self.config.pool_size,
            max_overflow=20,
            pool_pre_ping=True,
        )

    def _init_extensions(self) -> None:
        """Ensure PGMQ extension is installed."""
        with self.engine.connect() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;"))
            conn.commit()

    # =========================================================================
    # Connection Management
    # =========================================================================

    def _execute(self, sql: str, params: Optional[tuple] = None, conn=None) -> None:
        """Execute SQL without returning results."""
        converted_sql, param_dict = _sql.convert_sql_params(sql, params)

        def run_query(connection):
            if param_dict:
                connection.execute(text(converted_sql), param_dict)
            else:
                connection.execute(text(converted_sql))

        if conn is not None:
            run_query(conn)
        else:
            with self.engine.begin() as new_conn:
                run_query(new_conn)

    def _execute_with_result(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> List[tuple]:
        """Execute SQL and return all results."""
        converted_sql, param_dict = _sql.convert_sql_params(sql, params)

        def run_query(connection):
            if param_dict:
                return connection.execute(text(converted_sql), param_dict)
            else:
                return connection.execute(text(converted_sql))

        if conn is not None:
            return run_query(conn).fetchall()
        else:
            with self.engine.begin() as new_conn:
                return run_query(new_conn).fetchall()

    def _execute_one(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> Optional[tuple]:
        """Execute SQL and return first result or None."""
        results = self._execute_with_result(sql, params, conn)
        return results[0] if results else None

    # =========================================================================
    # Queue Management
    # =========================================================================

    @sqlalchemy_transaction
    def create_queue(self, queue: str, unlogged: bool = False, conn=None) -> None:
        """Create a new queue."""
        log_with_context(
            self.logger, logging.DEBUG, "Creating queue", queue=queue, unlogged=unlogged
        )
        sql = _sql.CREATE_UNLOGGED_QUEUE if unlogged else _sql.CREATE_QUEUE
        self._execute(sql, (queue,), conn=conn)

    @sqlalchemy_transaction
    def create_partitioned_queue(
        self,
        queue: str,
        partition_interval: Union[int, str] = 10000,
        retention_interval: Union[int, str] = 100000,
        conn=None,
    ) -> None:
        """Create a partitioned queue."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Creating partitioned queue",
            queue=queue,
            partition_interval=str(partition_interval),
            retention_interval=str(retention_interval),
        )
        self._execute(
            _sql.CREATE_PARTITIONED_QUEUE,
            (queue, str(partition_interval), str(retention_interval)),
            conn=conn,
        )

    @sqlalchemy_transaction
    def drop_queue(self, queue: str, conn=None) -> bool:
        """Drop a queue."""
        log_with_context(self.logger, logging.DEBUG, "Dropping queue", queue=queue)
        result = self._execute_one(_sql.DROP_QUEUE, (queue,), conn=conn)
        return result[0] if result else False

    def list_queues(self, conn=None) -> List[QueueRecord]:
        """
        List all queues with their metadata.

        .. versionchanged:: 2.0.0
            This method now returns a list of :class:`QueueRecord` objects
            instead of a list of strings. To get the queue name, access the
            ``queue_name`` attribute of the returned object.

        Returns:
            List[QueueRecord]: A list of queue metadata objects.
        """
        log_with_context(self.logger, logging.DEBUG, "Listing queues")
        warnings.warn(
            "list_queues() now returns List[QueueRecord] instead of List[str]. "
            "Access the queue name via the .queue_name attribute. "
            "This warning will be removed in a future version.",
            UserWarning,
            stacklevel=2,
        )
        rows = self._execute_with_result(_sql.LIST_QUEUES, conn=conn)
        return [QueueRecord.from_row(row) for row in rows]

    def validate_queue_name(self, queue_name: str, conn=None) -> bool:
        """Validate queue name format. Raises exception if invalid."""
        self._execute(_sql.VALIDATE_QUEUE_NAME, (queue_name,), conn=conn)
        return True

    # =========================================================================
    # Sending Messages
    # =========================================================================

    @sqlalchemy_transaction
    def send(
        self,
        queue: str,
        message: Dict[str, Any],
        headers: Optional[Dict[str, Any]] = None,
        delay: Union[int, datetime, None] = None,
        tz: Union[int, datetime, None] = None,  # Backward compatible alias
        conn=None,
    ) -> int:
        """Send a single message to a queue."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Sending message",
            queue=queue,
        )

        # Handle backward compatibility: 'tz' acts as 'delay'
        effective_delay = tz if tz is not None else delay

        has_headers = headers is not None
        has_delay = effective_delay is not None
        delay_is_ts = isinstance(effective_delay, datetime)

        sql = _sql.get_send_sql(has_headers, has_delay, delay_is_ts)

        params: List[Any] = [queue, message]
        if has_headers:
            params.append(headers)
        if has_delay:
            params.append(effective_delay)

        result = self._execute_one(sql, tuple(params), conn=conn)
        return result[0] if result else -1

    @sqlalchemy_transaction
    def send_batch(
        self,
        queue: str,
        messages: List[Dict[str, Any]],
        headers: Optional[List[Dict[str, Any]]] = None,
        delay: Union[int, datetime, None] = None,
        conn=None,
    ) -> List[int]:
        """Send multiple messages to a queue."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Sending batch",
            queue=queue,
            count=len(messages),
        )

        if not messages:
            return []

        if headers is not None and len(headers) != len(messages):
            raise ValueError("headers list must match messages list length")

        has_headers = headers is not None
        has_delay = delay is not None
        delay_is_ts = isinstance(delay, datetime)

        sql = _sql.get_send_batch_sql(has_headers, has_delay, delay_is_ts)

        params: List[Any] = [queue, messages]

        if has_headers:
            params.append(headers)
        if has_delay:
            params.append(delay)

        rows = self._execute_with_result(sql, tuple(params), conn=conn)
        return [row[0] for row in rows]

    # =========================================================================
    # Topic-Based Routing
    # =========================================================================

    @sqlalchemy_transaction
    def send_topic(
        self,
        routing_key: str,
        message: Dict[str, Any],
        headers: Optional[Dict[str, Any]] = None,
        delay: Optional[int] = None,
        conn=None,
    ) -> int:
        """Send message to all queues matching the routing key pattern."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Sending topic message",
            routing_key=routing_key,
            has_headers=headers is not None,
        )

        has_headers = headers is not None
        has_delay = delay is not None

        sql = _sql.get_send_topic_sql(has_headers, has_delay)

        params: List[Any] = [routing_key, message]
        if has_headers:
            params.append(headers)
        if has_delay:
            params.append(delay)

        result = self._execute_one(sql, tuple(params), conn=conn)
        return result[0] if result else 0

    @sqlalchemy_transaction
    def send_batch_topic(
        self,
        routing_key: str,
        messages: List[Dict[str, Any]],
        headers: Optional[List[Dict[str, Any]]] = None,
        delay: Union[int, datetime, None] = None,
        conn=None,
    ) -> List[BatchTopicResult]:
        """Send batch of messages to all matching queues."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Sending batch topic",
            routing_key=routing_key,
            count=len(messages),
        )

        if not messages:
            return []

        has_headers = headers is not None
        has_delay = delay is not None
        delay_is_ts = isinstance(delay, datetime)

        sql = _sql.get_send_batch_topic_sql(has_headers, has_delay, delay_is_ts)

        params: List[Any] = [routing_key, messages]

        if has_headers:
            if len(headers) != len(messages):
                raise ValueError("headers list must match messages list length")
            params.append(headers)
        if has_delay:
            params.append(delay)

        rows = self._execute_with_result(sql, tuple(params), conn=conn)
        return [BatchTopicResult.from_row(row) for row in rows]

    @sqlalchemy_transaction
    def bind_topic(self, pattern: str, queue_name: str, conn=None) -> None:
        """Bind a pattern to a queue for topic routing."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Binding topic",
            pattern=pattern,
            queue=queue_name,
        )
        self._execute(_sql.BIND_TOPIC, (pattern, queue_name), conn=conn)

    @sqlalchemy_transaction
    def unbind_topic(self, pattern: str, queue_name: str, conn=None) -> bool:
        """Remove a pattern binding from a queue."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Unbinding topic",
            pattern=pattern,
            queue=queue_name,
        )
        result = self._execute_one(_sql.UNBIND_TOPIC, (pattern, queue_name), conn=conn)
        return result[0] if result else False

    def list_topic_bindings(
        self, queue_name: Optional[str] = None, conn=None
    ) -> List[TopicBinding]:
        """List all topic bindings, optionally filtered by queue."""
        if queue_name:
            rows = self._execute_with_result(
                _sql.LIST_TOPIC_BINDINGS_FOR_QUEUE, (queue_name,), conn=conn
            )
        else:
            rows = self._execute_with_result(_sql.LIST_TOPIC_BINDINGS, conn=conn)
        return [TopicBinding.from_row(row) for row in rows]

    def test_routing(self, routing_key: str, conn=None) -> List[RoutingResult]:
        """Test which queues would receive a message without actually sending."""
        rows = self._execute_with_result(_sql.TEST_ROUTING, (routing_key,), conn=conn)
        return [RoutingResult.from_row(row) for row in rows]

    # =========================================================================
    # Reading Messages
    # =========================================================================

    @sqlalchemy_transaction
    def read(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        conditional: Optional[Dict[str, Any]] = None,
        conn=None,
    ) -> Optional[Union[Message, List[Message]]]:
        """Read message(s) from queue with visibility timeout."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Reading messages",
            queue=queue,
            vt=vt or self.vt,
            qty=qty,
        )

        actual_vt = vt or self.vt

        if conditional:
            sql = _sql.READ_CONDITIONAL
            params = (queue, actual_vt, qty, conditional)
        else:
            sql = _sql.READ
            params = (queue, actual_vt, qty)

        rows = self._execute_with_result(sql, params, conn=conn)
        messages = [Message.from_row(row, _parse_jsonb) for row in rows]

        if qty == 1:
            return messages[0] if messages else None
        return messages

    @sqlalchemy_transaction
    def read_batch(
        self, queue: str, vt: Optional[int] = None, batch_size: int = 1, conn=None
    ) -> List[Message]:
        """Read a batch of messages (backward compatibility alias)."""
        result = self.read(queue, vt=vt, qty=batch_size, conn=conn)
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return [result]

    @sqlalchemy_transaction
    def read_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conditional: Optional[Dict[str, Any]] = None,
        conn=None,
    ) -> List[Message]:
        """Read messages with long-polling."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Reading with poll",
            queue=queue,
            qty=qty,
            max_poll_seconds=max_poll_seconds,
        )

        actual_vt = vt or self.vt

        if conditional:
            sql = _sql.READ_WITH_POLL_CONDITIONAL
            params = (
                queue,
                actual_vt,
                qty,
                max_poll_seconds,
                poll_interval_ms,
                conditional,
            )
        else:
            sql = _sql.READ_WITH_POLL
            params = (queue, actual_vt, qty, max_poll_seconds, poll_interval_ms)

        rows = self._execute_with_result(sql, params, conn=conn)
        return [Message.from_row(row, _parse_jsonb) for row in rows]

    # =========================================================================
    # FIFO Operations
    # =========================================================================

    @sqlalchemy_transaction
    def read_grouped(
        self, queue: str, vt: Optional[int] = None, qty: int = 1, conn=None
    ) -> List[Message]:
        """Read messages with FIFO grouping (SQS-style batch filling)."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Reading grouped (SQS-style)",
            queue=queue,
            qty=qty,
        )
        params = (queue, vt or self.vt, qty)
        rows = self._execute_with_result(_sql.READ_GROUPED, params, conn=conn)
        return [Message.from_row(row, _parse_jsonb) for row in rows]

    @sqlalchemy_transaction
    def read_grouped_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conn=None,
    ) -> List[Message]:
        """FIFO grouped read with long-polling."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Reading grouped with poll",
            queue=queue,
            qty=qty,
        )
        params = (queue, vt or self.vt, qty, max_poll_seconds, poll_interval_ms)
        rows = self._execute_with_result(_sql.READ_GROUPED_WITH_POLL, params, conn=conn)
        return [Message.from_row(row, _parse_jsonb) for row in rows]

    @sqlalchemy_transaction
    def read_grouped_rr(
        self, queue: str, vt: Optional[int] = None, qty: int = 1, conn=None
    ) -> List[Message]:
        """Read messages with FIFO round-robin interleaving."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Reading grouped round-robin",
            queue=queue,
            qty=qty,
        )
        params = (queue, vt or self.vt, qty)
        rows = self._execute_with_result(_sql.READ_GROUPED_RR, params, conn=conn)
        return [Message.from_row(row, _parse_jsonb) for row in rows]

    @sqlalchemy_transaction
    def read_grouped_rr_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conn=None,
    ) -> List[Message]:
        """FIFO round-robin read with long-polling."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Reading grouped RR with poll",
            queue=queue,
            qty=qty,
        )
        params = (queue, vt or self.vt, qty, max_poll_seconds, poll_interval_ms)
        rows = self._execute_with_result(
            _sql.READ_GROUPED_RR_WITH_POLL, params, conn=conn
        )
        return [Message.from_row(row, _parse_jsonb) for row in rows]

    # =========================================================================
    # Pop (Read and Delete)
    # =========================================================================

    @sqlalchemy_transaction
    def pop(
        self, queue: str, qty: int = 1, conn=None
    ) -> Optional[Union[Message, List[Message]]]:
        """Pop message(s) from queue (read and immediately delete)."""
        log_with_context(
            self.logger, logging.DEBUG, "Popping messages", queue=queue, qty=qty
        )
        rows = self._execute_with_result(_sql.POP, (queue, qty), conn=conn)
        messages = [Message.from_row(row, _parse_jsonb) for row in rows]

        if qty == 1:
            return messages[0] if messages else None
        return messages

    # =========================================================================
    # Deleting and Archiving
    # =========================================================================

    @sqlalchemy_transaction
    def delete(self, queue: str, msg_id: int, conn=None) -> bool:
        """Delete a single message from queue."""
        log_with_context(
            self.logger, logging.DEBUG, "Deleting message", queue=queue, msg_id=msg_id
        )
        result = self._execute_one(_sql.DELETE, (queue, msg_id), conn=conn)
        return result[0] if result else False

    @sqlalchemy_transaction
    def delete_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Delete multiple messages."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Deleting batch",
            queue=queue,
            count=len(msg_ids),
        )
        rows = self._execute_with_result(_sql.DELETE_BATCH, (queue, msg_ids), conn=conn)
        return [row[0] for row in rows]

    @sqlalchemy_transaction
    def archive(self, queue: str, msg_id: int, conn=None) -> bool:
        """Archive a single message."""
        log_with_context(
            self.logger, logging.DEBUG, "Archiving message", queue=queue, msg_id=msg_id
        )
        result = self._execute_one(_sql.ARCHIVE, (queue, msg_id), conn=conn)
        return result[0] if result else False

    @sqlalchemy_transaction
    def archive_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Archive multiple messages."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Archiving batch",
            queue=queue,
            count=len(msg_ids),
        )
        rows = self._execute_with_result(
            _sql.ARCHIVE_BATCH, (queue, msg_ids), conn=conn
        )
        return [row[0] for row in rows]

    @sqlalchemy_transaction
    def purge(self, queue: str, conn=None) -> int:
        """Purge all messages from queue."""
        log_with_context(self.logger, logging.DEBUG, "Purging queue", queue=queue)
        result = self._execute_one(_sql.PURGE_QUEUE, (queue,), conn=conn)
        return result[0] if result else 0

    # =========================================================================
    # Visibility Timeout
    # =========================================================================

    @sqlalchemy_transaction
    def set_vt(
        self,
        queue: str,
        msg_id: Union[int, List[int]],
        vt: Union[int, datetime],
        conn=None,
    ) -> Optional[Union[Message, List[Message]]]:
        """Set visibility timeout for message(s)."""
        is_batch = isinstance(msg_id, list)
        vt_is_timestamp = isinstance(vt, datetime)

        log_with_context(
            self.logger,
            logging.DEBUG,
            "Setting visibility timeout",
            queue=queue,
            is_batch=is_batch,
        )

        # Robust SQL selection using helper
        sql = _sql.get_set_vt_sql(is_batch, vt_is_timestamp)
        params = (queue, msg_id, vt)

        rows = self._execute_with_result(sql, params, conn=conn)
        messages = [Message.from_row(row, _parse_jsonb) for row in rows]

        if is_batch:
            return messages
        return messages[0] if messages else None

    # =========================================================================
    # Metrics
    # =========================================================================

    def metrics(self, queue: str, conn=None) -> QueueMetrics:
        """Get metrics for a specific queue."""
        log_with_context(self.logger, logging.DEBUG, "Getting metrics", queue=queue)
        row = self._execute_one(_sql.METRICS, (queue,), conn=conn)
        if not row:
            raise ValueError(f"Queue '{queue}' not found")
        return QueueMetrics.from_row(row)

    def metrics_all(self, conn=None) -> List[QueueMetrics]:
        """Get metrics for all queues."""
        log_with_context(self.logger, logging.DEBUG, "Getting all metrics")
        rows = self._execute_with_result(_sql.METRICS_ALL, conn=conn)
        return [QueueMetrics.from_row(row) for row in rows]

    # =========================================================================
    # Notifications
    # =========================================================================

    @sqlalchemy_transaction
    def enable_notify(
        self, queue: str, throttle_interval_ms: int = 250, conn=None
    ) -> None:
        """Enable PostgreSQL NOTIFY for new message insertions."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Enabling notifications",
            queue=queue,
            throttle=throttle_interval_ms,
        )
        self._execute(_sql.ENABLE_NOTIFY, (queue, throttle_interval_ms), conn=conn)

    @sqlalchemy_transaction
    def disable_notify(self, queue: str, conn=None) -> None:
        """Disable NOTIFY triggers for a queue."""
        log_with_context(
            self.logger, logging.DEBUG, "Disabling notifications", queue=queue
        )
        self._execute(_sql.DISABLE_NOTIFY, (queue,), conn=conn)

    @sqlalchemy_transaction
    def update_notify(self, queue: str, throttle_interval_ms: int, conn=None) -> None:
        """Update throttle interval for notifications."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Updating notification throttle",
            queue=queue,
            throttle=throttle_interval_ms,
        )
        self._execute(_sql.UPDATE_NOTIFY, (queue, throttle_interval_ms), conn=conn)

    def list_notify_throttles(self, conn=None) -> List[NotificationThrottle]:
        """List all notification configurations."""
        rows = self._execute_with_result(_sql.LIST_NOTIFY_THROTTLES, conn=conn)
        return [NotificationThrottle.from_row(row) for row in rows]

    # =========================================================================
    # Utilities
    # =========================================================================

    def validate_routing_key(self, routing_key: str, conn=None) -> bool:
        """Validate routing key format."""
        try:
            self._execute(_sql.VALIDATE_ROUTING_KEY, (routing_key,), conn=conn)
            return True
        except Exception:
            return False

    def validate_topic_pattern(self, pattern: str, conn=None) -> bool:
        """Validate topic pattern format."""
        try:
            self._execute(_sql.VALIDATE_TOPIC_PATTERN, (pattern,), conn=conn)
            return True
        except Exception:
            return False

    @sqlalchemy_transaction
    def create_fifo_index(self, queue: str, conn=None) -> None:
        """Create GIN index on headers for FIFO performance."""
        log_with_context(self.logger, logging.DEBUG, "Creating FIFO index", queue=queue)
        self._execute(_sql.CREATE_FIFO_INDEX, (queue,), conn=conn)

    def create_fifo_indexes_all(self, conn=None) -> None:
        """Create FIFO indexes on all queues."""
        log_with_context(self.logger, logging.DEBUG, "Creating all FIFO indexes")
        self._execute(_sql.CREATE_FIFO_INDEXES_ALL, conn=conn)

    @sqlalchemy_transaction
    def convert_archive_partitioned(
        self,
        queue: str,
        partition_interval: Union[int, str] = 10000,
        retention_interval: Union[int, str] = 100000,
        leading_partition: int = 10,
        conn=None,
    ) -> None:
        """Convert archive table to partitioned."""
        log_with_context(
            self.logger, logging.DEBUG, "Converting archive to partitioned", queue=queue
        )
        self._execute(
            _sql.CONVERT_ARCHIVE_PARTITIONED,
            (
                queue,
                str(partition_interval),
                str(retention_interval),
                leading_partition,
            ),
            conn=conn,
        )

    @sqlalchemy_transaction
    def detach_archive(self, queue: str, conn=None) -> None:
        """Detach archive table from extension (deprecated in PGMQ v2.0)."""
        log_with_context(
            self.logger, logging.DEBUG, "Detaching archive (deprecated)", queue=queue
        )
        self._execute(_sql.DETACH_ARCHIVE, (queue,), conn=conn)

    def session(self) -> Session:
        """Return a new SQLAlchemy ORM Session bound to this queue's engine."""
        if self.engine is None:
            raise RuntimeError("Engine has not been initialized.")
        return self._session_factory()

    def dispose(self) -> None:
        """Dispose of the engine and close all connections.

        Note: If you passed in an external engine, this will also dispose it.
        You may want to skip calling this if the engine lifecycle is managed
        elsewhere.
        """
        if self.engine:
            self.engine.dispose()
            self.engine = None
