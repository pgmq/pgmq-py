# src/pgmq/sqlalchemy_async_queue.py
"""
Asynchronous PGMQ client implementation using SQLAlchemy.

This module provides the async PGMQueue class using SQLAlchemy's async engine
for high-performance asyncio-based database operations.
"""

from dataclasses import dataclass, field, fields
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
import os
import logging
import urllib.parse
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, async_sessionmaker
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy import text

from pgmq.base import BaseQueue
from pgmq import _sql
from pgmq.decorators import sqlalchemy_async_transaction
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
    """Parse JSONB value from asyncpg result."""
    if val is None:
        return None
    # asyncpg returns JSONB as dict/list directly
    return val


@dataclass
class PGMQueue(BaseQueue):
    """
    Asynchronous PGMQueue client using SQLAlchemy for PostgreSQL Message Queue operations.
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

    # --- SQLAlchemy Async Specific Fields ---
    engine: Optional[AsyncEngine] = field(default=None)  # type: ignore

    def __post_init__(self):
        """Initialize configuration after dataclass construction."""
        super().__init__(
            **{f.name: getattr(self, f.name) for f in fields(self.__class__)}
        )

    async def init(self) -> None:
        """Initialize the async SQLAlchemy engine.

        If an external engine was already provided, this is a no-op but will
        still initialize extensions if requested.
        """
        if self.engine is not None:
            if not isinstance(self.engine, AsyncEngine):
                raise TypeError(
                    f"Expected sqlalchemy.ext.asyncio.AsyncEngine, got {type(self.engine).__name__}"
                )
            if self.config.init_extension:
                async with self.engine.connect() as conn:
                    await conn.execute(
                        text("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")
                    )
                    await conn.commit()
            self._session_factory = async_sessionmaker(
                bind=self.engine, expire_on_commit=False
            )
            return

        log_with_context(self.logger, logging.DEBUG, "Creating async SQLAlchemy engine")
        if self.config.conn_string:
            # If a full connection string is provided, use it directly
            connection_url = self.config.conn_string
        else:
            # Otherwise, construct it from individual components
            user = urllib.parse.quote_plus(self.config.username)
            password = urllib.parse.quote_plus(self.config.password)
            connection_url = (
                f"postgresql+asyncpg://{user}:{password}@"
                f"{self.config.host}:{self.config.port}/{self.config.database}"
            )
        self.engine = create_async_engine(
            connection_url,
            poolclass=AsyncAdaptedQueuePool,
            pool_size=self.config.pool_size,
            max_overflow=20,
            pool_pre_ping=True,
        )

        if self.config.init_extension:
            async with self.engine.connect() as conn:
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;"))
                await conn.commit()

        self._session_factory = async_sessionmaker(
            bind=self.engine, expire_on_commit=False
        )

    def session(self):
        """Return an async SQLAlchemy ORM AsyncSession bound to this queue's engine.

        Usage:
            async with queue.session() as session:
                result = await session.execute(...)
        """
        if self.engine is None:
            raise RuntimeError("Engine has not been initialized. Call init() first.")
        return self._session_factory()

    async def close(self) -> None:
        """Close the engine and dispose of all connections.

        Note: If you passed in an external engine, this will also dispose it.
        You may want to skip calling this if the engine lifecycle is managed
        elsewhere.
        """
        if self.engine:
            await self.engine.dispose()
            self.engine = None

    # =========================================================================
    # Connection Management
    # =========================================================================

    async def _execute(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> None:
        """Execute SQL without returning results."""
        converted_sql, param_dict = _sql.convert_sql_params(sql, params)

        async def run_query(connection):
            if param_dict:
                await connection.execute(text(converted_sql), param_dict)
            else:
                await connection.execute(text(converted_sql))

        if conn is not None:
            await run_query(conn)
        else:
            async with self.engine.begin() as new_conn:
                await run_query(new_conn)

    async def _execute_with_result(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> List[tuple]:
        """Execute SQL and return all results."""
        converted_sql, param_dict = _sql.convert_sql_params(sql, params)

        async def run_query(connection):
            if param_dict:
                return await connection.execute(text(converted_sql), param_dict)
            else:
                return await connection.execute(text(converted_sql))

        if conn is not None:
            result = await run_query(conn)
            return result.fetchall()
        else:
            async with self.engine.begin() as new_conn:
                result = await run_query(new_conn)
                return result.fetchall()

    async def _execute_one(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> Optional[tuple]:
        """Execute SQL and return first result."""
        results = await self._execute_with_result(sql, params, conn)
        return results[0] if results else None

    # =========================================================================
    # Queue Management
    # =========================================================================

    @sqlalchemy_async_transaction
    async def create_queue(self, queue: str, unlogged: bool = False, conn=None) -> None:
        """Create a new queue."""
        log_with_context(
            self.logger, logging.DEBUG, "Creating queue", queue=queue, unlogged=unlogged
        )
        sql = _sql.CREATE_UNLOGGED_QUEUE if unlogged else _sql.CREATE_QUEUE
        await self._execute(sql, (queue,), conn=conn)

    @sqlalchemy_async_transaction
    async def create_partitioned_queue(
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
        await self._execute(
            _sql.CREATE_PARTITIONED_QUEUE,
            (queue, str(partition_interval), str(retention_interval)),
            conn=conn,
        )

    @sqlalchemy_async_transaction
    async def drop_queue(self, queue: str, conn=None) -> bool:
        """Drop a queue."""
        log_with_context(self.logger, logging.DEBUG, "Dropping queue", queue=queue)
        result = await self._execute_one(_sql.DROP_QUEUE, (queue,), conn=conn)
        return result[0] if result else False

    async def list_queues(self, conn=None) -> List[QueueRecord]:
        """
        List all queues with their metadata.
        """
        log_with_context(self.logger, logging.DEBUG, "Listing queues")
        rows = await self._execute_with_result(_sql.LIST_QUEUES, conn=conn)
        return [QueueRecord.from_row(row) for row in rows]

    async def validate_queue_name(self, queue_name: str, conn=None) -> bool:
        """Validate queue name format. Raises exception if invalid."""
        await self._execute(_sql.VALIDATE_QUEUE_NAME, (queue_name,), conn=conn)
        return True

    # =========================================================================
    # Sending Messages
    # =========================================================================

    @sqlalchemy_async_transaction
    async def send(
        self,
        queue: str,
        message: Dict[str, Any],
        headers: Optional[Dict[str, Any]] = None,
        delay: Union[int, datetime, None] = None,
        tz: Union[int, datetime, None] = None,  # Backward compatible alias
        conn=None,
    ) -> int:
        """Send a single message."""
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

        result = await self._execute_one(sql, tuple(params), conn=conn)
        return result[0] if result else -1

    @sqlalchemy_async_transaction
    async def send_batch(
        self,
        queue: str,
        messages: List[Dict[str, Any]],
        headers: Optional[List[Dict[str, Any]]] = None,
        delay: Union[int, datetime, None] = None,
        conn=None,
    ) -> List[int]:
        """Send multiple messages."""
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

        rows = await self._execute_with_result(sql, tuple(params), conn=conn)
        return [row[0] for row in rows]

    # =========================================================================
    # Topic-Based Routing
    # =========================================================================

    @sqlalchemy_async_transaction
    async def send_topic(
        self,
        routing_key: str,
        message: Dict[str, Any],
        headers: Optional[Dict[str, Any]] = None,
        delay: Optional[int] = None,
        conn=None,
    ) -> int:
        """Send message to all matching queues."""
        log_with_context(
            self.logger, logging.DEBUG, "Sending topic message", routing_key=routing_key
        )

        has_headers = headers is not None
        has_delay = delay is not None

        sql = _sql.get_send_topic_sql(has_headers, has_delay)

        params: List[Any] = [routing_key, message]
        if has_headers:
            params.append(headers)
        if has_delay:
            params.append(delay)

        result = await self._execute_one(sql, tuple(params), conn=conn)
        return result[0] if result else 0

    @sqlalchemy_async_transaction
    async def send_batch_topic(
        self,
        routing_key: str,
        messages: List[Dict[str, Any]],
        headers: Optional[List[Dict[str, Any]]] = None,
        delay: Union[int, datetime, None] = None,
        conn=None,
    ) -> List[BatchTopicResult]:
        """Send batch to all matching queues."""
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

        rows = await self._execute_with_result(sql, tuple(params), conn=conn)
        return [BatchTopicResult.from_row(row) for row in rows]

    @sqlalchemy_async_transaction
    async def bind_topic(self, pattern: str, queue_name: str, conn=None) -> None:
        """Bind pattern to queue."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Binding topic",
            pattern=pattern,
            queue=queue_name,
        )
        await self._execute(_sql.BIND_TOPIC, (pattern, queue_name), conn=conn)

    @sqlalchemy_async_transaction
    async def unbind_topic(self, pattern: str, queue_name: str, conn=None) -> bool:
        """Remove pattern binding."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Unbinding topic",
            pattern=pattern,
            queue=queue_name,
        )
        result = await self._execute_one(
            _sql.UNBIND_TOPIC, (pattern, queue_name), conn=conn
        )
        return result[0] if result else False

    async def list_topic_bindings(
        self, queue_name: Optional[str] = None, conn=None
    ) -> List[TopicBinding]:
        """List topic bindings."""
        if queue_name:
            rows = await self._execute_with_result(
                _sql.LIST_TOPIC_BINDINGS_FOR_QUEUE, (queue_name,), conn=conn
            )
        else:
            rows = await self._execute_with_result(_sql.LIST_TOPIC_BINDINGS, conn=conn)
        return [TopicBinding.from_row(row) for row in rows]

    async def test_routing(self, routing_key: str, conn=None) -> List[RoutingResult]:
        """Test routing without sending."""
        rows = await self._execute_with_result(
            _sql.TEST_ROUTING, (routing_key,), conn=conn
        )
        return [RoutingResult.from_row(row) for row in rows]

    # =========================================================================
    # Reading Messages
    # =========================================================================

    @sqlalchemy_async_transaction
    async def read(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        conditional: Optional[Dict[str, Any]] = None,
        conn=None,
    ) -> Optional[Union[Message, List[Message]]]:
        """Read message(s) from queue."""
        log_with_context(
            self.logger, logging.DEBUG, "Reading messages", queue=queue, qty=qty
        )

        actual_vt = vt or self.vt

        if conditional:
            sql = _sql.READ_CONDITIONAL
            params = (queue, actual_vt, qty, conditional)
        else:
            sql = _sql.READ
            params = (queue, actual_vt, qty)

        rows = await self._execute_with_result(sql, params, conn=conn)
        messages = [Message.from_row(row, _parse_jsonb) for row in rows]

        if qty == 1:
            return messages[0] if messages else None
        return messages

    async def read_batch(
        self, queue: str, vt: Optional[int] = None, batch_size: int = 1, conn=None
    ) -> List[Message]:
        """Read a batch of messages (backward compatibility alias)."""
        result = await self.read(queue, vt=vt, qty=batch_size, conn=conn)
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return [result]

    @sqlalchemy_async_transaction
    async def read_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conditional: Optional[Dict[str, Any]] = None,
        conn=None,
    ) -> List[Message]:
        """Read with long-polling."""
        log_with_context(
            self.logger, logging.DEBUG, "Reading with poll", queue=queue, qty=qty
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

        rows = await self._execute_with_result(sql, params, conn=conn)
        return [Message.from_row(row, _parse_jsonb) for row in rows]

    # =========================================================================
    # FIFO Operations
    # =========================================================================

    @sqlalchemy_async_transaction
    async def read_grouped(
        self, queue: str, vt: Optional[int] = None, qty: int = 1, conn=None
    ) -> List[Message]:
        """FIFO grouped read (SQS-style)."""
        log_with_context(
            self.logger, logging.DEBUG, "Reading grouped", queue=queue, qty=qty
        )
        params = (queue, vt or self.vt, qty)
        rows = await self._execute_with_result(_sql.READ_GROUPED, params, conn=conn)
        return [Message.from_row(row, _parse_jsonb) for row in rows]

    @sqlalchemy_async_transaction
    async def read_grouped_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conn=None,
    ) -> List[Message]:
        """FIFO grouped read with poll."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Reading grouped with poll",
            queue=queue,
            qty=qty,
        )
        params = (queue, vt or self.vt, qty, max_poll_seconds, poll_interval_ms)
        rows = await self._execute_with_result(
            _sql.READ_GROUPED_WITH_POLL, params, conn=conn
        )
        return [Message.from_row(row, _parse_jsonb) for row in rows]

    @sqlalchemy_async_transaction
    async def read_grouped_rr(
        self, queue: str, vt: Optional[int] = None, qty: int = 1, conn=None
    ) -> List[Message]:
        """FIFO round-robin read."""
        log_with_context(
            self.logger, logging.DEBUG, "Reading grouped RR", queue=queue, qty=qty
        )
        params = (queue, vt or self.vt, qty)
        rows = await self._execute_with_result(_sql.READ_GROUPED_RR, params, conn=conn)
        return [Message.from_row(row, _parse_jsonb) for row in rows]

    @sqlalchemy_async_transaction
    async def read_grouped_rr_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conn=None,
    ) -> List[Message]:
        """FIFO round-robin read with poll."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Reading grouped RR with poll",
            queue=queue,
            qty=qty,
        )
        params = (queue, vt or self.vt, qty, max_poll_seconds, poll_interval_ms)
        rows = await self._execute_with_result(
            _sql.READ_GROUPED_RR_WITH_POLL, params, conn=conn
        )
        return [Message.from_row(row, _parse_jsonb) for row in rows]

    # =========================================================================
    # Pop
    # =========================================================================

    @sqlalchemy_async_transaction
    async def pop(
        self, queue: str, qty: int = 1, conn=None
    ) -> Optional[Union[Message, List[Message]]]:
        """Pop messages (read and delete)."""
        log_with_context(
            self.logger, logging.DEBUG, "Popping messages", queue=queue, qty=qty
        )
        rows = await self._execute_with_result(_sql.POP, (queue, qty), conn=conn)
        messages = [Message.from_row(row, _parse_jsonb) for row in rows]

        if qty == 1:
            return messages[0] if messages else None
        return messages

    # =========================================================================
    # Deleting and Archiving
    # =========================================================================

    @sqlalchemy_async_transaction
    async def delete(self, queue: str, msg_id: int, conn=None) -> bool:
        """Delete single message."""
        log_with_context(
            self.logger, logging.DEBUG, "Deleting message", queue=queue, msg_id=msg_id
        )
        result = await self._execute_one(_sql.DELETE, (queue, msg_id), conn=conn)
        return result[0] if result else False

    @sqlalchemy_async_transaction
    async def delete_batch(
        self, queue: str, msg_ids: List[int], conn=None
    ) -> List[int]:
        """Delete multiple messages."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Deleting batch",
            queue=queue,
            count=len(msg_ids),
        )
        rows = await self._execute_with_result(
            _sql.DELETE_BATCH, (queue, msg_ids), conn=conn
        )
        return [row[0] for row in rows]

    @sqlalchemy_async_transaction
    async def archive(self, queue: str, msg_id: int, conn=None) -> bool:
        """Archive single message."""
        log_with_context(
            self.logger, logging.DEBUG, "Archiving message", queue=queue, msg_id=msg_id
        )
        result = await self._execute_one(_sql.ARCHIVE, (queue, msg_id), conn=conn)
        return result[0] if result else False

    @sqlalchemy_async_transaction
    async def archive_batch(
        self, queue: str, msg_ids: List[int], conn=None
    ) -> List[int]:
        """Archive multiple messages."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Archiving batch",
            queue=queue,
            count=len(msg_ids),
        )
        rows = await self._execute_with_result(
            _sql.ARCHIVE_BATCH, (queue, msg_ids), conn=conn
        )
        return [row[0] for row in rows]

    @sqlalchemy_async_transaction
    async def purge(self, queue: str, conn=None) -> int:
        """Purge all messages."""
        log_with_context(self.logger, logging.DEBUG, "Purging queue", queue=queue)
        result = await self._execute_one(_sql.PURGE_QUEUE, (queue,), conn=conn)
        return result[0] if result else 0

    # =========================================================================
    # Visibility Timeout
    # =========================================================================

    @sqlalchemy_async_transaction
    async def set_vt(
        self,
        queue: str,
        msg_id: Union[int, List[int]],
        vt: Union[int, datetime],
        conn=None,
    ) -> Optional[Union[Message, List[Message]]]:
        """Set visibility timeout."""
        is_batch = isinstance(msg_id, list)
        vt_is_timestamp = isinstance(vt, datetime)

        log_with_context(
            self.logger, logging.DEBUG, "Setting VT", queue=queue, is_batch=is_batch
        )

        sql = _sql.get_set_vt_sql(is_batch, vt_is_timestamp)
        params = (queue, msg_id, vt)

        rows = await self._execute_with_result(sql, params, conn=conn)
        messages = [Message.from_row(row, _parse_jsonb) for row in rows]

        if is_batch:
            return messages
        return messages[0] if messages else None

    # =========================================================================
    # Metrics
    # =========================================================================

    async def metrics(self, queue: str, conn=None) -> QueueMetrics:
        """Get queue metrics."""
        log_with_context(self.logger, logging.DEBUG, "Getting metrics", queue=queue)
        row = await self._execute_one(_sql.METRICS, (queue,), conn=conn)
        if not row:
            raise ValueError(f"Queue '{queue}' not found")
        return QueueMetrics.from_row(row)

    async def metrics_all(self, conn=None) -> List[QueueMetrics]:
        """Get all queue metrics."""
        log_with_context(self.logger, logging.DEBUG, "Getting all metrics")
        rows = await self._execute_with_result(_sql.METRICS_ALL, conn=conn)
        return [QueueMetrics.from_row(row) for row in rows]

    # =========================================================================
    # Notifications
    # =========================================================================

    @sqlalchemy_async_transaction
    async def enable_notify(
        self, queue: str, throttle_interval_ms: int = 250, conn=None
    ) -> None:
        """Enable NOTIFY for queue."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Enabling notifications",
            queue=queue,
            throttle=throttle_interval_ms,
        )
        await self._execute(
            _sql.ENABLE_NOTIFY, (queue, throttle_interval_ms), conn=conn
        )

    @sqlalchemy_async_transaction
    async def disable_notify(self, queue: str, conn=None) -> None:
        """Disable NOTIFY for queue."""
        log_with_context(
            self.logger, logging.DEBUG, "Disabling notifications", queue=queue
        )
        await self._execute(_sql.DISABLE_NOTIFY, (queue,), conn=conn)

    @sqlalchemy_async_transaction
    async def update_notify(
        self, queue: str, throttle_interval_ms: int, conn=None
    ) -> None:
        """Update notification throttle."""
        log_with_context(
            self.logger,
            logging.DEBUG,
            "Updating notification throttle",
            queue=queue,
            throttle=throttle_interval_ms,
        )
        await self._execute(
            _sql.UPDATE_NOTIFY, (queue, throttle_interval_ms), conn=conn
        )

    async def list_notify_throttles(self, conn=None) -> List[NotificationThrottle]:
        """List notification configurations."""
        rows = await self._execute_with_result(_sql.LIST_NOTIFY_THROTTLES, conn=conn)
        return [NotificationThrottle.from_row(row) for row in rows]

    # =========================================================================
    # Utilities
    # =========================================================================

    async def validate_routing_key(self, routing_key: str, conn=None) -> bool:
        """Validate routing key."""
        try:
            await self._execute(_sql.VALIDATE_ROUTING_KEY, (routing_key,), conn=conn)
            return True
        except Exception:
            return False

    async def validate_topic_pattern(self, pattern: str, conn=None) -> bool:
        """Validate topic pattern."""
        try:
            await self._execute(_sql.VALIDATE_TOPIC_PATTERN, (pattern,), conn=conn)
            return True
        except Exception:
            return False

    @sqlalchemy_async_transaction
    async def create_fifo_index(self, queue: str, conn=None) -> None:
        """Create FIFO index."""
        log_with_context(self.logger, logging.DEBUG, "Creating FIFO index", queue=queue)
        await self._execute(_sql.CREATE_FIFO_INDEX, (queue,), conn=conn)

    async def create_fifo_indexes_all(self, conn=None) -> None:
        """Create FIFO indexes on all queues."""
        log_with_context(self.logger, logging.DEBUG, "Creating all FIFO indexes")
        await self._execute(_sql.CREATE_FIFO_INDEXES_ALL, conn=conn)

    @sqlalchemy_async_transaction
    async def convert_archive_partitioned(
        self,
        queue: str,
        partition_interval: Union[int, str] = 10000,
        retention_interval: Union[int, str] = 100000,
        leading_partition: int = 10,
        conn=None,
    ) -> None:
        """Convert archive to partitioned."""
        log_with_context(
            self.logger, logging.DEBUG, "Converting archive to partitioned", queue=queue
        )
        await self._execute(
            _sql.CONVERT_ARCHIVE_PARTITIONED,
            (
                queue,
                str(partition_interval),
                str(retention_interval),
                leading_partition,
            ),
            conn=conn,
        )

    @sqlalchemy_async_transaction
    async def detach_archive(self, queue: str, conn=None) -> None:
        """Detach archive (deprecated)."""
        log_with_context(
            self.logger, logging.DEBUG, "Detaching archive (deprecated)", queue=queue
        )
        await self._execute(_sql.DETACH_ARCHIVE, (queue,), conn=conn)
