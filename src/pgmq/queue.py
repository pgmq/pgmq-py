# src/pgmq/queue.py (fixed sections)

from dataclasses import dataclass, field
from typing import Optional, List, Union
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool
import os
from pgmq.messages import Message, QueueMetrics
from pgmq.decorators import transaction
from pgmq.logger import PGMQLogger, create_logger
import logging
from datetime import datetime


@dataclass
class PGMQueue:
    """Base class for interacting with a queue"""

    host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    port: str = field(default_factory=lambda: os.getenv("PG_PORT", "5432"))
    database: str = field(default_factory=lambda: os.getenv("PG_DATABASE", "postgres"))
    username: str = field(default_factory=lambda: os.getenv("PG_USERNAME", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "postgres"))
    delay: int = 0
    vt: int = 30
    pool_size: int = 10
    kwargs: dict = field(default_factory=dict)
    verbose: bool = False
    log_filename: Optional[str] = None
    init_extension: bool = True
    # New logging options
    structured_logging: bool = False
    log_rotation: bool = False
    log_rotation_size: str = "10 MB"
    log_retention: str = "1 week"

    pool: ConnectionPool = field(init=False)
    logger: logging.Logger = field(init=False)

    def __post_init__(self) -> None:
        conninfo = f"""
        host={self.host}
        port={self.port}
        dbname={self.database}
        user={self.username}
        password={self.password}
        """
        self.pool = ConnectionPool(conninfo, open=True, **self.kwargs)
        self._initialize_logging()
        if self.init_extension:
            self._initialize_extensions()

    def _initialize_logging(self) -> None:
        """Initialize logging using the centralized logger module."""
        # Use create_logger for backward compatibility
        self.logger = create_logger(
            name=__name__, verbose=self.verbose, log_filename=self.log_filename
        )

        # If enhanced features are needed, reconfigure with PGMQLogger
        if self.structured_logging or self.log_rotation:
            # Remove existing handlers to avoid duplicates
            for handler in self.logger.handlers[:]:
                self.logger.removeHandler(handler)

            # Get enhanced logger
            self.logger = PGMQLogger.get_logger(
                name=__name__,
                verbose=self.verbose,
                log_filename=self.log_filename,
                structured=self.structured_logging,
                rotation=self.log_rotation_size if self.log_rotation else None,
                retention=self.log_retention if self.log_rotation else None,
            )

    def _initialize_extensions(self, conn=None) -> None:
        self._execute_query("create extension if not exists pgmq cascade;", conn=conn)

    def _execute_query(
        self, query: str, params: Optional[Union[List, tuple]] = None, conn=None
    ) -> None:
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Executing query",
            query=query,
            params=params,
            conn_id=id(conn) if conn else None,
        )
        if conn:
            conn.execute(query, params)
        else:
            with self.pool.connection() as conn:
                conn.execute(query, params)

    def _execute_query_with_result(
        self, query: str, params: Optional[Union[List, tuple]] = None, conn=None
    ):
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Executing query with result",
            query=query,
            params=params,
            conn_id=id(conn) if conn else None,
        )
        if conn:
            return conn.execute(query, params).fetchall()
        else:
            with self.pool.connection() as conn:
                return conn.execute(query, params).fetchall()

    @transaction
    def create_partitioned_queue(
        self,
        queue: str,
        partition_interval: int = 10000,
        retention_interval: int = 100000,
        conn=None,
    ) -> None:
        """Create a new queue"""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Creating partitioned queue",
            queue=queue,
            partition_interval=partition_interval,
            retention_interval=retention_interval,
        )
        query = "select pgmq.create_partitioned(queue_name=>%s, partition_interval=>%s::text, retention_interval=>%s::text);"
        params = [queue, partition_interval, retention_interval]
        self._execute_query(query, params, conn=conn)

    @transaction
    def create_queue(self, queue: str, unlogged: bool = False, conn=None) -> None:
        """Create a new queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Creating queue", queue=queue, unlogged=unlogged
        )
        query = (
            "select pgmq.create_unlogged(queue_name=>%s);"
            if unlogged
            else "select pgmq.create(queue_name=>%s);"
        )
        self._execute_query(query, [queue], conn=conn)

    def validate_queue_name(self, queue_name: str, conn=None) -> None:
        """Validate the length of a queue name."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Validating queue name", queue_name=queue_name
        )
        query = "select pgmq.validate_queue_name(queue_name=>%s);"
        self._execute_query(query, [queue_name], conn=conn)

    @transaction
    def drop_queue(self, queue: str, partitioned: bool = False, conn=None) -> bool:
        """Drop a queue."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Dropping queue",
            queue=queue,
            partitioned=partitioned,
        )
        query = "select pgmq.drop_queue(queue_name=>%s, partitioned=>%s);"
        result = self._execute_query_with_result(query, [queue, partitioned], conn=conn)
        return result[0][0]

    @transaction
    def list_queues(self, conn=None) -> List[str]:
        """List all queues."""
        PGMQLogger.log_with_context(self.logger, logging.DEBUG, "Listing queues")
        query = "select queue_name from pgmq.list_queues();"
        rows = self._execute_query_with_result(query, conn=conn)
        return [row[0] for row in rows]

    @transaction
    def send(
        self, queue: str, message: dict, delay: int = 0, tz: datetime = None, conn=None
    ) -> int:
        """Send a message to a queue."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Sending message",
            queue=queue,
            delay=delay,
            has_tz=tz is not None,
        )

        result = None
        if delay:
            query = "select * from pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, delay=>%s::integer);"
            result = self._execute_query_with_result(
                query, [queue, Jsonb(message), delay], conn=conn
            )
        elif tz:
            query = "select * from pgmq.send(queue_name=>%s::text, msg=>%s::jsonb, delay=>%s::timestamptz);"
            result = self._execute_query_with_result(
                query, [queue, Jsonb(message), tz], conn=conn
            )
        else:
            query = "select * from pgmq.send(queue_name=>%s::text, msg=>%s::jsonb);"
            result = self._execute_query_with_result(
                query, [queue, Jsonb(message)], conn=conn
            )

        # Log success with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Message sent successfully",
            queue=queue,
            msg_id=result[0][0],
        )
        return result[0][0]

    @transaction
    def send_batch(
        self,
        queue: str,
        messages: List[dict],
        delay: int = 0,
        tz: datetime = None,
        conn=None,
    ) -> List[int]:
        """Send a batch of messages to a queue."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Sending batch messages",
            queue=queue,
            batch_size=len(messages),
            delay=delay,
            has_tz=tz is not None,
        )

        result = None
        if delay:
            query = "select * from pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], delay=>%s::integer);"
            params = [queue, [Jsonb(message) for message in messages], delay]
            result = self._execute_query_with_result(query, params, conn=conn)
        elif tz:
            query = "select * from pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[], delay=>%s::timestamptz);"
            params = [queue, [Jsonb(message) for message in messages], tz]
            result = self._execute_query_with_result(query, params, conn=conn)
        else:
            query = "select * from pgmq.send_batch(queue_name=>%s::text, msgs=>%s::jsonb[]);"
            params = [queue, [Jsonb(message) for message in messages]]
            result = self._execute_query_with_result(query, params, conn=conn)

        msg_ids = [message[0] for message in result]
        # Log success with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Batch messages sent successfully",
            queue=queue,
            msg_ids=msg_ids,
            count=len(msg_ids),
        )
        return msg_ids

    @transaction
    def read(
        self, queue: str, vt: Optional[int] = None, conn=None
    ) -> Optional[Message]:
        """Read a message from a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Reading message", queue=queue, vt=vt or self.vt
        )
        query = "select * from pgmq.read(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer);"
        rows = self._execute_query_with_result(
            query, [queue, vt or self.vt, 1], conn=conn
        )
        messages = [
            Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4])
            for x in rows
        ]

        result = messages[0] if messages else None
        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Message read completed",
            queue=queue,
            msg_id=result.msg_id if result else None,
            has_message=result is not None,
        )
        return result

    @transaction
    def read_batch(
        self, queue: str, vt: Optional[int] = None, batch_size=1, conn=None
    ) -> Optional[List[Message]]:
        """Read a batch of messages from a queue."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Reading batch messages",
            queue=queue,
            vt=vt or self.vt,
            batch_size=batch_size,
        )
        query = "select * from pgmq.read(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer);"
        rows = self._execute_query_with_result(
            query, [queue, vt or self.vt, batch_size], conn=conn
        )
        messages = [
            Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4])
            for x in rows
        ]

        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Batch messages read completed",
            queue=queue,
            count=len(messages),
        )
        return messages

    @transaction
    def read_with_poll(
        self,
        queue: str,
        vt: Optional[int] = None,
        qty: int = 1,
        max_poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        conn=None,
    ) -> Optional[List[Message]]:
        """Read messages from a queue with polling."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Reading messages with poll",
            queue=queue,
            vt=vt or self.vt,
            qty=qty,
            max_poll_seconds=max_poll_seconds,
            poll_interval_ms=poll_interval_ms,
        )
        query = "select * from pgmq.read_with_poll(queue_name=>%s::text, vt=>%s::integer, qty=>%s::integer, max_poll_seconds=>%s::integer, poll_interval_ms=>%s::integer);"
        params = [queue, vt or self.vt, qty, max_poll_seconds, poll_interval_ms]
        rows = self._execute_query_with_result(query, params, conn=conn)
        messages = [
            Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4])
            for x in rows
        ]

        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Messages read with poll completed",
            queue=queue,
            count=len(messages),
        )
        return messages

    @transaction
    def pop(self, queue: str, conn=None) -> Message:
        """Pop a message from a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Popping message", queue=queue
        )
        query = "select * from pgmq.pop(queue_name=>%s);"
        rows = self._execute_query_with_result(query, [queue], conn=conn)
        messages = [
            Message(msg_id=x[0], read_ct=x[1], enqueued_at=x[2], vt=x[3], message=x[4])
            for x in rows
        ]

        result = messages[0]
        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Message popped successfully",
            queue=queue,
            msg_id=result.msg_id,
        )
        return result

    @transaction
    def delete(self, queue: str, msg_id: int, conn=None) -> bool:
        """Delete a message from a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Deleting message", queue=queue, msg_id=msg_id
        )
        query = "select pgmq.delete(queue_name=>%s, msg_id=>%s);"
        result = self._execute_query_with_result(query, [queue, msg_id], conn=conn)

        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Message deleted",
            queue=queue,
            msg_id=msg_id,
            success=result[0][0],
        )
        return result[0][0]

    @transaction
    def delete_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Delete multiple messages from a queue."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Deleting batch messages",
            queue=queue,
            msg_ids=msg_ids,
        )
        query = "select * from pgmq.delete(queue_name=>%s, msg_ids=>%s);"
        result = self._execute_query_with_result(query, [queue, msg_ids], conn=conn)

        deleted_ids = [x[0] for x in result]
        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Batch messages deleted",
            queue=queue,
            deleted_ids=deleted_ids,
            count=len(deleted_ids),
        )
        return deleted_ids

    @transaction
    def archive(self, queue: str, msg_id: int, conn=None) -> bool:
        """Archive a message from a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Archiving message", queue=queue, msg_id=msg_id
        )
        query = "select pgmq.archive(queue_name=>%s, msg_id=>%s);"
        result = self._execute_query_with_result(query, [queue, msg_id], conn=conn)
        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Message archived",
            queue=queue,
            msg_id=msg_id,
            success=result[0][0],
        )
        return result[0][0]

    @transaction
    def archive_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        """Archive multiple messages from a queue."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Archiving batch messages",
            queue=queue,
            msg_ids=msg_ids,
        )
        query = "select * from pgmq.archive(queue_name=>%s, msg_ids=>%s);"
        result = self._execute_query_with_result(query, [queue, msg_ids], conn=conn)

        archived_ids = [x[0] for x in result]
        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Batch messages archived",
            queue=queue,
            archived_ids=archived_ids,
            count=len(archived_ids),
        )
        return archived_ids

    @transaction
    def purge(self, queue: str, conn=None) -> int:
        """Purge a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Purging queue", queue=queue
        )
        query = "select pgmq.purge_queue(queue_name=>%s);"
        result = self._execute_query_with_result(query, [queue], conn=conn)

        # Log result with context
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Queue purged", queue=queue, count=result[0][0]
        )
        return result[0][0]

    @transaction
    def metrics(self, queue: str, conn=None) -> QueueMetrics:
        """Get metrics for a specific queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Getting queue metrics", queue=queue
        )
        query = "SELECT * FROM pgmq.metrics(queue_name=>%s);"
        result = self._execute_query_with_result(query, [queue], conn=conn)[0]
        metrics = QueueMetrics(
            queue_name=result[0],
            queue_length=result[1],
            newest_msg_age_sec=result[2],
            oldest_msg_age_sec=result[3],
            total_messages=result[4],
            scrape_time=result[5],
        )

        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Queue metrics retrieved",
            queue=queue,
            queue_length=metrics.queue_length,
            total_messages=metrics.total_messages,
        )
        return metrics

    @transaction
    def metrics_all(self, conn=None) -> List[QueueMetrics]:
        """Get metrics for all queues."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Getting all queue metrics"
        )

        query = "SELECT * FROM pgmq.metrics_all();"
        results = self._execute_query_with_result(query, conn=conn)
        metrics_list = [
            QueueMetrics(
                queue_name=row[0],
                queue_length=row[1],
                newest_msg_age_sec=row[2],
                oldest_msg_age_sec=row[3],
                total_messages=row[4],
                scrape_time=row[5],
            )
            for row in results
        ]

        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "All queue metrics retrieved",
            count=len(metrics_list),
        )
        return metrics_list

    @transaction
    def set_vt(self, queue: str, msg_id: int, vt: int, conn=None) -> Message:
        """Set the visibility timeout for a specific message."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Setting visibility timeout",
            queue=queue,
            msg_id=msg_id,
            vt=vt,
        )
        query = "select * from pgmq.set_vt(queue_name=>%s, msg_id=>%s, vt=>%s);"
        result = self._execute_query_with_result(query, [queue, msg_id, vt], conn=conn)[
            0
        ]
        message = Message(
            msg_id=result[0],
            read_ct=result[1],
            enqueued_at=result[2],
            vt=result[3],
            message=result[4],
        )

        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Visibility timeout set",
            queue=queue,
            msg_id=msg_id,
            new_vt=message.vt,
        )
        return message

    @transaction
    def detach_archive(self, queue: str, conn=None) -> None:
        """Detach an archive from a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Detaching archive", queue=queue
        )

        query = "select pgmq.detach_archive(queue_name=>%s);"
        self._execute_query(query, [queue], conn=conn)

        # Log completion with context
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Archive detached", queue=queue
        )
