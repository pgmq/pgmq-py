# src/pgmq/async_queue.py (fixed sections)

from dataclasses import dataclass, field
from typing import Optional, List
import asyncpg
import os
import logging
from datetime import datetime

from orjson import dumps, loads

from pgmq.messages import Message, QueueMetrics
from pgmq.decorators import async_transaction as transaction
from pgmq.logger import PGMQLogger, create_logger


@dataclass
class PGMQueue:
    """Asynchronous PGMQueue client for interacting with queues."""

    host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    port: str = field(default_factory=lambda: os.getenv("PG_PORT", "5432"))
    database: str = field(default_factory=lambda: os.getenv("PG_DATABASE", "postgres"))
    username: str = field(default_factory=lambda: os.getenv("PG_USERNAME", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "postgres"))
    delay: int = 0
    vt: int = 30
    pool_size: int = 10
    perform_transaction: bool = False
    verbose: bool = False
    log_filename: Optional[str] = None
    # New logging options
    structured_logging: bool = False
    log_rotation: bool = False
    log_rotation_size: str = "10 MB"
    log_retention: str = "1 week"

    pool: asyncpg.pool.Pool = field(init=False)
    logger: logging.Logger = field(init=False)

    def __post_init__(self) -> None:
        self.host = self.host or "localhost"
        self.port = self.port or "5432"
        self.database = self.database or "postgres"
        self.username = self.username or "postgres"
        self.password = self.password or "postgres"

        if not all([self.host, self.port, self.database, self.username, self.password]):
            raise ValueError("Incomplete database connection information provided.")

        self._initialize_logging()
        self.logger.debug("PGMQueue initialized")

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

    async def init(self, init_extension: bool = True):
        self.logger.debug("Creating asyncpg connection pool")
        self.pool = await asyncpg.create_pool(
            user=self.username,
            database=self.database,
            password=self.password,
            host=self.host,
            port=self.port,
            min_size=1,
            max_size=self.pool_size,
        )

        if init_extension:
            self.logger.debug("Initializing pgmq extension")
            async with self.pool.acquire() as conn:
                await conn.execute("create extension if not exists pgmq cascade;")

    @transaction
    async def create_partitioned_queue(
        self,
        queue: str,
        partition_interval: int = 10000,
        retention_interval: int = 100000,
        conn=None,
    ) -> None:
        """Create a new partitioned queue."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Creating partitioned queue",
            queue=queue,
            partition_interval=partition_interval,
            retention_interval=retention_interval,
        )
        if conn is None:
            async with self.pool.acquire() as conn:
                await self._create_partitioned_queue_internal(
                    queue, partition_interval, retention_interval, conn
                )
        else:
            await self._create_partitioned_queue_internal(
                queue, partition_interval, retention_interval, conn
            )

    async def _create_partitioned_queue_internal(
        self, queue, partition_interval, retention_interval, conn
    ):
        self.logger.debug(f"Creating partitioned queue '{queue}'")
        await conn.execute(
            "SELECT pgmq.create_partitioned(queue_name=>$1, partition_interval=>$2::text, retention_interval=>$3::text);",
            queue,
            partition_interval,
            retention_interval,
        )

    @transaction
    async def create_queue(self, queue: str, unlogged: bool = False, conn=None) -> None:
        """Create a new queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Creating queue", queue=queue, unlogged=unlogged
        )
        if conn is None:
            async with self.pool.acquire() as conn:
                await self._create_queue_internal(queue, unlogged, conn)
        else:
            await self._create_queue_internal(queue, unlogged, conn)

    async def _create_queue_internal(self, queue, unlogged, conn):
        self.logger.debug(f"Creating queue '{queue}' with unlogged={unlogged}")
        if unlogged:
            await conn.execute("SELECT pgmq.create_unlogged(queue_name=>$1);", queue)
        else:
            await conn.execute("SELECT pgmq.create(queue_name=>$1);", queue)

    async def validate_queue_name(self, queue_name: str) -> None:
        """Validate the length of a queue name."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Validating queue name", queue_name=queue_name
        )
        async with self.pool.acquire() as conn:
            await conn.execute(
                "SELECT pgmq.validate_queue_name(queue_name=>$1);", queue_name
            )

    @transaction
    async def drop_queue(
        self, queue: str, partitioned: bool = False, conn=None
    ) -> bool:
        """Drop a queue."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Dropping queue",
            queue=queue,
            partitioned=partitioned,
        )
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._drop_queue_internal(queue, partitioned, conn)
        else:
            return await self._drop_queue_internal(queue, partitioned, conn)

    async def _drop_queue_internal(self, queue, partitioned, conn):
        result = await conn.fetchrow(
            "SELECT pgmq.drop_queue(queue_name=>$1, partitioned=>$2);",
            queue,
            partitioned,
        )
        self.logger.debug(f"Queue '{queue}' dropped: {result[0]}")
        return result[0]

    @transaction
    async def list_queues(self, conn=None) -> List[str]:
        """List all queues."""
        PGMQLogger.log_with_context(self.logger, logging.DEBUG, "Listing queues")
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._list_queues_internal(conn)
        else:
            return await self._list_queues_internal(conn)

    async def _list_queues_internal(self, conn):
        rows = await conn.fetch("SELECT queue_name FROM pgmq.list_queues();")
        queues = [row["queue_name"] for row in rows]
        self.logger.debug(f"Queues listed: {queues}")
        return queues

    @transaction
    async def send(
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

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._send_internal(queue, message, delay, tz, conn)
        else:
            return await self._send_internal(queue, message, delay, tz, conn)

    async def _send_internal(
        self,
        queue: str,
        message: dict,
        delay: int = None,
        tz: datetime = None,
        conn=None,
    ):
        self.logger.debug(
            f"Sending message to queue '{queue}' with delay={delay}, tz={tz}"
        )
        result = None
        if delay:
            result = await conn.fetchrow(
                "SELECT * FROM pgmq.send(queue_name=>$1::text, msg=>$2::jsonb, delay=>$3::integer);",
                queue,
                dumps(message).decode("utf-8"),
                delay,
            )
        elif tz:
            result = await conn.fetchrow(
                "SELECT * FROM pgmq.send(queue_name=>$1::text, msg=>$2::jsonb, delay=>$3::timestamptz);",
                queue,
                dumps(message).decode("utf-8"),
                tz,
            )
        else:
            result = await conn.fetchrow(
                "SELECT * FROM pgmq.send(queue_name=>$1::text, msg=>$2::jsonb);",
                queue,
                dumps(message).decode("utf-8"),
            )

        # Log success with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Message sent successfully",
            queue=queue,
            msg_id=result[0],
        )
        return result[0]

    @transaction
    async def send_batch(
        self,
        queue: str,
        messages: List[dict],
        delay: int = 0,
        tz: str = None,
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

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._send_batch_internal(queue, messages, delay, tz, conn)
        else:
            return await self._send_batch_internal(queue, messages, delay, tz, conn)

    async def _send_batch_internal(
        self,
        queue: str,
        messages: list[dict],
        delay: int = None,
        tz: datetime = None,
        conn=None,
    ):
        self.logger.debug(
            f"Sending batch of messages to queue '{queue}' with delay={delay}, tz={tz}"
        )
        jsonb_array = [dumps(message).decode("utf-8") for message in messages]
        result = None
        if delay:
            result = await conn.fetch(
                "SELECT * FROM pgmq.send_batch(queue_name=>$1, msgs=>$2::jsonb[], delay=>$3::integer);",
                queue,
                jsonb_array,
                delay,
            )
        elif tz:
            result = await conn.fetch(
                "SELECT * FROM pgmq.send_batch(queue_name=>$1, msgs=>$2::jsonb[], delay=>$3::timestamptz);",
                queue,
                jsonb_array,
                tz,
            )
        else:
            result = await conn.fetch(
                "SELECT * FROM pgmq.send_batch(queue_name=>$1, msgs=>$2::jsonb[]);",
                queue,
                jsonb_array,
            )

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
    async def read(
        self, queue: str, vt: Optional[int] = None, conn=None
    ) -> Optional[Message]:
        """Read a message from a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Reading message", queue=queue, vt=vt or self.vt
        )

        batch_size = 1
        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._read_internal(queue, vt, batch_size, conn)
        else:
            return await self._read_internal(queue, vt, batch_size, conn)

    async def _read_internal(self, queue, vt, batch_size, conn):
        self.logger.debug(f"Reading message from queue '{queue}' with vt={vt}")
        rows = await conn.fetch(
            "SELECT * FROM pgmq.read(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer);",
            queue,
            vt or self.vt,
            batch_size,
        )
        messages = [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=loads(row[4]),
            )
            for row in rows
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
    async def read_batch(
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

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._read_batch_internal(queue, vt, batch_size, conn)
        else:
            return await self._read_batch_internal(queue, vt, batch_size, conn)

    async def _read_batch_internal(self, queue, vt, batch_size, conn):
        self.logger.debug(
            f"Reading batch of messages from queue '{queue}' with vt={vt}"
        )
        rows = await conn.fetch(
            "SELECT * FROM pgmq.read(queue_name=>$1::text, vt=>$2::integer, qty=>$3::integer);",
            queue,
            vt or self.vt,
            batch_size,
        )
        messages = [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=loads(row[4]),
            )
            for row in rows
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
    async def read_with_poll(
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

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._read_with_poll_internal(
                    queue, vt, qty, max_poll_seconds, poll_interval_ms, conn
                )
        else:
            return await self._read_with_poll_internal(
                queue, vt, qty, max_poll_seconds, poll_interval_ms, conn
            )

    async def _read_with_poll_internal(
        self, queue, vt, qty, max_poll_seconds, poll_interval_ms, conn
    ):
        self.logger.debug(f"Reading messages with polling from queue '{queue}'")
        rows = await conn.fetch(
            "SELECT * FROM pgmq.read_with_poll(queue_name=>$1, vt=>$2, qty=>$3, max_poll_seconds=>$4, poll_interval_ms=>$5);",
            queue,
            vt or self.vt,
            qty,
            max_poll_seconds,
            poll_interval_ms,
        )
        messages = [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=loads(row[4]),
            )
            for row in rows
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
    async def pop(self, queue: str, conn=None) -> Message:
        """Pop a message from a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Popping message", queue=queue
        )

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._pop_internal(queue, conn)
        else:
            return await self._pop_internal(queue, conn)

    async def _pop_internal(self, queue, conn):
        self.logger.debug(f"Popping message from queue '{queue}'")
        rows = await conn.fetch("SELECT * FROM pgmq.pop(queue_name=>$1);", queue)
        messages = [
            Message(
                msg_id=row[0],
                read_ct=row[1],
                enqueued_at=row[2],
                vt=row[3],
                message=loads(row[4]),
            )
            for row in rows
        ]

        result = messages[0] if messages else None
        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Message popped successfully",
            queue=queue,
            msg_id=result.msg_id if result else None,
            has_message=result is not None,
        )
        return result

    @transaction
    async def delete(self, queue: str, msg_id: int, conn=None) -> bool:
        """Delete a message from a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Deleting message", queue=queue, msg_id=msg_id
        )

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._delete_internal(queue, msg_id, conn)
        else:
            return await self._delete_internal(queue, msg_id, conn)

    async def _delete_internal(self, queue, msg_id, conn):
        self.logger.debug(f"Deleting message with msg_id={msg_id} from queue '{queue}'")
        row = await conn.fetchrow(
            "SELECT pgmq.delete(queue_name=>$1::text, msg_id=>$2::int);", queue, msg_id
        )

        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Message deleted",
            queue=queue,
            msg_id=msg_id,
            success=row[0],
        )
        return row[0]

    @transaction
    async def delete_batch(
        self, queue: str, msg_ids: List[int], conn=None
    ) -> List[int]:
        """Delete multiple messages from a queue."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Deleting batch messages",
            queue=queue,
            msg_ids=msg_ids,
        )

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._delete_batch_internal(queue, msg_ids, conn)
        else:
            return await self._delete_batch_internal(queue, msg_ids, conn)

    async def _delete_batch_internal(self, queue, msg_ids, conn):
        self.logger.debug(
            f"Deleting messages with msg_ids={msg_ids} from queue '{queue}'"
        )
        results = await conn.fetch(
            "SELECT * FROM pgmq.delete(queue_name=>$1::text, msg_ids=>$2::int[]);",
            queue,
            msg_ids,
        )

        deleted_ids = [result[0] for result in results]
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
    async def archive(self, queue: str, msg_id: int, conn=None) -> bool:
        """Archive a message from a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Archiving message", queue=queue, msg_id=msg_id
        )

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._archive_internal(queue, msg_id, conn)
        else:
            return await self._archive_internal(queue, msg_id, conn)

    async def _archive_internal(self, queue, msg_id, conn):
        self.logger.debug(
            f"Archiving message with msg_id={msg_id} from queue '{queue}'"
        )
        row = await conn.fetchrow(
            "SELECT pgmq.archive(queue_name=>$1::text, msg_id=>$2::int);", queue, msg_id
        )

        # Log result with context
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Message archived",
            queue=queue,
            msg_id=msg_id,
            success=row[0],
        )
        return row[0]

    @transaction
    async def archive_batch(
        self, queue: str, msg_ids: List[int], conn=None
    ) -> List[int]:
        """Archive multiple messages from a queue."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Archiving batch messages",
            queue=queue,
            msg_ids=msg_ids,
        )

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._archive_batch_internal(queue, msg_ids, conn)
        else:
            return await self._archive_batch_internal(queue, msg_ids, conn)

    async def _archive_batch_internal(self, queue, msg_ids, conn):
        self.logger.debug(
            f"Archiving messages with msg_ids={msg_ids} from queue '{queue}'"
        )
        results = await conn.fetch(
            "SELECT * FROM pgmq.archive(queue_name=>$1::text, msg_ids=>$2::int[]);",
            queue,
            msg_ids,
        )

        archived_ids = [result[0] for result in results]
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
    async def purge(self, queue: str, conn=None) -> int:
        """Purge a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Purging queue", queue=queue
        )

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._purge_internal(queue, conn)
        else:
            return await self._purge_internal(queue, conn)

    async def _purge_internal(self, queue, conn):
        self.logger.debug(f"Purging queue '{queue}'")
        row = await conn.fetchrow("SELECT pgmq.purge_queue(queue_name=>$1);", queue)
        # Log result with context
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Queue purged", queue=queue, count=row[0]
        )

        return row[0]

    @transaction
    async def metrics(self, queue: str, conn=None) -> QueueMetrics:
        """Get metrics for a specific queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Getting queue metrics", queue=queue
        )

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._metrics_internal(queue, conn)
        else:
            return await self._metrics_internal(queue, conn)

    async def _metrics_internal(self, queue, conn):
        self.logger.debug(f"Fetching metrics for queue '{queue}'")
        result = await conn.fetchrow(
            "SELECT * FROM pgmq.metrics(queue_name=>$1);", queue
        )
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
    async def metrics_all(self, conn=None) -> List[QueueMetrics]:
        """Get metrics for all queues."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Getting all queue metrics"
        )

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._metrics_all_internal(conn)
        else:
            return await self._metrics_all_internal(conn)

    async def _metrics_all_internal(self, conn):
        self.logger.debug("Fetching metrics for all queues")
        results = await conn.fetch("SELECT * FROM pgmq.metrics_all();")
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
    async def set_vt(self, queue: str, msg_id: int, vt: int, conn=None) -> Message:
        """Set the visibility timeout for a specific message."""
        PGMQLogger.log_with_context(
            self.logger,
            logging.DEBUG,
            "Setting visibility timeout",
            queue=queue,
            msg_id=msg_id,
            vt=vt,
        )

        if conn is None:
            async with self.pool.acquire() as conn:
                return await self._set_vt_internal(queue, msg_id, vt, conn)
        else:
            return await self._set_vt_internal(queue, msg_id, vt, conn)

    async def _set_vt_internal(self, queue: str, msg_id, vt, conn):
        self.logger.debug(
            f"Setting VT for msg_id={msg_id} in queue '{queue}' to vt={vt}"
        )
        row = await conn.fetchrow(
            "SELECT * FROM pgmq.set_vt(queue_name=>$1, msg_id=>$2, vt=>$3);",
            queue,
            msg_id,
            vt,
        )
        message = Message(
            msg_id=row[0],
            read_ct=row[1],
            enqueued_at=row[2],
            vt=row[3],
            message=loads(row[4]),
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
    async def detach_archive(self, queue: str, conn=None) -> None:
        """Detach an archive from a queue."""
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Detaching archive", queue=queue
        )

        if conn is None:
            async with self.pool.acquire() as conn:
                await self._detach_archive_internal(queue, conn)
        else:
            await self._detach_archive_internal(queue, conn)

    async def _detach_archive_internal(self, queue, conn):
        self.logger.debug(f"Detaching archive from queue '{queue}'")
        await conn.execute("SELECT pgmq.detach_archive(queue_name=>$1);", queue)

        # Log completion with context
        PGMQLogger.log_with_context(
            self.logger, logging.DEBUG, "Archive detached", queue=queue
        )
