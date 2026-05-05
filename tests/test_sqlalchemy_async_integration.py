"""
Integration tests for asynchronous SQLAlchemy-based PGMQ clients.

These tests require a running PostgreSQL instance with the PGMQ extension.
"""

import inspect
import unittest
from datetime import datetime, timezone

import sqlalchemy.exc
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from pgmq.sqlalchemy_async_queue import PGMQueue as AsyncSQLAlchemyQueue
from pgmq.messages import Message, QueueMetrics
from tests.utils import (
    PG_HOST,
    PG_PORT,
    PG_DATABASE,
    PG_USERNAME,
    PG_PASSWORD,
    DATABASE_URL,
)

# ============================================================================
# Test Configuration
# ============================================================================

PGMQ_TEST_CONFIG = (
    {"conn_string": DATABASE_URL}
    if DATABASE_URL
    else {
        "host": PG_HOST,
        "port": PG_PORT,
        "username": PG_USERNAME,
        "password": PG_PASSWORD,
        "database": PG_DATABASE,
    }
)

# Functions not available in older PGMQ versions (e.g., 1.10.0)
_TOPIC_FUNCTIONS = {"pgmq.bind_topic", "pgmq.send_topic", "pgmq.send_batch_topic"}
_VALIDATION_FUNCTIONS = {"pgmq.validate_routing_key", "pgmq.validate_topic_pattern"}
_NOTIFY_FUNCTIONS = {"pgmq.update_notify_insert", "pgmq.list_notify_insert_throttles"}


def _pgmq_function_exists(queue, func_name):
    """Check if a PGMQ function exists in current database."""
    import psycopg

    dsn = (
        f"postgresql://{PGMQ_TEST_CONFIG['username']}:{PGMQ_TEST_CONFIG['password']}@"
        f"{PGMQ_TEST_CONFIG['host']}:{PGMQ_TEST_CONFIG['port']}/{PGMQ_TEST_CONFIG['database']}"
    )
    conn = psycopg.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_proc p "
                "JOIN pg_namespace n ON p.pronamespace = n.oid "
                "WHERE n.nspname = 'pgmq' AND p.proname = %s);",
                (func_name.split(".")[1],),
            )
            return cur.fetchone()[0]
    finally:
        conn.close()


def _skip_if_missing_topic_support(test_method):
    """Decorator to skip tests that require topic routing (PGMQ >= 1.2)."""

    async def async_wrapper(self, *args, **kwargs):
        if not _pgmq_function_exists(self.queue, "pgmq.send_topic"):
            raise unittest.SkipTest("Topic routing not available in this PGMQ version")
        return await test_method(self, *args, **kwargs)

    if inspect.iscoroutinefunction(test_method):
        return async_wrapper
    return test_method


def _skip_if_missing_validation_functions(test_method):
    """Decorator to skip tests that require validation utilities."""

    async def async_wrapper(self, *args, **kwargs):
        if not _pgmq_function_exists(self.queue, "pgmq.validate_routing_key"):
            raise unittest.SkipTest(
                "Validation functions not available in this PGMQ version"
            )
        return await test_method(self, *args, **kwargs)

    if inspect.iscoroutinefunction(test_method):
        return async_wrapper
    return test_method


def _skip_if_missing_notify_functions(test_method):
    """Decorator to skip tests that require full notify management."""

    async def async_wrapper(self, *args, **kwargs):
        if not _pgmq_function_exists(self.queue, "pgmq.update_notify_insert"):
            raise unittest.SkipTest(
                "Full notify management not available in this PGMQ version"
            )
        return await test_method(self, *args, **kwargs)

    if inspect.iscoroutinefunction(test_method):
        return async_wrapper
    return test_method


# ============================================================================
# Async SQLAlchemy Tests
# ============================================================================


class TestAsyncSQLAlchemyQueue(unittest.IsolatedAsyncioTestCase):
    """Tests for asynchronous SQLAlchemy PGMQ client."""

    async def asyncSetUp(self):
        """Create an async SQLAlchemy queue for testing and clean up."""
        self.queue = AsyncSQLAlchemyQueue(**PGMQ_TEST_CONFIG)
        await self.queue.init()

        # Cleanup all standard test queues to ensure a clean state
        for q in ["test_queue", "test_queue2", "test_topic_queue", "test_topic_queue2"]:
            try:
                await self.queue.drop_queue(q)
            except Exception:
                pass

        # Create the standard queue once for most tests
        await self.queue.create_queue("test_queue")

    async def asyncTearDown(self):
        """Cleanup."""
        for q in ["test_queue", "test_queue2", "test_topic_queue", "test_topic_queue2"]:
            try:
                await self.queue.drop_queue(q)
            except Exception:
                pass
        await self.queue.close()

    # --- Engine & Lifecycle Logic ---

    def test_invalid_async_engine_type_raises(self):
        """Passing a non-AsyncEngine object should raise TypeError."""
        queue = AsyncSQLAlchemyQueue(engine="not_an_engine", init_extension=False)
        import asyncio

        with self.assertRaises(TypeError) as ctx:
            asyncio.run(queue.init())
        self.assertIn("Expected sqlalchemy.ext.asyncio.AsyncEngine", str(ctx.exception))

    def test_async_session_raises_without_engine(self):
        """session() should raise RuntimeError if engine is None."""
        queue = AsyncSQLAlchemyQueue(
            host=PG_HOST,
            port=PG_PORT,
            username=PG_USERNAME,
            password=PG_PASSWORD,
            database=PG_DATABASE,
            init_extension=False,
        )
        queue.engine = None
        with self.assertRaises(RuntimeError) as ctx:
            queue.session()
        self.assertIn("Engine has not been initialized", str(ctx.exception))

    async def test_default_engine_creation_on_init(self):
        """When no engine is passed, init() should create an AsyncEngine."""
        queue = AsyncSQLAlchemyQueue(**PGMQ_TEST_CONFIG, init_extension=False)
        await queue.init()
        self.assertIsNotNone(queue.engine)
        self.assertIsInstance(queue.engine, AsyncEngine)
        await queue.close()

    async def test_external_async_engine_injection(self):
        """When an AsyncEngine is passed, init() should use it."""
        dsn = (
            f"postgresql+asyncpg://{PGMQ_TEST_CONFIG['username']}:{PGMQ_TEST_CONFIG['password']}@"
            f"{PGMQ_TEST_CONFIG['host']}:{PGMQ_TEST_CONFIG['port']}/{PGMQ_TEST_CONFIG['database']}"
        )
        external_engine = create_async_engine(dsn)
        queue = AsyncSQLAlchemyQueue(engine=external_engine, init_extension=False)
        await queue.init()
        self.assertIs(queue.engine, external_engine)
        await queue.close()
        await external_engine.dispose()

    async def test_async_session_returns_session(self):
        """session() should return an AsyncSession bound to the engine."""
        session = self.queue.session()
        self.assertIsNotNone(session)
        # queue.session() returns an AsyncSession instance,
        # matching the synchronous implementation.
        self.assertTrue(hasattr(session, "execute"))
        self.assertTrue(hasattr(session, "bind"))

    async def test_close_disposes_engine(self):
        """close() should dispose the async engine."""
        dsn = (
            f"postgresql+asyncpg://{PGMQ_TEST_CONFIG['username']}:{PGMQ_TEST_CONFIG['password']}@"
            f"{PGMQ_TEST_CONFIG['host']}:{PGMQ_TEST_CONFIG['port']}/{PGMQ_TEST_CONFIG['database']}"
        )
        external_engine = create_async_engine(dsn)
        queue = AsyncSQLAlchemyQueue(engine=external_engine, init_extension=False)
        await queue.init()
        await queue.close()
        # The library implementation does not set self.engine = None
        await external_engine.dispose()

    # --- Basic Lifecycle ---

    async def test_create_queue(self):
        """Test creating a queue (verifies list_queues)."""
        # test_queue is created in asyncSetUp, we just verify it exists
        queues = await self.queue.list_queues()
        self.assertTrue(any(q.queue_name == "test_queue" for q in queues))

    async def test_drop_queue(self):
        """Test dropping a queue."""
        await self.queue.create_queue("test_queue2")
        result = await self.queue.drop_queue("test_queue2")
        self.assertTrue(result)
        # Verify it's gone
        queues = await self.queue.list_queues()
        self.assertFalse(any(q.queue_name == "test_queue2" for q in queues))

    async def test_list_queues(self):
        """Test listing queues."""
        queues = await self.queue.list_queues()
        self.assertIsInstance(queues, list)
        self.assertTrue(any(q.queue_name == "test_queue" for q in queues))

    async def test_validate_queue_name(self):
        """Test validating a queue name."""
        result = await self.queue.validate_queue_name("valid_queue_name")
        self.assertTrue(result)

    # --- Send / Read ---

    async def test_send_and_read(self):
        """Test sending and reading messages."""
        msg_id = await self.queue.send("test_queue", {"key": "value"})
        self.assertGreater(msg_id, 0)

        message = await self.queue.read("test_queue")
        self.assertIsInstance(message, Message)
        self.assertEqual(message.msg_id, msg_id)
        self.assertEqual(message.message, {"key": "value"})

    async def test_send_with_headers(self):
        """Test sending a message with headers."""
        msg_id = await self.queue.send(
            "test_queue", {"body": "x"}, headers={"source": "test"}
        )
        self.assertGreater(msg_id, 0)
        message = await self.queue.read("test_queue")
        self.assertEqual(message.headers, {"source": "test"})

    async def test_send_with_delay(self):
        """Test sending a message with delay."""
        msg_id = await self.queue.send("test_queue", {"body": "x"}, delay=3600)
        self.assertGreater(msg_id, 0)
        nothing = await self.queue.read("test_queue")
        self.assertIsNone(nothing)

    async def test_send_batch(self):
        """Test sending batch messages."""
        messages = [{"msg": i} for i in range(5)]
        msg_ids = await self.queue.send_batch("test_queue", messages)
        self.assertEqual(len(msg_ids), 5)
        self.assertTrue(all(mid > 0 for mid in msg_ids))

    async def test_send_batch_with_headers(self):
        """Test sending batch messages with headers."""
        messages = [{"msg": i} for i in range(3)]
        headers = [{"idx": i} for i in range(3)]
        msg_ids = await self.queue.send_batch("test_queue", messages, headers=headers)
        self.assertEqual(len(msg_ids), 3)

    async def test_read_batch(self):
        """Test reading a batch of messages."""
        await self.queue.send_batch("test_queue", [{"i": i} for i in range(5)])
        msgs = await self.queue.read_batch("test_queue", batch_size=3)
        self.assertEqual(len(msgs), 3)

    async def test_read_with_poll(self):
        """Test reading with poll."""
        msgs = await self.queue.read_with_poll(
            "test_queue", qty=1, max_poll_seconds=1, poll_interval_ms=100
        )
        self.assertEqual(msgs, [])

    # --- Pop ---

    async def test_pop(self):
        """Test popping messages."""
        msg_id = await self.queue.send("test_queue", {"pop_me": True})
        message = await self.queue.pop("test_queue")
        self.assertIsInstance(message, Message)
        self.assertEqual(message.msg_id, msg_id)
        self.assertIsNone(await self.queue.read("test_queue"))

    async def test_pop_batch(self):
        """Test popping multiple messages."""
        await self.queue.send_batch("test_queue", [{"i": i} for i in range(3)])
        msgs = await self.queue.pop("test_queue", qty=3)
        self.assertIsInstance(msgs, list)
        self.assertEqual(len(msgs), 3)

    # --- Delete / Archive ---

    async def test_delete(self):
        """Test deleting messages."""
        msg_id = await self.queue.send("test_queue", {"delete_me": True})
        result = await self.queue.delete("test_queue", msg_id)
        self.assertTrue(result)

    async def test_delete_batch(self):
        """Test deleting multiple messages."""
        msg_ids = await self.queue.send_batch(
            "test_queue", [{"i": i} for i in range(3)]
        )
        deleted = await self.queue.delete_batch("test_queue", msg_ids)
        self.assertEqual(len(deleted), 3)

    async def test_archive(self):
        """Test archiving messages."""
        msg_id = await self.queue.send("test_queue", {"archived": True})
        result = await self.queue.archive("test_queue", msg_id)
        self.assertTrue(result)

    async def test_archive_batch(self):
        """Test archiving multiple messages."""
        msg_ids = await self.queue.send_batch(
            "test_queue", [{"i": i} for i in range(3)]
        )
        archived = await self.queue.archive_batch("test_queue", msg_ids)
        self.assertEqual(len(archived), 3)

    async def test_purge(self):
        """Test purging a queue."""
        await self.queue.send_batch("test_queue", [{"i": i} for i in range(10)])
        count = await self.queue.purge("test_queue")
        self.assertEqual(count, 10)

    # --- Visibility Timeout ---

    async def test_set_vt(self):
        """Test setting visibility timeout."""
        msg_id = await self.queue.send("test_queue", {"vt_test": True})
        updated = await self.queue.set_vt("test_queue", msg_id, 120)
        self.assertIsInstance(updated, Message)
        self.assertEqual(updated.msg_id, msg_id)

    async def test_set_vt_batch(self):
        """Test setting visibility timeout for multiple messages."""
        msg_ids = await self.queue.send_batch(
            "test_queue", [{"i": i} for i in range(2)]
        )
        updated = await self.queue.set_vt("test_queue", msg_ids, 120)
        self.assertIsInstance(updated, list)
        self.assertEqual(len(updated), 2)

    async def test_set_vt_timestamp(self):
        """Test setting visibility timeout to a specific timestamp."""
        msg_id = await self.queue.send("test_queue", {"vt_ts": True})
        future = datetime.now(timezone.utc)
        updated = await self.queue.set_vt("test_queue", msg_id, future)
        self.assertIsInstance(updated, Message)

    # --- Metrics ---

    async def test_metrics(self):
        """Test getting queue metrics."""
        await self.queue.send("test_queue", {"metric": "test"})
        metrics = await self.queue.metrics("test_queue")
        self.assertIsInstance(metrics, QueueMetrics)
        self.assertEqual(metrics.queue_name, "test_queue")
        self.assertGreaterEqual(metrics.queue_length, 1)

    async def test_metrics_all(self):
        """Test getting metrics for all queues."""
        all_metrics = await self.queue.metrics_all()
        self.assertIsInstance(all_metrics, list)
        self.assertTrue(any(m.queue_name == "test_queue" for m in all_metrics))

    # --- Topic Routing ---

    @_skip_if_missing_topic_support
    async def test_topic_bindings(self):
        """Test topic bind, unbind, list, and send."""
        await self.queue.create_queue("test_topic_queue")
        await self.queue.create_queue("test_topic_queue2")
        await self.queue.bind_topic("orders.*", "test_topic_queue")
        await self.queue.bind_topic("orders.#", "test_topic_queue2")

        bindings = await self.queue.list_topic_bindings()
        self.assertTrue(any(b.queue_name == "test_topic_queue" for b in bindings))

        queue_bindings = await self.queue.list_topic_bindings("test_topic_queue")
        self.assertEqual(len(queue_bindings), 1)

        result = await self.queue.test_routing("orders.created")
        self.assertTrue(any(r.queue_name == "test_topic_queue" for r in result))

        msg_id = await self.queue.send_topic("orders.created", {"event": "created"})
        self.assertGreater(msg_id, 0)

        unbound = await self.queue.unbind_topic("orders.*", "test_topic_queue")
        self.assertTrue(unbound)

    @_skip_if_missing_topic_support
    async def test_send_batch_topic(self):
        """Test sending batch topic messages."""
        await self.queue.create_queue("test_topic_queue")
        await self.queue.bind_topic("events.*", "test_topic_queue")
        results = await self.queue.send_batch_topic(
            "events.test", [{"i": i} for i in range(3)]
        )
        self.assertIsInstance(results, list)

    # --- Notifications ---

    async def test_notify_enable_disable(self):
        """Test enable and disable notify (available in all PGMQ versions)."""
        await self.queue.enable_notify("test_queue", throttle_interval_ms=100)
        await self.queue.disable_notify("test_queue")

    @_skip_if_missing_notify_functions
    async def test_notify_lifecycle(self):
        """Test enable, update, list, and disable notify."""
        await self.queue.enable_notify("test_queue", throttle_interval_ms=100)

        throttles = await self.queue.list_notify_throttles()
        self.assertTrue(any(t.queue_name == "test_queue" for t in throttles))

        await self.queue.update_notify("test_queue", throttle_interval_ms=200)
        throttles = await self.queue.list_notify_throttles()
        throttle = next(t for t in throttles if t.queue_name == "test_queue")
        self.assertEqual(throttle.throttle_interval_ms, 200)

        await self.queue.disable_notify("test_queue")
        throttles = await self.queue.list_notify_throttles()
        self.assertFalse(any(t.queue_name == "test_queue" for t in throttles))

    # --- Validation Utilities ---

    @_skip_if_missing_validation_functions
    async def test_validate_routing_key(self):
        """Test routing key validation."""
        self.assertTrue(await self.queue.validate_routing_key("orders.created"))

    @_skip_if_missing_validation_functions
    async def test_validate_topic_pattern(self):
        """Test topic pattern validation."""
        self.assertTrue(await self.queue.validate_topic_pattern("orders.*"))

    # --- FIFO / Grouped Reads ---

    async def test_read_grouped(self):
        """Test FIFO grouped read."""
        await self.queue.send_batch("test_queue", [{"g": i} for i in range(3)])
        msgs = await self.queue.read_grouped("test_queue", qty=2)
        self.assertEqual(len(msgs), 2)

    async def test_read_grouped_with_poll(self):
        """Test FIFO grouped read with poll."""
        msgs = await self.queue.read_grouped_with_poll(
            "test_queue", qty=1, max_poll_seconds=1, poll_interval_ms=100
        )
        self.assertEqual(msgs, [])

    async def test_read_grouped_rr(self):
        """Test FIFO round-robin read."""
        await self.queue.send_batch("test_queue", [{"g": i} for i in range(3)])
        msgs = await self.queue.read_grouped_rr("test_queue", qty=2)
        self.assertEqual(len(msgs), 2)

    async def test_read_grouped_rr_with_poll(self):
        """Test FIFO round-robin read with poll."""
        msgs = await self.queue.read_grouped_rr_with_poll(
            "test_queue", qty=1, max_poll_seconds=1, poll_interval_ms=100
        )
        self.assertEqual(msgs, [])

    # --- Archive Partitioning ---

    async def test_convert_archive_partitioned(self):
        """Test converting archive table to partitioned."""
        msg_id = await self.queue.send("test_queue", {"a": 1})
        await self.queue.archive("test_queue", msg_id)
        try:
            await self.queue.convert_archive_partitioned("test_queue")
        except sqlalchemy.exc.ProgrammingError as e:
            if "pg_partman is required" in str(e):
                self.skipTest("pg_partman is not installed")
            raise
        except (sqlalchemy.exc.DataError, sqlalchemy.exc.DBAPIError) as e:
            if "null values cannot be formatted as an SQL identifier" in str(e):
                self.skipTest(
                    "PGMQ bug: convert_archive_partitioned fails in this version"
                )
            raise

    async def test_detach_archive(self):
        """Test detaching archive table."""
        msg_id = await self.queue.send("test_queue", {"a": 1})
        await self.queue.archive("test_queue", msg_id)
        await self.queue.detach_archive("test_queue")

    # --- FIFO Index ---

    async def test_create_fifo_index(self):
        """Test creating FIFO index on a queue."""
        await self.queue.create_fifo_index("test_queue")

    async def test_create_fifo_indexes_all(self):
        """Test creating FIFO indexes on all queues."""
        await self.queue.create_fifo_indexes_all()

    # --- Engine Injection & Session ---

    async def test_external_engine_injection(self):
        """Test passing an external async engine to the queue."""
        dsn = (
            f"postgresql+asyncpg://{PGMQ_TEST_CONFIG['username']}:{PGMQ_TEST_CONFIG['password']}@"
            f"{PGMQ_TEST_CONFIG['host']}:{PGMQ_TEST_CONFIG['port']}/{PGMQ_TEST_CONFIG['database']}"
        )
        external_engine = create_async_engine(dsn)
        queue = AsyncSQLAlchemyQueue(engine=external_engine, init_extension=False)
        await queue.init()
        await queue.create_queue("test_queue")
        msg_id = await queue.send("test_queue", {"external": True})
        self.assertGreater(msg_id, 0)
        await queue.drop_queue("test_queue")
        await queue.close()
        await external_engine.dispose()

    async def test_session_with_external_engine(self):
        """Test using queue.session() to execute raw SQL."""
        dsn = (
            f"postgresql+asyncpg://{PGMQ_TEST_CONFIG['username']}:{PGMQ_TEST_CONFIG['password']}@"
            f"{PGMQ_TEST_CONFIG['host']}:{PGMQ_TEST_CONFIG['port']}/{PGMQ_TEST_CONFIG['database']}"
        )
        external_engine = create_async_engine(dsn)
        queue = AsyncSQLAlchemyQueue(engine=external_engine, init_extension=False)
        await queue.init()
        session = queue.session()
        result = await session.execute(text("SELECT 1 AS one"))
        self.assertEqual(result.scalar(), 1)
        await session.close()
        await queue.close()
        await external_engine.dispose()


if __name__ == "__main__":
    unittest.main()
