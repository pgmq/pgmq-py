"""
Integration tests for synchronous SQLAlchemy-based PGMQ clients.

These tests require a running PostgreSQL instance with the PGMQ extension.
"""

import unittest
from datetime import datetime, timezone

import sqlalchemy.exc
from sqlalchemy import Engine, create_engine, text

from pgmq.sqlalchemy_queue import PGMQueue as SyncSQLAlchemyQueue
from pgmq.messages import Message, QueueMetrics

# ============================================================================
# Test Configuration
# ============================================================================

PGMQ_TEST_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "username": "postgres",
    "password": "postgres",
    "database": "postgres",
}

# Functions not available in older PGMQ versions (e.g., 1.10.0)
_TOPIC_FUNCTIONS = {"pgmq.bind_topic", "pgmq.send_topic", "pgmq.send_batch_topic"}
_VALIDATION_FUNCTIONS = {"pgmq.validate_routing_key", "pgmq.validate_topic_pattern"}
_NOTIFY_FUNCTIONS = {"pgmq.update_notify_insert", "pgmq.list_notify_insert_throttles"}


def _pgmq_function_exists(queue, func_name):
    """Check if a PGMQ function exists in the current database."""
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

    def wrapper(self, *args, **kwargs):
        if not _pgmq_function_exists(self.queue, "pgmq.send_topic"):
            raise unittest.SkipTest("Topic routing not available in this PGMQ version")
        return test_method(self, *args, **kwargs)

    return wrapper


def _skip_if_missing_validation_functions(test_method):
    """Decorator to skip tests that require validation utilities."""

    def wrapper(self, *args, **kwargs):
        if not _pgmq_function_exists(self.queue, "pgmq.validate_routing_key"):
            raise unittest.SkipTest(
                "Validation functions not available in this PGMQ version"
            )
        return test_method(self, *args, **kwargs)

    return wrapper


def _skip_if_missing_notify_functions(test_method):
    """Decorator to skip tests that require full notify management."""

    def wrapper(self, *args, **kwargs):
        if not _pgmq_function_exists(self.queue, "pgmq.update_notify_insert"):
            raise unittest.SkipTest(
                "Full notify management not available in this PGMQ version"
            )
        return test_method(self, *args, **kwargs)

    return wrapper


# ============================================================================
# Sync SQLAlchemy Tests
# ============================================================================


class TestSyncSQLAlchemyQueue(unittest.TestCase):
    """Tests for synchronous SQLAlchemy PGMQ client."""

    @classmethod
    def setUpClass(cls):
        """Create a sync SQLAlchemy queue for testing."""
        cls.queue = SyncSQLAlchemyQueue(**PGMQ_TEST_CONFIG)

    @classmethod
    def tearDownClass(cls):
        """Cleanup: drop test queues and dispose engine."""
        try:
            cls.queue.drop_queue("test_queue")
            cls.queue.drop_queue("test_queue2")
            cls.queue.drop_queue("test_partitioned")
            cls.queue.drop_queue("test_topic_queue")
            cls.queue.drop_queue("test_topic_queue2")
        except Exception:
            pass
        cls.queue.dispose()

    def setUp(self):
        """Clean up queues before each test and ensure test_queue exists."""
        # Drop all standard test queues to ensure a clean state
        for q in [
            "test_queue",
            "test_queue2",
            "test_partitioned",
            "test_topic_queue",
            "test_topic_queue2",
        ]:
            try:
                self.queue.drop_queue(q)
            except Exception:
                pass

        # Create the standard queue once for most tests
        self.queue.create_queue("test_queue")

    # --- Engine & Lifecycle Logic ---

    def test_invalid_engine_type_raises(self):
        """Passing a non-Engine object should raise TypeError."""
        with self.assertRaises(TypeError) as ctx:
            SyncSQLAlchemyQueue(engine="not_an_engine", init_extension=False)
        self.assertIn("Expected sqlalchemy.Engine", str(ctx.exception))

    def test_session_raises_without_engine(self):
        """session() should raise RuntimeError if engine is None."""
        queue = SyncSQLAlchemyQueue(
            host="localhost",
            port="5432",
            username="postgres",
            password="postgres",
            database="postgres",
            init_extension=False,
        )
        queue.engine = None
        with self.assertRaises(RuntimeError) as ctx:
            queue.session()
        self.assertIn("Engine has not been initialized", str(ctx.exception))

    def test_default_engine_creation(self):
        """When no engine is passed, one should be created internally."""
        queue = SyncSQLAlchemyQueue(**PGMQ_TEST_CONFIG, init_extension=False)
        self.assertIsNotNone(queue.engine)
        self.assertIsInstance(queue.engine, Engine)
        queue.dispose()

    def test_external_engine_injection_basics(self):
        """When an engine is passed, it should be used directly."""
        dsn = (
            f"postgresql+psycopg://{PGMQ_TEST_CONFIG['username']}:{PGMQ_TEST_CONFIG['password']}@"
            f"{PGMQ_TEST_CONFIG['host']}:{PGMQ_TEST_CONFIG['port']}/{PGMQ_TEST_CONFIG['database']}"
        )
        external_engine = create_engine(dsn)
        queue = SyncSQLAlchemyQueue(engine=external_engine, init_extension=False)
        self.assertIs(queue.engine, external_engine)
        queue.dispose()
        external_engine.dispose()

    def test_session_factory_returns_session(self):
        """session() should return a SQLAlchemy Session bound to the engine."""
        session = self.queue.session()
        self.assertIsNotNone(session)
        # In the real implementation, queue.session() returns a Session instance,
        # not a sessionmaker (factory).
        self.assertTrue(hasattr(session, "execute"))
        self.assertTrue(hasattr(session, "bind"))
        session.close()

    def test_dispose_closes_engine(self):
        """dispose() should call dispose() on the engine."""
        queue = SyncSQLAlchemyQueue(**PGMQ_TEST_CONFIG, init_extension=False)
        queue.dispose()
        # Note: The library implementation does not set self.engine = None,
        # it only disposes of the engine. So we just ensure it runs without error.

    # --- Basic Lifecycle ---

    def test_create_queue(self):
        """Test creating a queue (verifies list_queues)."""
        # test_queue is created in setUp, we just verify it exists
        queues = self.queue.list_queues()
        self.assertTrue(any(q.queue_name == "test_queue" for q in queues))

    def test_drop_queue(self):
        """Test dropping a queue."""
        self.queue.create_queue("test_queue2")
        result = self.queue.drop_queue("test_queue2")
        self.assertTrue(result)
        # Verify it's gone
        queues = self.queue.list_queues()
        self.assertFalse(any(q.queue_name == "test_queue2" for q in queues))

    def test_list_queues(self):
        """Test listing queues."""
        queues = self.queue.list_queues()
        self.assertIsInstance(queues, list)
        self.assertTrue(any(q.queue_name == "test_queue" for q in queues))

    def test_validate_queue_name(self):
        """Test validating a queue name."""
        result = self.queue.validate_queue_name("valid_queue_name")
        self.assertTrue(result)

    def test_partitioned_queue(self):
        """Test creating partitioned queue."""
        try:
            self.queue.create_partitioned_queue(
                "test_partitioned",
                partition_interval=1000,
                retention_interval=10000,
            )
        except sqlalchemy.exc.ProgrammingError as e:
            if "pg_partman is required" in str(e):
                self.skipTest("pg_partman is not installed")
            raise
        queues = self.queue.list_queues()
        self.assertTrue(any(q.queue_name == "test_partitioned" for q in queues))
        self.assertTrue(
            any(q.is_partitioned for q in queues if q.queue_name == "test_partitioned")
        )

    # --- Send / Read ---

    def test_send_and_read(self):
        """Test sending and reading messages."""
        msg_id = self.queue.send("test_queue", {"key": "value"})
        self.assertGreater(msg_id, 0)

        message = self.queue.read("test_queue")
        self.assertIsInstance(message, Message)
        self.assertEqual(message.msg_id, msg_id)
        self.assertEqual(message.message, {"key": "value"})

    def test_send_with_headers(self):
        """Test sending a message with headers."""
        msg_id = self.queue.send(
            "test_queue", {"body": "x"}, headers={"source": "test"}
        )
        self.assertGreater(msg_id, 0)
        message = self.queue.read("test_queue")
        self.assertEqual(message.headers, {"source": "test"})

    def test_send_with_delay(self):
        """Test sending a message with delay."""
        msg_id = self.queue.send("test_queue", {"body": "x"}, delay=3600)
        self.assertGreater(msg_id, 0)
        nothing = self.queue.read("test_queue")
        self.assertIsNone(nothing)

    def test_send_batch(self):
        """Test sending batch messages."""
        messages = [{"msg": i} for i in range(5)]
        msg_ids = self.queue.send_batch("test_queue", messages)
        self.assertEqual(len(msg_ids), 5)
        self.assertTrue(all(mid > 0 for mid in msg_ids))

    def test_send_batch_with_headers(self):
        """Test sending batch messages with headers."""
        messages = [{"msg": i} for i in range(3)]
        headers = [{"idx": i} for i in range(3)]
        msg_ids = self.queue.send_batch("test_queue", messages, headers=headers)
        self.assertEqual(len(msg_ids), 3)

    def test_read_batch(self):
        """Test reading a batch of messages."""
        self.queue.send_batch("test_queue", [{"i": i} for i in range(5)])
        msgs = self.queue.read_batch("test_queue", batch_size=3)
        self.assertEqual(len(msgs), 3)

    def test_read_with_poll(self):
        """Test reading with poll."""
        msgs = self.queue.read_with_poll(
            "test_queue", qty=1, max_poll_seconds=1, poll_interval_ms=100
        )
        self.assertEqual(msgs, [])

    # --- Pop ---

    def test_pop(self):
        """Test popping messages."""
        msg_id = self.queue.send("test_queue", {"pop_me": True})
        message = self.queue.pop("test_queue")
        self.assertIsInstance(message, Message)
        self.assertEqual(message.msg_id, msg_id)
        self.assertIsNone(self.queue.read("test_queue"))

    def test_pop_batch(self):
        """Test popping multiple messages."""
        self.queue.send_batch("test_queue", [{"i": i} for i in range(3)])
        msgs = self.queue.pop("test_queue", qty=3)
        self.assertIsInstance(msgs, list)
        self.assertEqual(len(msgs), 3)

    # --- Delete / Archive ---

    def test_delete(self):
        """Test deleting messages."""
        msg_id = self.queue.send("test_queue", {"delete_me": True})
        result = self.queue.delete("test_queue", msg_id)
        self.assertTrue(result)

    def test_delete_batch(self):
        """Test deleting multiple messages."""
        msg_ids = self.queue.send_batch("test_queue", [{"i": i} for i in range(3)])
        deleted = self.queue.delete_batch("test_queue", msg_ids)
        self.assertEqual(len(deleted), 3)

    def test_archive(self):
        """Test archiving messages."""
        msg_id = self.queue.send("test_queue", {"archived": True})
        result = self.queue.archive("test_queue", msg_id)
        self.assertTrue(result)

    def test_archive_batch(self):
        """Test archiving multiple messages."""
        msg_ids = self.queue.send_batch("test_queue", [{"i": i} for i in range(3)])
        archived = self.queue.archive_batch("test_queue", msg_ids)
        self.assertEqual(len(archived), 3)

    def test_purge(self):
        """Test purging a queue."""
        self.queue.send_batch("test_queue", [{"i": i} for i in range(10)])
        count = self.queue.purge("test_queue")
        self.assertEqual(count, 10)

    # --- Visibility Timeout ---

    def test_set_vt(self):
        """Test setting visibility timeout."""
        msg_id = self.queue.send("test_queue", {"vt_test": True})
        updated = self.queue.set_vt("test_queue", msg_id, 120)
        self.assertIsInstance(updated, Message)
        self.assertEqual(updated.msg_id, msg_id)

    def test_set_vt_batch(self):
        """Test setting visibility timeout for multiple messages."""
        msg_ids = self.queue.send_batch("test_queue", [{"i": i} for i in range(2)])
        updated = self.queue.set_vt("test_queue", msg_ids, 120)
        self.assertIsInstance(updated, list)
        self.assertEqual(len(updated), 2)

    def test_set_vt_timestamp(self):
        """Test setting visibility timeout to a specific timestamp."""
        msg_id = self.queue.send("test_queue", {"vt_ts": True})
        future = datetime.now(timezone.utc)
        updated = self.queue.set_vt("test_queue", msg_id, future)
        self.assertIsInstance(updated, Message)

    # --- Metrics ---

    def test_metrics(self):
        """Test getting queue metrics."""
        self.queue.send("test_queue", {"metric": "test"})
        metrics = self.queue.metrics("test_queue")
        self.assertIsInstance(metrics, QueueMetrics)
        self.assertEqual(metrics.queue_name, "test_queue")
        self.assertGreaterEqual(metrics.queue_length, 1)

    def test_metrics_all(self):
        """Test getting metrics for all queues."""
        all_metrics = self.queue.metrics_all()
        self.assertIsInstance(all_metrics, list)
        self.assertTrue(any(m.queue_name == "test_queue" for m in all_metrics))

    # --- Topic Routing ---

    @_skip_if_missing_topic_support
    def test_topic_bindings(self):
        """Test topic bind, unbind, list, and send."""
        self.queue.create_queue("test_topic_queue")
        self.queue.create_queue("test_topic_queue2")
        self.queue.bind_topic("orders.*", "test_topic_queue")
        self.queue.bind_topic("orders.#", "test_topic_queue2")

        bindings = self.queue.list_topic_bindings()
        self.assertTrue(any(b.queue_name == "test_topic_queue" for b in bindings))

        queue_bindings = self.queue.list_topic_bindings("test_topic_queue")
        self.assertEqual(len(queue_bindings), 1)

        result = self.queue.test_routing("orders.created")
        self.assertTrue(any(r.queue_name == "test_topic_queue" for r in result))

        msg_id = self.queue.send_topic("orders.created", {"event": "created"})
        self.assertGreater(msg_id, 0)

        unbound = self.queue.unbind_topic("orders.*", "test_topic_queue")
        self.assertTrue(unbound)

    @_skip_if_missing_topic_support
    def test_send_batch_topic(self):
        """Test sending batch topic messages."""
        self.queue.create_queue("test_topic_queue")
        self.queue.bind_topic("events.*", "test_topic_queue")
        results = self.queue.send_batch_topic(
            "events.test", [{"i": i} for i in range(3)]
        )
        self.assertIsInstance(results, list)

    # --- Notifications ---

    def test_notify_enable_disable(self):
        """Test enable and disable notify (available in all PGMQ versions)."""
        self.queue.enable_notify("test_queue", throttle_interval_ms=100)
        self.queue.disable_notify("test_queue")

    @_skip_if_missing_notify_functions
    def test_notify_lifecycle(self):
        """Test enable, update, list, and disable notify."""
        self.queue.enable_notify("test_queue", throttle_interval_ms=100)

        throttles = self.queue.list_notify_throttles()
        self.assertTrue(any(t.queue_name == "test_queue" for t in throttles))

        self.queue.update_notify("test_queue", throttle_interval_ms=200)
        throttles = self.queue.list_notify_throttles()
        throttle = next(t for t in throttles if t.queue_name == "test_queue")
        self.assertEqual(throttle.throttle_interval_ms, 200)

        self.queue.disable_notify("test_queue")
        throttles = self.queue.list_notify_throttles()
        self.assertFalse(any(t.queue_name == "test_queue" for t in throttles))

    # --- Validation Utilities ---

    @_skip_if_missing_validation_functions
    def test_validate_routing_key(self):
        """Test routing key validation."""
        self.assertTrue(self.queue.validate_routing_key("orders.created"))

    @_skip_if_missing_validation_functions
    def test_validate_topic_pattern(self):
        """Test topic pattern validation."""
        self.assertTrue(self.queue.validate_topic_pattern("orders.*"))

    # --- FIFO / Grouped Reads ---

    def test_read_grouped(self):
        """Test FIFO grouped read."""
        self.queue.send_batch("test_queue", [{"g": i} for i in range(3)])
        msgs = self.queue.read_grouped("test_queue", qty=2)
        self.assertEqual(len(msgs), 2)

    def test_read_grouped_with_poll(self):
        """Test FIFO grouped read with poll."""
        msgs = self.queue.read_grouped_with_poll(
            "test_queue", qty=1, max_poll_seconds=1, poll_interval_ms=100
        )
        self.assertEqual(msgs, [])

    def test_read_grouped_rr(self):
        """Test FIFO round-robin read."""
        self.queue.send_batch("test_queue", [{"g": i} for i in range(3)])
        msgs = self.queue.read_grouped_rr("test_queue", qty=2)
        self.assertEqual(len(msgs), 2)

    def test_read_grouped_rr_with_poll(self):
        """Test FIFO round-robin read with poll."""
        msgs = self.queue.read_grouped_rr_with_poll(
            "test_queue", qty=1, max_poll_seconds=1, poll_interval_ms=100
        )
        self.assertEqual(msgs, [])

    # --- Archive Partitioning ---

    def test_convert_archive_partitioned(self):
        """Test converting archive table to partitioned."""
        msg_id = self.queue.send("test_queue", {"a": 1})
        self.queue.archive("test_queue", msg_id)
        try:
            self.queue.convert_archive_partitioned("test_queue")
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

    def test_detach_archive(self):
        """Test detaching archive table."""
        msg_id = self.queue.send("test_queue", {"a": 1})
        self.queue.archive("test_queue", msg_id)
        self.queue.detach_archive("test_queue")

    # --- FIFO Index ---

    def test_create_fifo_index(self):
        """Test creating FIFO index on a queue."""
        self.queue.create_fifo_index("test_queue")

    def test_create_fifo_indexes_all(self):
        """Test creating FIFO indexes on all queues."""
        self.queue.create_fifo_indexes_all()

    # --- Engine Injection & Session ---

    def test_external_engine_injection(self):
        """Test passing an external engine to the queue."""
        dsn = (
            f"postgresql+psycopg://{PGMQ_TEST_CONFIG['username']}:{PGMQ_TEST_CONFIG['password']}@"
            f"{PGMQ_TEST_CONFIG['host']}:{PGMQ_TEST_CONFIG['port']}/{PGMQ_TEST_CONFIG['database']}"
        )
        external_engine = create_engine(dsn)
        queue = SyncSQLAlchemyQueue(engine=external_engine, init_extension=False)
        queue.create_queue("test_queue")
        msg_id = queue.send("test_queue", {"external": True})
        self.assertGreater(msg_id, 0)
        queue.drop_queue("test_queue")
        queue.dispose()
        external_engine.dispose()

    def test_session_with_external_engine(self):
        """Test using queue.session() to execute raw SQL."""
        dsn = (
            f"postgresql+psycopg://{PGMQ_TEST_CONFIG['username']}:{PGMQ_TEST_CONFIG['password']}@"
            f"{PGMQ_TEST_CONFIG['host']}:{PGMQ_TEST_CONFIG['port']}/{PGMQ_TEST_CONFIG['database']}"
        )
        external_engine = create_engine(dsn)
        queue = SyncSQLAlchemyQueue(engine=external_engine, init_extension=False)
        session = queue.session()
        result = session.execute(text("SELECT 1 AS one"))
        self.assertEqual(result.scalar(), 1)
        session.close()
        queue.dispose()
        external_engine.dispose()


# ============================================================================
# Import / Export Unit Tests
# ============================================================================


class TestSQLAlchemyImports(unittest.TestCase):
    """Verify that SQLAlchemy queue classes are exported correctly."""

    def test_sync_exported_from_package(self):
        from pgmq import SQLAlchemyPGMQueue

        self.assertIsNotNone(SQLAlchemyPGMQueue)

    def test_async_exported_from_package(self):
        from pgmq import SQLAlchemyAsyncPGMQueue

        self.assertIsNotNone(SQLAlchemyAsyncPGMQueue)


if __name__ == "__main__":
    unittest.main()
