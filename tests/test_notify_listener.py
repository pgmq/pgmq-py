# tests/test_notify_listener.py
import unittest
import asyncio
import threading
import time
from psycopg.errors import UndefinedFunction

from pgmq import PGMQueue
from pgmq.async_queue import PGMQueue as AsyncPGMQueue
from pgmq.sqlalchemy_queue import PGMQueue as SQLAlchemyPGMQueue
from pgmq.sqlalchemy_async_queue import PGMQueue as AsyncSQLAlchemyPGMQueue
from pgmq.notify_listener import SyncNotificationListener, AsyncNotificationListener
from tests.utils import (
    PG_HOST,
    PG_PORT,
    PG_DATABASE,
    PG_USERNAME,
    PG_PASSWORD,
    DATABASE_URL,
)


# =============================================================================
# Sync Listener Tests — Raw psycopg Queue
# =============================================================================


class TestSyncNotificationListener(unittest.TestCase):
    """Test the synchronous notification listener with the raw psycopg queue."""

    def setUp(self):
        if DATABASE_URL:
            self.queue = PGMQueue(conn_string=DATABASE_URL, verbose=False)
        else:
            self.queue = PGMQueue(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DATABASE,
                username=PG_USERNAME,
                password=PG_PASSWORD,
                verbose=False,
            )
        self.queue_name = "test_sync_notify"
        try:
            self.queue.create_queue(self.queue_name)
            self.queue.enable_notify(self.queue_name, throttle_interval_ms=0)
        except UndefinedFunction as e:
            self.queue.pool.close()
            raise unittest.SkipTest(f"Notification functions not supported by DB: {e}")

    def tearDown(self):
        try:
            self.queue.drop_queue(self.queue_name)
        except Exception:  # noqa: S110
            pass
        self.queue.pool.close()

    def test_sync_listener_receives_notification(self):
        """Test that SyncNotificationListener catches INSERT events."""
        received_payloads = []
        event = threading.Event()

        def callback(payload):
            print("SYNC CALLBACK RECEIVED:", payload)
            received_payloads.append(payload)
            event.set()

        listener = SyncNotificationListener(self.queue)
        listener_thread = threading.Thread(
            target=listener.listen,
            args=(self.queue_name, callback),
            kwargs={"timeout": 5.0},
            daemon=True,
        )

        try:
            listener_thread.start()
            time.sleep(2.0)  # Give LISTEN time to register

            msg = {"data": "test_notify_sync"}
            self.queue.send(self.queue_name, msg)
            print("Message sent (sync test)")

            success = event.wait(timeout=10.0)

            self.assertTrue(success, "Notification not received within timeout")
            self.assertGreaterEqual(len(received_payloads), 1, "No payloads received")
            self.assertIn("event", received_payloads[0])
            self.assertEqual(received_payloads[0]["event"], "insert")

        finally:
            listener.stop()
            listener_thread.join(timeout=3.0)

    def test_sync_listener_timeout_exits_when_idle(self):
        """Test that listen() returns after timeout when no notifications arrive."""
        listener = SyncNotificationListener(self.queue)
        listener_thread = threading.Thread(
            target=listener.listen,
            args=(self.queue_name, lambda p: None),
            kwargs={"timeout": 1.0},
            daemon=True,
        )

        listener_thread.start()
        # Give LISTEN time to register, then wait for idle timeout
        time.sleep(2.5)

        # Thread should have finished on its own
        listener_thread.join(timeout=3.0)
        self.assertFalse(
            listener_thread.is_alive(), "Listener thread should exit after idle timeout"
        )

    def test_sync_listener_multiple_notifications(self):
        """Test that multiple messages produce multiple callbacks."""
        received_payloads = []
        event = threading.Event()
        expected_count = 3

        def callback(payload):
            received_payloads.append(payload)
            if len(received_payloads) >= expected_count:
                event.set()

        listener = SyncNotificationListener(self.queue)
        listener_thread = threading.Thread(
            target=listener.listen,
            args=(self.queue_name, callback),
            daemon=True,
        )

        try:
            listener_thread.start()
            time.sleep(2.0)

            for i in range(expected_count):
                self.queue.send(self.queue_name, {"seq": i})
                time.sleep(0.2)

            success = event.wait(timeout=10.0)
            self.assertTrue(success, f"Expected {expected_count} notifications")
            self.assertGreaterEqual(len(received_payloads), expected_count)

        finally:
            listener.stop()
            listener_thread.join(timeout=3.0)

    def test_sync_listener_stop_gracefully(self):
        """Test that stop() terminates the listener promptly."""
        listener = SyncNotificationListener(self.queue)
        listener_thread = threading.Thread(
            target=listener.listen,
            args=(self.queue_name, lambda p: None),
            daemon=True,
        )

        listener_thread.start()
        time.sleep(0.5)
        listener.stop()
        listener_thread.join(timeout=3.0)
        self.assertFalse(
            listener_thread.is_alive(), "Listener thread should exit after stop()"
        )


# =============================================================================
# Sync Listener Tests — SQLAlchemy Queue
# =============================================================================


class TestSyncNotificationListenerSQLAlchemy(unittest.TestCase):
    """Test SyncNotificationListener with the SQLAlchemy-backed sync queue."""

    def setUp(self):
        if DATABASE_URL:
            self.queue = SQLAlchemyPGMQueue(conn_string=DATABASE_URL, verbose=False)
        else:
            self.queue = SQLAlchemyPGMQueue(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DATABASE,
                username=PG_USERNAME,
                password=PG_PASSWORD,
                verbose=False,
            )
        self.queue_name = "test_sync_sqlalchemy_notify"
        try:
            self.queue.create_queue(self.queue_name)
            self.queue.enable_notify(self.queue_name, throttle_interval_ms=0)
        except Exception as e:
            self.queue.dispose()
            raise unittest.SkipTest(f"Notification functions not supported by DB: {e}")

    def tearDown(self):
        try:
            self.queue.drop_queue(self.queue_name)
        except Exception:  # noqa: S110
            pass
        self.queue.dispose()

    def test_sync_sqlalchemy_listener_receives_notification(self):
        """Test that SyncNotificationListener works with SQLAlchemyPGMQueue."""
        received_payloads = []
        event = threading.Event()

        def callback(payload):
            print("SYNC SQLALCHEMY CALLBACK RECEIVED:", payload)
            received_payloads.append(payload)
            event.set()

        listener = SyncNotificationListener(self.queue)
        listener_thread = threading.Thread(
            target=listener.listen,
            args=(self.queue_name, callback),
            kwargs={"timeout": 5.0},
            daemon=True,
        )

        try:
            listener_thread.start()
            time.sleep(2.0)

            self.queue.send(self.queue_name, {"data": "test_notify_sqlalchemy_sync"})
            print("Message sent (sync sqlalchemy test)")

            success = event.wait(timeout=10.0)

            self.assertTrue(success, "Notification not received within timeout")
            self.assertGreaterEqual(len(received_payloads), 1)
            self.assertIn("event", received_payloads[0])
            self.assertEqual(received_payloads[0]["event"], "insert")

        finally:
            listener.stop()
            listener_thread.join(timeout=3.0)


# =============================================================================
# Async Listener Tests — Raw asyncpg Queue
# =============================================================================


class TestAsyncNotificationListener(unittest.IsolatedAsyncioTestCase):
    """Test the asynchronous notification listener with the raw asyncpg queue."""

    async def asyncSetUp(self):
        if DATABASE_URL:
            self.queue = AsyncPGMQueue(conn_string=DATABASE_URL, verbose=False)
        else:
            self.queue = AsyncPGMQueue(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DATABASE,
                username=PG_USERNAME,
                password=PG_PASSWORD,
                verbose=False,
            )
        await self.queue.init()
        self.queue_name = "test_async_notify"

        try:
            await self.queue.create_queue(self.queue_name)
            await self.queue.enable_notify(self.queue_name, throttle_interval_ms=0)
            self.notifications_enabled = True
        except UndefinedFunction as e:
            await self.queue.close()
            raise unittest.SkipTest(f"Notification functions not supported by DB: {e}")

    async def asyncTearDown(self):
        if hasattr(self, "queue") and self.queue.pool:
            try:
                await self.queue.drop_queue(self.queue_name)
            except Exception:  # noqa: S110
                pass
            await self.queue.close()

    async def test_async_listener_receives_notification(self):
        """Test that AsyncNotificationListener catches INSERT events."""
        if not self.notifications_enabled:
            return

        received_payloads = []
        event = asyncio.Event()

        async def callback(payload):
            print("ASYNC CALLBACK RECEIVED:", payload)
            received_payloads.append(payload)
            event.set()

        listener = AsyncNotificationListener(self.queue)
        listen_task = asyncio.create_task(listener.listen(self.queue_name, callback))

        try:
            await asyncio.sleep(2.0)

            msg = {"data": "test_notify_async"}
            await self.queue.send(self.queue_name, msg)
            print("Message sent (async test)")

            await asyncio.wait_for(event.wait(), timeout=10.0)

            self.assertGreaterEqual(len(received_payloads), 1, "No payloads received")
            self.assertIn("event", received_payloads[0])
            self.assertEqual(received_payloads[0]["event"], "insert")

        finally:
            listener.stop()
            listen_task.cancel()
            try:
                await listen_task
            except asyncio.CancelledError:
                pass

    async def test_async_listener_multiple_notifications(self):
        """Test that multiple messages produce multiple async callbacks."""
        if not self.notifications_enabled:
            return

        received_payloads = []
        event = asyncio.Event()
        expected_count = 3

        async def callback(payload):
            received_payloads.append(payload)
            if len(received_payloads) >= expected_count:
                event.set()

        listener = AsyncNotificationListener(self.queue)
        listen_task = asyncio.create_task(listener.listen(self.queue_name, callback))

        try:
            await asyncio.sleep(2.0)

            for i in range(expected_count):
                await self.queue.send(self.queue_name, {"seq": i})
                await asyncio.sleep(0.2)

            await asyncio.wait_for(event.wait(), timeout=10.0)
            self.assertGreaterEqual(len(received_payloads), expected_count)

        finally:
            listener.stop()
            listen_task.cancel()
            try:
                await listen_task
            except asyncio.CancelledError:
                pass

    async def test_async_listener_stop_gracefully(self):
        """Test that stop() terminates the async listener promptly."""
        if not self.notifications_enabled:
            return

        async def callback(payload):
            pass

        listener = AsyncNotificationListener(self.queue)
        listen_task = asyncio.create_task(listener.listen(self.queue_name, callback))

        await asyncio.sleep(0.5)
        listener.stop()

        # Give the listener a short time to see the stop event
        for _ in range(30):
            if listen_task.done():
                break
            await asyncio.sleep(0.1)

        if not listen_task.done():
            listen_task.cancel()
            try:
                await listen_task
            except asyncio.CancelledError:
                pass

        self.assertTrue(
            listen_task.done(), "Async listen task should exit after stop()"
        )


# =============================================================================
# Async Listener Tests — SQLAlchemy Async Queue
# =============================================================================


class TestAsyncNotificationListenerSQLAlchemy(unittest.IsolatedAsyncioTestCase):
    """Test AsyncNotificationListener with the SQLAlchemy-backed async queue."""

    async def asyncSetUp(self):
        if DATABASE_URL:
            self.queue = AsyncSQLAlchemyPGMQueue(
                conn_string=DATABASE_URL, verbose=False
            )
        else:
            self.queue = AsyncSQLAlchemyPGMQueue(
                host=PG_HOST,
                port=PG_PORT,
                database=PG_DATABASE,
                username=PG_USERNAME,
                password=PG_PASSWORD,
                verbose=False,
            )
        await self.queue.init()
        self.queue_name = "test_async_sqlalchemy_notify"

        try:
            await self.queue.create_queue(self.queue_name)
            await self.queue.enable_notify(self.queue_name, throttle_interval_ms=0)
            self.notifications_enabled = True
        except Exception as e:
            await self.queue.close()
            raise unittest.SkipTest(f"Notification functions not supported by DB: {e}")

    async def asyncTearDown(self):
        if hasattr(self, "queue") and self.queue.engine:
            try:
                await self.queue.drop_queue(self.queue_name)
            except Exception:  # noqa: S110
                pass
            await self.queue.close()

    async def test_async_sqlalchemy_listener_receives_notification(self):
        """Test that AsyncNotificationListener works with AsyncSQLAlchemyPGMQueue."""
        if not self.notifications_enabled:
            return

        received_payloads = []
        event = asyncio.Event()

        async def callback(payload):
            print("ASYNC SQLALCHEMY CALLBACK RECEIVED:", payload)
            received_payloads.append(payload)
            event.set()

        listener = AsyncNotificationListener(self.queue)
        listen_task = asyncio.create_task(listener.listen(self.queue_name, callback))

        try:
            await asyncio.sleep(2.0)

            await self.queue.send(
                self.queue_name, {"data": "test_notify_sqlalchemy_async"}
            )
            print("Message sent (async sqlalchemy test)")

            await asyncio.wait_for(event.wait(), timeout=10.0)

            self.assertGreaterEqual(len(received_payloads), 1, "No payloads received")
            self.assertIn("event", received_payloads[0])
            self.assertEqual(received_payloads[0]["event"], "insert")

        finally:
            listener.stop()
            listen_task.cancel()
            try:
                await listen_task
            except asyncio.CancelledError:
                pass
