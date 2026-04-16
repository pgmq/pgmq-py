import unittest
import asyncio
import threading
import time
from psycopg.errors import UndefinedFunction

from pgmq import PGMQueue
from pgmq.async_queue import PGMQueue as AsyncPGMQueue
from pgmq.notify_listener import SyncNotificationListener, AsyncNotificationListener
from .utils import PG_HOST, PG_PORT, PG_DATABASE, PG_USERNAME, PG_PASSWORD


class TestSyncNotificationListener(unittest.TestCase):
    """Test the synchronous notification listener."""

    def test_sync_listener_receives_notification(self):
        """Test that SyncNotificationListener catches INSERT events."""
        queue_name = "test_sync_notify"
        queue = PGMQueue(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            username=PG_USERNAME,
            password=PG_PASSWORD,
            verbose=False,
        )

        try:
            queue.create_queue(queue_name)
            queue.enable_notify(queue_name, throttle_interval_ms=0)
        except UndefinedFunction as e:
            queue.pool.close()
            self.skipTest(f"Notification functions not supported by DB: {e}")

        received_payloads = []
        event = threading.Event()

        def callback(payload):
            print("SYNC CALLBACK RECEIVED:", payload)
            received_payloads.append(payload)
            event.set()

        listener = SyncNotificationListener(queue)
        listener_thread = threading.Thread(
            target=listener.listen,
            args=(queue_name, callback),
            kwargs={"timeout": 5.0},
            daemon=True,
        )

        try:
            listener_thread.start()
            time.sleep(2.0)  # Give LISTEN time to register

            msg = {"data": "test_notify_sync"}
            queue.send(queue_name, msg)
            print("Message sent (sync test)")

            success = event.wait(timeout=10.0)

            self.assertTrue(success, "Notification not received within timeout")
            self.assertGreaterEqual(len(received_payloads), 1, "No payloads received")
            # We no longer assert on ["data"] because payload is empty in PGMQ
            # Optional: check for our fallback dict
            self.assertIn("event", received_payloads[0])
            self.assertEqual(received_payloads[0]["event"], "insert")

        finally:
            listener.stop()
            listener_thread.join(timeout=3.0)
            queue.drop_queue(queue_name)
            queue.pool.close()


class TestAsyncNotificationListener(unittest.IsolatedAsyncioTestCase):
    """Test the asynchronous notification listener."""

    async def asyncSetUp(self):
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
            self.notifications_enabled = False
            await self.queue.close()
            self.skipTest(f"Notification functions not supported by DB: {e}")

    async def asyncTearDown(self):
        if hasattr(self, "queue") and self.queue.pool:
            try:
                await self.queue.drop_queue(self.queue_name)
            except:  # noqa: E722
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
            await asyncio.sleep(2.0)  # Wait for listener to start

            msg = {"data": "test_notify_async"}
            await self.queue.send(self.queue_name, msg)
            print("Message sent (async test)")

            await asyncio.wait_for(event.wait(), timeout=10.0)

            self.assertGreaterEqual(len(received_payloads), 1, "No payloads received")
            # No ["data"] key expected
            self.assertIn("event", received_payloads[0])
            self.assertEqual(received_payloads[0]["event"], "insert")

        finally:
            listener.stop()
            listen_task.cancel()
            try:
                await listen_task
            except asyncio.CancelledError:
                pass
