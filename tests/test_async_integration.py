# tests/test_async_queue.py
import unittest
import asyncio
import os
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, AsyncMock

from pgmq.async_queue import PGMQueue as AsyncPGMQueue
from pgmq.decorators import async_transaction
from tests.utils import (
    PG_HOST,
    PG_PORT,
    PG_DATABASE,
    PG_USERNAME,
    PG_PASSWORD,
    DATABASE_URL,
)


class TestAsyncQueue(unittest.IsolatedAsyncioTestCase):
    """Comprehensive Async Tests (Merged test_async_queue.py & test_async_integration.py)."""

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
        self.test_queue = "async_test_queue"
        self.test_message = {"hello": "async"}
        await self.queue.create_queue(self.test_queue)
        await self.queue.purge(self.test_queue)

    async def asyncTearDown(self):
        await self.queue.drop_queue(self.test_queue)
        await self.queue.close()

    # --- Standard & V1 Coverage ---

    async def test_send_and_read(self):
        msg_id = await self.queue.send(self.test_queue, self.test_message)
        msg = await self.queue.read(self.test_queue)
        self.assertEqual(msg.msg_id, msg_id)

    async def test_send_delay(self):
        msg_id = await self.queue.send(self.test_queue, self.test_message, delay=5)
        msg = await self.queue.read(self.test_queue)
        self.assertIsNone(msg)
        await asyncio.sleep(6)
        msg = await self.queue.read(self.test_queue)
        self.assertIsNotNone(msg)
        self.assertEqual(msg.msg_id, msg_id)

    async def test_send_datetime_delay(self):
        ts = datetime.now(timezone.utc) + timedelta(seconds=5)
        await self.queue.send(self.test_queue, self.test_message, tz=ts)
        msg = await self.queue.read(self.test_queue)
        self.assertIsNone(msg)
        await asyncio.sleep(6)
        msg = await self.queue.read(self.test_queue)
        self.assertIsNotNone(msg)

    async def test_batch_operations(self):
        ids = await self.queue.send_batch(
            self.test_queue, [self.test_message, self.test_message]
        )
        self.assertEqual(len(ids), 2)
        msgs = await self.queue.read_batch(self.test_queue, batch_size=2)
        self.assertEqual(len(msgs), 2)

    async def test_pop(self):
        msg_id = await self.queue.send(self.test_queue, self.test_message)
        msg = await self.queue.pop(self.test_queue)
        self.assertEqual(msg.msg_id, msg_id)

    async def test_archive_delete(self):
        msg_id = await self.queue.send(self.test_queue, self.test_message)
        await self.queue.archive(self.test_queue, msg_id)
        self.assertIsNone(await self.queue.read(self.test_queue))

        msg_id2 = await self.queue.send(self.test_queue, self.test_message)
        await self.queue.delete(self.test_queue, msg_id2)
        self.assertIsNone(await self.queue.read(self.test_queue))

    async def test_metrics(self):
        await self.queue.send(self.test_queue, self.test_message)
        stats = await self.queue.metrics(self.test_queue)
        self.assertEqual(stats.queue_length, 1)

    async def test_metrics_all(self):
        await self.queue.send(self.test_queue, self.test_message)
        all_stats = await self.queue.metrics_all()
        self.assertGreaterEqual(len(all_stats), 1)

    async def test_set_vt(self):
        msg_id = await self.queue.send(self.test_queue, self.test_message)
        future = datetime.now(timezone.utc) + timedelta(seconds=30)
        msg = await self.queue.set_vt(self.test_queue, msg_id, vt=future)
        self.assertAlmostEqual(msg.vt.timestamp(), future.timestamp(), delta=1)

    async def test_list_queues(self):
        await self.queue.create_queue("test_queue_2")
        queues = await self.queue.list_queues()
        queue_names = [q.queue_name for q in queues]
        self.assertIn(self.test_queue, queue_names)
        self.assertIn("test_queue_2", queue_names)
        await self.queue.drop_queue("test_queue_2")

    async def test_read_with_poll(self):
        await self.queue.send(self.test_queue, self.test_message)
        messages = await self.queue.read_with_poll(
            self.test_queue, vt=20, qty=1, max_poll_seconds=5, poll_interval_ms=100
        )
        self.assertEqual(len(messages), 1)

    async def test_purge_queue(self):
        messages = [self.test_message, self.test_message]
        await self.queue.send_batch(self.test_queue, messages)
        purged = await self.queue.purge(self.test_queue)
        self.assertEqual(purged, len(messages))

    async def test_validate_queue_name(self):
        valid_queue_name = "a" * 47
        invalid_queue_name = "a" * 49
        await self.queue.validate_queue_name(valid_queue_name)
        with self.assertRaises(Exception) as context:
            await self.queue.validate_queue_name(invalid_queue_name)
        self.assertIn("queue name is too long", str(context.exception))

    # --- Transaction Tests ---

    async def test_transaction_rollback(self):
        @async_transaction
        async def fail_txn(queue, conn=None):
            await queue.send(self.test_queue, self.test_message, conn=conn)
            raise Exception("Rollback")

        try:
            await fail_txn(self.queue)
        except:  # noqa: E722
            pass
        self.assertIsNone(await self.queue.read(self.test_queue))

    async def test_transaction_commit(self):
        @async_transaction
        async def txn_success(queue, conn=None):
            await queue.send(self.test_queue, self.test_message, conn=conn)

        await txn_success(self.queue)
        self.assertIsNotNone(await self.queue.read(self.test_queue))

    async def test_send_with_headers(self):
        """Headers support."""
        headers = {"key": "value"}
        await self.queue.send(self.test_queue, self.test_message, headers=headers)
        msg = await self.queue.read(self.test_queue)
        self.assertEqual(msg.headers["key"], "value")

    async def test_send_batch_with_delay(self):
        """Batch with delay."""
        ids = await self.queue.send_batch(self.test_queue, [self.test_message], delay=5)
        self.assertEqual(len(ids), 1)
        self.assertIsNone(await self.queue.read(self.test_queue))

    async def test_conditional_read(self):
        """Conditional read."""
        await self.queue.send(self.test_queue, {"type": "take"})
        try:
            msg = await self.queue.read(self.test_queue, conditional={"type": "take"})
            if msg:
                self.assertEqual(msg.message["type"], "take")
        except Exception as e:
            self.skipTest(f"Conditional read unsupported: {e}")

    async def test_fifo_methods(self):
        """Async FIFO reads."""
        await self.queue.create_fifo_index(self.test_queue)
        await self.queue.send(self.test_queue, {"g": 1}, headers={"gid": "A"})

        # Grouped
        msgs = await self.queue.read_grouped(self.test_queue, qty=1)
        self.assertIsInstance(msgs, list)

        # Grouped RR
        msgs_rr = await self.queue.read_grouped_rr(self.test_queue, qty=1)
        self.assertIsInstance(msgs_rr, list)

        # Grouped Poll
        msgs_poll = await self.queue.read_grouped_with_poll(
            self.test_queue, qty=1, max_poll_seconds=1
        )
        self.assertIsInstance(msgs_poll, list)

    async def test_external_pool(self):
        """Test passing an external asyncpg pool to the queue."""
        import asyncpg

        dsn = (
            f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@"
            f"{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
        )
        pool = await asyncpg.create_pool(dsn)
        try:
            queue = AsyncPGMQueue(pool=pool, init_extension=False)
            await queue.init()
            self.assertIs(queue.pool, pool)
            self.assertFalse(queue._own_pool)

            test_queue = f"async_extpool_{uuid.uuid4().hex[:8]}"
            await queue.create_queue(test_queue)
            msg_id = await queue.send(test_queue, {"test": "external_pool"})
            self.assertGreater(msg_id, 0)
            await queue.drop_queue(test_queue)
        finally:
            await pool.close()

    async def test_external_pool_not_closed_by_queue(self):
        """Queue.close() should not close a user-provided pool."""
        import asyncpg

        dsn = (
            f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@"
            f"{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
        )
        pool = await asyncpg.create_pool(dsn)
        queue = AsyncPGMQueue(pool=pool, init_extension=False)
        await queue.init()
        await queue.close()

        async with pool.acquire() as conn:
            result = await conn.fetchval("SELECT 1")
            self.assertEqual(result, 1)

        await pool.close()


class TestAsyncInitNoExtension(unittest.IsolatedAsyncioTestCase):
    async def test_no_extension_async(self):
        """Ensure extension is not created when flag is False."""
        # Use new_callable=AsyncMock because create_pool is a coroutine function
        with patch("asyncpg.create_pool", new_callable=AsyncMock) as mock_create_pool:
            # Configure the return value of the async create_pool function
            # The return value is the pool object, which we also mock as AsyncMock
            # so methods like pool.acquire() and pool.close() work as async context managers/coroutines
            mock_pool = AsyncMock()
            mock_create_pool.return_value = mock_pool

            # Mock the connection context manager returned by pool.acquire()
            mock_conn = AsyncMock()
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            if DATABASE_URL:
                q = AsyncPGMQueue(conn_string=DATABASE_URL, init_extension=False)
            else:
                q = AsyncPGMQueue(
                    host=PG_HOST,
                    port=PG_PORT,
                    database=PG_DATABASE,
                    username=PG_USERNAME,
                    password=PG_PASSWORD,
                    init_extension=False,
                )

            await q.init()

            # Verify config
            self.assertFalse(q.config.init_extension)

            # Verify pool creation was awaited
            mock_create_pool.assert_awaited_once()

            # Verify that 'CREATE EXTENSION' was NOT executed
            # Since init_extension is False, the execute call should never happen
            mock_conn.execute.assert_not_awaited()

            # Cleanup
            await q.close()


class TestAsyncPGMQueueWithEnv(unittest.IsolatedAsyncioTestCase):
    """Test initialization using environment variables."""

    async def asyncSetUp(self):
        self.queue = AsyncPGMQueue()
        await self.queue.init()
        self.test_queue = "env_async_test_queue"
        self.test_message = {"hello": "world"}
        await self.queue.create_queue(self.test_queue)

    async def asyncTearDown(self):
        await self.queue.drop_queue(self.test_queue)
        await self.queue.close()

    async def test_env_connection(self):
        """Verify connection works with env vars."""
        await self.queue.send(self.test_queue, {"env": "test"})
        msg = await self.queue.read(self.test_queue)
        self.assertIsNotNone(msg)


class TestAsyncDatabaseURL(unittest.IsolatedAsyncioTestCase):
    """Test connection string parsing (DATABASE_URL) for Async."""

    async def test_conn_string_uri(self):
        """Test explicit conn_string argument (URI format)."""
        dsn = f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

        q = AsyncPGMQueue(conn_string=dsn, verbose=False)
        await q.init()
        try:
            await q.create_queue("async_conn_str_test")
            await q.drop_queue("async_conn_str_test")
            self.assertEqual(q.config.host, PG_HOST)
        finally:
            await q.close()

    async def test_env_database_url(self):
        """Test DATABASE_URL environment variable fallback."""
        dsn = f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

        original = os.environ.get("DATABASE_URL")
        os.environ["DATABASE_URL"] = dsn

        try:
            q = AsyncPGMQueue(verbose=False)
            await q.init()
            await q.create_queue("async_env_db_url_test")
            await q.drop_queue("async_env_db_url_test")
            self.assertEqual(q.config.username, PG_USERNAME)
            await q.close()
        finally:
            if original:
                os.environ["DATABASE_URL"] = original
            else:
                del os.environ["DATABASE_URL"]


if __name__ == "__main__":
    unittest.main()
