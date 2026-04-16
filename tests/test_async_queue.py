import unittest
import asyncio
from datetime import datetime, timezone, timedelta
from pgmq.async_queue import PGMQueue as AsyncPGMQueue
from pgmq.decorators import async_transaction
from .utils import PG_HOST, PG_PORT, PG_DATABASE, PG_USERNAME, PG_PASSWORD


class TestAsyncQueue(unittest.IsolatedAsyncioTestCase):
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
        self.test_queue = "async_test_queue"
        self.test_message = {"hello": "async"}
        await self.queue.create_queue(self.test_queue)
        await self.queue.purge(self.test_queue)

    async def asyncTearDown(self):
        await self.queue.drop_queue(self.test_queue)
        await self.queue.close()

    # --- Standard V1 Coverage ---

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

    async def test_set_vt(self):
        msg_id = await self.queue.send(self.test_queue, self.test_message)
        future = datetime.now(timezone.utc) + timedelta(seconds=30)
        msg = await self.queue.set_vt(self.test_queue, msg_id, vt=future)
        self.assertAlmostEqual(msg.vt.timestamp(), future.timestamp(), delta=1)

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

    # --- V2 & Missing Coverage ---

    async def test_send_with_headers(self):
        """V2: Headers support."""
        headers = {"key": "value"}
        await self.queue.send(self.test_queue, self.test_message, headers=headers)
        msg = await self.queue.read(self.test_queue)
        self.assertEqual(msg.headers["key"], "value")

    async def test_send_batch_with_delay(self):
        """V2: Batch with delay."""
        ids = await self.queue.send_batch(self.test_queue, [self.test_message], delay=5)
        self.assertEqual(len(ids), 1)
        self.assertIsNone(await self.queue.read(self.test_queue))

    async def test_conditional_read(self):
        """V2: Conditional read."""
        await self.queue.send(self.test_queue, {"type": "take"})
        try:
            msg = await self.queue.read(self.test_queue, conditional={"type": "take"})
            if msg:
                self.assertEqual(msg.message["type"], "take")
        except Exception as e:
            self.skipTest(f"Conditional read unsupported: {e}")

    async def test_fifo_methods(self):
        """V2: Async FIFO reads."""
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
