from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
import unittest

try:
    from fastapi.testclient import TestClient
    from pgmq import Message, QueueMetrics, create_app
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False


class FakeQueue:
    def __init__(self) -> None:
        self._queues: Dict[str, List[Message]] = {}
        self._next_id = 1

    def list_queues(self) -> List[str]:
        return list(self._queues.keys())

    def _ensure_queue(self, queue: str) -> None:
        self._queues.setdefault(queue, [])

    def send(self, queue: str, message: Dict[str, Any], delay: int = 0, tz=None, conn=None) -> int:
        self._ensure_queue(queue)
        msg_id = self._next_id
        self._next_id += 1
        now = datetime.now(timezone.utc)
        self._queues[queue].append(
            Message(msg_id=msg_id, read_ct=0, enqueued_at=now, vt=now, message=message)
        )
        return msg_id

    def send_batch(
        self, queue: str, messages: List[Dict[str, Any]], delay: int = 0, tz=None, conn=None
    ) -> List[int]:
        return [self.send(queue, message, delay=delay, tz=tz, conn=conn) for message in messages]

    def read_batch(self, queue: str, vt: int | None = None, batch_size: int = 1, conn=None):
        self._ensure_queue(queue)
        messages = self._queues[queue][:batch_size]
        return messages

    def pop(self, queue: str, conn=None):
        self._ensure_queue(queue)
        if not self._queues[queue]:
            raise IndexError("empty queue")
        return self._queues[queue].pop(0)

    def delete(self, queue: str, msg_id: int, conn=None) -> bool:
        self._ensure_queue(queue)
        for idx, message in enumerate(self._queues[queue]):
            if message.msg_id == msg_id:
                del self._queues[queue][idx]
                return True
        return False

    def delete_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        deleted: List[int] = []
        for msg_id in msg_ids:
            if self.delete(queue, msg_id, conn=conn):
                deleted.append(msg_id)
        return deleted

    def archive(self, queue: str, msg_id: int, conn=None) -> bool:
        return self.delete(queue, msg_id, conn=conn)

    def archive_batch(self, queue: str, msg_ids: List[int], conn=None) -> List[int]:
        return self.delete_batch(queue, msg_ids, conn=conn)

    def metrics(self, queue: str, conn=None) -> QueueMetrics:
        self._ensure_queue(queue)
        now = datetime.now(timezone.utc)
        length = len(self._queues[queue])
        return QueueMetrics(
            queue_name=queue,
            queue_length=length,
            newest_msg_age_sec=0,
            oldest_msg_age_sec=0,
            total_messages=length,
            scrape_time=now,
        )

    def metrics_all(self, conn=None) -> List[QueueMetrics]:
        return [self.metrics(queue) for queue in self._queues]


def create_client():
    if not FASTAPI_AVAILABLE:
        return None
    fake_queue = FakeQueue()
    app = create_app(queue=fake_queue)
    return TestClient(app)


@unittest.skipIf(not FASTAPI_AVAILABLE, "FastAPI not available")
class TestAPI(unittest.TestCase):
    def test_send_and_read_messages(self):
        client = create_client()

        send_response = client.post("/queues/test/messages", json={"message": {"foo": "bar"}})
        self.assertEqual(send_response.status_code, 200)
        msg_id = send_response.json()["msg_id"]

        read_response = client.post("/queues/test/messages/read", json={"batch_size": 1})
        self.assertEqual(read_response.status_code, 200)
        messages = read_response.json()["messages"]
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["msg_id"], msg_id)
        self.assertEqual(messages[0]["message"], {"foo": "bar"})

    def test_batch_operations_and_metrics(self):
        client = create_client()

        batch_response = client.post(
            "/queues/test/messages/batch",
            json={"messages": [{"foo": 1}, {"bar": 2}]},
        )
        self.assertEqual(batch_response.status_code, 200)
        msg_ids = batch_response.json()["msg_ids"]
        self.assertEqual(len(msg_ids), 2)

        delete_response = client.delete(
            "/queues/test/messages",
            json={"msg_ids": [msg_ids[0]]},
        )
        self.assertEqual(delete_response.status_code, 200)
        self.assertEqual(delete_response.json()["deleted"], [msg_ids[0]])

        metrics_response = client.get("/queues/test/metrics")
        self.assertEqual(metrics_response.status_code, 200)
        metrics = metrics_response.json()["metrics"]
        self.assertEqual(metrics["queue_name"], "test")
        self.assertEqual(metrics["queue_length"], 1)

        all_metrics = client.get("/queues/metrics")
        self.assertEqual(all_metrics.status_code, 200)
        self.assertEqual(len(all_metrics.json()["metrics"]), 1)
