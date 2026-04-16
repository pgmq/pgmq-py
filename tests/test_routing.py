from psycopg.errors import UndefinedFunction
from .utils import PGMQTestCase


class TestTopicRouting(PGMQTestCase):
    """Test V2 Topic Routing features."""

    def test_bind_and_send_topic(self):
        q1 = self.get_queue_name("r1")
        q2 = self.get_queue_name("r2")

        try:
            self.queue.create_queue(q1)
            self.queue.create_queue(q2)

            self.queue.bind_topic("orders.*", q1)
            self.queue.bind_topic("orders.created", q2)

            # Send to 'orders.created'
            count = self.queue.send_topic("orders.created", {"id": 100})
            self.assertEqual(count, 2)

            msg1 = self.queue.read(q1)
            self.assertIsNotNone(msg1)
            self.assertEqual(msg1.message["id"], 100)

            msg2 = self.queue.read(q2)
            self.assertIsNotNone(msg2)
            self.assertEqual(msg2.message["id"], 100)
        except UndefinedFunction as e:
            self.skipTest(f"Topic routing not supported by this DB version: {e}")
        finally:
            # Cleanup
            try:
                self.queue.drop_queue(q1)
                self.queue.drop_queue(q2)
            except:  # noqa: E722
                pass

    def test_send_batch_topic(self):
        q1 = self.get_queue_name("br1")
        try:
            self.queue.create_queue(q1)
            self.queue.bind_topic("logs.*", q1)

            msgs = [{"lvl": "info"}, {"lvl": "err"}]
            results = self.queue.send_batch_topic("logs.info", msgs)

            self.assertEqual(len(results), 2)
            self.assertTrue(all(r.queue_name == q1 for r in results))
        except UndefinedFunction as e:
            self.skipTest(f"Topic routing not supported by this DB version: {e}")
        finally:
            try:
                self.queue.drop_queue(q1)
            except:  # noqa: E722
                pass

    def test_unbind_topic(self):
        q1 = self.get_queue_name("unb")
        try:
            self.queue.create_queue(q1)

            self.queue.bind_topic("test.unbind", q1)
            self.queue.unbind_topic("test.unbind", q1)

            count = self.queue.send_topic("test.unbind", {"x": 1})
            self.assertEqual(count, 0)
        except UndefinedFunction as e:
            self.skipTest(f"Topic routing not supported by this DB version: {e}")
        finally:
            try:
                self.queue.drop_queue(q1)
            except:  # noqa: E722
                pass

    def test_test_routing(self):
        q1 = self.get_queue_name("tr")
        try:
            self.queue.create_queue(q1)
            self.queue.bind_topic("route.test", q1)

            results = self.queue.test_routing("route.test")
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].queue_name, q1)
        except UndefinedFunction as e:
            self.skipTest(f"Topic routing not supported by this DB version: {e}")
        finally:
            try:
                self.queue.drop_queue(q1)
            except:  # noqa: E722
                pass
