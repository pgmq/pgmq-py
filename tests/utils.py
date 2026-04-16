import os
import unittest
import uuid
from pgmq import PGMQueue

# Configuration matches your docker-compose setup
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "postgres")
PG_USERNAME = os.getenv("PG_USERNAME", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")


class PGMQTestCase(unittest.TestCase):
    """Base class for synchronous tests."""

    @classmethod
    def setUpClass(cls):
        cls.queue = PGMQueue(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DATABASE,
            username=PG_USERNAME,
            password=PG_PASSWORD,
            verbose=False,  # Keep test output clean
        )
        cls.test_queue = f"test_queue_{uuid.uuid4().hex[:8]}"
        cls.test_message = {"hello": "world"}
        cls.queue.create_queue(cls.test_queue)

    @classmethod
    def tearDownClass(cls):
        # Attempt to clean up the test queue
        try:
            cls.queue.drop_queue(cls.test_queue)
        except:  # noqa: E722
            pass
        if cls.queue.pool:
            cls.queue.pool.close()

    def setUp(self):
        # Purge before each test to ensure clean state
        self.queue.purge(self.test_queue)

    def get_queue_name(self, prefix="test"):
        return f"{prefix}_{uuid.uuid4().hex[:8]}"
