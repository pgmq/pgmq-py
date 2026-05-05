# tests/utils.py
import unittest
import uuid
from pgmq import PGMQueue
from pgmq.base import PGMQConfig

# Use the library's own config class so DATABASE_URL and PG_* env vars
# are handled exactly like the production code.
_config = PGMQConfig()

PG_HOST = _config.host
PG_PORT = _config.port
PG_DATABASE = _config.database
PG_USERNAME = _config.username
PG_PASSWORD = _config.password
DATABASE_URL = _config.conn_string


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
        cls.queue.pool.close()

    def setUp(self):
        # Purge before each test to ensure clean state
        self.queue.purge(self.test_queue)

    def get_queue_name(self, prefix="test"):
        return f"{prefix}_{uuid.uuid4().hex[:8]}"
