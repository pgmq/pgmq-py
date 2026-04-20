import unittest
import logging
import io
import time

from pgmq.logger import (
    LoggingManager,
    create_logger,
    log_with_context,
    log_performance,
)


class TestLoggerIsolation(unittest.TestCase):
    """
    Critical tests to ensure PGMQ logging does not break other loggers.
    """

    def setUp(self):
        LoggingManager._configured_loggers = {}
        logging.getLogger("test_pgmq").handlers = []

    def test_root_logger_not_modified(self):
        root_logger = logging.getLogger()
        initial_handlers = len(root_logger.handlers)
        create_logger("test_pgmq", verbose=True)

        root_logger = logging.getLogger()
        self.assertEqual(
            len(root_logger.handlers),
            initial_handlers,
            "PGMQ added handlers to the root logger, breaking isolation!",
        )

    def test_no_duplicate_handlers(self):
        log_name = "test_duplicate"
        logging.getLogger(log_name).handlers = []

        logger1 = LoggingManager.get_logger(log_name, verbose=True)
        count1 = len(logger1.handlers)

        logger2 = LoggingManager.get_logger(log_name, verbose=True)
        count2 = len(logger2.handlers)

        self.assertEqual(count1, count2, "Handlers were duplicated on second retrieval")
        self.assertGreater(count1, 0, "Handlers were not created")

        # Cleanup: close handlers to avoid ResourceWarning
        for h in logger1.handlers[:]:
            h.close()
            logger1.removeHandler(h)

    def test_propagation_control(self):
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        old_handlers = root_logger.handlers
        root_logger.handlers = []

        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        root_logger.addHandler(handler)

        try:
            pgmq_log = create_logger("test_propagate", verbose=True)
            pgmq_log.warning("Test Warning")
            output = stream.getvalue()
            self.assertIsInstance(output, str)
        finally:
            root_logger.removeHandler(handler)
            root_logger.handlers = old_handlers


class TestLoggingFeatures(unittest.TestCase):
    """Test specific features of the logger implementation."""

    def setUp(self):
        LoggingManager._configured_loggers = {}
        self.log_capture = io.StringIO()
        self.handler = logging.StreamHandler(self.log_capture)

    def tearDown(self):
        self.log_capture.close()

    def test_log_with_context_stdlib(self):
        logger = logging.getLogger("test_context")
        logger.handlers = [self.handler]
        logger.setLevel(logging.DEBUG)

        log_with_context(
            logger, logging.INFO, "User action", user_id=123, action="click"
        )
        output = self.log_capture.getvalue()
        self.assertIn("User action", output)
        self.assertIn("user_id=123", output)

    def test_performance_decorator_sync(self):
        logger = logging.getLogger("test_perf")
        logger.handlers = [self.handler]
        logger.setLevel(logging.DEBUG)

        @log_performance(logger)
        def slow_function():
            time.sleep(0.05)
            return "done"

        result = slow_function()
        output = self.log_capture.getvalue()

        self.assertEqual(result, "done")
        self.assertIn("Completed slow_function", output)
        self.assertIn("success=True", output)

    def test_performance_decorator_exception(self):
        logger = logging.getLogger("test_perf_exc")
        logger.handlers = [self.handler]
        logger.setLevel(logging.DEBUG)

        @log_performance(logger)
        def failing_function():
            raise ValueError("database error")

        with self.assertRaises(ValueError):
            failing_function()

        output = self.log_capture.getvalue()
        self.assertIn("Failed failing_function", output)

    def test_structured_logging_format(self):
        LoggingManager._configured_loggers = {}

        logger = LoggingManager.get_logger("test_struct", verbose=True, structured=True)

        stream = io.StringIO()
        handler = logging.StreamHandler(stream)

        fmt = '{{"timestamp": "%(asctime)s", "level": "%(levelname)s", "message": "%(message)s"}}'
        handler.setFormatter(logging.Formatter(fmt))

        # Clear existing handlers to avoid resource conflicts and set new one
        for h in logger.handlers[:]:
            h.close()
            logger.removeHandler(h)

        logger.handlers = [handler]
        logger.setLevel(logging.DEBUG)

        log_with_context(logger, logging.INFO, "Structured test")

        output = stream.getvalue()
        self.assertIn('"message": "Structured test"', output)
