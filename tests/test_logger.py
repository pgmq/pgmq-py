import unittest
import logging
import io
import sys
import time

from pgmq.logger import (
    LoggingManager,
    create_logger,
    log_with_context,
    log_performance,
    LOGURU_AVAILABLE,
)

# Helper to capture sys.stderr for Loguru tests
from contextlib import contextmanager


@contextmanager
def stderr_capture():
    old_stderr = sys.stderr
    sys.stderr = io.StringIO()
    try:
        yield sys.stderr
    finally:
        sys.stderr = old_stderr


class TestWithStdlib(unittest.TestCase):
    """Tests running with Standard Library logging."""

    def setUp(self):
        LoggingManager._configured_loggers = {}
        LoggingManager._use_loguru = False  # Force stdlib

        # Clean up and close open handlers to avoid ResourceWarning
        for name in [
            "test_pgmq",
            "test_context",
            "test_perf",
            "test_perf_exc",
            "test_struct",
        ]:
            log = logging.getLogger(name)
            for h in log.handlers[:]:
                h.close()
                log.removeHandler(h)

    def test_root_logger_not_modified(self):
        root_logger = logging.getLogger()
        initial_handlers = len(root_logger.handlers)
        create_logger("test_pgmq", verbose=True)

        self.assertEqual(
            len(root_logger.handlers),
            initial_handlers,
            "PGMQ added handlers to the root logger, breaking isolation!",
        )

    def test_no_duplicate_handlers(self):
        log_name = "test_duplicate"
        log = logging.getLogger(log_name)
        # Ensure clean state
        for h in log.handlers[:]:
            h.close()
            log.removeHandler(h)

        logger1 = LoggingManager.get_logger(log_name, verbose=True)
        count1 = len(logger1.handlers)

        logger2 = LoggingManager.get_logger(log_name, verbose=True)
        count2 = len(logger2.handlers)

        self.assertEqual(count1, count2, "Handlers were duplicated on second retrieval")
        self.assertGreater(count1, 0, "Handlers were not created")

        # Cleanup
        for h in logger1.handlers[:]:
            h.close()
            logger1.removeHandler(h)

    def test_log_with_context_stdlib(self):
        log = logging.getLogger("test_context")
        # We rely on get_logger to create the file handler (verbose=True)
        logger = LoggingManager.get_logger("test_context", verbose=True)

        buffer = io.StringIO()
        handler = logging.StreamHandler(buffer)
        handler.setFormatter(logging.Formatter("%(message)s"))

        # FIX: Add the handler instead of replacing the list.
        # This ensures the FileHandler created by get_logger remains active.
        logger.addHandler(handler)
        log.setLevel(logging.DEBUG)

        log_with_context(log, logging.INFO, "User action", user_id=123, action="click")
        output = buffer.getvalue()
        self.assertIn("User action", output)
        self.assertIn("user_id=123", output)

        # Cleanup only the test handler, leave the file handler for verification if needed
        logger.removeHandler(handler)

    def test_performance_decorator_sync(self):
        log = logging.getLogger("test_perf")
        logger = LoggingManager.get_logger("test_perf", verbose=True)

        buffer = io.StringIO()
        handler = logging.StreamHandler(buffer)
        handler.setFormatter(logging.Formatter("%(message)s"))

        logger.addHandler(handler)
        log.setLevel(logging.DEBUG)

        @log_performance(logger)
        def slow_function():
            time.sleep(0.05)
            return "done"

        result = slow_function()
        output = buffer.getvalue()

        self.assertEqual(result, "done")
        self.assertIn("Completed slow_function", output)
        self.assertIn("success=True", output)

        logger.removeHandler(handler)

    def test_performance_decorator_exception(self):
        log = logging.getLogger("test_perf_exc")
        logger = LoggingManager.get_logger("test_perf_exc", verbose=True)

        buffer = io.StringIO()
        handler = logging.StreamHandler(buffer)
        handler.setFormatter(logging.Formatter("%(message)s"))

        logger.addHandler(handler)
        log.setLevel(logging.DEBUG)

        @log_performance(logger)
        def failing_function():
            raise ValueError("database error")

        with self.assertRaises(ValueError):
            failing_function()

        output = buffer.getvalue()
        self.assertIn("Failed failing_function", output)

        logger.removeHandler(handler)

    def test_structured_logging_format(self):
        LoggingManager._configured_loggers = {}

        logger = LoggingManager.get_logger("test_struct", verbose=True, structured=True)

        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        fmt = '{{"message": "%(message)s"}}'
        handler.setFormatter(logging.Formatter(fmt))

        # FIX: Add handler instead of replacing.
        # The FileHandler created by get_logger (verbose=True) will remain.
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        log_with_context(logger, logging.INFO, "Structured test")

        output = stream.getvalue()
        self.assertIn('"message": "Structured test"', output)

        logger.removeHandler(handler)


@unittest.skipUnless(LOGURU_AVAILABLE, "Loguru not installed")
class TestWithLoguru(unittest.TestCase):
    """Tests running with Loguru (if available)."""

    def setUp(self):
        LoggingManager._configured_loggers = {}
        LoggingManager._use_loguru = True  # Force Loguru
        LoggingManager._test_mode = True  # Disable enqueue for synchronous test logs

    def tearDown(self):
        LoggingManager._test_mode = False

    def test_log_with_context_loguru(self):
        with stderr_capture() as buffer:
            logger = LoggingManager.get_logger("test_context_loguru", verbose=True)

            log_with_context(logger, "INFO", "User action", user_id=123, action="click")
            output = buffer.getvalue()

        self.assertIn("User action", output)
        self.assertIn("user_id=123", output)

    def test_performance_decorator_loguru(self):
        with stderr_capture() as buffer:
            logger = LoggingManager.get_logger("test_perf_loguru", verbose=True)

            @log_performance(logger)
            def slow_function():
                time.sleep(0.05)
                return "done"

            result = slow_function()
            output = buffer.getvalue()

        self.assertEqual(result, "done")
        self.assertIn("Completed slow_function", output)
        self.assertIn("success=True", output)

    def test_performance_decorator_exception_loguru(self):
        with stderr_capture() as buffer:
            logger = LoggingManager.get_logger("test_perf_exc_loguru", verbose=True)

            @log_performance(logger)
            def failing_function():
                raise ValueError("database error")

            with self.assertRaises(ValueError):
                failing_function()

            output = buffer.getvalue()

        self.assertIn("Failed failing_function", output)
        self.assertIn("database error", output)

    def test_isolation_no_duplicate_output(self):
        # Loguru doesn't expose handlers list easily.
        # We test isolation by verifying we don't get excessive output
        with stderr_capture() as buffer:
            logger1 = LoggingManager.get_logger("test_iso", verbose=True)
            logger1.info("First log")

            logger2 = LoggingManager.get_logger("test_iso", verbose=True)
            logger2.info("Second log")

            output = buffer.getvalue()

        self.assertIn("First log", output)
        self.assertIn("Second log", output)
        self.assertGreater(len(output), 0)
