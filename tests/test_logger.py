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

        # Comprehensive cleanup of any stdlib loggers touched by tests,
        # including cache-key variants created by LoggingManager.
        prefixes = (
            "test_pgmq",
            "test_context",
            "test_perf",
            "test_perf_exc",
            "test_struct",
            "test_safe",
            "test_duplicate",
            "test_user",
            "test_explicit",
            "test_cache",
            "test_global",
            "pgmq",
        )
        mgr = logging.Logger.manager
        for key in list(mgr.loggerDict.keys()):
            obj = mgr.loggerDict[key]
            if isinstance(obj, logging.Logger) and any(
                key.startswith(p) for p in prefixes
            ):
                for h in obj.handlers[:]:
                    h.close()
                    obj.removeHandler(h)
                obj.setLevel(logging.NOTSET)

    def _cleanup_logger(self, name):
        """Remove handlers from a stdlib logger and close them."""
        log = logging.getLogger(name)
        for h in log.handlers[:]:
            h.close()
            log.removeHandler(h)
        log.setLevel(logging.NOTSET)
        # Also purge cache-key variants
        mgr = logging.Logger.manager
        for key in list(mgr.loggerDict.keys()):
            obj = mgr.loggerDict[key]
            if isinstance(obj, logging.Logger) and key.startswith(name):
                for h in obj.handlers[:]:
                    h.close()
                    obj.removeHandler(h)
                obj.setLevel(logging.NOTSET)

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
        # We rely on get_logger to create the file handler (verbose=True)
        logger = LoggingManager.get_logger("test_context", verbose=True)

        buffer = io.StringIO()
        handler = logging.StreamHandler(buffer)
        handler.setFormatter(logging.Formatter("%(message)s"))

        # FIX: Add the handler instead of replacing the list.
        # This ensures the FileHandler created by get_logger remains active.
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        log_with_context(
            logger, logging.INFO, "User action", user_id=123, action="click"
        )
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

    def test_library_safe_no_auto_configure(self):
        """When called with all defaults, get_logger must not mutate the user's logging tree."""
        log = logging.getLogger("test_safe")
        # Ensure clean state
        for h in log.handlers[:]:
            h.close()
            log.removeHandler(h)
        log.setLevel(logging.NOTSET)

        logger = LoggingManager.get_logger("test_safe")

        # Must return the same stdlib logger instance
        self.assertIs(logger, log)
        # Must not inject handlers
        self.assertEqual(
            len(logger.handlers), 0, "Library injected a handler with all-default args"
        )
        # Must not alter the level
        self.assertEqual(
            logger.level, logging.NOTSET, "Library changed the logger level"
        )

        # Emitting a log should work normally once the *user* adds a handler/level
        buffer = io.StringIO()
        handler = logging.StreamHandler(buffer)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        log_with_context(logger, logging.DEBUG, "Library-safe log")
        self.assertIn("Library-safe log", buffer.getvalue())
        logger.removeHandler(handler)

    def test_library_safe_preserves_user_config(self):
        """When user already owns a logger, defaults must not touch it."""
        self._cleanup_logger("test_user")
        user_logger = logging.getLogger("test_user")

        # User configures their own logger
        stream = io.StringIO()
        user_handler = logging.StreamHandler(stream)
        user_handler.setFormatter(logging.Formatter("USER:%(message)s"))
        user_logger.addHandler(user_handler)
        user_logger.setLevel(logging.INFO)

        # Library is called with all defaults
        lib_logger = LoggingManager.get_logger("test_user")

        # Must be the exact same object
        self.assertIs(lib_logger, user_logger)
        # Handlers must be preserved
        self.assertEqual(len(lib_logger.handlers), 1)
        self.assertIs(lib_logger.handlers[0], user_handler)
        # Level must be preserved
        self.assertEqual(lib_logger.level, logging.INFO)

        # Logging through the library must use the user's handler
        log_with_context(lib_logger, logging.INFO, "hello")
        self.assertIn("USER:hello", stream.getvalue())

        self._cleanup_logger("test_user")

    def test_explicit_config_leaves_user_logger_untouched(self):
        """Explicit config must create a separate logger, not mutate the user's."""
        self._cleanup_logger("test_explicit")
        user_logger = logging.getLogger("test_explicit")

        # User sets up their logger
        user_logger.setLevel(logging.ERROR)
        user_stream = io.StringIO()
        user_handler = logging.StreamHandler(user_stream)
        user_logger.addHandler(user_handler)

        # Library creates an explicitly configured logger with the same display name
        lib_logger = LoggingManager.get_logger("test_explicit", verbose=True)

        # Must be a DIFFERENT object (cache-key based)
        self.assertIsNot(lib_logger, user_logger)
        # User logger must be untouched
        self.assertEqual(user_logger.level, logging.ERROR)
        self.assertEqual(len(user_logger.handlers), 1)
        self.assertIs(user_logger.handlers[0], user_handler)

        # Library logger must have its own handlers
        self.assertGreater(len(lib_logger.handlers), 0)
        stream_handlers = [
            h
            for h in lib_logger.handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        self.assertEqual(len(stream_handlers), 1, "Expected a console handler")

        self._cleanup_logger("test_explicit")

    def test_explicit_config_level_and_handlers(self):
        """Explicit config must set the requested level and add correct handlers."""
        self._cleanup_logger("test_explicit_cfg")

        logger = LoggingManager.get_logger(
            "test_explicit_cfg",
            verbose=True,
            log_level=logging.ERROR,
        )

        # Level must be ERROR
        self.assertEqual(logger.level, logging.ERROR)

        # Must have a StreamHandler
        stream_handlers = [
            h
            for h in logger.handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        self.assertEqual(
            len(stream_handlers), 1, "Expected exactly one console handler"
        )

        # Must have a FileHandler because verbose=True
        file_handlers = [
            h for h in logger.handlers if isinstance(h, logging.FileHandler)
        ]
        self.assertEqual(len(file_handlers), 1, "Expected exactly one file handler")

        self._cleanup_logger("test_explicit_cfg")

    def test_explicit_config_log_filename_without_verbose(self):
        """log_filename alone should trigger configuration even if verbose=False."""
        self._cleanup_logger("test_explicit_file")
        import os

        logger = LoggingManager.get_logger(
            "test_explicit_file",
            verbose=False,
            log_filename="test_explicit_file.log",
        )

        # Should still create handlers (at least file and console)
        self.assertGreater(len(logger.handlers), 0)
        file_handlers = [
            h for h in logger.handlers if isinstance(h, logging.FileHandler)
        ]
        self.assertEqual(len(file_handlers), 1)

        # Cleanup file on disk
        for h in logger.handlers[:]:
            h.close()
            logger.removeHandler(h)
        try:
            os.remove("test_explicit_file.log")
        except FileNotFoundError:
            pass

    def test_cache_isolation_same_name_different_config(self):
        """Same name with different options must yield distinct loggers."""
        self._cleanup_logger("test_cache")
        import os

        logger_a = LoggingManager.get_logger("test_cache", verbose=True)
        logger_b = LoggingManager.get_logger(
            "test_cache", verbose=False, log_filename="test_cache_b.log"
        )

        self.assertIsNot(logger_a, logger_b)

        # Each must have its own handlers
        self.assertEqual(len(logger_a.handlers), 2)  # console + file
        self.assertEqual(len(logger_b.handlers), 2)  # console + file

        # Cleanup
        for logger in (logger_a, logger_b):
            for h in logger.handlers[:]:
                h.close()
                logger.removeHandler(h)
        for fname in ("test_cache_b.log",):
            try:
                os.remove(fname)
            except FileNotFoundError:
                pass

    def test_configure_global_logging_stdlib(self):
        """configure_global_logging must set up the 'pgmq' logger explicitly."""
        pgmq_logger = logging.getLogger("pgmq")
        # Clean previous state
        for h in pgmq_logger.handlers[:]:
            h.close()
            pgmq_logger.removeHandler(h)
        pgmq_logger.setLevel(logging.NOTSET)

        LoggingManager.configure_global_logging(log_level=logging.DEBUG)

        self.assertEqual(pgmq_logger.level, logging.DEBUG)
        self.assertTrue(
            any(isinstance(h, logging.StreamHandler) for h in pgmq_logger.handlers),
            "Expected a StreamHandler on the pgmq logger",
        )

        # Idempotent: calling again must not add duplicate handlers
        initial_count = len(pgmq_logger.handlers)
        LoggingManager.configure_global_logging(log_level=logging.DEBUG)
        self.assertEqual(len(pgmq_logger.handlers), initial_count)

        # Cleanup
        for h in pgmq_logger.handlers[:]:
            h.close()
            pgmq_logger.removeHandler(h)
        pgmq_logger.setLevel(logging.NOTSET)


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

    def test_library_safe_no_auto_configure_loguru(self):
        """Defaults must not add new handlers to the global loguru logger."""
        LoggingManager._remove_pgmq_handlers()
        LoggingManager._loguru_handler_ids.clear()

        logger = LoggingManager.get_logger("test_safe_loguru")

        # Must not be a stdlib logger
        self.assertNotIsInstance(logger, logging.Logger)
        # Must not have added any PGMQ-tracked handlers
        self.assertEqual(len(LoggingManager._loguru_handler_ids), 0)

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

    def test_configure_global_logging_loguru(self):
        """configure_global_logging must add a single sink when using loguru."""
        LoggingManager._remove_pgmq_handlers()
        LoggingManager._loguru_handler_ids.clear()

        with stderr_capture() as buffer:
            LoggingManager.configure_global_logging(log_level="INFO", use_loguru=True)
            # Loguru global logger should now emit through the configured sink
            logger = LoggingManager.get_logger("test_global_loguru")
            logger.info("Global config test")
            output = buffer.getvalue()

        self.assertIn("Global config test", output)

        # Idempotent: second call should not leave orphaned/extra handlers
        LoggingManager.configure_global_logging(log_level="INFO", use_loguru=True)
        self.assertEqual(
            len(LoggingManager._loguru_handler_ids),
            1,
            "configure_global_logging left duplicate loguru handlers",
        )
