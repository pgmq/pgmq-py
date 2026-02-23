# src/pgmq/logger.py

import logging
import logging.handlers
import os
import sys
import functools
import asyncio
import time
from datetime import datetime
from typing import Optional, Dict, Any, Union, Set

# Attempt to import loguru; fall back to standard logging if unavailable
try:
    from loguru import logger as loguru_logger

    LOGURU_AVAILABLE = True
except ImportError:
    LOGURU_AVAILABLE = False


class PGMQLogger:
    """
    Centralized logging manager for PGMQueue with dual backend support.

    Provides a unified interface for both standard library logging and loguru,
    with automatic backend detection and backward compatibility with existing
    PGMQueue implementations.
    """

    _loggers: Dict[str, Union[logging.Logger, Any]] = {}
    _configured: bool = False
    _use_loguru: bool = LOGURU_AVAILABLE

    _handler_ids: Set[int] = set()

    @classmethod
    def get_logger(
        cls,
        name: str,
        verbose: bool = False,
        log_filename: Optional[str] = None,
        log_format: Optional[str] = None,
        log_level: Optional[Union[int, str]] = None,
        enable_rotation: bool = False,
        max_bytes: int = 10 * 1024 * 1024,
        backup_count: int = 5,
        structured: bool = False,
        rotation: Optional[str] = None,
        retention: Optional[str] = None,
        compression: Optional[str] = None,
    ) -> Union[logging.Logger, Any]:
        """
        Retrieve or create a configured logger instance.

        Returns cached logger if name exists. Otherwise creates new logger
        using detected backend (loguru preferred if available).

        Args:
            name: Unique identifier for the logger instance.
            verbose: Enable DEBUG level output; defaults to WARNING.
            log_filename: Path for file output; auto-generated if verbose=True.
            log_format: Override default message format string.
            log_level: Explicit level override (int for stdlib, str for loguru).
            enable_rotation: Activate RotatingFileHandler (stdlib only).
            max_bytes: Rotation trigger size in bytes (stdlib only).
            backup_count: Number of archived log files to retain (stdlib only).
            structured: Output JSON format instead of plain text.
            rotation: Rotation policy expression (loguru only, e.g., "10 MB").
            retention: Archive cleanup policy (loguru only, e.g., "1 week").
            compression: Archive compression format (loguru only, e.g., "gz").

        Returns:
            Configured logger compatible with the active backend.
        """
        if name in cls._loggers:
            return cls._loggers[name]

        if cls._use_loguru:
            logger = cls._get_loguru_logger(
                name=name,
                verbose=verbose,
                log_filename=log_filename,
                log_format=log_format,
                log_level=log_level,
                structured=structured,
                rotation=rotation,
                retention=retention,
                compression=compression,
            )
        else:
            logger = cls._get_standard_logger(
                name=name,
                verbose=verbose,
                log_filename=log_filename,
                log_format=log_format,
                log_level=log_level,
                enable_rotation=enable_rotation,
                max_bytes=max_bytes,
                backup_count=backup_count,
                structured=structured,
            )

        cls._loggers[name] = logger
        return logger

    @classmethod
    def _remove_pgmq_handlers(cls):
        """Remove all handlers previously added by this class."""
        if not LOGURU_AVAILABLE or not cls._use_loguru:
            return

        ids_to_remove = list(cls._handler_ids)
        for handler_id in ids_to_remove:
            try:
                loguru_logger.remove(handler_id)
                cls._handler_ids.discard(handler_id)
            except Exception:
                # Handler already removed or invalid; clean up tracking set
                cls._handler_ids.discard(handler_id)

    @classmethod
    def _get_standard_logger(
        cls,
        name: str,
        verbose: bool = False,
        log_filename: Optional[str] = None,
        log_format: Optional[str] = None,
        log_level: Optional[int] = None,
        enable_rotation: bool = False,
        max_bytes: int = 10 * 1024 * 1024,
        backup_count: int = 5,
        structured: bool = False,
    ) -> logging.Logger:
        """Configure and return a standard library Logger instance."""
        logger = logging.getLogger(name)

        if logger.handlers:
            return logger

        if log_level is not None:
            logger.setLevel(log_level)
        elif verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.WARNING)

        if log_format is None:
            if structured:
                log_format = '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
            else:
                log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        formatter = logging.Formatter(log_format)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        if verbose or log_filename:
            if log_filename is None:
                log_filename = datetime.now().strftime("pgmq_debug_%Y%m%d_%H%M%S.log")

            log_path = os.path.join(os.getcwd(), log_filename)

            if enable_rotation:
                file_handler = logging.handlers.RotatingFileHandler(
                    filename=log_path, maxBytes=max_bytes, backupCount=backup_count
                )
            else:
                file_handler = logging.FileHandler(filename=log_path)

            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    @classmethod
    def _get_loguru_logger(
        cls,
        name: str,
        verbose: bool = False,
        log_filename: Optional[str] = None,
        log_format: Optional[str] = None,
        log_level: Optional[Union[int, str]] = None,
        structured: bool = False,
        rotation: Optional[str] = None,
        retention: Optional[str] = None,
        compression: Optional[str] = None,
    ) -> Any:
        """
        Configure and return a loguru logger instance.

        When verbose=False and no log_filename specified, returns a bound
        logger without adding handlers to avoid polluting host application logs.
        """

        effective_level = "DEBUG" if verbose else "WARNING"
        if log_level is not None:
            if isinstance(log_level, int):
                level_map = {
                    logging.DEBUG: "DEBUG",
                    logging.INFO: "INFO",
                    logging.WARNING: "WARNING",
                    logging.ERROR: "ERROR",
                    logging.CRITICAL: "CRITICAL",
                }
                effective_level = level_map.get(log_level, "INFO")
            else:
                effective_level = str(log_level)

        if log_format is None:
            if structured:
                log_format = '{{"timestamp": "{time:YYYY-MM-DD HH:mm:ss.SSS}", "level": "{level}", "logger": "{extra[logger]}", "message": "{message}"}}'
            else:
                # Omit {extra[logger]} to prevent KeyError when host app logs without context
                log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

        needs_custom_handler = bool(verbose or log_filename)

        if needs_custom_handler:
            cls._remove_pgmq_handlers()

            console_id = loguru_logger.add(
                sys.stderr,
                format=log_format,
                level=effective_level,
                enqueue=True,
                backtrace=True,
                diagnose=True,
            )
            cls._handler_ids.add(console_id)

            if log_filename is None:
                log_filename = datetime.now().strftime("pgmq_debug_%Y%m%d_%H%M%S.log")

            log_path = os.path.join(os.getcwd(), log_filename)

            file_id = loguru_logger.add(
                log_path,
                format=log_format,
                level=effective_level,
                rotation=rotation or "10 MB",
                retention=retention or "1 week",
                compression=compression,
                enqueue=True,
                backtrace=True,
                diagnose=True,
            )
            cls._handler_ids.add(file_id)

        logger = loguru_logger.bind(logger=name)
        return logger

    @classmethod
    def configure_global_logging(
        cls,
        log_level: Union[int, str] = logging.INFO,
        log_format: Optional[str] = None,
        structured: bool = False,
        use_loguru: Optional[bool] = None,
    ) -> None:
        """
        Apply global logging configuration across all PGMQ loggers.

        Modifies class-level defaults and configures root handlers.

        Args:
            log_level: Default severity threshold for all loggers.
            log_format: Default output format template.
            structured: Enable JSON output for all loggers.
            use_loguru: Force specific backend (None enables auto-detection).
        """
        cls._configured = True

        if use_loguru is not None:
            cls._use_loguru = use_loguru and LOGURU_AVAILABLE

        if cls._use_loguru:
            cls._remove_pgmq_handlers()

            if log_level is None:
                log_level = "INFO"
            elif isinstance(log_level, int):
                level_map = {
                    logging.DEBUG: "DEBUG",
                    logging.INFO: "INFO",
                    logging.WARNING: "WARNING",
                    logging.ERROR: "ERROR",
                    logging.CRITICAL: "CRITICAL",
                }
                log_level = level_map.get(log_level, "INFO")

            if log_format is None:
                if structured:
                    log_format = '{{"timestamp": "{time:YYYY-MM-DD HH:mm:ss.SSS}", "level": "{level}", "logger": "{extra[logger]}", "message": "{message}"}}'
                else:
                    # Also omit {extra[logger]} here to prevent KeyError in host applications
                    # Users can override with custom log_format if they need logger names
                    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

            handler_id = loguru_logger.add(
                sys.stderr, format=log_format, level=log_level, enqueue=True
            )
            cls._handler_ids.add(handler_id)
        else:
            root_logger = logging.getLogger("pgmq")
            root_logger.setLevel(log_level)

            if log_format is None:
                if structured:
                    log_format = '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
                else:
                    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

            formatter = logging.Formatter(log_format)

            if not any(
                isinstance(h, logging.StreamHandler) for h in root_logger.handlers
            ):
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(formatter)
                root_logger.addHandler(console_handler)

    @classmethod
    def log_with_context(
        cls,
        logger: Union[logging.Logger, Any],
        level: Union[int, str],
        message: str,
        **context,
    ) -> None:
        """
        Emit a log entry with structured context data.

        Automatically adapts output format for active backend.

        Args:
            logger: Target logger instance.
            level: Severity level (int for stdlib, str for loguru).
            message: Primary log message content.
            **context: Key-value pairs to include in log output.
        """
        if cls._use_loguru:
            if context:
                logger = logger.bind(**context)

            if isinstance(level, int):
                level_map = {
                    logging.DEBUG: "DEBUG",
                    logging.INFO: "INFO",
                    logging.WARNING: "WARNING",
                    logging.ERROR: "ERROR",
                    logging.CRITICAL: "CRITICAL",
                }
                level = level_map.get(level, "INFO")

            logger.log(level, message)
        else:
            if context:
                context_str = " | ".join([f"{k}={v}" for k, v in context.items()])
                message = f"{message} | {context_str}"

            logger.log(level, message)

    @classmethod
    def log_transaction_start(
        cls, logger: Union[logging.Logger, Any], func_name: str, **context
    ):
        """Record transaction initiation event."""
        cls.log_with_context(
            logger,
            logging.DEBUG,
            f"Transaction started: {func_name}",
            event="transaction_start",
            function=func_name,
            **context,
        )

    @classmethod
    def log_transaction_success(
        cls, logger: Union[logging.Logger, Any], func_name: str, **context
    ):
        """Record transaction completion event."""
        cls.log_with_context(
            logger,
            logging.DEBUG,
            f"Transaction completed: {func_name}",
            event="transaction_success",
            function=func_name,
            **context,
        )

    @classmethod
    def log_transaction_error(
        cls,
        logger: Union[logging.Logger, Any],
        func_name: str,
        error: Exception,
        **context,
    ):
        """Record transaction failure and rollback event."""
        cls.log_with_context(
            logger,
            logging.ERROR,
            f"Transaction failed: {func_name} - {str(error)}",
            event="transaction_error",
            function=func_name,
            error_type=type(error).__name__,
            error_message=str(error),
            **context,
        )


def create_logger(
    name: str, verbose: bool = False, log_filename: Optional[str] = None
) -> Union[logging.Logger, Any]:
    """
    Factory function for backward-compatible logger creation.

    Simplified interface matching legacy PGMQueue API.

    Args:
        name: Logger identifier.
        verbose: Enable debug output.
        log_filename: Optional file output path.

    Returns:
        Configured logger instance.
    """
    return PGMQLogger.get_logger(name=name, verbose=verbose, log_filename=log_filename)


def log_performance(logger: Union[logging.Logger, Any]):
    """
    Decorator factory for function execution timing.

    Captures elapsed time and success/failure status.
    Supports both synchronous and asynchronous functions.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                elapsed = (time.time() - start_time) * 1000
                PGMQLogger.log_with_context(
                    logger,
                    logging.DEBUG,
                    f"Completed {func.__name__}",
                    function=func.__name__,
                    elapsed_ms=round(elapsed, 2),
                    success=True,
                )
                return result
            except Exception as e:
                elapsed = (time.time() - start_time) * 1000
                PGMQLogger.log_with_context(
                    logger,
                    logging.ERROR,
                    f"Failed {func.__name__}: {str(e)}",
                    function=func.__name__,
                    elapsed_ms=round(elapsed, 2),
                    success=False,
                    error=str(e),
                )
                raise

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                elapsed = (time.time() - start_time) * 1000
                PGMQLogger.log_with_context(
                    logger,
                    logging.DEBUG,
                    f"Completed {func.__name__}",
                    function=func.__name__,
                    elapsed_ms=round(elapsed, 2),
                    success=True,
                )
                return result
            except Exception as e:
                elapsed = (time.time() - start_time) * 1000
                PGMQLogger.log_with_context(
                    logger,
                    logging.ERROR,
                    f"Failed {func.__name__}: {str(e)}",
                    function=func.__name__,
                    elapsed_ms=round(elapsed, 2),
                    success=False,
                    error=str(e),
                )
                raise

        return async_wrapper if asyncio.iscoroutinefunction(func) else wrapper

    return decorator
