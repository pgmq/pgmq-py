# src/pgmq/logger.py

import logging
import logging.handlers
import os
import sys
import functools
import inspect
import time
from datetime import datetime
from typing import Optional, Dict, Any, Union, Set

# Attempt to import loguru; fall back to standard logging if unavailable
try:
    from loguru import logger as loguru_logger  # type: ignore

    LOGURU_AVAILABLE = True
except ImportError:
    LOGURU_AVAILABLE = False


class LoggingManager:
    """
    Centralized logging manager for PGMqueue with dual backend support.

    Provides a unified interface for both standard library logging and loguru,
    with automatic backend detection and backward compatibility with existing
    PGMQueue implementations.
    """

    _configured_loggers: Dict[str, Any] = {}
    _loguru_handler_ids: Set[int] = set()
    _use_loguru: bool = LOGURU_AVAILABLE
    _configured: bool = False

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
        """
        # Create a cache key based on configuration to avoid mixing configs
        cache_key = f"{name}:{verbose}:{log_filename}:{structured}:{rotation}:{retention}:{compression}"

        if cache_key in cls._configured_loggers:
            return cls._configured_loggers[cache_key]

        if cls._use_loguru:
            logger = cls._configure_loguru(
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
            logger = cls._configure_stdlib(
                name=name,
                verbose=verbose,
                log_filename=log_filename,
                log_format=log_format,
                log_level=log_level,
                structured=structured,
                enable_rotation=enable_rotation,
                max_bytes=max_bytes,
                backup_count=backup_count,
            )

        cls._configured_loggers[cache_key] = logger
        return logger

    @classmethod
    def _remove_pgmq_handlers(cls):
        """Remove all handlers previously added by this class."""
        if not LOGURU_AVAILABLE or not cls._use_loguru:
            return

        ids_to_remove = list(cls._loguru_handler_ids)
        for handler_id in ids_to_remove:
            try:
                loguru_logger.remove(handler_id)
                cls._loguru_handler_ids.discard(handler_id)
            except Exception:
                cls._loguru_handler_ids.discard(handler_id)

    @classmethod
    def _configure_stdlib(
        cls,
        name: str,
        verbose: bool,
        log_filename: Optional[str],
        log_format: Optional[str],
        log_level: Optional[Union[int, str]],
        structured: bool,
        enable_rotation: bool,
        max_bytes: int,
        backup_count: int,
    ) -> logging.Logger:
        """Configure and return a standard library Logger instance."""
        logger = logging.getLogger(name)

        # Return existing if already configured
        if logger.handlers:
            return logger

        # Set Level
        if log_level is not None:
            if isinstance(log_level, str):
                level = getattr(logging, log_level.upper(), logging.WARNING)
                logger.setLevel(level)
            else:
                logger.setLevel(log_level)
        elif verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.WARNING)

        # Set Format
        if log_format is None:
            if structured:
                log_format = '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
            else:
                log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        formatter = logging.Formatter(log_format)

        # Console Handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File Handler
        if verbose or log_filename:
            filename = log_filename or datetime.now().strftime("pgmq_%Y%m%d_%H%M%S.log")
            filepath = os.path.join(os.getcwd(), filename)

            if enable_rotation:
                file_handler = logging.handlers.RotatingFileHandler(
                    filepath, maxBytes=max_bytes, backupCount=backup_count
                )
            else:
                file_handler = logging.FileHandler(filepath)

            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    @classmethod
    def _configure_loguru(
        cls,
        name: str,
        verbose: bool,
        log_filename: Optional[str],
        log_format: Optional[str],
        log_level: Optional[Union[int, str]],
        structured: bool,
        rotation: Optional[str],
        retention: Optional[str],
        compression: Optional[str],
    ) -> Any:
        """
        Configure and return a loguru logger instance.
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
                log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

        needs_custom_handler = bool(verbose or log_filename)

        if needs_custom_handler:
            # Remove previous handlers to avoid duplication in interactive sessions
            cls._remove_pgmq_handlers()

            # Console Handler
            console_id = loguru_logger.add(
                sys.stderr,
                format=log_format,
                level=effective_level,
                enqueue=True,
                backtrace=True,
                diagnose=True,
            )
            cls._loguru_handler_ids.add(console_id)

            # File Handler
            if log_filename:
                filepath = os.path.join(os.getcwd(), log_filename)
                file_id = loguru_logger.add(
                    filepath,
                    format=log_format,
                    level=effective_level,
                    rotation=rotation or "10 MB",
                    retention=retention or "1 week",
                    compression=compression,
                    enqueue=True,
                    backtrace=True,  # Restored
                    diagnose=True,  # Restored
                )
                cls._loguru_handler_ids.add(file_id)

        return loguru_logger.bind(logger=name)

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
        """
        cls._configured = True

        if use_loguru is not None:
            cls._use_loguru = use_loguru and LOGURU_AVAILABLE

        if cls._use_loguru:
            cls._remove_pgmq_handlers()

            if isinstance(log_level, int):
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
                    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

            handler_id = loguru_logger.add(
                sys.stderr, format=log_format, level=log_level, enqueue=True
            )
            cls._loguru_handler_ids.add(handler_id)
        else:
            root_logger = logging.getLogger("pgmq")
            root_logger.setLevel(log_level)
            if not any(
                isinstance(h, logging.StreamHandler) for h in root_logger.handlers
            ):
                console_handler = logging.StreamHandler()
                console_handler = logging.StreamHandler()
                formatter = logging.Formatter(
                    log_format or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
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

            if isinstance(level, str):
                level = getattr(logging, level.upper(), logging.INFO)

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
    """
    return LoggingManager.get_logger(
        name=name, verbose=verbose, log_filename=log_filename
    )


def log_with_context(
    logger: Union[logging.Logger, Any], level: int, message: str, **context: Any
) -> None:
    """
    Emit log entry with structured context.

    Automatically adapts to logger type (stdlib vs loguru).
    """
    LoggingManager.log_with_context(logger, level, message, **context)


def log_performance(logger: Union[logging.Logger, Any]):
    """
    Decorator factory for function execution timing.

    Captures elapsed time and success/failure status.
    Supports both synchronous and asynchronous functions.
    """

    def decorator(func):
        is_async = inspect.iscoroutinefunction(func)

        if is_async:

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                start = time.time()
                try:
                    result = await func(*args, **kwargs)
                    elapsed = (time.time() - start) * 1000
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        f"Completed {func.__name__}",
                        function=func.__name__,
                        elapsed_ms=round(elapsed, 2),
                        success=True,
                    )
                    return result
                except Exception as e:
                    elapsed = (time.time() - start) * 1000
                    log_with_context(
                        logger,
                        logging.ERROR,
                        f"Failed {func.__name__}: {e}",
                        function=func.__name__,
                        elapsed_ms=round(elapsed, 2),
                        success=False,
                        error=str(e),
                    )
                    raise

            return async_wrapper
        else:

            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                start = time.time()
                try:
                    result = func(*args, **kwargs)
                    elapsed = (time.time() - start) * 1000
                    log_with_context(
                        logger,
                        logging.DEBUG,
                        f"Completed {func.__name__}",
                        function=func.__name__,
                        elapsed_ms=round(elapsed, 2),
                        success=True,
                    )
                    return result
                except Exception as e:
                    elapsed = (time.time() - start) * 1000
                    log_with_context(
                        logger,
                        logging.ERROR,
                        f"Failed {func.__name__}: {e}",
                        function=func.__name__,
                        elapsed_ms=round(elapsed, 2),
                        success=False,
                        error=str(e),
                    )
                    raise

            return sync_wrapper

    return decorator


# Backward compatibility alias
PGMQLogger = LoggingManager
