# src/pgmq/logger.py (fixed)

import logging
import logging.handlers
import os
import sys
import functools
import asyncio
import time
from datetime import datetime
from typing import Optional, Dict, Any, Union

# Try to import loguru, fall back to standard logging if not available
try:
    from loguru import logger as loguru_logger

    LOGURU_AVAILABLE = True
except ImportError:
    LOGURU_AVAILABLE = False


class PGMQLogger:
    """
    Centralized logger for PGMQueue with enhanced features.
    Backward compatible with existing PGMQueue implementation.
    Supports both standard logging and loguru (if installed).
    """

    _loggers: Dict[str, Union[logging.Logger, Any]] = {}
    _configured: bool = False
    _use_loguru: bool = LOGURU_AVAILABLE

    @classmethod
    def get_logger(
        cls,
        name: str,
        verbose: bool = False,
        log_filename: Optional[str] = None,
        log_format: Optional[str] = None,
        log_level: Optional[Union[int, str]] = None,
        enable_rotation: bool = False,
        max_bytes: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
        structured: bool = False,
        rotation: Optional[str] = None,
        retention: Optional[str] = None,
        compression: Optional[str] = None,
    ) -> Union[logging.Logger, Any]:
        """
        Get or create a logger with the specified configuration.

        Args:
            name: Logger name
            verbose: Enable debug logging
            log_filename: Log file path (auto-generated if None and verbose is True)
            log_format: Custom log format string
            log_level: Override log level
            enable_rotation: Enable log rotation (standard logging only)
            max_bytes: Maximum bytes before rotation (standard logging only)
            backup_count: Number of backup files to keep (standard logging only)
            structured: Enable structured JSON logging
            rotation: Log rotation setting (loguru only, e.g., "10 MB", "1 day")
            retention: Log retention setting (loguru only, e.g., "1 week")
            compression: Compression setting (loguru only, e.g., "gz")

        Returns:
            Configured logger instance (either logging.Logger or loguru logger)
        """
        if name in cls._loggers:
            return cls._loggers[name]

        # Use loguru if available and not explicitly disabled
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
        """Get a standard Python logging logger."""
        logger = logging.getLogger(name)

        # Skip if already configured with handlers
        if logger.handlers:
            return logger

        # Set log level
        if log_level is not None:
            logger.setLevel(log_level)
        elif verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.WARNING)

        # Default format
        if log_format is None:
            if structured:
                log_format = '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
            else:
                log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

        formatter = logging.Formatter(log_format)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # File handler (if verbose or log_filename provided)
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
        """Get a loguru logger."""
        # Remove default handler
        loguru_logger.remove()

        # Set log level
        if log_level is None:
            log_level = "DEBUG" if verbose else "WARNING"
        elif isinstance(log_level, int):
            # Convert standard logging levels to loguru levels
            level_map = {
                logging.DEBUG: "DEBUG",
                logging.INFO: "INFO",
                logging.WARNING: "WARNING",
                logging.ERROR: "ERROR",
                logging.CRITICAL: "CRITICAL",
            }
            log_level = level_map.get(log_level, "INFO")

        # Default format
        if log_format is None:
            if structured:
                log_format = '{{"timestamp": "{time:YYYY-MM-DD HH:mm:ss.SSS}", "level": "{level}", "logger": "{extra[logger]}", "message": "{message}"}}'
            else:
                log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{extra[logger]}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"

        # Add console handler
        loguru_logger.add(
            sys.stderr,
            format=log_format,
            level=log_level,
            enqueue=True,
            backtrace=True,
            diagnose=True,
        )

        # Add file handler if needed
        if verbose or log_filename:
            if log_filename is None:
                log_filename = datetime.now().strftime("pgmq_debug_%Y%m%d_%H%M%S.log")

            log_path = os.path.join(os.getcwd(), log_filename)

            loguru_logger.add(
                log_path,
                format=log_format,
                level=log_level,
                rotation=rotation or "10 MB",
                retention=retention or "1 week",
                compression=compression,
                enqueue=True,
                backtrace=True,
                diagnose=True,
            )

        # Bind logger name to extra context
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
        Configure global logging settings for all PGMQ loggers.

        Args:
            log_level: Default log level
            log_format: Default log format
            structured: Enable structured JSON logging
            use_loguru: Force use of loguru (None = auto-detect)
        """
        cls._configured = True

        if use_loguru is not None:
            cls._use_loguru = use_loguru and LOGURU_AVAILABLE

        if cls._use_loguru:
            # Configure loguru globally
            loguru_logger.remove()

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
                    log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{extra[logger]}</cyan> - <level>{message}</level>"

            loguru_logger.add(
                sys.stderr, format=log_format, level=log_level, enqueue=True
            )
        else:
            # Configure standard logging globally
            root_logger = logging.getLogger("pgmq")
            root_logger.setLevel(log_level)

            if log_format is None:
                if structured:
                    log_format = '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
                else:
                    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

            formatter = logging.Formatter(log_format)

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
        Log a message with additional context.

        Args:
            logger: Logger instance
            level: Log level
            message: Log message
            **context: Additional context data
        """
        if cls._use_loguru:
            # Use loguru's bind method for context
            if context:
                logger = logger.bind(**context)

            # Map standard logging levels to loguru
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
            # Use standard logging
            if context:
                context_str = " | ".join([f"{k}={v}" for k, v in context.items()])
                message = f"{message} | {context_str}"

            logger.log(level, message)

    @classmethod
    def log_transaction_start(
        cls, logger: Union[logging.Logger, Any], func_name: str, **context
    ):
        """Log the start of a transaction."""
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
        """Log successful transaction completion."""
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
        """Log transaction error and rollback."""
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


# Backward compatibility function
def create_logger(
    name: str, verbose: bool = False, log_filename: Optional[str] = None
) -> Union[logging.Logger, Any]:
    """
    Create a logger with backward-compatible interface.

    Args:
        name: Logger name
        verbose: Enable debug logging
        log_filename: Log file path

    Returns:
        Configured logger instance
    """
    return PGMQLogger.get_logger(name=name, verbose=verbose, log_filename=log_filename)


# Performance decorator for logging
def log_performance(logger: Union[logging.Logger, Any]):
    """Decorator to log function performance."""

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
