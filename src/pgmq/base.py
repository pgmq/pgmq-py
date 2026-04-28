# src/pgmq/base.py
"""
Base configuration and shared utilities for PGMQ clients.
"""

from dataclasses import dataclass, field
from typing import Optional
import os
import logging
import urllib.parse

from pgmq.logger import LoggingManager, log_with_context
from psycopg.conninfo import conninfo_to_dict


@dataclass
class PGMQConfig:
    """
    Configuration shared between sync and async PGMQ clients.

    All parameters can be set via environment variables or explicitly.
    Environment variables take precedence over defaults but not over explicit values.

    A full connection string can be provided via `conn_string` or the `DATABASE_URL`
    environment variable, which will populate individual connection fields.
    """

    # Input field for full connection string (URI or libpq format)
    conn_string: Optional[str] = field(
        default_factory=lambda: os.getenv("DATABASE_URL"), repr=False
    )

    host: str = field(default_factory=lambda: os.getenv("PG_HOST", "localhost"))
    port: str = field(default_factory=lambda: os.getenv("PG_PORT", "5432"))
    database: str = field(default_factory=lambda: os.getenv("PG_DATABASE", "postgres"))
    username: str = field(default_factory=lambda: os.getenv("PG_USERNAME", "postgres"))
    password: str = field(default_factory=lambda: os.getenv("PG_PASSWORD", "postgres"))
    delay: int = 0
    vt: int = 30
    pool_size: int = 10
    verbose: bool = False
    log_filename: Optional[str] = None
    structured_logging: bool = False
    log_rotation: bool = False
    log_rotation_size: str = "10 MB"
    log_retention: str = "1 week"
    init_extension: bool = True

    def __post_init__(self) -> None:
        """Validate and normalize configuration."""
        # If a full connection string is provided, parse it and override individual fields
        if self.conn_string:
            self._parse_conn_string(self.conn_string)

        # Ensure defaults for empty strings from env vars
        self.host = self.host or "localhost"
        self.port = self.port or "5432"
        self.database = self.database or "postgres"
        self.username = self.username or "postgres"
        self.password = self.password or "postgres"

        # Validate required fields
        if not all([self.host, self.port, self.database, self.username, self.password]):
            raise ValueError("Incomplete database connection information provided.")

    def _parse_conn_string(self, conn_string: str) -> None:
        """
        Parse a connection string (URI or libpq format) and populate fields.

        Supports:
        - URI: postgresql://user:pass@host:port/database
        - Libpq: host=localhost port=5432 dbname=database user=postgres password=postgres
        """
        # URI Format
        if "://" in conn_string:
            try:
                parsed = urllib.parse.urlparse(conn_string)

                if parsed.hostname:
                    self.host = parsed.hostname
                if parsed.port:
                    self.port = str(parsed.port)
                if parsed.path and len(parsed.path) > 1:
                    # path is usually '/dbname', slice off the leading '/'
                    self.database = parsed.path[1:]
                if parsed.username:
                    self.username = parsed.username
                if parsed.password:
                    self.password = parsed.password

            except Exception as e:
                raise ValueError(f"Failed to parse connection URI: {e}")

        # Libpq Format (key=value pairs separated by spaces)
        elif "=" in conn_string:
            try:
                # Use psycopg's built-in parser for robust libpq string handling
                params = conninfo_to_dict(conn_string)
                if "host" in params:
                    self.host = params["host"]
                if "port" in params:
                    self.port = str(params["port"])
                if "dbname" in params:
                    self.database = params["dbname"]
                if "user" in params:
                    self.username = params["user"]
                if "password" in params:
                    self.password = params["password"]
            except Exception as e:
                raise ValueError(f"Failed to parse libpq connection string: {e}")
        else:
            # Treat as just host if no special characters found
            self.host = conn_string

    @property
    def dsn(self) -> str:
        """Build PostgreSQL connection string (libpq format)."""
        return (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.database} "
            f"user={self.username} "
            f"password={self.password}"
        )

    @property
    def async_dsn(self) -> str:
        """Build asyncpg-compatible connection string (URI format)."""
        user = urllib.parse.quote_plus(self.username)
        password = urllib.parse.quote_plus(self.password)
        return (
            f"postgresql://{user}:{password}@"
            f"{self.host}:{self.port}/{self.database}"
        )


class BaseQueue:
    """
    Base class providing shared initialization and utilities for queue clients.

    This class handles configuration management, logging setup.
    """

    config: PGMQConfig
    logger: logging.Logger

    def __init__(self, **kwargs):
        """
        Initialize base queue with configuration.

        Supports both legacy-style initialization (individual kwargs) and
        modern style (passing a PGMQConfig object).
        """
        # Handle both config object and legacy kwargs
        if "config" in kwargs and isinstance(kwargs["config"], PGMQConfig):
            self.config = kwargs["config"]
        else:
            # Filter kwargs to only include valid config fields
            valid_fields = set(PGMQConfig.__dataclass_fields__.keys())
            config_kwargs = {k: v for k, v in kwargs.items() if k in valid_fields}
            self.config = PGMQConfig(**config_kwargs)

        # Setup logging
        self.logger = LoggingManager.get_logger(
            name=self.__class__.__module__,
            verbose=self.config.verbose,
            log_filename=self.config.log_filename,
            structured=self.config.structured_logging,
            rotation=self.config.log_rotation_size
            if self.config.log_rotation
            else None,
            retention=self.config.log_retention if self.config.log_rotation else None,
        )

        log_with_context(
            self.logger,
            logging.DEBUG,
            f"{self.__class__.__name__} initialized",
            host=self.config.host,
            database=self.config.database,
        )
