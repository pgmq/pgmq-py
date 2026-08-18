# src/pgmq/base.py
"""
Base configuration and shared utilities for PGMQ clients.
"""

from dataclasses import dataclass, field
from typing import Any, Optional
import os
import logging
import urllib.parse

from pgmq.logger import LoggingManager, log_with_context
from psycopg.conninfo import conninfo_to_dict


def _env_bool(name: str, default: bool = True) -> bool:
    """Parse a boolean environment variable."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes")


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
    init_extension: bool = field(
        default_factory=lambda: _env_bool("PG_INIT_EXTENSION", True)
    )

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

        For URIs, this uses a libpq-compatible parser that splits the netloc on
        the *last* ``@`` (so passwords containing ``@`` work) and tolerates
        unescaped ``/`` or ``+`` inside the password.
        """
        # URI Format
        if "://" in conn_string:
            try:
                # Try standard urlparse first
                parsed = urllib.parse.urlparse(conn_string)

                # Mimic libpq: split on the LAST @ to handle passwords containing @
                scheme, rest = conn_string.split("://", 1)
                if "@" in rest:
                    userinfo, hostpath = rest.rsplit("@", 1)
                    if ":" in userinfo:
                        self.username, self.password = userinfo.split(":", 1)
                    else:
                        self.username = userinfo

                    self.username = urllib.parse.unquote(self.username)
                    if self.password:
                        self.password = urllib.parse.unquote(self.password)
                else:
                    hostpath = rest

                # Parse hostpath (host:port/database?options)
                # Prepend a dummy scheme to use urlparse safely on the remaining part
                hp_parsed = urllib.parse.urlparse("http://" + hostpath)
                if hp_parsed.hostname:
                    self.host = hp_parsed.hostname
                if hp_parsed.port:
                    self.port = str(hp_parsed.port)
                if hp_parsed.path and len(hp_parsed.path) > 1:
                    # path is '/dbname', slice off the leading '/'
                    self.database = urllib.parse.unquote(hp_parsed.path[1:])

                # If urlparse gave us userinfo that we didn't extract above,
                # prefer the rsplit result (handles passwords with @)
                if not self.username and parsed.username is not None:
                    self.username = urllib.parse.unquote(parsed.username)
                if not self.password and parsed.password is not None:
                    self.password = urllib.parse.unquote(parsed.password)

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
        return f"postgresql://{user}:{password}@{self.host}:{self.port}/{self.database}"


_CONNECTION_FIELDS = frozenset(
    {"host", "port", "database", "username", "password", "conn_string"}
)


def resolve_pgmq_config(
    *,
    config: Optional[PGMQConfig] = None,
    dsn: Optional[str] = None,
    config_kwargs: Optional[dict[str, Any]] = None,
) -> PGMQConfig:
    """
    Build a :class:`PGMQConfig` from explicit fields, a DSN, or keyword arguments.

    Libpq connection options (for example ``sslmode`` or ``connect_timeout``) must
    be included in ``dsn`` or ``conn_string``, not passed as keyword arguments.
    """
    if config is not None:
        if dsn is not None or config_kwargs:
            raise ValueError("Cannot combine config with dsn or connection kwargs")
        return config

    valid_fields = set(PGMQConfig.__dataclass_fields__.keys())
    raw_kwargs = config_kwargs or {}
    unsupported = sorted(k for k in raw_kwargs if k not in valid_fields)
    if unsupported:
        raise ValueError(
            f"Unsupported connection parameters: {unsupported}. "
            "Pass libpq options (for example sslmode or connect_timeout) "
            "via dsn or conn_string."
        )
    filtered_kwargs = dict(raw_kwargs)

    if dsn is not None:
        filtered_kwargs["conn_string"] = dsn
        return PGMQConfig(**filtered_kwargs)
    if filtered_kwargs:
        explicit_connection = _CONNECTION_FIELDS.intersection(filtered_kwargs)
        if explicit_connection - {"conn_string"}:
            filtered_kwargs.setdefault("conn_string", None)
        return PGMQConfig(**filtered_kwargs)
    return PGMQConfig()


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
        if "config" in kwargs and isinstance(kwargs["config"], PGMQConfig):
            if len(kwargs) > 1:
                raise ValueError("Cannot combine config with other connection kwargs")
            self.config = kwargs["config"]
        else:
            self.config = resolve_pgmq_config(config_kwargs=kwargs)

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
