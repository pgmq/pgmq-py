"""
Install the SQL-only PGMQ schema.

Published packages include ``pgmq.sql`` fetched from a pinned PGMQ extension
release. SQL-only install does not support extension versioning or upgrades.
"""

from __future__ import annotations

import logging
from importlib.resources import files
from typing import Any, Optional

import psycopg
from psycopg import Connection

from pgmq.base import PGMQConfig, resolve_pgmq_config
from pgmq.logger import log_with_context

logger = logging.getLogger(__name__)

_SQL_DIR = "sql"
_VERSION_NAME = "VERSION"
_SQL_NAME = "pgmq.sql"
_MISSING_SQL_HINT = (
    " In a source checkout, run `make vendor-pgmq-sql` to download "
    "the pinned PGMQ extension SQL."
)


class PGMQInstallError(Exception):
    """Raised when PGMQ SQL installation fails."""


def _read_package_text(name: str) -> str:
    """Read a text file from package data under ``pgmq/sql``."""
    return files("pgmq").joinpath(_SQL_DIR, name).read_text(encoding="utf-8")


def get_embedded_sql_version() -> str:
    """Return the PGMQ extension version pinned for this package."""
    try:
        text = _read_package_text(_VERSION_NAME)
    except Exception as exc:
        raise PGMQInstallError(
            f"Failed to read embedded PGMQ SQL version pin: {exc}"
        ) from exc
    for line in text.splitlines():
        version = line.strip()
        if version and not version.startswith("#"):
            return version
    raise PGMQInstallError("Embedded PGMQ SQL version pin is empty")


def get_embedded_install_sql() -> str:
    """
    Load the PGMQ SQL install script bundled with this package.

    Returns:
        Raw SQL script contents.
    """
    try:
        sql_content = _read_package_text(_SQL_NAME)
    except Exception as exc:
        message = f"Failed to read embedded PGMQ SQL script: {exc}"
        if isinstance(exc, FileNotFoundError):
            message += _MISSING_SQL_HINT
        raise PGMQInstallError(message) from exc
    if not sql_content.strip():
        raise PGMQInstallError("Embedded PGMQ SQL script is empty")
    return sql_content


def _execute_sql_script(conn: Connection, sql: str) -> None:
    """Execute a multi-statement SQL script within a transaction."""
    with conn.transaction():
        conn.execute(sql)


def install_pgmq_sql(
    sql: str,
    *,
    conn: Optional[Connection] = None,
    config: Optional[PGMQConfig] = None,
    dsn: Optional[str] = None,
    **config_kwargs: Any,
) -> None:
    """
    Execute a PGMQ SQL install script on a PostgreSQL database.

    Args:
        sql: SQL script contents, typically from :func:`get_embedded_install_sql`.
        conn: Existing psycopg connection. When provided, the caller owns the
            connection lifecycle. The connection must use ``autocommit=False``.
        config: Connection settings used when ``conn`` is not provided.
        dsn: Libpq connection string used when ``conn`` and ``config`` are not
            provided.
        **config_kwargs: :class:`PGMQConfig` fields (``host``, ``port``,
            ``database``, etc.). Libpq options such as ``sslmode`` or
            ``connect_timeout`` must be included in ``dsn`` or ``conn_string``,
            not passed as keyword arguments.

    Raises:
        PGMQInstallError: When connection or SQL execution fails.
        ValueError: When connection arguments are invalid or conflicting.
    """
    if not sql.strip():
        raise PGMQInstallError("PGMQ SQL script is empty")

    own_conn = False
    if conn is None:
        resolved_config = resolve_pgmq_config(
            config=config, dsn=dsn, config_kwargs=config_kwargs or None
        )
        try:
            conn = psycopg.connect(
                resolved_config.conn_string or resolved_config.dsn, autocommit=False
            )
            own_conn = True
        except Exception as exc:
            raise PGMQInstallError(f"Failed to connect to PostgreSQL: {exc}") from exc

    try:
        log_with_context(logger, logging.INFO, "Executing PGMQ installation SQL")
        _execute_sql_script(conn, sql)
        log_with_context(
            logger, logging.INFO, "PGMQ installation completed successfully"
        )
    except Exception as exc:
        raise PGMQInstallError(f"Failed to execute PGMQ SQL: {exc}") from exc
    finally:
        if own_conn and conn is not None:
            conn.close()


def install_pgmq_from_sql(
    *,
    conn: Optional[Connection] = None,
    config: Optional[PGMQConfig] = None,
    dsn: Optional[str] = None,
    **config_kwargs: Any,
) -> str:
    """
    Install PGMQ using the SQL script bundled with this package.

    Args:
        conn: Existing psycopg connection. Must use ``autocommit=False`` when
            provided by the caller.
        config: Connection settings used when ``conn`` is not provided.
        dsn: Libpq connection string used when ``conn`` and ``config`` are not
            provided.
        **config_kwargs: :class:`PGMQConfig` fields (``host``, ``port``,
            ``database``, etc.). Libpq options such as ``sslmode`` or
            ``connect_timeout`` must be included in ``dsn`` or ``conn_string``,
            not passed as keyword arguments.

    Returns:
        The bundled PGMQ version used for installation.

    Note:
        SQL-only install does not support extension versioning or upgrades.
        Prefer ``CREATE EXTENSION pgmq`` when the host allows extensions.
        This applies a snapshot of ``pgmq.sql`` using ``CREATE ... IF NOT EXISTS``
        guards; re-running it on an existing ``pgmq`` schema is not supported.
    """
    version = get_embedded_sql_version()
    log_with_context(
        logger,
        logging.INFO,
        "Installing PGMQ from bundled SQL",
        version=version,
    )
    install_pgmq_sql(
        get_embedded_install_sql(),
        conn=conn,
        config=config,
        dsn=dsn,
        **config_kwargs,
    )
    return version
