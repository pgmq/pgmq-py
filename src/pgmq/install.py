"""
Install the SQL-only PGMQ schema from GitHub.

Downloads ``pgmq-extension/sql/pgmq.sql`` from the PGMQ repository and executes
it against a target PostgreSQL database. Version upgrades are not supported yet.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from typing import Any, Optional

import psycopg
from psycopg import Connection

from pgmq.base import PGMQConfig
from pgmq.logger import log_with_context

GITHUB_RELEASES_LATEST = "https://api.github.com/repos/pgmq/pgmq/releases/latest"
USER_AGENT = "pgmq-py"

logger = logging.getLogger(__name__)


class PGMQInstallError(Exception):
    """Raised when PGMQ SQL installation fails."""


def _github_request(url: str) -> bytes:
    """Fetch a URL from GitHub with a User-Agent header."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        raise PGMQInstallError(f"Failed to fetch {url}: HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise PGMQInstallError(f"Failed to fetch {url}: {exc.reason}") from exc


def _is_git_hash(version: str) -> bool:
    """Return True if version looks like a git commit hash."""
    return (
        7 <= len(version) <= 64
        and version.isascii()
        and all(c in "0123456789abcdefABCDEF" for c in version)
    )


def build_install_sql_url(version: str) -> str:
    """
    Build the raw GitHub URL for ``pgmq.sql`` from a release tag or git hash.

    Args:
        version: Release tag (``v1.9.0`` or ``1.9.0``) or git commit hash.

    Returns:
        Raw GitHub URL for the SQL install script.
    """
    if _is_git_hash(version):
        return (
            f"https://raw.githubusercontent.com/pgmq/pgmq/{version}"
            f"/pgmq-extension/sql/pgmq.sql"
        )

    version_tag = version if version.startswith("v") else f"v{version}"
    return (
        f"https://raw.githubusercontent.com/pgmq/pgmq/refs/tags/{version_tag}"
        f"/pgmq-extension/sql/pgmq.sql"
    )


def get_latest_release_tag() -> str:
    """
    Fetch the latest PGMQ release tag from GitHub.

    Returns:
        Release tag name, e.g. ``v1.9.0``.
    """
    log_with_context(logger, logging.INFO, "Fetching latest PGMQ release tag")
    payload = json.loads(_github_request(GITHUB_RELEASES_LATEST).decode("utf-8"))
    tag_name = payload.get("tag_name")
    if not tag_name:
        raise PGMQInstallError("GitHub release response did not include tag_name")
    log_with_context(logger, logging.INFO, "Latest PGMQ release", tag=tag_name)
    return tag_name


def get_install_sql(version: Optional[str] = None) -> str:
    """
    Download the PGMQ SQL install script from GitHub.

    Args:
        version: Release tag, bare version number, or git hash. When omitted,
            the latest GitHub release is used.

    Returns:
        Raw SQL script contents.
    """
    version_to_use = version or get_latest_release_tag()
    sql_url = build_install_sql_url(version_to_use)
    log_with_context(logger, logging.INFO, "Fetching PGMQ SQL", url=sql_url)
    sql_content = _github_request(sql_url).decode("utf-8")
    if not sql_content.strip():
        raise PGMQInstallError(f"Downloaded SQL from {sql_url} is empty")
    return sql_content


def _resolve_config(
    *,
    config: Optional[PGMQConfig] = None,
    dsn: Optional[str] = None,
    config_kwargs: Optional[dict[str, Any]] = None,
) -> PGMQConfig:
    if config is not None:
        if dsn is not None or config_kwargs:
            raise ValueError("Cannot combine config with dsn or connection kwargs")
        return config
    if dsn is not None:
        merged = {**(config_kwargs or {}), "conn_string": dsn}
        return PGMQConfig(**merged)
    if config_kwargs:
        return PGMQConfig(**config_kwargs)
    return PGMQConfig()


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
        sql: SQL script contents, typically from :func:`get_install_sql`.
        conn: Existing psycopg connection. When provided, the caller owns the
            connection lifecycle.
        config: Connection settings used when ``conn`` is not provided.
        dsn: Libpq connection string used when ``conn`` and ``config`` are not
            provided.
        **config_kwargs: Connection settings passed to :class:`PGMQConfig` when
            neither ``conn``, ``config``, nor ``dsn`` are provided.
    """
    own_conn = False
    if conn is None:
        resolved_config = _resolve_config(
            config=config, dsn=dsn, config_kwargs=config_kwargs or None
        )
        conn = psycopg.connect(resolved_config.dsn, autocommit=False)
        own_conn = True

    try:
        log_with_context(logger, logging.INFO, "Executing PGMQ installation SQL")
        _execute_sql_script(conn, sql)
        log_with_context(
            logger, logging.INFO, "PGMQ installation completed successfully"
        )
    except Exception as exc:
        raise PGMQInstallError(f"Failed to execute PGMQ SQL: {exc}") from exc
    finally:
        if own_conn:
            conn.close()


def install_pgmq_from_sql(
    version: Optional[str] = None,
    *,
    conn: Optional[Connection] = None,
    config: Optional[PGMQConfig] = None,
    dsn: Optional[str] = None,
    **config_kwargs: Any,
) -> str:
    """
    Download and install PGMQ SQL from GitHub.

    Args:
        version: Release tag, bare version number, or git hash. When omitted,
            the latest GitHub release is used.
        conn: Existing psycopg connection.
        config: Connection settings used when ``conn`` is not provided.
        dsn: Libpq connection string used when ``conn`` and ``config`` are not
            provided.
        **config_kwargs: Connection settings passed to :class:`PGMQConfig` when
            neither ``conn``, ``config``, nor ``dsn`` are provided.

    Returns:
        The version tag or git hash used for installation.

    Note:
        Version upgrades are not supported yet. This performs a fresh SQL-only
        install using ``CREATE ... IF NOT EXISTS`` guards in the upstream script.
    """
    version_to_use = version or get_latest_release_tag()
    log_with_context(
        logger, logging.INFO, "Installing PGMQ from GitHub", version=version_to_use
    )
    sql = get_install_sql(version_to_use)
    install_pgmq_sql(sql, conn=conn, config=config, dsn=dsn, **config_kwargs)
    return version_to_use


async def install_pgmq_from_sql_async(
    version: Optional[str] = None,
    *,
    conn=None,
    config: Optional[PGMQConfig] = None,
    dsn: Optional[str] = None,
    **config_kwargs: Any,
) -> str:
    """
    Download and install PGMQ SQL from GitHub using asyncpg.

    Args:
        version: Release tag, bare version number, or git hash. When omitted,
            the latest GitHub release is used.
        conn: Existing asyncpg connection.
        config: Connection settings used when ``conn`` is not provided.
        dsn: URI connection string used when ``conn`` and ``config`` are not
            provided.
        **config_kwargs: Connection settings passed to :class:`PGMQConfig` when
            neither ``conn``, ``config``, nor ``dsn`` are provided.

    Returns:
        The version tag or git hash used for installation.
    """
    try:
        import asyncpg
    except ImportError as exc:  # pragma: no cover
        raise ImportError(
            "asyncpg is required for install_pgmq_from_sql_async. "
            "Install with: pip install pgmq[async]"
        ) from exc

    version_to_use = version or get_latest_release_tag()
    log_with_context(
        logger,
        logging.INFO,
        "Installing PGMQ from GitHub (async)",
        version=version_to_use,
    )
    sql = get_install_sql(version_to_use)

    own_conn = False
    if conn is None:
        resolved_config = _resolve_config(
            config=config, dsn=dsn, config_kwargs=config_kwargs or None
        )
        conn = await asyncpg.connect(resolved_config.async_dsn)
        own_conn = True

    try:
        log_with_context(logger, logging.INFO, "Executing PGMQ installation SQL")
        async with conn.transaction():
            await conn.execute(sql)
        log_with_context(
            logger, logging.INFO, "PGMQ installation completed successfully"
        )
    except Exception as exc:
        raise PGMQInstallError(f"Failed to execute PGMQ SQL: {exc}") from exc
    finally:
        if own_conn:
            await conn.close()

    return version_to_use
