"""
Install the SQL-only PGMQ schema.

The package bundles ``pgmq.sql`` for offline installation. Version upgrades
are not supported yet.
"""

from __future__ import annotations

import logging
from importlib.resources import files
from typing import Any, Optional

import psycopg
from psycopg import Connection

from pgmq.base import PGMQConfig
from pgmq.logger import log_with_context

EMBEDDED_SQL_VERSION = "1.11.1"

logger = logging.getLogger(__name__)


class PGMQInstallError(Exception):
    """Raised when PGMQ SQL installation fails."""


def get_embedded_sql_version() -> str:
    """Return the PGMQ version bundled with this package."""
    return EMBEDDED_SQL_VERSION


def get_embedded_install_sql() -> str:
    """
    Load the PGMQ SQL install script bundled with this package.

    Returns:
        Raw SQL script contents.
    """
    sql_content = files("pgmq").joinpath("sql", "pgmq.sql").read_text(encoding="utf-8")
    if not sql_content.strip():
        raise PGMQInstallError("Embedded PGMQ SQL script is empty")
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

    valid_fields = set(PGMQConfig.__dataclass_fields__.keys())
    filtered_kwargs = {
        k: v for k, v in (config_kwargs or {}).items() if k in valid_fields
    }

    if dsn is not None:
        filtered_kwargs["conn_string"] = dsn
        return PGMQConfig(**filtered_kwargs)
    if filtered_kwargs:
        return PGMQConfig(**filtered_kwargs)
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
        sql: SQL script contents, typically from :func:`get_embedded_install_sql`.
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
        try:
            conn = psycopg.connect(resolved_config.dsn, autocommit=False)
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
        conn: Existing psycopg connection.
        config: Connection settings used when ``conn`` is not provided.
        dsn: Libpq connection string used when ``conn`` and ``config`` are not
            provided.
        **config_kwargs: Connection settings passed to :class:`PGMQConfig` when
            neither ``conn``, ``config``, nor ``dsn`` are provided.

    Returns:
        The bundled PGMQ version used for installation.

    Note:
        Version upgrades are not supported yet. This performs a fresh SQL-only
        install using ``CREATE ... IF NOT EXISTS`` guards in the upstream script.
    """
    log_with_context(
        logger,
        logging.INFO,
        "Installing PGMQ from bundled SQL",
        version=EMBEDDED_SQL_VERSION,
    )
    sql = get_embedded_install_sql()
    install_pgmq_sql(sql, conn=conn, config=config, dsn=dsn, **config_kwargs)
    return EMBEDDED_SQL_VERSION


# ---------------------------------------------------------------------------
# GitHub-based install (disabled for now; bundled SQL is the supported path)
# ---------------------------------------------------------------------------
#
# import json
# import os
# import urllib.error
# import urllib.request
#
# GITHUB_RELEASES_LATEST = "https://api.github.com/repos/pgmq/pgmq/releases/latest"
# USER_AGENT = "pgmq-py"
#
#
# def _github_auth_headers() -> dict[str, str]:
#     """Build GitHub request headers, including auth when a token is available."""
#     headers = {"User-Agent": USER_AGENT, "Accept": "application/vnd.github+json"}
#     token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
#     if token:
#         headers["Authorization"] = f"Bearer {token}"
#     return headers
#
#
# def _github_request(url: str) -> bytes:
#     """Fetch a URL from GitHub."""
#     request = urllib.request.Request(url, headers=_github_auth_headers())
#     try:
#         with urllib.request.urlopen(request, timeout=60) as response:
#             return response.read()
#     except urllib.error.HTTPError as exc:
#         raise PGMQInstallError(f"Failed to fetch {url}: HTTP {exc.code}") from exc
#     except urllib.error.URLError as exc:
#         raise PGMQInstallError(f"Failed to fetch {url}: {exc.reason}") from exc
#
#
# def _decode_github_json(payload: bytes, context: str) -> dict[str, Any]:
#     """Decode a GitHub API JSON response."""
#     try:
#         return json.loads(payload.decode("utf-8"))
#     except (json.JSONDecodeError, UnicodeDecodeError) as exc:
#         raise PGMQInstallError(
#             f"Failed to parse GitHub {context} response: {exc}"
#         ) from exc
#
#
# def _is_git_hash(version: str) -> bool:
#     """Return True if version looks like a git commit hash."""
#     return (
#         7 <= len(version) <= 64
#         and version.isascii()
#         and all(c in "0123456789abcdefABCDEF" for c in version)
#     )
#
#
# def build_install_sql_url(version: str) -> str:
#     """
#     Build the raw GitHub URL for ``pgmq.sql`` from a release tag or git hash.
#     """
#     if _is_git_hash(version):
#         return (
#             f"https://raw.githubusercontent.com/pgmq/pgmq/{version}"
#             f"/pgmq-extension/sql/pgmq.sql"
#         )
#
#     version_tag = version if version.startswith("v") else f"v{version}"
#     return (
#         f"https://raw.githubusercontent.com/pgmq/pgmq/refs/tags/{version_tag}"
#         f"/pgmq-extension/sql/pgmq.sql"
#     )
#
#
# def get_latest_release_tag() -> str:
#     """Fetch the latest PGMQ release tag from GitHub."""
#     log_with_context(logger, logging.INFO, "Fetching latest PGMQ release tag")
#     payload = _decode_github_json(
#         _github_request(GITHUB_RELEASES_LATEST),
#         "releases/latest",
#     )
#     tag_name = payload.get("tag_name")
#     if not tag_name:
#         raise PGMQInstallError("GitHub release response did not include tag_name")
#     log_with_context(logger, logging.INFO, "Latest PGMQ release", tag=tag_name)
#     return tag_name
#
#
# def get_install_sql(version: Optional[str] = None) -> str:
#     """Download the PGMQ SQL install script from GitHub."""
#     version_to_use = version or get_latest_release_tag()
#     sql_url = build_install_sql_url(version_to_use)
#     log_with_context(logger, logging.INFO, "Fetching PGMQ SQL", url=sql_url)
#     try:
#         sql_content = _github_request(sql_url).decode("utf-8")
#     except UnicodeDecodeError as exc:
#         raise PGMQInstallError(
#             f"Failed to decode SQL downloaded from {sql_url}: {exc}"
#         ) from exc
#     if not sql_content.strip():
#         raise PGMQInstallError(f"Downloaded SQL from {sql_url} is empty")
#     return sql_content
#
#
# def install_pgmq_from_github(
#     version: Optional[str] = None,
#     *,
#     conn: Optional[Connection] = None,
#     config: Optional[PGMQConfig] = None,
#     dsn: Optional[str] = None,
#     **config_kwargs: Any,
# ) -> str:
#     """Download and install PGMQ SQL from GitHub."""
#     version_to_use = version or get_latest_release_tag()
#     log_with_context(
#         logger, logging.INFO, "Installing PGMQ from GitHub", version=version_to_use
#     )
#     sql = get_install_sql(version_to_use)
#     install_pgmq_sql(sql, conn=conn, config=config, dsn=dsn, **config_kwargs)
#     return version_to_use
#
#
# async def install_pgmq_from_github_async(
#     version: Optional[str] = None,
#     *,
#     conn: Optional["asyncpg.Connection"] = None,
#     config: Optional[PGMQConfig] = None,
#     dsn: Optional[str] = None,
#     **config_kwargs: Any,
# ) -> str:
#     """Download and install PGMQ SQL from GitHub using asyncpg."""
#     try:
#         import asyncpg
#     except ImportError as exc:  # pragma: no cover
#         raise ImportError(
#             "asyncpg is required for install_pgmq_from_github_async. "
#             "Install with: pip install pgmq[async]"
#         ) from exc
#
#     version_to_use = version or get_latest_release_tag()
#     log_with_context(
#         logger,
#         logging.INFO,
#         "Installing PGMQ from GitHub (async)",
#         version=version_to_use,
#     )
#     sql = get_install_sql(version_to_use)
#
#     own_conn = False
#     if conn is None:
#         resolved_config = _resolve_config(
#             config=config, dsn=dsn, config_kwargs=config_kwargs or None
#         )
#         conn = await asyncpg.connect(resolved_config.async_dsn)
#         own_conn = True
#
#     try:
#         log_with_context(logger, logging.INFO, "Executing PGMQ installation SQL")
#         async with conn.transaction():
#             await conn.execute(sql)
#         log_with_context(
#             logger, logging.INFO, "PGMQ installation completed successfully"
#         )
#     except Exception as exc:
#         raise PGMQInstallError(f"Failed to execute PGMQ SQL: {exc}") from exc
#     finally:
#         if own_conn:
#             await conn.close()
#
#     return version_to_use
