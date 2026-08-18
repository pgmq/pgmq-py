# src/pgmq/api/auth.py
"""Hash, verify, and issue named API keys."""

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
import asyncio
import hashlib
import hmac
import logging
import re
import secrets
import uuid

from pgmq.api.auth_sql import (
    create_schema_sql,
    create_table_sql,
    insert_key_sql,
    lookup_by_hash_sql,
    table_exists_sql,
    touch_last_used_sql,
    validate_identifier,
    validate_schema,
)
from pgmq.api.config import PGMQAPIConfig
from pgmq.logger import LoggingManager, log_with_context

logger = LoggingManager.get_logger("pgmq.api")

KEY_PREFIX_LEN = 8


def uses_db(api_config: PGMQAPIConfig) -> bool:
    """Return True when named keys may be stored in Postgres."""
    mode = (api_config.auth_mode or "none").lower()
    return bool(api_config.use_db or mode in ("workers", "both"))


def require_pepper(api_config: PGMQAPIConfig) -> None:
    """Refuse the optional DB store when the pepper is unset."""
    if uses_db(api_config) and not api_config.pepper:
        raise RuntimeError(
            "auth.use_db is set but the key pepper is unset. "
            "Set PGMQ_API_KEY_PEPPER in .env."
        )


def match_env_key(
    api_config: PGMQAPIConfig, presented: str
) -> Optional[Dict[str, str]]:
    """Return ``{name, id}`` for a matching env key, or None."""
    revoked = api_config.revoked_ids or set()
    if api_config.api_key and hmac.compare_digest(
        str(presented), str(api_config.api_key)
    ):
        return {"name": "default", "id": ""}
    for item in api_config.issued_keys or []:
        key_id = item.get("id") or ""
        if key_id and key_id in revoked:
            continue
        secret = item.get("secret") or ""
        if secret and hmac.compare_digest(str(presented), str(secret)):
            return {"name": item.get("name") or key_id, "id": key_id}
    for name, secret in (api_config.named_keys or {}).items():
        if secret and hmac.compare_digest(str(presented), str(secret)):
            return {"name": name, "id": ""}
    return None


def hash_api_key(pepper: str, raw_key: str) -> str:
    """Return HMAC-SHA256(pepper, raw_key) as hex."""
    return hmac.new(
        pepper.encode("utf-8"),
        raw_key.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def generate_raw_key() -> str:
    """Generate a random API key. Print it once from the CLI only."""
    return secrets.token_urlsafe(32)


def key_prefix_of(raw_key: str) -> str:
    """Return the short prefix stored and logged for a raw key."""
    return raw_key[:KEY_PREFIX_LEN]


def parse_expires(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO date or datetime. A date-only value is midnight UTC."""
    if not value:
        return None
    if len(value) == 10:
        parsed_date = datetime.strptime(value, "%Y-%m-%d")
        return parsed_date.replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _to_asyncpg_sql(sql: str) -> str:
    index = 0

    def _replace(_match: re.Match) -> str:
        nonlocal index
        index += 1
        return f"${index}"

    return re.sub(r"%s", _replace, sql)


def _row_get(row: Any, name: str, index: int) -> Any:
    if isinstance(row, dict):
        return row.get(name)
    mapping = getattr(row, "_mapping", None)
    if mapping is not None and name in mapping:
        return mapping[name]
    try:
        return row[name]
    except Exception:
        return row[index]


async def run_auth_sql(
    queue: Any,
    sql: str,
    params: Optional[Tuple[Any, ...]] = None,
    fetch: bool = False,
) -> Optional[List[Any]]:
    """Run auth SQL on the client pool or engine. Do not open a second pool."""
    pool = getattr(queue, "pool", None)
    if pool is not None:
        acquire = getattr(pool, "acquire", None)
        if callable(acquire):
            ctx = acquire()
            if hasattr(ctx, "__aenter__"):
                async_sql = _to_asyncpg_sql(sql)
                async with ctx as conn:
                    if fetch:
                        return list(await conn.fetch(async_sql, *(params or ())))
                    await conn.execute(async_sql, *(params or ()))
                    return None
        connection = getattr(pool, "connection", None)
        if callable(connection):

            def _sync_pool() -> Optional[List[Any]]:
                with pool.connection() as conn:
                    result = conn.execute(sql, params)
                    if fetch:
                        return list(result.fetchall())
                    return None

            return await asyncio.to_thread(_sync_pool)

    engine = getattr(queue, "engine", None)
    if engine is not None:
        from sqlalchemy import text

        from pgmq._sql import convert_sql_params

        converted, param_dict = convert_sql_params(sql, params)
        begin = engine.begin()
        if hasattr(begin, "__aenter__"):
            async with begin as conn:
                result = await conn.execute(text(converted), param_dict or {})
                if fetch:
                    return list(result.fetchall())
                return None

        def _sync_engine() -> Optional[List[Any]]:
            with engine.begin() as conn:
                result = conn.execute(text(converted), param_dict or {})
                if fetch:
                    return list(result.fetchall())
                return None

        return await asyncio.to_thread(_sync_engine)

    from pgmq.api.factory import APIError

    raise APIError(503, "dependency_unavailable", "queue has no pool or engine")


async def ensure_auth_table(queue: Any, api_config: PGMQAPIConfig) -> None:
    """Create or check the optional key table on first /ready after init."""
    from pgmq.api.factory import APIError, map_db_exception

    if not uses_db(api_config):
        return
    try:
        schema = api_config.workers_schema
        table = api_config.workers_table
        validate_schema(schema)
        validate_identifier(table)
        if api_config.workers_create_table:
            await run_auth_sql(queue, create_schema_sql(schema))
            await run_auth_sql(queue, create_table_sql(schema, table))
            return
        rows = await run_auth_sql(
            queue, table_exists_sql(), (schema, table), fetch=True
        )
        if not rows:
            raise APIError(
                503,
                "dependency_unavailable",
                f"auth table {schema}.{table} does not exist",
            )
    except APIError:
        raise
    except Exception as exc:
        mapped = map_db_exception(exc)
        if mapped is not None:
            raise mapped from exc
        raise APIError(503, "dependency_unavailable", str(exc)) from exc


async def verify_worker_key(
    queue: Any,
    api_config: PGMQAPIConfig,
    presented: str,
) -> str:
    """Lookup a worker key by hash. Reject unknown, expired, or revoked keys."""
    from pgmq.api.factory import APIError, map_db_exception

    pepper = api_config.pepper
    if not pepper:
        raise APIError(401, "unauthorized", "Invalid API key")
    digest = hash_api_key(pepper, presented)
    try:
        rows = await run_auth_sql(
            queue,
            lookup_by_hash_sql(api_config.workers_schema, api_config.workers_table),
            (digest,),
            fetch=True,
        )
    except APIError:
        raise
    except Exception as exc:
        mapped = map_db_exception(exc)
        if mapped is not None:
            raise mapped from exc
        raise APIError(401, "unauthorized", "Invalid API key") from exc

    if not rows:
        raise APIError(401, "unauthorized", "Invalid API key")

    row = rows[0]
    name = _row_get(row, "name", 0)
    prefix = _row_get(row, "key_prefix", 1)
    expires_at = _row_get(row, "expires_at", 2)
    revoked_at = _row_get(row, "revoked_at", 3)
    if revoked_at is not None:
        raise APIError(401, "unauthorized", "Invalid API key")
    if expires_at is not None:
        expiry = expires_at
        if getattr(expiry, "tzinfo", None) is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        if expiry <= datetime.now(timezone.utc):
            raise APIError(401, "unauthorized", "Invalid API key")

    log_with_context(
        logger,
        logging.INFO,
        "API key accepted",
        key_prefix=prefix,
        name=name,
    )
    try:
        await run_auth_sql(
            queue,
            touch_last_used_sql(api_config.workers_schema, api_config.workers_table),
            (digest,),
        )
    except Exception:
        log_with_context(
            logger,
            logging.DEBUG,
            "Could not update last_used_at",
            key_prefix=prefix,
            name=name,
        )
    return presented


def build_key_record(
    name: str,
    pepper: Optional[str] = None,
    expires_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Build a key record with a UUID id. The raw key is for the CLI print only."""
    if not name or not str(name).strip():
        raise ValueError("name is required")
    raw = generate_raw_key()
    key_id = uuid.uuid4()
    record: Dict[str, Any] = {
        "id": str(key_id),
        "raw_key": raw,
        "name": str(name).strip(),
        "key_prefix": key_prefix_of(raw),
        "expires_at": expires_at,
    }
    if pepper:
        record["key_hash"] = hash_api_key(pepper, raw)
    return record


async def insert_key_record(
    queue: Any,
    api_config: PGMQAPIConfig,
    record: Dict[str, Any],
) -> None:
    """Store the hash and prefix. Do not store the raw key."""
    await run_auth_sql(
        queue,
        insert_key_sql(api_config.workers_schema, api_config.workers_table),
        (
            record["id"],
            record["name"],
            record["key_prefix"],
            record["key_hash"],
            record.get("expires_at"),
            None,
        ),
    )
