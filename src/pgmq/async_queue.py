# src/pgmq/async_queue.py
"""
Asynchronous PGMQ client implementation.

This module provides the async PGMQueue class using asyncpg for high-performance
asyncio-based database operations.
"""

from dataclasses import dataclass, field, fields
from typing import Optional, List, Dict, Any
import logging

import asyncpg
from asyncpg import Pool
import orjson

from pgmq.base import BaseQueue
from pgmq._client_fields import PGMQueueClientFields
from pgmq import _sql
from pgmq.decorators import async_transaction
from pgmq.logger import log_with_context
from pgmq.async_operations import AsyncPGMQueueOperationsMixin


def _parse_jsonb(val) -> Any:
    """Parse asyncpg JSONB value."""
    if val is None:
        return None
    if isinstance(val, (str, bytes)):
        return orjson.loads(val)
    return val


class AsyncpgBackend:
    """asyncpg connection pool and SQL execution for the async client."""

    _async_transaction_decorator = async_transaction

    def _encode_jsonb(self, value: Dict[str, Any]) -> str:
        return orjson.dumps(value).decode("utf-8")

    def _encode_jsonb_list(self, values: List[Dict[str, Any]]) -> List[str]:
        return [orjson.dumps(v).decode("utf-8") for v in values]

    @property
    def _json_parser(self):
        return _parse_jsonb

    async def init(self) -> None:
        """Initialize the asyncpg connection pool."""
        if self.pool is not None:
            if not self._own_pool:
                if not isinstance(self.pool, Pool):
                    raise TypeError(
                        f"Expected asyncpg.Pool, got {type(self.pool).__name__}"
                    )
                log_with_context(
                    self.logger, logging.DEBUG, "Using user-provided connection pool"
                )
                if self.config.init_extension:
                    async with self.pool.acquire() as conn:
                        await conn.execute(
                            "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;"
                        )
            return

        self._own_pool = True
        log_with_context(self.logger, logging.DEBUG, "Creating asyncpg pool")
        self.pool = await asyncpg.create_pool(
            self.config.async_dsn,
            min_size=1,
            max_size=self.config.pool_size,
        )

        if self.config.init_extension:
            async with self.pool.acquire() as conn:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")

    async def close(self) -> None:
        """Close the connection pool if it was created by this client."""
        if self.pool:
            if self._own_pool:
                await self.pool.close()
            self.pool = None

    async def _execute(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> None:
        """Execute SQL without returning results."""
        async_sql = _sql.ASYNC_SQL_MAP.get(sql, sql)

        if conn:
            await conn.execute(async_sql, *params if params else ())
        else:
            async with self.pool.acquire() as c:
                await c.execute(async_sql, *params if params else ())

    async def _execute_with_result(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> List[tuple]:
        """Execute SQL and return all results."""
        async_sql = _sql.ASYNC_SQL_MAP.get(sql, sql)

        if conn:
            return await conn.fetch(async_sql, *params if params else ())
        else:
            async with self.pool.acquire() as c:
                return await c.fetch(async_sql, *params if params else ())

    async def _execute_one(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> Optional[tuple]:
        """Execute SQL and return first result."""
        results = await self._execute_with_result(sql, params, conn)
        return results[0] if results else None


@dataclass
class PGMQueue(
    PGMQueueClientFields,
    AsyncpgBackend,
    AsyncPGMQueueOperationsMixin,
    BaseQueue,
):
    """
    Asynchronous PGMQueue client for PostgreSQL Message Queue operations.
    """

    pool: Optional[Pool] = None
    _own_pool: bool = field(init=False, default=True)

    def __post_init__(self) -> None:
        """Initialize configuration after dataclass construction."""
        BaseQueue.__init__(
            self,
            **{f.name: getattr(self, f.name) for f in fields(PGMQueueClientFields)},
        )
        if self.pool is not None:
            self._own_pool = False
