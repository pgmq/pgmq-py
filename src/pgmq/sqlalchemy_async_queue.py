# src/pgmq/sqlalchemy_async_queue.py
"""
Asynchronous PGMQ client implementation using SQLAlchemy.

This module provides the async PGMQueue class using SQLAlchemy's async engine
for high-performance asyncio-based database operations.
"""

from dataclasses import dataclass, field, fields
from typing import Optional, List, Dict, Any
import logging

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, async_sessionmaker
from sqlalchemy.pool import AsyncAdaptedQueuePool
from sqlalchemy import text

from pgmq.base import BaseQueue
from pgmq._client_fields import PGMQueueClientFields
from pgmq import _sql
from pgmq.decorators import sqlalchemy_async_transaction
from pgmq.logger import log_with_context
from pgmq.async_operations import AsyncPGMQueueOperationsMixin


def _parse_jsonb(val) -> Any:
    """Parse JSONB value from asyncpg result."""
    if val is None:
        return None
    return val


class SQLAlchemyAsyncBackend:
    """SQLAlchemy async engine and SQL execution for the async client."""

    _async_transaction_decorator = sqlalchemy_async_transaction

    def _encode_jsonb(self, value: Dict[str, Any]) -> Dict[str, Any]:
        return value

    def _encode_jsonb_list(self, values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return values

    @property
    def _json_parser(self):
        return _parse_jsonb

    async def init(self) -> None:
        """Initialize the async SQLAlchemy engine."""
        if self.engine is not None:
            if not isinstance(self.engine, AsyncEngine):
                raise TypeError(
                    f"Expected sqlalchemy.ext.asyncio.AsyncEngine, got {type(self.engine).__name__}"
                )
            if self.config.init_extension:
                async with self.engine.connect() as conn:
                    await conn.execute(
                        text("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")
                    )
                    await conn.commit()
            self._session_factory = async_sessionmaker(
                bind=self.engine, expire_on_commit=False
            )
            return

        log_with_context(self.logger, logging.DEBUG, "Creating async SQLAlchemy engine")
        connection_url = self.config.async_dsn.replace(
            "postgresql://", "postgresql+asyncpg://", 1
        )
        self.engine = create_async_engine(
            connection_url,
            poolclass=AsyncAdaptedQueuePool,
            pool_size=self.config.pool_size,
            max_overflow=20,
            pool_pre_ping=True,
        )

        if self.config.init_extension:
            async with self.engine.connect() as conn:
                await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;"))
                await conn.commit()

        self._session_factory = async_sessionmaker(
            bind=self.engine, expire_on_commit=False
        )

    def session(self):
        """Return an async SQLAlchemy ORM AsyncSession bound to this queue's engine."""
        if self.engine is None:
            raise RuntimeError("Engine has not been initialized. Call init() first.")
        return self._session_factory()

    async def close(self) -> None:
        """Close the engine and dispose of all connections."""
        if self.engine:
            await self.engine.dispose()
            self.engine = None

    async def _execute(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> None:
        """Execute SQL without returning results."""
        converted_sql, param_dict = _sql.convert_sql_params(sql, params)

        async def run_query(connection):
            if param_dict:
                await connection.execute(text(converted_sql), param_dict)
            else:
                await connection.execute(text(converted_sql))

        if conn is not None:
            await run_query(conn)
        else:
            async with self.engine.begin() as new_conn:
                await run_query(new_conn)

    async def _execute_with_result(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> List[tuple]:
        """Execute SQL and return all results."""
        converted_sql, param_dict = _sql.convert_sql_params(sql, params)

        async def run_query(connection):
            if param_dict:
                return await connection.execute(text(converted_sql), param_dict)
            else:
                return await connection.execute(text(converted_sql))

        if conn is not None:
            result = await run_query(conn)
            return result.fetchall()
        else:
            async with self.engine.begin() as new_conn:
                result = await run_query(new_conn)
                return result.fetchall()

    async def _execute_one(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> Optional[tuple]:
        """Execute SQL and return first result."""
        results = await self._execute_with_result(sql, params, conn)
        return results[0] if results else None


@dataclass
class PGMQueue(
    PGMQueueClientFields,
    SQLAlchemyAsyncBackend,
    AsyncPGMQueueOperationsMixin,
    BaseQueue,
):
    """
    Asynchronous PGMQueue client using SQLAlchemy for PostgreSQL Message Queue operations.
    """

    engine: Optional[AsyncEngine] = field(default=None)  # type: ignore

    def __post_init__(self):
        """Initialize configuration after dataclass construction."""
        BaseQueue.__init__(
            self,
            **{f.name: getattr(self, f.name) for f in fields(PGMQueueClientFields)},
        )
