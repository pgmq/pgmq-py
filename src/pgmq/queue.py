# src/pgmq/queue.py
"""
Synchronous PGMQ client implementation.

This module provides the main PGMQueue class for synchronous database operations,
with full support for all PGMQ extension features including topics, FIFO, and notifications.
"""

from dataclasses import dataclass, field, fields
from typing import Optional, List, Dict, Any
import logging

from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from pgmq.base import BaseQueue
from pgmq._client_fields import PGMQueueClientFields
from pgmq.decorators import transaction
from pgmq.logger import log_with_context
from pgmq.sync_operations import SyncPGMQueueOperationsMixin


class PsycopgSyncBackend:
    """psycopg connection pool and SQL execution for the sync client."""

    _transaction_decorator = transaction

    def _encode_jsonb(self, value: Dict[str, Any]) -> Jsonb:
        return Jsonb(value)

    def _encode_jsonb_list(self, values: List[Dict[str, Any]]) -> List[Jsonb]:
        return [Jsonb(v) for v in values]

    @property
    def _json_parser(self):
        return lambda x: x

    def _init_pool(self) -> None:
        """Initialize the connection pool."""
        log_with_context(self.logger, logging.DEBUG, "Creating connection pool")
        self.pool = ConnectionPool(
            self.config.dsn,
            min_size=1,
            max_size=self.config.pool_size,
            open=True,
        )

    def _init_extensions(self) -> None:
        """Ensure PGMQ extension is installed."""
        with self.pool.connection() as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")

    def _execute(self, sql: str, params: Optional[tuple] = None, conn=None) -> None:
        """Execute SQL without returning results."""
        if conn:
            conn.execute(sql, params)
        else:
            with self.pool.connection() as c:
                c.execute(sql, params)

    def _execute_with_result(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> List[tuple]:
        """Execute SQL and return all results."""
        if conn:
            return conn.execute(sql, params).fetchall()
        else:
            with self.pool.connection() as c:
                return c.execute(sql, params).fetchall()

    def _execute_one(
        self, sql: str, params: Optional[tuple] = None, conn=None
    ) -> Optional[tuple]:
        """Execute SQL and return first result or None."""
        results = self._execute_with_result(sql, params, conn)
        return results[0] if results else None


@dataclass
class PGMQueue(
    PGMQueueClientFields,
    PsycopgSyncBackend,
    SyncPGMQueueOperationsMixin,
    BaseQueue,
):
    """
    Synchronous PGMQueue client for PostgreSQL Message Queue operations.
    """

    pool: ConnectionPool = field(init=False, default=None)  # type: ignore

    def __post_init__(self):
        """Initialize connection pool after dataclass construction."""
        BaseQueue.__init__(
            self,
            **{f.name: getattr(self, f.name) for f in fields(PGMQueueClientFields)},
        )
        self._init_pool()
        if self.config.init_extension:
            self._init_extensions()
